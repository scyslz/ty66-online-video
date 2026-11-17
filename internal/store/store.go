// Package store 维护“上游帖子 id → 视频信息”的映射，并负责增量同步与持久化。
//
// 数据流是两条独立流水线：
//
//	列表流水线：定时抓列表页，比对 id，新条目入队（list.go）
//	详情流水线：worker 从队列取 id，解析播放地址并写回（detail.go）
//
// 映射以帖子 id 为主键，因此上游列表怎么变（置顶、翻页、下架）都不会丢失
// 已经解析好的播放地址。读取侧按条数窗口（offset/limit）分页，见 window.go。
package store

import (
	"sync"
	"time"

	"paresevideo/internal/upstream"
)

const (
	MaxPage = 2500

	DefaultMaxItems = 10000                  // 映射与导出接口的默认条数上限
	maxItemsPerPage = 1000                   // 单页最多接受的条目数，防上游异常
	maxListBatch    = 8                      // 一次唤醒最多连抓的列表页数
	maxActivePages  = 5                      // 定时刷新只覆盖最近访问的页数
	maxFillPages    = 6                      // 单次窗口请求最多顺延抓取的列表页数
	ListFetchGap    = 150 * time.Millisecond // 连续抓列表页之间的间隔
	windowWait      = 20 * time.Second       // 窗口内直链解析的最长等待
	MaxWindowSize   = 500                    // 单次窗口最多返回的条数
)

// Item 是 upstream.Item 的别名，调用方无需同时引入两个包。
type Item = upstream.Item

// pageState 记录上游某一页的展示顺序与同步状态。
type pageState struct {
	ids         []string
	lastList    time.Time // 上次成功同步时间
	lastAttempt int64     // 上次尝试完成时间（Unix 纳秒）
	lastUsed    time.Time // 最近一次被请求的时间
	empty       bool      // 上游该页返回空列表，说明已经翻到末尾
	lastErr     string
}

// Options 是构造 Store 的参数。
type Options struct {
	File     string        // 映射文件路径
	ListTTL  time.Duration // 列表同步间隔
	VideoTTL time.Duration // 直链有效期，超时后换新签名
	Keep     time.Duration // 下架映射保留时长，0 表示永久
	Workers  int           // 详情解析并发数
	MaxItems int           // 映射最大条数
}

// Store 是映射的持有者与两条流水线的调度者。
//
// 锁约定：
//
//	mu        保护 items / pages / pending / inflight / syncing / syncReq
//	          只在内存操作期间持有，绝不在持有期间发网络请求
//	windowMu  串行化窗口补页，避免并发重复抓取
//	listMu    串行化列表页请求，并保证请求之间有间隔
//	pageMu    保护 pageLock 映射本身
//	pageLock  每页一把，保证同一上游页不会被并发抓取
//	saveMu    串行化落盘
type Store struct {
	file     string
	listTTL  time.Duration
	videoTTL time.Duration
	keep     time.Duration
	workers  int
	maxItems int

	mu       sync.Mutex
	items    map[string]*Item
	pages    map[int]*pageState
	pending  map[string]bool // 已入队待解析（去重）
	inflight map[string]bool // 正在解析中
	syncing  map[int]bool    // 正在抓取的列表页
	syncReq  map[int]bool    // 待同步的页，用于请求合并
	dropped  int             // 因超过上限而未收录的条目数

	queue chan string
	wake  chan int
	dirty chan struct{}

	listMu    sync.Mutex
	lastFetch time.Time

	windowMu sync.Mutex

	pageMu   sync.Mutex
	pageLock map[int]*sync.Mutex

	saveMu sync.Mutex
}

// New 创建 Store（尚未载入磁盘数据，需再调用 Load）。
func New(opt Options) *Store {
	if opt.Workers < 1 {
		opt.Workers = 1
	}
	if opt.MaxItems < 1 {
		opt.MaxItems = DefaultMaxItems
	}
	return &Store{
		file:     opt.File,
		listTTL:  opt.ListTTL,
		videoTTL: opt.VideoTTL,
		keep:     opt.Keep,
		workers:  opt.Workers,
		maxItems: opt.MaxItems,
		items:    make(map[string]*Item),
		pages:    make(map[int]*pageState),
		pending:  make(map[string]bool),
		inflight: make(map[string]bool),
		syncing:  make(map[int]bool),
		syncReq:  make(map[int]bool),
		pageLock: make(map[int]*sync.Mutex),
		queue:    make(chan string, 8192),
		wake:     make(chan int, 256),
		dirty:    make(chan struct{}, 1),
	}
}

// MaxItems 返回映射条数上限。
func (s *Store) MaxItems() int { return s.maxItems }

// ListTTL 返回列表同步间隔。
func (s *Store) ListTTL() time.Duration { return s.listTTL }

// VideoTTL 返回直链有效期。
func (s *Store) VideoTTL() time.Duration { return s.videoTTL }

// needsDetail 判断条目是否需要（重新）解析详情。
// 失败条目按 RetryAt 节流：新帖没挂上播放链接时重试更密，普通失败按退避节奏。
func needsDetail(it *Item, now time.Time) bool {
	if it.PlayURL != "" {
		return false
	}
	if it.UpdatedAt == 0 {
		return true // 从未解析过
	}
	return it.RetryAt <= now.Unix()
}

// enqueueLocked 把 id 放入详情队列（去重）。调用方需持有 s.mu。
func (s *Store) enqueueLocked(id string) {
	if s.pending[id] {
		return
	}
	s.pending[id] = true
	select {
	case s.queue <- id:
	default:
		delete(s.pending, id) // 队列满，下一轮同步再补
	}
}
