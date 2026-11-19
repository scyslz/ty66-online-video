// Package store 维护「站点 + 条目 id → 视频信息」的映射，并负责增量同步与持久化。
//
// 数据流是两条独立流水线：
//
//	列表流水线：定时抓列表页，比对 id，新条目入队（list.go）
//	详情流水线：worker 从队列取 id，解析播放地址并写回（detail.go）
//
// 支持多个来源站：每个站点实现 provider.Provider，store 通过注册表拿到实现。
// 映射键是 "站点:条目id"（itemKey），页键是 "站点/页号"（pageKey），
// 因此不同站点的同名页号、同号帖子不会互相覆盖。
//
// 映射以条目键为主键，因此上游列表怎么变（置顶、翻页、下架）都不会丢失
// 已经解析好的播放地址。读取侧按条数窗口（offset/limit）分页，见 window.go。
package store

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"paresevideo/internal/provider"
)

const (
	// MaxPage 是单站点最大页数上限。
	MaxPage = 2500

	DefaultMaxItems     = 10000                  // 映射与导出接口的默认条数上限
	maxItemsPerPage     = 1000                   // 单页最多接受的条目数，防上游异常
	maxFillPages        = 6                      // 单次窗口请求最多顺延抓取的列表页数
	ListFetchGap        = 150 * time.Millisecond // 连续抓列表页之间的间隔（默认值）
	windowWait          = 20 * time.Second       // 窗口内直链解析的最长等待
	MaxWindowSize       = 500                    // 单次窗口最多返回的条数
	siteFailsCooldown   = 3                      // 站点连续失败多少次触发熔断
	siteCooldown        = 30 * time.Minute       // 首次熔断的冷却时长
	maxDeepPages        = 30                     // 单站点默认最多深挖到第几页（防打爆上游）
	maxIncrementalPages = 5                      // 增量同步最多往下翻几页（未追平本地时）
)

// Item 是 provider.Item 的别名，调用方无需同时引入两个包。
type Item = provider.Item

// itemKey 生成映射主键：站点 + 条目 id。
func itemKey(site, id string) string {
	if site == "" {
		return id
	}
	return site + ":" + id
}

// splitKey 把映射主键拆回站点与条目 id。
// 兼容旧数据：没有 "site:" 前缀的键，站点为空（由 store 归到默认站点）。
func splitKey(key string) (site, id string) {
	if i := strings.IndexByte(key, ':'); i > 0 {
		return key[:i], key[i+1:]
	}
	return "", key
}

// pageKey 生成页键：站点 + 页号。
func pageKey(site string, page int) string {
	return site + "/" + strconv.Itoa(page)
}

// splitPageKey 把页键拆回站点与页号。
func splitPageKey(key string) (site string, page int) {
	i := strings.LastIndexByte(key, '/')
	if i < 0 {
		p, _ := strconv.Atoi(key)
		return "", p
	}
	p, err := strconv.Atoi(key[i+1:])
	if err != nil {
		return key, 0
	}
	return key[:i], p
}

// pageState 记录某个站点某一页的展示顺序与同步状态。
type pageState struct {
	site        string
	page        int
	ids         []string // 存储的是完整 itemKey
	lastList    time.Time
	lastAttempt int64 // 上次尝试完成时间（Unix 纳秒）
	empty       bool  // 上游该页返回空列表，说明再深也没内容
	fails       int   // 连续失败次数
	lastErr     string
}

// siteState 记录站点的全局状态（跨页），实现三态熔断器。
//
// 状态流转：
//
//	closed    正常抓取。连续失败达 cooldownFails 次 → open
//	open      冷却中，一律不抓。冷却期结束 → halfOpen
//	halfOpen  试探：只放行一次请求。
//	            成功 → closed（计数清零）
//	            失败 → open，且冷却时长翻倍（指数退避，上限 MaxCooldown）
//
// 这样站点不会被永久钉死：上游恢复后最多等一个冷却周期就会重新被探测。
type siteState struct {
	failStreak    int       // 连续失败次数（成功清零）
	coolUntil     time.Time // 冷却截止时间
	openStreak    int       // 连续进入 open 的次数，用于指数退避
	probeInFlight bool      // halfOpen 状态下是否已有试探请求在跑
	lastErr       string

	// lastSyncAdded / lastSyncAt 记录最近一轮增量同步新增的条数与时间。
	// 供前端同步面板显示「最后更新新增 N 条」，不参与抓取逻辑。
	lastSyncAdded int
	lastSyncAt    time.Time
}

// 冷却时长：首次 30 分钟，之后指数退避，最长 6 小时。
var (
	siteCooldownBase = siteCooldown
	maxSiteCooldown  = 6 * time.Hour
)

// cooling 判断站点是否仍在冷却期。
func (st *siteState) cooling(now time.Time) bool {
	return now.Before(st.coolUntil)
}

// allow 判断是否允许发起一次抓取。
//
//   - 正常状态（closed）：直接放行，不做任何拦截
//   - 冷却期内（open）：拒绝
//   - 冷却刚结束（halfOpen）：只放行一个试探请求，避免同时打一堆
func (st *siteState) allow(now time.Time) bool {
	if st.cooling(now) {
		st.probeInFlight = false // 冷却中不留残余标记
		return false
	}
	// 未熔断过：正常放行。
	if st.failStreak < siteFailsCooldown {
		return true
	}
	// 熔断后冷却结束：只放行一次试探。
	if st.probeInFlight {
		return false
	}
	st.probeInFlight = true
	return true
}

// onSuccess 记录一次成功，完全恢复。
func (st *siteState) onSuccess() {
	st.failStreak = 0
	st.coolUntil = time.Time{}
	st.openStreak = 0
	st.probeInFlight = false
	st.lastErr = ""
}

// onFailure 记录一次失败；返回本次是否进入了冷却，以及冷却时长。
func (st *siteState) onFailure(err string) (cooling bool, cooldown time.Duration) {
	st.failStreak++
	st.probeInFlight = false
	st.lastErr = err
	// 已经处于冷却期内：说明是冷却前发出的请求刚回来，不延长冷却，
	// 否则只要有在途请求就会不断把冷却往后推，永远恢复不了。
	if time.Now().Before(st.coolUntil) {
		return true, time.Until(st.coolUntil)
	}
	if st.failStreak < siteFailsCooldown {
		return false, 0
	}
	// 进入 open：冷却时长按连续 open 次数翻倍。
	st.openStreak++
	d := siteCooldownBase
	for i := 1; i < st.openStreak && d < maxSiteCooldown; i++ {
		d *= 2
	}
	if d > maxSiteCooldown {
		d = maxSiteCooldown
	}
	st.coolUntil = time.Now().Add(d)
	return true, d
}

// Options 是构造 Store 的参数。
type Options struct {
	File      string            // 映射文件路径
	ListTTL   time.Duration     // 列表同步间隔
	Keep      time.Duration     // 下架映射保留时长，0 表示永久
	Workers   int               // 详情解析并发数
	MaxItems  int               // 映射最大条数
	DeepPages int               // 单站点最多深挖到第几页（0 = 默认 30）
	ListGap   time.Duration     // 同站点连续列表请求间隔（0=默认 150ms，负数=禁用）
	Lazy      bool              // 懒加载：只抓列表，直链等播放时按需解析
	Provider  provider.Provider // 默认站点（单站部署）
	// Providers 是全部站点；为空时退化为只有 Provider。
	Providers []provider.Provider
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
//	detailMu  保护 detailLock 映射本身
//	detailLock 每条一把，保证同一条目不会被并发重解析（后台队列与按需修复互斥）
//	saveMu    串行化落盘
type Store struct {
	file      string
	listTTL   time.Duration
	keep      time.Duration
	workers   int
	maxItems  int
	deepPages int           // 深挖页数上限，0 表示用默认值
	listGap   time.Duration // 同站点连续列表请求的最小间隔
	lazy      bool          // 懒加载：默认不主动抓详情，等播放时按需解析

	sites   []provider.Provider          // 按注册顺序，决定列表展示顺序
	byName  map[string]provider.Provider // 站名 → 实现
	defSite string                       // 旧数据（无站点前缀）归属的站点

	mu       sync.Mutex
	items    map[string]*Item       // itemKey → 条目
	orders   map[string]*orderQueue // 站点 → 展示顺序（唯一真源）
	siteStat map[string]*siteState  // 站点 → 熔断状态
	pages    map[string]*pageState
	pending  map[string]bool // 已入队待解析（去重）
	inflight map[string]bool // 正在解析中
	syncing  map[string]bool // 正在抓取的列表页
	syncReq  map[string]bool // 已排队或正在抓取（去重标记，结束才清除）
	dropped  int             // 因超过上限而未收录的条目数

	queue     chan string   // 详情队列：存放 itemKey
	syncQueue []string      // 补页队列：存放 pageKey（按入队顺序）
	wake      chan struct{} // 唤醒补页 worker
	dirty     chan struct{}

	listWorkersN int // 补页 worker 数量

	limiterMu sync.Mutex
	limiters  map[string]*fetchLimiter

	windowMu sync.Mutex

	pageMu   sync.Mutex
	pageLock map[string]*sync.Mutex

	detailMu   sync.Mutex
	detailLock map[string]*sync.Mutex
	flights    map[string]*repairFlight
	recent     map[string]recentRepair

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
	// ListGap < 0 表示禁用间隔（测试用）；0 表示使用默认值。
	if opt.ListGap == 0 {
		opt.ListGap = ListFetchGap
	} else if opt.ListGap < 0 {
		opt.ListGap = 0
	}
	sites := opt.Providers
	if len(sites) == 0 && opt.Provider != nil {
		sites = []provider.Provider{opt.Provider}
	}
	byName := make(map[string]provider.Provider, len(sites))
	def := ""
	for _, p := range sites {
		if p == nil {
			continue
		}
		byName[p.Name()] = p
		if def == "" {
			def = p.Name()
		}
	}
	return &Store{
		file:         opt.File,
		listTTL:      opt.ListTTL,
		keep:         opt.Keep,
		workers:      opt.Workers,
		maxItems:     opt.MaxItems,
		deepPages:    opt.DeepPages,
		lazy:         opt.Lazy,
		sites:        sites,
		byName:       byName,
		defSite:      def,
		items:        make(map[string]*Item),
		orders:       make(map[string]*orderQueue),
		siteStat:     make(map[string]*siteState),
		pages:        make(map[string]*pageState),
		pending:      make(map[string]bool),
		inflight:     make(map[string]bool),
		syncing:      make(map[string]bool),
		syncReq:      make(map[string]bool),
		pageLock:     make(map[string]*sync.Mutex),
		detailLock:   make(map[string]*sync.Mutex),
		flights:      make(map[string]*repairFlight),
		recent:       make(map[string]recentRepair),
		limiters:     make(map[string]*fetchLimiter),
		queue:        make(chan string, 8192),
		wake:         make(chan struct{}, 64),
		dirty:        make(chan struct{}, 1),
		listWorkersN: 0, // 0 = 按站点数自动决定
	}
}

// provider 返回站点实现。
func (s *Store) provider(site string) provider.Provider {
	if site == "" {
		site = s.defSite
	}
	return s.byName[site]
}

// Providers 返回已注册的站点名（按注册顺序）。
func (s *Store) Providers() []string {
	out := make([]string, 0, len(s.sites))
	for _, p := range s.sites {
		if p != nil {
			out = append(out, p.Name())
		}
	}
	return out
}

// MaxItems 返回映射条数上限。
func (s *Store) MaxItems() int { return s.maxItems }

// ListTTL 返回列表同步间隔。
func (s *Store) ListTTL() time.Duration { return s.listTTL }

// orderOfLocked 返回某站点的展示队列（首次调用时创建）。调用方需持有 s.mu。
func (s *Store) orderOfLocked(site string) *orderQueue {
	if site == "" {
		site = s.defSite
	}
	q, ok := s.orders[site]
	if !ok {
		q = newOrderQueue()
		s.orders[site] = q
	}
	return q
}

// stateOfLocked 返回某站点的熔断状态（首次调用时创建）。调用方需持有 s.mu。
func (s *Store) stateOfLocked(site string) *siteState {
	if site == "" {
		site = s.defSite
	}
	st, ok := s.siteStat[site]
	if !ok {
		st = &siteState{}
		s.siteStat[site] = st
	}
	return st
}

// ErrThrottled 表示本次抓取因节流被跳过（不是上游故障，不应计入熔断）。
var ErrThrottled = errors.New("请求节流中")

// BreakerState 返回站点的熔断状态：closed / open / half-open。
func (s *Store) BreakerState(site string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateOfLocked(site)
	now := time.Now()
	switch {
	case st.cooling(now):
		return "open"
	case st.failStreak >= siteFailsCooldown:
		return "half-open"
	default:
		return "closed"
	}
}

// needsDetail 判断条目是否需要（重新）解析详情。
// 失败条目按 RetryAt 节流：新帖没挂上播放链接时重试更密，普通失败按退避节奏。
// 只拿到网页播放页（/embed/...）而没有直链的，同样按重试节奏再解析一次——
// 旧版本不会去挖这类页面的直链，重试后才能补上。
func needsDetail(it *Item, now time.Time) bool {
	if provider.IsMediaURL(it.PlayURL) {
		return false
	}
	if it.UpdatedAt == 0 {
		return true // 从未解析过
	}
	return it.RetryAt <= now.Unix()
}

// NeedsDetail 判断条目是否需要（重新）解析详情；懒加载模式下恒为 false。
//
// 懒加载时列表只负责「抓出来显示」，直链一律留空，等用户真的点了播放
// （Repair）再去请求上游。这样可以避免列表一刷新就打几百次详情接口被风控。
func (s *Store) NeedsDetail(it *Item, now time.Time) bool {
	if s.lazy {
		return false
	}
	return needsDetail(it, now)
}

// Lazy 返回是否处于懒加载模式（只抓列表，不主动抓详情）。
func (s *Store) Lazy() bool { return s.lazy }

// enqueueLocked 把 itemKey 放入详情队列（去重）。调用方需持有 s.mu。
// 懒加载模式下不入队：直链留给播放时的按需解析。
func (s *Store) enqueueLocked(key string) {
	if s.lazy {
		return
	}
	if s.pending[key] {
		return
	}
	s.pending[key] = true
	select {
	case s.queue <- key:
	default:
		delete(s.pending, key) // 队列满，下一轮同步再补
	}
}
