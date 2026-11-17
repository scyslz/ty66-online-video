package store

// 本文件实现「懒加载」的核心：播放失败时按需重解析。
//
// 设计：后台不再定期给已有直链换签名（直链往往能用很久，而是否失效只有
// 真正播放时才知道）。前端播放失败时调用 POST /api/resolve，后端只重解析
// 这一条，返回新地址；前端拿到新地址接着播。
//
// 去重分两层：
//   - flights：同一条目同时在途的修复请求只发一次，后来者等同一份结果；
//   - keyLock：与后台详情队列互斥，同一条目不会有两路请求同时进行。

import (
	"sync"
	"time"

	"paresevideo/internal/provider"
)

// keyLock 返回某条目的解析锁（首次调用时创建）。
//
// 与 pageLock 同理：只保护单条目的解析，粒度小，不阻塞其它条目。
func (s *Store) keyLock(key string) *sync.Mutex {
	s.detailMu.Lock()
	defer s.detailMu.Unlock()
	l, ok := s.detailLock[key]
	if !ok {
		l = &sync.Mutex{}
		s.detailLock[key] = l
	}
	return l
}

// dropKeyLock 清理条目锁，避免映射无限增长（条目被淘汰时调用）。
func (s *Store) dropKeyLock(key string) {
	s.detailMu.Lock()
	delete(s.detailLock, key)
	s.detailMu.Unlock()
}

// repairResult 描述一次按需解析的结果。
type repairResult struct {
	Item    Item `json:"item"`
	Changed bool `json:"changed"` // 是否真的换到了不同的地址
	Waited  bool `json:"waited"`  // 是否是复用别人刚做完的结果
}

// repairFlight 是一次在途的按需修复，后来者共享同一份结果。
type repairFlight struct {
	done chan struct{}
	res  repairResult
}

// repairCooldown 内重复请求同一条目直接复用上次结果。
//
// 上游对我们是按 IP 限流的，用户连点「重新加载」时不应该连打几次播放页。
const repairCooldown = time.Minute

// recentRepair 是某条目最近一次按需修复的结果。
type recentRepair struct {
	at  time.Time
	res repairResult
}

// Repair 按需重解析一条：拿到新的直链就写回映射并返回。
//
// 触发场景：前端用旧直链播放失败。可能的结局有三种：
//   - 上游给了新签名直链 → Changed=true，前端直接换地址重播；
//   - 只拿到播放页、换不出直链 → Changed=false，前端回落到播放页/原网页；
//   - 上游限流/网络失败 → Changed=false，Item.Error 说明原因。
//
// 同一条目在途时，后来者会等待并拿到同一份结果（Waited=true）；
// 一分钟内重复请求同一条目也直接复用结果，不会重复打上游。
func (s *Store) Repair(site, id string) repairResult {
	if site == "" {
		site = s.defSite
	}
	key := itemKey(site, id)

	s.detailMu.Lock()
	if f, ok := s.flights[key]; ok {
		s.detailMu.Unlock()
		<-f.done
		res := f.res
		res.Waited = true
		return res
	}
	if r, ok := s.recent[key]; ok && time.Since(r.at) < repairCooldown {
		s.detailMu.Unlock()
		res := r.res
		res.Waited = true
		return res
	}
	f := &repairFlight{done: make(chan struct{})}
	if s.flights == nil {
		s.flights = make(map[string]*repairFlight)
	}
	s.flights[key] = f
	s.detailMu.Unlock()

	res := s.doRepair(key)

	s.detailMu.Lock()
	delete(s.flights, key)
	if s.recent == nil {
		s.recent = make(map[string]recentRepair)
	}
	// 缓存「最近一次结果」，防止连点造成重复请求；超过一定规模时顺手清理过期项。
	if len(s.recent) > 4096 {
		for k, r := range s.recent {
			if time.Since(r.at) >= repairCooldown {
				delete(s.recent, k)
			}
		}
	}
	s.recent[key] = recentRepair{at: time.Now(), res: res}
	f.res = res
	close(f.done)
	s.detailMu.Unlock()
	return res
}

// doRepair 执行一次真正的重解析（调用方保证同一条目不会并发进来）。
func (s *Store) doRepair(key string) repairResult {
	s.mu.Lock()
	it, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		return repairResult{}
	}
	cur := *it
	site, _ := splitKey(key)
	if site == "" {
		site = s.defSite
	}
	p := s.provider(site)
	s.mu.Unlock()

	if p == nil {
		return repairResult{Item: cur}
	}

	// 与后台详情队列互斥：同一条目不会有两路请求同时进行。
	lk := s.keyLock(key)
	lk.Lock()
	defer lk.Unlock()

	old := cur.PlayURL
	var out Item
	// 有播放页就只打播放页（一条请求），否则回退到抓详情页。
	if cur.PlayPage != "" {
		var ok bool
		out, ok = p.Refresh(cur)
		if !ok {
			out = p.Enrich(cur)
		}
	} else {
		out = p.Enrich(cur)
	}
	out.Site = site

	s.finish(key, out)
	got := s.itemSnapshot(key)

	// 换到新地址才算修复成功；同一地址重复解析没有意义。
	changed := provider.IsMediaURL(got.PlayURL) && got.PlayURL != old
	return repairResult{Item: got, Changed: changed}
}

// itemSnapshot 返回某条目的当前快照（不存在时返回零值）。
func (s *Store) itemSnapshot(key string) Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it, ok := s.items[key]; ok {
		return *it
	}
	return Item{}
}

// MarkStale 记下「这条播放失败过」，等下次后台扫描时再试一次。
//
// 与 Repair 的区别：Repair 立刻同步解析（用户正等着看）；
// MarkStale 只是把 RetryAt 置为现在，让后台 RefreshStale 顺带处理，
// 用于前端已经回落到网页播放页、不急着要直链的情况。
func (s *Store) MarkStale(site, id string) bool {
	key := itemKey(site, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.items[key]
	if !ok {
		return false
	}
	it.RetryAt = time.Now().Unix()
	if it.Error == "" {
		it.Error = "播放失败，等待重新解析"
	}
	if !s.pending[key] {
		s.enqueueLocked(key)
	}
	return true
}
