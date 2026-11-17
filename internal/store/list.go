package store

// 本文件是列表流水线：唯一的列表抓取者，负责把上游列表增量合并进映射，
// 并把需要解析的条目排进详情队列。

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"paresevideo/internal/provider/siteutil"
)

// RequestSync 让列表线程尽快同步某站点的某一页（同页请求合并）。
func (s *Store) RequestSync(page int) {
	s.RequestSyncSite(s.defSite, page)
}

// RequestSyncSite 让列表线程同步指定站点的某一页。
func (s *Store) RequestSyncSite(site string, page int) {
	if site == "" {
		site = s.defSite
	}
	s.requestPageAsync(pageKey(site, page))
}

// fetchLimiter 是单个站点的列表抓取限流器：
// 保证同一站点两次列表请求之间至少间隔 gap，不同站点之间互不影响。
type fetchLimiter struct {
	mu   sync.Mutex
	last time.Time
}

// allow 尝试占用一次列表请求机会；间隔未满足返回 false（不阻塞）。
func (f *fetchLimiter) allow(gap time.Duration) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	if now.Sub(f.last) < gap {
		return false
	}
	f.last = now
	return true
}

// limiter 返回某站点的限流器（首次调用时创建）。
func (s *Store) limiter(site string) *fetchLimiter {
	s.limiterMu.Lock()
	defer s.limiterMu.Unlock()
	l, ok := s.limiters[site]
	if !ok {
		l = &fetchLimiter{}
		s.limiters[site] = l
	}
	return l
}

// fetchList 抓列表页。限流按站点进行：同一站点串行且有最小间隔，
// 不同站点之间可以并发——这样「一次性向所有站点要下一页」才真的省时间。
func (s *Store) fetchList(site string, page int) ([]Item, error) {
	p := s.provider(site)
	if p == nil {
		return nil, fmt.Errorf("未知站点 %q", site)
	}
	// 站内列表请求间隔未满足：不阻塞，直接让本轮失败并安排重试。
	if s.listGap > 0 && !s.limiter(site).allow(s.listGap) {
		return nil, ErrThrottled
	}
	return p.FetchList(page)
}

// listLoop 是唯一的列表抓取者：按 TTL 周期性同步已跟踪的页，
// 同时响应按需同步请求。启动时不预抓，等首个请求或第一个 tick。
// listLoop 负责定时任务；按需补页由 queue.go 里的固定 worker 池消费。
func (s *Store) listLoop(ctx context.Context) {
	ticker := time.NewTicker(s.listTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 只刷新最近被访问过的页，冷门页按需再抓。
			for _, k := range s.activePages() {
				site, page := splitPageKey(k)
				s.SyncPageSite(site, page, false)
			}
			s.pruneOrphans()
		}
	}
}

// activePages 返回最近被访问过的页键（按最近使用时间倒序），用于定时刷新。
func (s *Store) activePages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	type entry struct {
		key  string
		used time.Time
	}
	entries := make([]entry, 0, len(s.pages))
	for key, ps := range s.pages {
		if len(ps.ids) == 0 {
			continue
		}
		entries = append(entries, entry{key, ps.lastUsed})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].used.After(entries[j].used) })
	if len(entries) > maxActivePages {
		entries = entries[:maxActivePages]
	}

	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.key
	}
	return keys
}

// pageSyncLock 返回某页列表抓取的互斥锁，保证同一页不会被并发抓取。
func (s *Store) pageSyncLock(key string) *sync.Mutex {
	s.pageMu.Lock()
	defer s.pageMu.Unlock()
	l, ok := s.pageLock[key]
	if !ok {
		l = &sync.Mutex{}
		s.pageLock[key] = l
	}
	return l
}

// SyncPage 抓默认站点的一页列表（兼容旧调用）。
func (s *Store) SyncPage(page int, force bool) int {
	return s.SyncPageSite(s.defSite, page, force)
}

// SyncPageSite 抓某站点的一页列表，用 id 与本地映射做增量合并。
// 全程持有该页的抓取锁：同一页的并发调用会排队，第二次进来时若刚抓过会直接跳过。
// force=true 时忽略 TTL（用于手动刷新）。
// 返回该页当前的条目数；失败或数据异常时保留原有顺序不变。
func (s *Store) SyncPageSite(site string, page int, force bool) int {
	if site == "" {
		site = s.defSite
	}
	if p := s.provider(site); p == nil {
		log.Printf("[List] 未知站点 %q，跳过第 %d 页", site, page)
		return 0
	}
	key := pageKey(site, page)
	lock := s.pageSyncLock(key)
	lock.Lock()
	defer lock.Unlock()

	s.mu.Lock()
	// 站点级熔断（三态）：
	//   冷却期内       → 直接跳过
	//   冷却期已结束    → 放行一次试探请求（halfOpen），由它的结果决定恢复或再熔断
	// force=true      → 手动刷新，始终放行，并重置熔断状态
	if !force {
		if st := s.stateOfLocked(site); !st.allow(time.Now()) {
			n := 0
			if ps, ok := s.pages[key]; ok {
				n = len(ps.ids)
			}
			s.mu.Unlock()
			return n
		}
	} else {
		// 手动刷新：无条件重试并重置熔断状态。
		s.stateOfLocked(site).onSuccess()
	}
	ps, ok := s.pages[key]
	if !ok {
		ps = &pageState{site: site, page: page}
		s.pages[key] = ps
	}
	// 刚同步过（TTL 内）或正在别处抓取，都不重复请求（force 除外）。
	if !force && !ps.lastList.IsZero() && time.Since(ps.lastList) < s.listTTL {
		n := len(ps.ids)
		s.mu.Unlock()
		return n
	}
	if s.syncing[key] {
		n := len(ps.ids)
		s.mu.Unlock()
		return n
	}
	s.syncing[key] = true
	known := len(ps.ids)
	ps.lastAttempt = time.Now().UnixNano()
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.syncing, key)
		s.mu.Unlock()
	}()

	listed, err := s.fetchList(site, page)
	now := time.Now()

	s.mu.Lock()
	ps.lastAttempt = now.UnixNano()
	// 上游返回空列表 = 翻到末尾，这是正常终止而不是失败：
	// 立刻打上 empty 标记，后续 hasNextPage 会跳过这个站点。
	if err == nil && len(listed) == 0 {
		ps.empty = true
		ps.lastList = now
		ps.lastErr = ""
		ps.fails = 0
		s.stateOfLocked(site).onSuccess()
		n := len(ps.ids)
		s.mu.Unlock()
		log.Printf("[List] %s 第 %d 页为空，判定为站点末尾", site, page)
		s.markDirty()
		return n
	}
	switch {
	case err != nil:
	case len(listed) > maxItemsPerPage:
		err = fmt.Errorf("上游单页返回 %d 条，超过上限 %d，疑似异常数据", len(listed), maxItemsPerPage)
	case known > 0 && len(listed) < known/3:
		// 列表骤减多半是被截断或拦截，宁可沿用缓存也不要覆盖。
		err = fmt.Errorf("上游单页仅返回 %d 条（原 %d 条），疑似不完整", len(listed), known)
	}
	if err != nil {
		// 节流跳过不是上游故障：不计入熔断，只安排稍后重试。
		if errors.Is(err, ErrThrottled) || errors.Is(err, siteutil.ErrRateLimited) {
			ps.lastErr = err.Error()
			s.mu.Unlock()
			log.Printf("[List] %s 第 %d 页因节流跳过，稍后重试", site, page)
			return 0
		}
		ps.lastErr = err.Error()
		ps.fails++
		st := s.stateOfLocked(site)
		nowCooling, cooldown := st.onFailure(err.Error())
		streak := st.failStreak
		s.mu.Unlock()

		if nowCooling {
			log.Printf("[List] %s 连续失败 %d 次，熔断 %s（结束后自动试探恢复）: %v",
				site, streak, cooldown, err)
			s.markDirty()
		} else {
			log.Printf("[List] %s 第 %d 页同步失败（第 %d 次，沿用缓存 %d 条）: %v",
				site, page, streak, known, err)
		}
		return 0
	}
	ps.lastErr = ""
	ps.fails = 0
	// 成功一次即完全恢复（closed）。
	s.stateOfLocked(site).onSuccess()

	// 先去重（按站点内 id 去重，键为 itemKey）。
	type listedRow struct {
		key string
		it  Item
	}
	uniq := make([]listedRow, 0, len(listed))
	listFields := make(map[string]Item, len(listed))
	seen := make(map[string]bool, len(listed))
	for _, l := range listed {
		id := l.ID
		if id == "" {
			continue
		}
		l.Site = site
		k := itemKey(site, id)
		if seen[k] {
			continue
		}
		seen[k] = true
		uniq = append(uniq, listedRow{key: k, it: l})
		listFields[k] = l
	}

	// 容量不足时，先淘汰“新列表里已经没有”的旧条目给新帖腾位置。
	capacity := s.maxItems - len(s.items)
	future := make(map[string]bool, len(uniq))
	for _, r := range uniq {
		future[r.key] = true
	}
	evicted := 0
	pendingNew := 0
	for _, r := range uniq {
		if _, ok := s.items[r.key]; !ok {
			pendingNew++
		}
	}
	if pendingNew > capacity {
		evicted = s.evictUnlistedLocked(pendingNew-capacity, key, future)
		capacity += evicted
	}

	// 按上游顺序重建本页：已有条目复用映射，新帖在容量允许时入库。
	keys := make([]string, 0, len(uniq))
	added, skipped := 0, 0
	for _, r := range uniq {
		if _, ok := s.items[r.key]; ok {
			keys = append(keys, r.key)
			continue
		}
		if added >= capacity {
			skipped++
			continue
		}
		it := r.it
		it.ID = splitID(r.key)
		it.Site = site
		it.Page = page
		s.items[r.key] = &it
		keys = append(keys, r.key)
		added++
		s.enqueueLocked(r.key)
	}
	s.dropped += skipped

	// 已有条目只更新列表字段，播放地址原样保留。
	for _, k := range keys {
		it, ok := s.items[k]
		if !ok {
			continue
		}
		if l, ok := listFields[k]; ok {
			it.Title = l.Title
			it.Duration = l.Duration
			it.ThreadURL = l.ThreadURL
			it.Site = site
			it.Page = page
		}
		if needsDetail(it, now) {
			s.enqueueLocked(k)
		}
	}
	// 展示顺序交给站点队列维护（唯一真源），队列本身去重：
	//   - 第 1 页：按上游顺序整体插到队首，旧条目自然顺移一格
	//     （这就是「新数据在最前」且不会重复/漏条的原因）
	//   - 其它页：保持既有顺序不变，只把本页新出现的条目追加到队尾
	//     （中间页被刷新时若整体重排，会让用户已看到的顺序跳动）
	q := s.orderOfLocked(site)
	switch {
	case page == 1:
		q.PrependPage(keys)
	case q.Len() == 0:
		q.AppendKeys(keys)
	default:
		q.AppendKeys(newKeysOnly(q, keys))
	}
	ps.site = site
	ps.page = page
	ps.empty = false
	ps.fails = 0
	ps.ids = keys
	ps.lastList = now
	queued := len(s.pending)
	total := len(s.items)
	dropped := s.dropped
	multi := len(s.sites) > 1
	s.mu.Unlock()

	if evicted > 0 {
		log.Printf("[Limit] 映射已满 %d 条，淘汰 %d 条已下架旧条目", s.maxItems, evicted)
	}
	if skipped > 0 {
		log.Printf("[Limit] 淘汰后仍超出上限，跳过 %d 条新帖", skipped)
	}
	if multi {
		log.Printf("[List] %s 第 %d 页已同步：列表 %d 条，新增 %d 条，总计映射 %d/%d 条（待解析 %d，累计跳过 %d）",
			site, page, len(keys), added, total, s.maxItems, queued, dropped)
	} else {
		log.Printf("[List] 第 %d 页已同步：列表 %d 条，新增 %d 条，总计映射 %d/%d 条（待解析 %d，累计跳过 %d）",
			page, len(keys), added, total, s.maxItems, queued, dropped)
	}
	s.markDirty()
	return len(keys)
}

// newKeysOnly 过滤出尚未进入队列的键（保持原顺序）。
func newKeysOnly(q *orderQueue, keys []string) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if !q.Has(key) {
			out = append(out, key)
		}
	}
	return out
}

// splitID 从 itemKey 取出条目 id。
func splitID(key string) string {
	_, id := splitKey(key)
	return id
}

// evictUnlistedLocked 淘汰最多 n 条“不会出现在任何列表里”的旧映射。
// future 是即将生效的新键集合；被覆盖页的旧顺序因此被忽略，
// 其余页的引用仍然有效（同一条目可能同时挂在多页）。
func (s *Store) evictUnlistedLocked(n int, replacingPage string, future map[string]bool) int {
	if n <= 0 {
		return 0
	}
	referenced := make(map[string]bool, len(s.items))
	for p, ps := range s.pages {
		if p == replacingPage {
			continue // 本页顺序即将被 future 覆盖
		}
		for _, k := range ps.ids {
			referenced[k] = true
		}
	}
	for k := range future {
		referenced[k] = true
	}

	// 优先淘汰最久没被解析/更新的条目。
	candidates := make([]*Item, 0, n)
	keys := make([]string, 0, n)
	for k, it := range s.items {
		if referenced[k] || s.pending[k] {
			continue
		}
		candidates = append(candidates, it)
		keys = append(keys, k)
	}
	order := make([]int, len(candidates))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return candidates[order[i]].UpdatedAt < candidates[order[j]].UpdatedAt
	})
	if len(order) > n {
		order = order[:n]
	}
	for _, i := range order {
		delete(s.items, keys[i])
	}
	return len(order)
}

// pruneOrphans 清理掉出所有列表且超过保留期的映射，防止无限增长。
func (s *Store) pruneOrphans() {
	if s.keep <= 0 {
		return
	}
	cutoff := time.Now().Add(-s.keep).Unix()

	s.mu.Lock()
	referenced := make(map[string]bool, len(s.items))
	for _, ps := range s.pages {
		for _, k := range ps.ids {
			referenced[k] = true
		}
	}
	removed := 0
	for k, it := range s.items {
		if !referenced[k] && !s.pending[k] && it.UpdatedAt > 0 && it.UpdatedAt < cutoff {
			delete(s.items, k)
			removed++
		}
	}
	s.mu.Unlock()

	if removed > 0 {
		log.Printf("[Prune] 清理 %d 条长期未在列表出现的映射", removed)
		s.markDirty()
	}
}
