package store

// 本文件是列表流水线：唯一的列表抓取者，负责把上游列表增量合并进映射，
// 并把需要解析的条目排进详情队列。

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
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

// listLoop 是唯一的列表抓取者：按 TTL 周期性做增量同步，
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
			s.syncIncrementalAll()
			s.pruneOrphans()
		}
	}
}

// syncIncrementalAll 对所有站点做一轮增量同步。
func (s *Store) syncIncrementalAll() {
	for _, p := range s.sites {
		if p == nil {
			continue
		}
		s.syncIncrementalSite(p.Name())
	}
}

// syncIncrementalSite 从第 1 页开始往下逐页比对，直到「撞上本地已有的条目」。
//
// 判定逻辑：每抓一页，就和本地映射里已有的 ID 比对——
//   - 该页出现本地已有的 ID，说明已经追平上次同步的位置（例如上次排到 123，
//     这次翻到含 123 的那一页），后面的页都是老内容，立刻停；
//   - 整页都是没见过的 ID，说明还没追平，继续往下一页；
//   - 一直追不平（例如首次同步），最多抓 maxIncrementalPages 页就收手，
//     避免一口气把上游全站翻一遍触发风控。
//
// 顺序处理：第 1 页整页按上游顺序放到队首（旧条目自然后移一格），
// 更深页只把「新增的」按顺序接在第 1 页之后。这样上游插到第 1 页的新帖
// 一定在最前，被挤下去的旧帖位置后移，不重复也不遗漏。
func (s *Store) syncIncrementalSite(site string) {
	if site == "" {
		site = s.defSite
	}
	if s.provider(site) == nil {
		return
	}

	front := make([]string, 0, 64) // 待放到队首的键（第 1 页整页 + 更深页的新增）
	pages, added := 0, 0

	// 本轮开始前就存在的键：用来判断「这一页有没有老条目」，即是否已追平。
	preKnown := s.knownKeys(site)

	for page := 1; page <= maxIncrementalPages; page++ {
		res, _ := s.syncPageMerge(site, page, false, preKnown)
		if !res.ok {
			break // 空页、失败或节流：本轮到此为止
		}
		pages++
		added += len(res.newKeys)
		if page == 1 {
			front = append(front, res.keys...)
		} else {
			front = append(front, res.newKeys...)
		}
		if res.hasKnown {
			break // 撞上本地已有的 ID：已追平上次同步的位置
		}
	}

	if len(front) > 0 {
		s.mu.Lock()
		s.orderOfLocked(site).PrependPage(front)
		s.mu.Unlock()
		s.markDirty()
	}

	if pages > 0 {
		log.Printf("[List] %s 更新 %d 页 %d 条", site, pages, added)
	}
}

// knownKeys 返回某站点当前已入库的全部键（本轮同步开始前的快照）。
func (s *Store) knownKeys(site string) map[string]bool {
	prefix := site + ":"
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool, len(s.items))
	for k := range s.items {
		if strings.HasPrefix(k, prefix) {
			out[k] = true
		}
	}
	return out
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
	res, _ := s.syncPageMerge(s.defSite, page, force, nil)
	return len(res.keys)
}

// SyncPageSite 抓某站点的一页列表，用 id 与本地映射做增量合并。
// 全程持有该页的抓取锁：同一页的并发调用会排队，第二次进来时若刚抓过会直接跳过。
// force=true 时忽略 TTL（用于手动刷新）。
// 返回该页当前的条目数；失败或数据异常时保留原有顺序不变。
func (s *Store) SyncPageSite(site string, page int, force bool) int {
	res, _ := s.syncPageMerge(site, page, force, nil)
	return len(res.keys)
}

// pageMergeResult 是一页列表合并进映射后的结果，供增量同步判断是否继续。
type pageMergeResult struct {
	keys     []string // 本页按上游顺序的键
	newKeys  []string // 其中本次新入库的键（保持上游顺序）
	hasKnown bool     // 本页是否出现了「同步前就已存在」的条目
	ok       bool     // 是否抓到了内容（空页 / 失败 / 节流为 false）
}

// syncPageMerge 抓一页并合并进映射，返回合并结果。
//
// 展示顺序只做「兜底」处理（见 orderAfterPageLocked）；增量同步会拿到
// 结果后统一把新内容插到队首，见 syncIncrementalSite。
//
// preKnown 是「本轮同步开始前就已存在的键」，用来判断这一页是否已经包含
// 上次同步过的内容；为 nil 时按本次抓取前的映射计算。
func (s *Store) syncPageMerge(site string, page int, force bool, preKnown map[string]bool) (pageMergeResult, string) {
	var empty pageMergeResult
	if site == "" {
		site = s.defSite
	}
	if p := s.provider(site); p == nil {
		log.Printf("[List] 未知站点 %q，跳过第 %d 页", site, page)
		return empty, site
	}
	key := pageKey(site, page)
	lock := s.pageSyncLock(key)
	lock.Lock()
	defer lock.Unlock()

	s.mu.Lock()
	// 站点级熔断（三态）：
	//   冷却期内       → 直接跳过
	//   冷却期已结束    → 放行一次试探请求（halfOpen），由它的结果决定恢复或再熔断
	//   force=true      → 手动刷新，始终放行，并重置熔断状态
	if !force {
		if st := s.stateOfLocked(site); !st.allow(time.Now()) {
			var cached []string
			if ps, ok := s.pages[key]; ok {
				cached = append(cached, ps.ids...)
			}
			s.mu.Unlock()
			return pageMergeResult{keys: cached}, site
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
	// 抓取前的快照：判断「本页有没有老条目」要用它，别把本页新入库的算进去。
	if preKnown == nil {
		preKnown = make(map[string]bool, len(s.items))
		for k := range s.items {
			preKnown[k] = true
		}
	}
	// 刚同步过（TTL 内）或正在别处抓取，都不重复请求（force 除外）。
	if !force && !ps.lastList.IsZero() && time.Since(ps.lastList) < s.listTTL {
		cached := append([]string(nil), ps.ids...)
		known := false
		for _, k := range cached {
			if preKnown[k] {
				known = true
				break
			}
		}
		s.mu.Unlock()
		return pageMergeResult{keys: cached, hasKnown: known, ok: len(cached) > 0}, site
	}
	if s.syncing[key] {
		cached := append([]string(nil), ps.ids...)
		s.mu.Unlock()
		return pageMergeResult{keys: cached, ok: len(cached) > 0}, site
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
		cached := append([]string(nil), ps.ids...)
		s.mu.Unlock()
		log.Printf("[List] %s 第 %d 页为空，判定为站点末尾", site, page)
		s.markDirty()
		return pageMergeResult{keys: cached}, site
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
			return empty, site
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
		return empty, site
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

	// 本页是否出现「同步前就已存在」的条目：出现即说明已追平上次同步位置。
	hasKnown := false
	for _, r := range uniq {
		if preKnown[r.key] {
			hasKnown = true
			break
		}
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
	newKeys := make([]string, 0, len(uniq))
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
		newKeys = append(newKeys, r.key)
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
		if s.NeedsDetail(it, now) {
			s.enqueueLocked(k)
		}
	}
	// 展示顺序：单页刷新走兜底规则；增量同步随后会再统一插到队首（幂等）。
	s.orderAfterPageLocked(site, page, keys, newKeys)
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
	return pageMergeResult{keys: keys, newKeys: newKeys, hasKnown: hasKnown, ok: len(keys) > 0}, site
}

// orderAfterPageLocked 是单页刷新时的展示顺序兜底规则：
//   - 第 1 页：按上游顺序整体插到队首，旧条目自然顺移
//   - 其它页：只把本页新增的条目接在队尾（避免中间页刷新导致顺序跳动）
func (s *Store) orderAfterPageLocked(site string, page int, keys, newKeys []string) {
	q := s.orderOfLocked(site)
	switch {
	case page == 1:
		q.PrependPage(keys)
	case q.Len() == 0:
		q.AppendKeys(keys)
	default:
		q.AppendKeys(newKeys)
	}
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
	// 注意：这里持有 s.mu，不能去拿 detailMu（按需修复是 detailMu → s.mu 的顺序，
	// 反着来会死锁）。淘汰条目的解析锁留给 pruneOrphans 之外的路径自然复用，
	// 这些锁对象很小，数量与条目同阶，不会无限增长。
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
	removed := make([]string, 0, 8)
	for k, it := range s.items {
		if !referenced[k] && !s.pending[k] && it.UpdatedAt > 0 && it.UpdatedAt < cutoff {
			delete(s.items, k)
			removed = append(removed, k)
		}
	}
	s.mu.Unlock()

	// 条目已删除，顺手清掉它的解析锁，避免映射无限增长。
	for _, k := range removed {
		s.dropKeyLock(k)
	}

	if len(removed) > 0 {
		log.Printf("[Prune] 清理 %d 条长期未在列表出现的映射", len(removed))
		s.markDirty()
	}
}
