package store

// 本文件实现分页读取：只读缓存 + 缺页时对所有站点并发补下一页。
//
// 设计要点（按需求确定）：
//
//	1. 读取路径不阻塞抓取。缓存不够时立刻返回现有数据，同时把补页请求
//	   投给后台列表线程，前端用 loading 标志决定是否再取一次。
//	2. 补页规则：只要「已缓存的总条数」小于本次请求需要的条数
//	   （offset+limit），就向「每个尚未到达最后一页的站点」各要一页。
//	   已经到最后一页的站点直接跳过，不再浪费请求。
//	3. 各站点并发抓取；同一站点内部仍串行且保持最小间隔（见 fetchLimiter），
//	   既快又不会把单个上游打爆。

import (
	"log"
	"sort"
	"strings"
	"time"

	"paresevideo/internal/provider"
)

// Window 是一次分页读取的结果。
type Window struct {
	Items   []Item // 本窗口的条目
	Total   int    // 过滤后的总条数
	Pending int    // 其中仍在解析的数量
	// Loading 表示后台正在为该查询补页；前端可稍后重试。
	Loading bool
	// HasMore 表示还有站点没到最后一页，理论上还能取到更多内容。
	HasMore bool
}

// FillWindow 读取 [offset, offset+limit) 区间，不带过滤。
func (s *Store) FillWindow(offset, limit int) Window {
	return s.FillWindowSource(offset, limit, "")
}

// FillWindowSource 读取窗口，只保留 source 匹配的条目（空串表示不过滤）。
func (s *Store) FillWindowSource(offset, limit int, source string) Window {
	return s.FillWindowQuery(offset, limit, source, "")
}

// FillWindowQuery 读取窗口，可按来源域名与站点过滤。
//
// 只读缓存：数据不足时立即返回已有内容，并把补页请求投给后台。
func (s *Store) FillWindowQuery(offset, limit int, source, site string) Window {
	offset = max(offset, 0)
	if limit < 1 {
		limit = 24
	}
	limit = min(limit, MaxWindowSize)

	items, total := s.readWindow(offset, limit, source, site)

	need := offset + limit
	short := total < need
	pending := false
	if short {
		// 缓存不够：对每个未到末尾的站点并发补一页。
		// queued>0 说明刚投递出去，即使后台还没开始抓也算 loading，
		// 避免前端因为「投递与开抓之间的瞬间空档」而少轮询一次。
		queued := s.RequestNextPage(site)
		pending = queued > 0 || s.pageLoading(site)
	}

	s.resolveWindowItems(items)

	return Window{
		Items:   s.refreshItems(items),
		Total:   total,
		Pending: s.pendingCount(items),
		Loading: short && pending,
		HasMore: s.HasMore(site),
	}
}

// readWindow 从缓存里读出 [offset, offset+limit)，并返回过滤后的总数。
func (s *Store) readWindow(offset, limit int, source, site string) ([]Item, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := s.listedKeysLocked(site)
	out := make([]Item, 0, limit)
	total := 0
	for _, key := range keys {
		it, ok := s.items[key]
		if !ok {
			continue
		}
		if source != "" && !SameSource(it.Source, source) {
			continue
		}
		idx := total
		total++
		if idx < offset {
			continue
		}
		if len(out) >= limit {
			continue
		}
		out = append(out, *it)
	}
	return out, total
}

// listedKeysLocked 按展示顺序返回键（site 为空表示所有站点）。
//
// 顺序的真源是每个站点的 orderQueue：站点按注册顺序拼接。
// 队列本身就是去重的，所以这里不需要再去重。
func (s *Store) listedKeysLocked(site string) []string {
	sites := s.siteList(site)
	out := make([]string, 0, len(s.items))
	for _, name := range sites {
		q, ok := s.orders[name]
		if !ok {
			continue
		}
		out = append(out, q.Keys(s.maxItems-len(out))...)
		if len(out) >= s.maxItems {
			break
		}
	}
	return out
}

// rebuildOrderLocked 用 pages 的内容重建某站点的队列（仅迁移/兜底用）。
// 调用方需持有 s.mu。
func (s *Store) rebuildOrderLocked(site string) {
	q := s.orderOfLocked(site)
	var keys []string
	for _, pageKey := range s.orderedPageKeysLocked() {
		ps := s.pages[pageKey]
		psSite := ps.site
		if psSite == "" {
			psSite = s.defSite
		}
		if psSite != site {
			continue
		}
		keys = append(keys, ps.ids...)
	}
	q.Reset(keys)
}

// orderedPageKeysLocked 返回有内容的页键（站点注册顺序、页号升序）。
// 调用方需持有 s.mu。
func (s *Store) orderedPageKeysLocked() []string {
	keys := make([]string, 0, len(s.pages))
	for key, ps := range s.pages {
		if len(ps.ids) > 0 {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		si, pi := splitPageKey(keys[i])
		sj, pj := splitPageKey(keys[j])
		if si != sj {
			return s.siteOrderLocked(si) < s.siteOrderLocked(sj)
		}
		return pi < pj
	})
	return keys
}

// siteOrderLocked 返回站点在注册表中的序号（未知站点排到最后）。
func (s *Store) siteOrderLocked(site string) int {
	for i, p := range s.sites {
		if p != nil && p.Name() == site {
			return i
		}
	}
	return len(s.sites)
}

// SameSource 比较来源域名（忽略大小写与 www. 前缀）。
func SameSource(a, b string) bool {
	return strings.EqualFold(NormalizeSource(a), NormalizeSource(b))
}

// NormalizeSource 归一化来源域名：小写并去掉 www. 前缀。
func NormalizeSource(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	return strings.TrimPrefix(s, "www.")
}

// resolveWindowItems 把窗口里缺播放地址的条目排进详情队列（不等待）。
func (s *Store) resolveWindowItems(items []Item) {
	if len(items) == 0 {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range items {
		if provider.IsMediaURL(it.PlayURL) {
			continue
		}
		key := itemKey(it.Site, it.ID)
		if cur, ok := s.items[key]; ok && needsDetail(cur, now) {
			s.enqueueLocked(key)
		}
	}
}

// refreshItems 用最新映射刷新给定条目（保持原顺序）。
func (s *Store) refreshItems(items []Item) []Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Item, 0, len(items))
	for _, it := range items {
		if cur, ok := s.items[itemKey(it.Site, it.ID)]; ok {
			out = append(out, *cur)
			continue
		}
		out = append(out, it)
	}
	return out
}

// pendingCount 统计给定条目里还没有播放地址的数量。
func (s *Store) pendingCount(items []Item) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, it := range items {
		if provider.IsMediaURL(it.PlayURL) {
			continue
		}
		if cur, ok := s.items[itemKey(it.Site, it.ID)]; ok && !provider.IsMediaURL(cur.PlayURL) {
			n++
		}
	}
	return n
}

// siteList 返回参与本次查询的站点：指定了 site 就只算它，否则算全部。
// 指定了未注册的站点名时返回空，调用方据此返回空结果（而不是退化成全部）。
func (s *Store) siteList(site string) []string {
	if site != "" {
		if p := s.provider(site); p != nil {
			return []string{p.Name()}
		}
		return nil
	}
	return s.Providers()
}

// HasMore 判断是否还有站点理论上能取到更多内容。
//
// 这里只看「硬边界」（深挖上限、页数上限、上游空页），不看熔断冷却：
// 熔断是暂时的，冷却结束后会自动恢复试探。请求补页时另行判断冷却。
func (s *Store) HasMore(site string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range s.siteList(site) {
		if s.hasMoreIgnoringBreakerLocked(name) {
			return true
		}
	}
	return false
}

// hasMoreIgnoringBreakerLocked 只判断硬边界（忽略熔断冷却）。
func (s *Store) hasMoreIgnoringBreakerLocked(site string) bool {
	if site == "" {
		site = s.defSite
	}
	next := s.nextPageLocked(site)
	if next > s.maxPagesOf(site) {
		return false
	}
	if limit := s.deepLimit(site); limit > 0 && next > limit {
		return false
	}
	if last := next - 1; last > 0 {
		if ps, ok := s.pages[pageKey(site, last)]; ok && ps.empty {
			return false
		}
	}
	return true
}

// pageLoading 判断是否有站点的补页正在进行（排队或抓取中）。
func (s *Store) pageLoading(site string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range s.siteList(site) {
		if s.syncingNextLocked(name) {
			return true
		}
	}
	return false
}

// syncingNextLocked 判断某站点是否有「补下一页」在排队或进行中。
// 调用方需持有 s.mu。
func (s *Store) syncingNextLocked(site string) bool {
	for key := range s.syncReq {
		if ks, _ := splitPageKey(key); ks == site || (ks == "" && site == s.defSite) {
			return true
		}
	}
	for key := range s.syncing {
		if ks, _ := splitPageKey(key); ks == site || (ks == "" && site == s.defSite) {
			return true
		}
	}
	return false
}

// RequestNextPage 对所有「未到最后一页」的站点投递一次「抓下一页」，
// 返回本次新入队的站点数。
//
// 完全异步：只入队并唤醒 worker，立刻返回。
// 去重由 syncReq 保证——同一页在排队或抓取中时不会被重复入队，
// 所以前端轮询多少次都不会放大请求。
func (s *Store) RequestNextPage(site string) int {
	sites := s.siteList(site)
	if len(sites) == 0 {
		return 0
	}

	queued := make([]string, 0, len(sites))
	for _, name := range sites {
		s.mu.Lock()
		if !s.hasNextPageSiteLocked(name) {
			// 已到最后一页、达到深挖上限，或正在熔断冷却，跳过。
			s.mu.Unlock()
			continue
		}
		// 该站点已有任务在排队/抓取时不再排新页：
		// 补页是「一次推进一页」，避免前端轮询把第 2、3、4 页全排上，
		// 那样会成倍放大对上游的请求。
		if s.siteHasPendingLocked(name) {
			s.mu.Unlock()
			continue
		}
		key := pageKey(name, s.nextPageLocked(name))
		added := s.enqueuePageLocked(key)
		s.mu.Unlock()
		if added {
			queued = append(queued, key)
		}
	}

	if len(queued) > 0 {
		// 有多少个站点入队就唤醒多少个 worker，保证多站点并行抓取。
		s.broadcastWake(len(queued))
		log.Printf("[Fill] 缓存不足，入队 %d 个站点的下一页: %v", len(queued), queued)
	}
	return len(queued)
}

// siteHasPendingLocked 判断某站点是否已有页在排队或正在抓取。
// 调用方需持有 s.mu。
func (s *Store) siteHasPendingLocked(site string) bool {
	for key := range s.syncReq {
		if ks, _ := splitPageKey(key); ks == site || (ks == "" && site == s.defSite) {
			return true
		}
	}
	return false
}

// nextPageLocked 返回某站点下一个要抓的页号。调用方需持有 s.mu。
//
// 以「已知的最大页号 + 1」为准，空页也计入（它代表末尾）。
func (s *Store) nextPageLocked(site string) int {
	last := 0
	for key, ps := range s.pages {
		psSite := ps.site
		if psSite == "" {
			psSite = s.defSite
		}
		if psSite != site {
			continue
		}
		page := ps.page
		if page == 0 {
			_, page = splitPageKey(key)
		}
		if page > last {
			last = page
		}
	}
	return last + 1
}

// hasNextPageSiteLocked 判断某站点是否还有可能拉到更多内容。
//
// 判据二选一即视为到底：
//   - 已到站点声明的页数上限
//   - 已知的最大页是上游确认的空页（翻到末尾了）
func (s *Store) hasNextPageSiteLocked(site string) bool {
	if site == "" {
		site = s.defSite
	}
	// 熔断冷却期内不补页（冷却结束后会自动恢复试探）。
	if st := s.stateOfLocked(site); st.cooling(time.Now()) {
		return false
	}
	return s.hasMoreIgnoringBreakerLocked(site)
}

// hasNextPageSite 判断某站点是否还有可能拉到更多内容。
func (s *Store) hasNextPageSite(site string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasNextPageSiteLocked(site)
}

// deepLimit 返回站点允许深挖到的页数上限（0 表示不限）。
// 可由 -max-pages 配置；默认为 maxDeepPages。
func (s *Store) deepLimit(site string) int {
	if s.deepPages > 0 {
		return s.deepPages
	}
	return maxDeepPages
}

// maxPagesOf 返回站点的最大页数。
func (s *Store) maxPagesOf(site string) int {
	if p := s.provider(site); p != nil {
		if n := p.MaxPages(); n > 0 {
			return n
		}
	}
	return MaxPage
}

// syncNextPageSite 同步某站点最深页的下一页（同步版，供后台与测试使用）。
func (s *Store) syncNextPageSite(site string) bool {
	if site == "" {
		site = s.defSite
	}
	s.mu.Lock()
	if !s.hasNextPageSiteLocked(site) {
		s.mu.Unlock()
		return false
	}
	next := s.nextPageLocked(site)
	key := pageKey(site, next)
	if s.syncing[key] {
		s.mu.Unlock()
		return true
	}
	if _, ok := s.pages[key]; !ok {
		s.pages[key] = &pageState{site: site, page: next}
	}
	s.mu.Unlock()

	if n := s.SyncPageSite(site, next, false); n > 0 {
		return true
	}
	// 没拿到内容：标记为空页，后续不再尝试（这就是「到末尾」的判定）。
	s.mu.Lock()
	if p, ok := s.pages[key]; ok && len(p.ids) == 0 {
		p.empty = true
	}
	s.mu.Unlock()
	return false
}

// ItemCount 返回映射总数。
func (s *Store) ItemCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// CachedPages 返回有内容的页号（单站兼容：只返回默认站点的页号）。
func (s *Store) CachedPages() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.pages))
	for _, key := range s.orderedPageKeysLocked() {
		site, page := splitPageKey(key)
		if site == "" || site == s.defSite {
			out = append(out, page)
		}
	}
	return out
}

// CachedPagesBySite 返回各站点已缓存的页号。
func (s *Store) CachedPagesBySite() map[string][]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]int, len(s.sites))
	for _, key := range s.orderedPageKeysLocked() {
		site, page := splitPageKey(key)
		if site == "" {
			site = s.defSite
		}
		out[site] = append(out[site], page)
	}
	return out
}

// AllItems 按展示顺序汇总当前列表中的条目（去重），最多 MaxItems 条。
func (s *Store) AllItems() []Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allItemsLocked()
}

func (s *Store) allItemsLocked() []Item {
	out := make([]Item, 0, len(s.items))
	for _, key := range s.listedKeysLocked("") {
		if it, ok := s.items[key]; ok {
			out = append(out, *it)
		}
	}
	return out
}
