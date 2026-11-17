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

	"paresevideo/internal/upstream"
)

// RequestSync 让列表线程尽快同步某一页（同页请求合并）。
func (s *Store) RequestSync(page int) {
	s.mu.Lock()
	if s.syncReq[page] || s.syncing[page] {
		s.mu.Unlock()
		return
	}
	s.syncReq[page] = true
	s.mu.Unlock()

	select {
	case s.wake <- page:
	default:
		s.mu.Lock()
		delete(s.syncReq, page)
		s.mu.Unlock()
	}
}

// fetchList 抓列表页，带全局限流：同一时刻只允许一个列表请求，
// 连续抓取之间至少间隔 ListFetchGap，避免突发流量打满上游。
func (s *Store) fetchList(page int) ([]Item, error) {
	s.listMu.Lock()
	defer s.listMu.Unlock()

	if wait := ListFetchGap - time.Since(s.lastFetch); wait > 0 {
		time.Sleep(wait)
	}
	items, err := upstream.FetchList(page)
	s.lastFetch = time.Now()
	return items, err
}

// listLoop 是唯一的列表抓取者：按 TTL 周期性同步已跟踪的页，
// 同时响应按需同步请求。启动时不预抓，等首个请求或第一个 tick。
func (s *Store) listLoop(ctx context.Context) {
	ticker := time.NewTicker(s.listTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case page := <-s.wake:
			s.mu.Lock()
			delete(s.syncReq, page)
			s.mu.Unlock()
			s.SyncPage(page, false)

			// 顺带处理其它排队请求，单轮有上限，避免长时间占住列表线程。
			for i := 0; i < maxListBatch; i++ {
				p, ok := s.takeSyncReq()
				if !ok {
					break
				}
				s.SyncPage(p, false)
			}
		case <-ticker.C:
			// 只刷新最近被访问过的页，冷门页按需再抓。
			for _, page := range s.activePages() {
				s.SyncPage(page, false)
			}
			s.pruneOrphans()
		}
	}
}

// takeSyncReq 取一个待同步的页（调用方已保证串行使用）。
func (s *Store) takeSyncReq() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for page := range s.syncReq {
		delete(s.syncReq, page)
		return page, true
	}
	return 0, false
}

// activePages 返回最近被访问过的页（按最近使用时间倒序），用于定时刷新。
func (s *Store) activePages() []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	type entry struct {
		page int
		used time.Time
	}
	entries := make([]entry, 0, len(s.pages))
	for page, ps := range s.pages {
		if len(ps.ids) == 0 {
			continue
		}
		entries = append(entries, entry{page, ps.lastUsed})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].used.After(entries[j].used) })
	if len(entries) > maxActivePages {
		entries = entries[:maxActivePages]
	}

	pages := make([]int, len(entries))
	for i, e := range entries {
		pages[i] = e.page
	}
	return pages
}

// pageSyncLock 返回某页列表抓取的互斥锁，保证同一页不会被并发抓取。
func (s *Store) pageSyncLock(page int) *sync.Mutex {
	s.pageMu.Lock()
	defer s.pageMu.Unlock()
	l, ok := s.pageLock[page]
	if !ok {
		l = &sync.Mutex{}
		s.pageLock[page] = l
	}
	return l
}

// SyncPage 抓一页列表，用 id 与本地映射做增量合并。
// 全程持有该页的抓取锁：同一页的并发调用会排队，第二次进来时若刚抓过会直接跳过。
// force=true 时忽略 TTL（用于手动刷新）。
// 返回该页当前的条目数；失败或数据异常时保留原有顺序不变。
func (s *Store) SyncPage(page int, force bool) int {
	lock := s.pageSyncLock(page)
	lock.Lock()
	defer lock.Unlock()

	s.mu.Lock()
	ps, ok := s.pages[page]
	if !ok {
		ps = &pageState{}
		s.pages[page] = ps
	}
	// 刚同步过（TTL 内）或正在别处抓取，都不重复请求（force 除外）。
	if !force && !ps.lastList.IsZero() && time.Since(ps.lastList) < s.listTTL {
		n := len(ps.ids)
		s.mu.Unlock()
		return n
	}
	if s.syncing[page] {
		n := len(ps.ids)
		s.mu.Unlock()
		return n
	}
	s.syncing[page] = true
	known := len(ps.ids)
	ps.lastAttempt = time.Now().UnixNano()
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.syncing, page)
		s.mu.Unlock()
	}()

	listed, err := s.fetchList(page)
	now := time.Now()

	s.mu.Lock()
	ps.lastAttempt = now.UnixNano()
	switch {
	case err != nil:
	case len(listed) == 0:
		err = errors.New("上游返回空列表")
	case len(listed) > maxItemsPerPage:
		err = fmt.Errorf("上游单页返回 %d 条，超过上限 %d，疑似异常数据", len(listed), maxItemsPerPage)
	case known > 0 && len(listed) < known/3:
		// 列表骤减多半是被截断或拦截，宁可沿用缓存也不要覆盖。
		err = fmt.Errorf("上游单页仅返回 %d 条（原 %d 条），疑似不完整", len(listed), known)
	}
	if err != nil {
		ps.lastErr = err.Error()
		s.mu.Unlock()
		log.Printf("[List] 第 %d 页同步失败（沿用缓存 %d 条）: %v", page, known, err)
		return 0
	}
	ps.lastErr = ""

	// 先去重。
	uniq := make([]Item, 0, len(listed))
	listFields := make(map[string]Item, len(listed))
	seen := make(map[string]bool, len(listed))
	for _, l := range listed {
		if l.ID == "" || seen[l.ID] {
			continue
		}
		seen[l.ID] = true
		uniq = append(uniq, l)
		listFields[l.ID] = l
	}

	// 容量不足时，先淘汰“新列表里已经没有”的旧条目给新帖腾位置。
	capacity := s.maxItems - len(s.items)
	future := make(map[string]bool, len(uniq))
	for _, l := range uniq {
		future[l.ID] = true
	}
	evicted := 0
	if pendingNew := countNew(uniq, s.items); pendingNew > capacity {
		evicted = s.evictUnlistedLocked(pendingNew-capacity, page, future)
		capacity += evicted
	}

	// 按上游顺序重建本页：已有条目复用映射，新帖在容量允许时入库。
	ids := make([]string, 0, len(uniq))
	added, skipped := 0, 0
	for _, l := range uniq {
		if _, ok := s.items[l.ID]; ok {
			ids = append(ids, l.ID)
			continue
		}
		if added >= capacity {
			skipped++
			continue
		}
		l.Page = page
		it := l
		s.items[l.ID] = &it
		ids = append(ids, l.ID)
		added++
		s.enqueueLocked(l.ID)
	}
	s.dropped += skipped

	// 已有条目只更新列表字段，播放地址原样保留。
	for _, id := range ids {
		it, ok := s.items[id]
		if !ok {
			continue
		}
		if l, ok := listFields[id]; ok {
			it.Title = l.Title
			it.Duration = l.Duration
			it.ThreadURL = l.ThreadURL
			it.Page = page
		}
		if needsDetail(it, now) {
			s.enqueueLocked(id)
		}
	}
	ps.ids = ids
	ps.lastList = now
	queued := len(s.pending)
	total := len(s.items)
	dropped := s.dropped
	s.mu.Unlock()

	if evicted > 0 {
		log.Printf("[Limit] 映射已满 %d 条，淘汰 %d 条已下架旧条目", s.maxItems, evicted)
	}
	if skipped > 0 {
		log.Printf("[Limit] 淘汰后仍超出上限，跳过 %d 条新帖", skipped)
	}
	log.Printf("[List] 第 %d 页已同步：列表 %d 条，新增 %d 条，总计映射 %d/%d 条（待解析 %d，累计跳过 %d）",
		page, len(ids), added, total, s.maxItems, queued, dropped)
	s.markDirty()
	return len(ids)
}

// countNew 统计列表中尚未入库的条目数。
func countNew(listed []Item, items map[string]*Item) int {
	n := 0
	for _, l := range listed {
		if _, ok := items[l.ID]; !ok {
			n++
		}
	}
	return n
}

// evictUnlistedLocked 淘汰最多 n 条“不会出现在任何列表里”的旧映射。
// future 是第 page 页即将生效的新 id 集合；该页旧顺序因此被忽略，
// 其余页的引用仍然有效（同一 id 可能同时挂在多页）。
func (s *Store) evictUnlistedLocked(n, page int, future map[string]bool) int {
	if n <= 0 {
		return 0
	}
	referenced := make(map[string]bool, len(s.items))
	for p, ps := range s.pages {
		if p == page {
			continue // 本页顺序即将被 future 覆盖
		}
		for _, id := range ps.ids {
			referenced[id] = true
		}
	}
	for id := range future {
		referenced[id] = true
	}

	// 优先淘汰最久没被解析/更新的条目。
	candidates := make([]*Item, 0, n)
	for id, it := range s.items {
		if referenced[id] || s.pending[id] {
			continue
		}
		candidates = append(candidates, it)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].UpdatedAt < candidates[j].UpdatedAt
	})
	if len(candidates) > n {
		candidates = candidates[:n]
	}
	for _, it := range candidates {
		delete(s.items, it.ID)
	}
	return len(candidates)
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
		for _, id := range ps.ids {
			referenced[id] = true
		}
	}
	removed := 0
	for id, it := range s.items {
		if !referenced[id] && !s.pending[id] && it.UpdatedAt > 0 && it.UpdatedAt < cutoff {
			delete(s.items, id)
			removed++
		}
	}
	s.mu.Unlock()

	if removed > 0 {
		log.Printf("[Prune] 清理 %d 条长期未在列表出现的映射", removed)
		s.markDirty()
	}
}
