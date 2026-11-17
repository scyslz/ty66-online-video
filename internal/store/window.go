package store

// 本文件实现按条数窗口读取（offset/limit，可按来源过滤），以及配套的只读访问器。

import (
	"log"
	"sort"
	"strings"
	"time"
)

// Window 是一次分页读取的结果。
type Window struct {
	Items   []Item // 本窗口的条目
	Total   int    // 过滤后的总条数
	Pending int    // 其中仍在解析的数量
}

// FillWindow 读取 [offset, offset+limit) 区间，不带来源过滤。
func (s *Store) FillWindow(offset, limit int) Window {
	return s.FillWindowSource(offset, limit, "")
}

// FillWindowSource 读取窗口，只保留 source 匹配的条目（空串表示不过滤）。
//
// 不足时会按需同步上游下一个列表页，直到凑满或上游到底。
// 整个补页过程持有窗口锁：并发请求依次执行，先到的把页抓好后，
// 后面的请求直接命中缓存，不会重复打上游。
func (s *Store) FillWindowSource(offset, limit int, source string) Window {
	offset = max(offset, 0)
	if limit < 1 {
		limit = 24
	}
	limit = min(limit, MaxWindowSize)

	if source != "" {
		return s.filteredWindow(offset, limit, source)
	}

	s.windowMu.Lock()
	defer s.windowMu.Unlock()

	need := offset + limit
	for attempt := 0; attempt < maxFillPages; attempt++ {
		items, total, complete := s.snapshotWindow(offset, need-offset)
		if complete || total >= need || !s.hasNextPage() {
			items = s.resolveWindowItems(items)
			return Window{Items: items, Total: total, Pending: s.pendingCount(items)}
		}
		if !s.syncNextPage() {
			// 上游已到底或本页同步失败，按现有数据返回。
			break
		}
	}

	items, total, _ := s.snapshotWindow(offset, need-offset)
	items = s.resolveWindowItems(items)
	return Window{Items: items, Total: total, Pending: s.pendingCount(items)}
}

// filteredWindow 在已缓存的列表里按来源筛选，再切窗口。
func (s *Store) filteredWindow(offset, limit int, source string) Window {
	s.windowMu.Lock()
	all := s.allItemsLocked()
	s.windowMu.Unlock()

	matched := make([]Item, 0, len(all))
	for _, it := range all {
		if SameSource(it.Source, source) {
			matched = append(matched, it)
		}
	}

	total := len(matched)
	offset = min(offset, total)
	end := min(offset+limit, total)
	items := s.resolveWindowItems(matched[offset:end])
	return Window{Items: items, Total: total, Pending: s.pendingCount(items)}
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

// resolveWindowItems 把窗口里缺播放地址的条目排进详情队列并等待完成。
// 已在队列或解析中的条目不会重复排队。
func (s *Store) resolveWindowItems(items []Item) []Item {
	if len(items) == 0 {
		return items
	}
	now := time.Now()
	s.mu.Lock()
	for _, it := range items {
		if it.PlayURL != "" {
			continue
		}
		if cur, ok := s.items[it.ID]; ok && needsDetail(cur, now) {
			s.enqueueLocked(it.ID)
		}
	}
	s.mu.Unlock()

	s.waitDetailsFor(items)
	// 解析完成后重新取一次快照，确保返回最新结果。
	return s.refreshItems(items)
}

// refreshItems 用最新映射刷新给定条目（保持原顺序）。
func (s *Store) refreshItems(items []Item) []Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Item, 0, len(items))
	for _, it := range items {
		if cur, ok := s.items[it.ID]; ok {
			out = append(out, *cur)
			continue
		}
		out = append(out, it)
	}
	return out
}

// snapshotWindow 返回 [offset, offset+need) 区间的条目与总数；
// complete 表示该窗口已经取满（无需再顺延抓页）。
func (s *Store) snapshotWindow(offset, need int) (items []Item, total int, complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pages := s.orderedPagesLocked()
	items = make([]Item, 0, min(need, 512))
	seen := make(map[string]bool)
	index := 0
	for _, page := range pages {
		for _, id := range s.pages[page].ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			idx := index
			index++
			total++
			if idx < offset || len(items) >= need {
				continue
			}
			if it, ok := s.items[id]; ok {
				items = append(items, *it)
			}
		}
	}
	return items, total, len(items) >= need
}

// orderedPagesLocked 返回有内容的页号（升序）。调用方需持有 s.mu。
func (s *Store) orderedPagesLocked() []int {
	pages := make([]int, 0, len(s.pages))
	for page, ps := range s.pages {
		if len(ps.ids) > 0 {
			pages = append(pages, page)
		}
	}
	sort.Ints(pages)
	return pages
}

// hasNextPage 判断是否还有可能拉到更多内容的上游页。
func (s *Store) hasNextPage() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasNextPageLocked()
}

func (s *Store) hasNextPageLocked() bool {
	if len(s.pages) >= MaxPage {
		return false
	}
	last := 0
	for page := range s.pages {
		if page > last {
			last = page
		}
	}
	// 已知最后一页若是空的，说明上游到底了。
	if ps, ok := s.pages[last]; ok && ps.empty {
		return false
	}
	return true
}

// syncNextPage 同步当前最深页的下一页，返回是否拉到了新内容。
func (s *Store) syncNextPage() bool {
	s.mu.Lock()
	next := 1
	for page := range s.pages {
		if len(s.pages[page].ids) > 0 || !s.pages[page].empty {
			if page >= next {
				next = page + 1
			}
		}
	}
	if ps, ok := s.pages[next]; ok && ps.empty {
		s.mu.Unlock()
		return false
	}
	if s.syncing[next] {
		// 列表线程正在抓这一页，等它抓完即可。
		s.mu.Unlock()
		return true
	}
	s.pages[next] = &pageState{}
	s.mu.Unlock()

	if n := s.SyncPage(next, false); n > 0 {
		return true
	}
	// 没拿到内容：标记为空页，后续不再尝试。
	s.mu.Lock()
	if p, ok := s.pages[next]; ok && len(p.ids) == 0 {
		p.empty = true
	}
	s.mu.Unlock()
	return false
}

// pendingCount 统计给定条目里还没有播放地址的数量。
func (s *Store) pendingCount(items []Item) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, it := range items {
		if it.PlayURL != "" {
			continue
		}
		if cur, ok := s.items[it.ID]; ok && cur.PlayURL == "" {
			n++
		}
	}
	return n
}

// waitDetailsFor 等内容里的条目解析出播放地址（有超时兜底）。
func (s *Store) waitDetailsFor(items []Item) {
	deadline := time.Now().Add(windowWait)
	for {
		if s.pendingCount(items) == 0 || time.Now().After(deadline) {
			return
		}
		time.Sleep(60 * time.Millisecond)
	}
}

// ItemCount 返回映射总数。
func (s *Store) ItemCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// CachedPages 返回有内容的页号。
func (s *Store) CachedPages() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.orderedPagesLocked()
}

// AllItems 按页顺序汇总当前列表中的条目（去重），最多 MaxItems 条。
func (s *Store) AllItems() []Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allItemsLocked()
}

func (s *Store) allItemsLocked() []Item {
	var out []Item
	seen := make(map[string]bool)
	for _, id := range s.listedIDsLocked() {
		if seen[id] {
			continue
		}
		seen[id] = true
		if it, ok := s.items[id]; ok {
			out = append(out, *it)
		}
	}
	return out
}

// listedIDsLocked 返回当前所有列表里的 id（按页序、去重）。调用方需持有 s.mu。
func (s *Store) listedIDsLocked() []string {
	pages := s.orderedPagesLocked()

	out := make([]string, 0, len(s.items))
	seen := make(map[string]bool, len(s.items))
	for _, page := range pages {
		for _, id := range s.pages[page].ids {
			if seen[id] || len(out) >= s.maxItems {
				continue
			}
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// MapEntry 是映射表中的一行。
type MapEntry struct {
	ID        string `json:"id"`
	ThreadURL string `json:"thread_url"`
	PlayURL   string `json:"play_url,omitempty"`
	PlayPage  string `json:"play_page,omitempty"`
	Source    string `json:"source,omitempty"`
	Resolved  bool   `json:"resolved"`
	Page      int    `json:"page"`
	Listed    bool   `json:"listed"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
}

// MapEntries 返回完整的 id → 地址映射，包含已下架但仍在保留期内的条目。
// 列表中的条目在前（按页序），已下架的按 id 倒序排后。
func (s *Store) MapEntries() []MapEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	listed := make(map[string]bool, len(s.items))
	pos := make(map[string]int, len(s.items))
	index := 0
	pages := make([]int, 0, len(s.pages))
	for page := range s.pages {
		pages = append(pages, page)
	}
	sort.Ints(pages)
	for _, page := range pages {
		for _, id := range s.pages[page].ids {
			listed[id] = true
			if _, ok := pos[id]; !ok {
				pos[id] = index
				index++
			}
		}
	}

	items := make([]Item, 0, len(s.items))
	for _, it := range s.items {
		items = append(items, *it)
	}
	sort.SliceStable(items, func(i, j int) bool {
		pi, iok := pos[items[i].ID]
		pj, jok := pos[items[j].ID]
		switch {
		case iok && jok:
			return pi < pj
		case iok != jok:
			return iok
		default:
			return items[i].ID > items[j].ID
		}
	})

	out := make([]MapEntry, 0, len(items))
	for _, it := range items {
		out = append(out, MapEntry{
			ID:        it.ID,
			ThreadURL: it.ThreadURL,
			PlayURL:   it.PlayURL,
			PlayPage:  it.PlayPage,
			Source:    it.Source,
			Resolved:  it.Resolved,
			Page:      it.Page,
			Listed:    listed[it.ID],
			UpdatedAt: it.UpdatedAt,
		})
	}
	return out
}

// SourceCount 是来源域名及其条目数。
type SourceCount struct {
	Source string `json:"source"`
	Count  int    `json:"count"`
}

// Sources 统计当前列表中各来源的条目数，按数量倒序。
func (s *Store) Sources() []SourceCount {
	s.mu.Lock()
	counts := make(map[string]int, 64)
	for _, id := range s.listedIDsLocked() {
		it, ok := s.items[id]
		if !ok {
			continue
		}
		src := NormalizeSource(it.Source)
		if src == "" {
			continue
		}
		counts[src]++
	}
	s.mu.Unlock()

	out := make([]SourceCount, 0, len(counts))
	for src, n := range counts {
		out = append(out, SourceCount{Source: src, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Source < out[j].Source
	})
	return out
}

// Stats 返回概况：映射总数、页数、待解析数、最近一次列表同步时间。
func (s *Store) Stats() (items, pages, pending int, lastList time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ps := range s.pages {
		if ps.lastList.After(lastList) {
			lastList = ps.lastList
		}
	}
	return len(s.items), len(s.pages), len(s.pending), lastList
}

// RefreshStale 把需要更新的条目重新入队，由详情线程处理：
//   - 直链临近过期（换新签名）
//   - 之前解析失败且已到重试时间（例如上游限流返回 403）
func (s *Store) RefreshStale() {
	now := time.Now()
	expired, failed := 0, 0
	s.mu.Lock()
	for id, it := range s.items {
		if s.pending[id] || s.inflight[id] {
			continue
		}
		// 直链过期：只换签名，成本最低，优先处理。
		if it.PlayURL != "" && it.PlayPage != "" &&
			now.Sub(time.Unix(it.UpdatedAt, 0)) > s.videoTTL {
			s.enqueueLocked(id)
			expired++
			continue
		}
		// 解析失败（无地址）且已到重试时间。
		if it.PlayURL == "" && it.Error != "" && it.RetryAt > 0 && it.RetryAt <= now.Unix() {
			s.enqueueLocked(id)
			failed++
		}
	}
	s.mu.Unlock()

	if expired > 0 || failed > 0 {
		log.Printf("[Refresh] 入队 %d 条过期直链、%d 条失败重试", expired, failed)
	}
}
