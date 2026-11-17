package store

// 本文件提供映射的只读访问器：映射表导出、来源统计、运行概况、
// 以及定时任务用的过期/失败重排。

import (
	"log"
	"sort"
	"time"

	"paresevideo/internal/provider"
)

// MapEntry 是映射表中的一行。
type MapEntry struct {
	ID        string `json:"id"`
	Site      string `json:"site,omitempty"`
	ThreadURL string `json:"thread_url"`
	PlayURL   string `json:"play_url,omitempty"`
	PlayPage  string `json:"play_page,omitempty"`
	Source    string `json:"source,omitempty"`
	Resolved  bool   `json:"resolved"`
	Page      int    `json:"page"`
	Listed    bool   `json:"listed"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
}

// MapEntries 返回完整的 站点:id → 地址 映射，包含已下架但仍在保留期内的条目。
// 列表中的条目在前（按展示顺序），已下架的按 id 倒序排后。
func (s *Store) MapEntries() []MapEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	listed := make(map[string]bool, len(s.items))
	pos := make(map[string]int, len(s.items))
	index := 0
	for _, key := range s.listedKeysLocked("") {
		listed[key] = true
		if _, ok := pos[key]; !ok {
			pos[key] = index
			index++
		}
	}

	items := make([]Item, 0, len(s.items))
	for _, it := range s.items {
		items = append(items, *it)
	}
	keyOf := func(it Item) string { return itemKey(it.Site, it.ID) }
	sort.SliceStable(items, func(i, j int) bool {
		pi, iok := pos[keyOf(items[i])]
		pj, jok := pos[keyOf(items[j])]
		switch {
		case iok && jok:
			return pi < pj
		case iok != jok:
			return iok
		default:
			return keyOf(items[i]) > keyOf(items[j])
		}
	})

	out := make([]MapEntry, 0, len(items))
	for _, it := range items {
		out = append(out, MapEntry{
			ID:        it.ID,
			Site:      it.Site,
			ThreadURL: it.ThreadURL,
			PlayURL:   it.PlayURL,
			PlayPage:  it.PlayPage,
			Source:    it.Source,
			Resolved:  it.Resolved,
			Page:      it.Page,
			Listed:    listed[keyOf(it)],
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
	for _, key := range s.listedKeysLocked("") {
		it, ok := s.items[key]
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
//   - 只有网页播放页、还没挖出直链
//   - 之前解析失败且已到重试时间（例如上游限流返回 403）
func (s *Store) RefreshStale() {
	now := time.Now()
	expired, failed, embedOnly := 0, 0, 0
	s.mu.Lock()
	for key, it := range s.items {
		if s.pending[key] || s.inflight[key] {
			continue
		}
		// 直链过期：只换签名，成本最低，优先处理。
		if provider.IsMediaURL(it.PlayURL) && it.PlayPage != "" &&
			now.Sub(time.Unix(it.UpdatedAt, 0)) > s.videoTTL {
			s.enqueueLocked(key)
			expired++
			continue
		}
		// 只有网页播放页、还没挖出直链：重解析一次。
		if it.PlayPage != "" && !provider.IsMediaURL(it.PlayURL) && it.Error != "" &&
			it.RetryAt > 0 && it.RetryAt <= now.Unix() {
			s.enqueueLocked(key)
			embedOnly++
			continue
		}
		// 解析失败（无地址）且已到重试时间。
		if it.PlayURL == "" && it.Error != "" && it.RetryAt > 0 && it.RetryAt <= now.Unix() {
			s.enqueueLocked(key)
			failed++
		}
	}
	s.mu.Unlock()

	if expired > 0 || failed > 0 || embedOnly > 0 {
		log.Printf("[Refresh] 入队 %d 条过期直链、%d 条失败重试、%d 条待挖嵌入页直链",
			expired, failed, embedOnly)
	}
}
