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

// SiteSync 是一个站点的列表同步状态（供前端展示「上次同步时间」）。
type SiteSync struct {
	Site        string `json:"site"`
	LastSync    int64  `json:"last_sync"`    // 最近一次成功同步的 Unix 时间（0 表示还没成功过）
	LastAttempt int64  `json:"last_attempt"` // 最近一次尝试的时间（含失败）
	Pages       int    `json:"pages"`        // 已缓存的有内容的页数
	Items       int    `json:"items"`        // 各页条目数之和（有重复，仅供参考）
	Empty       bool   `json:"empty"`        // 已翻到站点末尾
	Error       string `json:"error,omitempty"`
	Breaker     string `json:"breaker,omitempty"` // closed / open / half-open
}

// SiteSyncs 汇总各站点的列表同步状态，用于「上游有没有更新」的观察。
func (s *Store) SiteSyncs() []SiteSync {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := make([]SiteSync, 0, len(s.sites))
	for _, p := range s.sites {
		if p == nil {
			continue
		}
		name := p.Name()
		row := SiteSync{Site: name}
		for _, ps := range s.pages {
			site := ps.site
			if site == "" {
				site = s.defSite
			}
			if site != name {
				continue
			}
			if ps.lastList.After(time.Unix(row.LastSync, 0)) {
				row.LastSync = ps.lastList.Unix()
			}
			if ps.lastAttempt > row.LastAttempt {
				row.LastAttempt = ps.lastAttempt / int64(time.Second)
			}
			if len(ps.ids) > 0 {
				row.Pages++
				row.Items += len(ps.ids)
			}
			if ps.empty {
				row.Empty = true
			}
			row.Error = ps.lastErr
		}
		st := s.stateOfLocked(name)
		switch {
		case st.cooling(now):
			row.Breaker = "open"
		case st.failStreak >= siteFailsCooldown:
			row.Breaker = "half-open"
		default:
			row.Breaker = "closed"
		}
		out = append(out, row)
	}
	return out
}

// RefreshStale 把「还没拿到直链、且已到重试时间」的条目重新入队。
//
// 注意这里**不按时间刷新已有直链**：直链的签名通常能用很久，而且是否有
// 效只有播放时才真正知道。已解析出直链的条目一律不动，等前端播放失败时
// 调用 Repair 按需重解析（懒加载）。
//
// 顺带修一批历史脏数据：旧版本把「节流跳过」当成一次失败写回，导致明明
// 有直链的条目上也挂着 error。这些条目能播，直接清掉错误标记即可。
func (s *Store) RefreshStale() {
	now := time.Now().Unix()
	retry, never, healed := 0, 0, 0
	s.mu.Lock()
	for key, it := range s.items {
		if provider.IsMediaURL(it.PlayURL) {
			if it.Error != "" || it.RetryAt != 0 {
				it.Error = ""
				it.RetryAt = 0
				healed++
			}
			continue
		}
		// 懒加载模式：不主动补解析，等播放时按需（Repair）再请求上游。
		if s.lazy {
			continue
		}
		if s.pending[key] || s.inflight[key] {
			continue
		}
		// 还没到重试时间：让一让，避免节流时反复空转。
		if it.RetryAt > now {
			continue
		}
		s.enqueueLocked(key)
		if it.Error != "" {
			retry++
		} else {
			never++
		}
	}
	s.mu.Unlock()

	if retry > 0 || never > 0 || healed > 0 {
		log.Printf("[Refresh] 入队 %d 条待重试、%d 条尚未解析出直链，清理 %d 条无效错误标记",
			retry, never, healed)
	}
	if healed > 0 {
		s.markDirty()
	}
}
