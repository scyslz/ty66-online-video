package store

// 本文件负责持久化：载入映射、原子写入、以及后台合并落盘。
//
// 文件格式版本：
//
//	v1（旧）：pages 的键是纯页号 "1"，items 的键是纯帖子 id "7511316"，条目里没有 site
//	v2（当前）：pages 的键是 "站点/页号"，items 的键是 "站点:帖子id"
//
// 载入 v1 文件时会自动把键补上默认站点前缀并继续使用，不需要重新抓取。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// currentVersion 是落盘格式版本。
const currentVersion = 3

type persisted struct {
	Version int                      `json:"version"`
	SavedAt int64                    `json:"saved_at"`
	Pages   map[string]persistedPage `json:"pages"`
	Items   map[string]Item          `json:"items"`
	// Order 是各站点的展示顺序（唯一真源）。缺失时按 pages 重建。
	Order map[string][]string `json:"order,omitempty"`
}

type persistedPage struct {
	IDs      []string `json:"ids"`
	LastList int64    `json:"last_list"`
	// Site/Page 用于 v2；v1 的键里已含信息，留空即可。
	Site string `json:"site,omitempty"`
	Page int    `json:"page,omitempty"`
}

// Load 从磁盘载入映射。文件不存在时从空库开始。
func (s *Store) Load() error {
	raw, err := os.ReadFile(s.file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("[Init] 无历史数据，从空库开始")
			return nil
		}
		return err
	}

	var p persisted
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("解析 %s: %w", s.file, err)
	}

	legacy := p.Version < 2
	migrated := 0

	s.mu.Lock()
	// 条目：v1 的键没有站点前缀，补上默认站点。
	for rawKey, it := range p.Items {
		v := it
		if v.ID == "" {
			_, v.ID = splitKey(rawKey)
		}
		site, _ := splitKey(rawKey)
		if site == "" {
			site = s.defSite
			migrated++
		}
		v.Site = site
		key := itemKey(site, v.ID)
		s.items[key] = &v
	}
	// 页：v1 的键是纯页号。
	for rawKey, pp := range p.Pages {
		site, page := pp.Site, pp.Page
		if site == "" {
			if legacy {
				site = s.defSite
			} else {
				site, page = splitPageKey(rawKey)
			}
		}
		if page == 0 {
			if n, err := strconv.Atoi(strings.TrimPrefix(rawKey, "/")); err == nil {
				page = n
			} else {
				_, page = splitPageKey(rawKey)
			}
		}
		if page <= 0 {
			continue
		}
		// 丢弃不属于任何已注册站点的页（例如历史版本误建的未知站点页）。
		if s.provider(site) == nil {
			continue
		}
		// v1 的 ids 是裸 id，需要补站点前缀。
		ids := make([]string, 0, len(pp.IDs))
		for _, id := range pp.IDs {
			key := id
			if legacy && !strings.Contains(id, ":") {
				key = itemKey(site, id)
			}
			if _, ok := s.items[key]; ok {
				ids = append(ids, key)
			}
		}
		s.pages[pageKey(site, page)] = &pageState{
			site:     site,
			page:     page,
			ids:      ids,
			lastList: time.Unix(pp.LastList, 0),
		}
	}
	// 展示顺序：优先用文件里的 order；没有则按 pages 重建（兼容 v2 及更早）。
	sites := make(map[string]bool, len(s.sites))
	for _, p := range s.sites {
		if p != nil {
			sites[p.Name()] = true
		}
	}
	for name, keys := range p.Order {
		if !sites[name] {
			continue
		}
		// 只保留仍存在于映射里的键，避免历史残留。
		valid := make([]string, 0, len(keys))
		for _, key := range keys {
			if _, ok := s.items[key]; ok {
				valid = append(valid, key)
			}
		}
		s.orderOfLocked(name).Reset(valid)
	}
	for name := range sites {
		if _, ok := s.orders[name]; !ok {
			s.rebuildOrderLocked(name)
		}
	}

	n, np := len(s.items), len(s.pages)
	s.mu.Unlock()

	if legacy && migrated > 0 {
		log.Printf("[Init] 已把 %d 条旧格式映射迁移到站点 %q", migrated, s.defSite)
	}
	log.Printf("[Init] 已载入 %s：%d 条映射 / %d 页", s.file, n, np)
	return nil
}

// Save 原子写入映射文件：先写临时文件再 rename。
func (s *Store) Save() error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()

	p := persisted{
		Version: currentVersion,
		SavedAt: time.Now().Unix(),
		Pages:   make(map[string]persistedPage),
		Items:   make(map[string]Item),
		Order:   make(map[string][]string),
	}

	s.mu.Lock()
	for key, ps := range s.pages {
		site, page := splitPageKey(key)
		p.Pages[key] = persistedPage{
			IDs:      append([]string(nil), ps.ids...),
			LastList: ps.lastList.Unix(),
			Site:     site,
			Page:     page,
		}
	}
	for key, it := range s.items {
		p.Items[key] = *it
	}
	for name, q := range s.orders {
		if keys := q.Snapshot(); len(keys) > 0 {
			p.Order[name] = keys
		}
	}
	s.mu.Unlock()

	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	tmp := s.file + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.file)
}

// markDirty 标记有变更，由 saveLoop 合并后落盘。
func (s *Store) markDirty() {
	select {
	case s.dirty <- struct{}{}:
	default:
	}
}

// saveLoop 合并短时间内的多次更新，静默 1.5s 后落盘一次。
func (s *Store) saveLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.dirty:
		}
		quiet := false
		for !quiet {
			select {
			case <-ctx.Done():
				return
			case <-s.dirty:
			case <-time.After(1500 * time.Millisecond):
				quiet = true
			}
		}
		if err := s.Save(); err != nil {
			log.Printf("[Save] 写入 %s 失败: %v", s.file, err)
		}
	}
}
