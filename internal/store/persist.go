package store

// 本文件负责持久化：载入映射、原子写入、以及后台合并落盘。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type persisted struct {
	Version int                      `json:"version"`
	SavedAt int64                    `json:"saved_at"`
	Pages   map[string]persistedPage `json:"pages"`
	Items   map[string]Item          `json:"items"`
}

type persistedPage struct {
	IDs      []string `json:"ids"`
	LastList int64    `json:"last_list"`
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

	s.mu.Lock()
	for id, it := range p.Items {
		v := it
		if v.ID == "" {
			v.ID = id
		}
		s.items[id] = &v
	}
	for key, pp := range p.Pages {
		page, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		ids := make([]string, 0, len(pp.IDs))
		for _, id := range pp.IDs {
			if _, ok := s.items[id]; ok {
				ids = append(ids, id)
			}
		}
		s.pages[page] = &pageState{ids: ids, lastList: time.Unix(pp.LastList, 0)}
	}
	n, np := len(s.items), len(s.pages)
	s.mu.Unlock()

	log.Printf("[Init] 已载入 %s：%d 条映射 / %d 页", s.file, n, np)
	return nil
}

// Save 原子写入映射文件：先写临时文件再 rename。
func (s *Store) Save() error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()

	p := persisted{
		Version: 1,
		SavedAt: time.Now().Unix(),
		Pages:   make(map[string]persistedPage),
		Items:   make(map[string]Item),
	}

	s.mu.Lock()
	for page, ps := range s.pages {
		p.Pages[strconv.Itoa(page)] = persistedPage{
			IDs:      append([]string(nil), ps.ids...),
			LastList: ps.lastList.Unix(),
		}
	}
	for id, it := range s.items {
		p.Items[id] = *it
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
