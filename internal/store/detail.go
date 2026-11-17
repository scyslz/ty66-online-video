package store

// 本文件是详情流水线：worker 从队列取 id，解析播放地址并增量写回映射。

import (
	"context"
	"sync"
	"time"

	"paresevideo/internal/upstream"
)

// detailLoop 启动一组 worker 消费队列：取 id → 解析 → 写回映射。
func (s *Store) detailLoop(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case id := <-s.queue:
					s.resolveOne(id)
				}
			}
		}()
	}
	wg.Wait()
}

// resolveOne 解析单条详情并增量写回。
// 已解析完成的条目直接跳过；正在解析的条目不会重复请求。
func (s *Store) resolveOne(id string) {
	s.mu.Lock()
	it, ok := s.items[id]
	if !ok {
		delete(s.pending, id)
		s.mu.Unlock()
		return
	}
	if s.inflight[id] {
		// 已有 worker 在处理这条，直接出队即可，避免重复请求。
		delete(s.pending, id)
		s.mu.Unlock()
		return
	}
	cur := *it
	// 已解析出结果且签名未过期的，不再请求。
	if cur.PlayURL != "" && cur.RetryAt == 0 &&
		(cur.UpdatedAt == 0 || time.Since(time.Unix(cur.UpdatedAt, 0)) <= s.videoTTL) {
		delete(s.pending, id)
		s.mu.Unlock()
		return
	}
	s.inflight[id] = true
	s.mu.Unlock()

	stale := cur.UpdatedAt > 0 && time.Since(time.Unix(cur.UpdatedAt, 0)) > s.videoTTL

	var out Item
	if stale && cur.PlayPage != "" {
		// 直链快过期：优先只打播放器接口换新签名，省掉一次详情页请求。
		if v, ok := upstream.Refresh(cur); ok {
			out = v
		} else {
			out = upstream.Enrich(cur)
		}
	} else {
		out = upstream.Enrich(cur)
	}
	s.finish(id, out)
}

// finish 把解析结果写回映射并出队。
func (s *Store) finish(id string, out Item) {
	s.mu.Lock()
	if cur, ok := s.items[id]; ok {
		copyPlayback(cur, &out)
	}
	delete(s.pending, id)
	delete(s.inflight, id)
	s.mu.Unlock()
	s.markDirty()
}

// copyPlayback 只覆盖播放相关字段，避免把并发更新过的列表信息写回旧值。
func copyPlayback(dst, src *Item) {
	dst.RedirectURL = src.RedirectURL
	dst.PlayPage = src.PlayPage
	dst.PlayURL = src.PlayURL
	dst.Source = src.Source
	dst.Resolved = src.Resolved
	dst.Error = src.Error
	dst.UpdatedAt = src.UpdatedAt
	dst.Attempts = src.Attempts
	dst.RetryAt = src.RetryAt
}
