package store

// 本文件是详情流水线：worker 从队列取 itemKey，解析播放地址并增量写回映射。

import (
	"context"
	"sync"
	"time"

	"paresevideo/internal/provider"
)

// detailLoop 启动一组 worker 消费队列：取 key → 解析 → 写回映射。
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
				case key := <-s.queue:
					s.resolveOne(key)
				}
			}
		}()
	}
	wg.Wait()
}

// resolveOne 解析单条详情并增量写回。
// 已解析完成的条目直接跳过；正在解析的条目不会重复请求。
func (s *Store) resolveOne(key string) {
	s.mu.Lock()
	it, ok := s.items[key]
	if !ok {
		delete(s.pending, key)
		s.mu.Unlock()
		return
	}
	if s.inflight[key] {
		// 已有 worker 在处理这条，直接出队即可，避免重复请求。
		delete(s.pending, key)
		s.mu.Unlock()
		return
	}
	site, _ := splitKey(key)
	if site == "" {
		site = s.defSite
	}
	cur := *it
	// 已解析出可播放直链且签名未过期的，不再请求。
	// 只有网页播放页（/embed/...）的条目要再解析一次，才能挖出里面的直链。
	if provider.IsMediaURL(cur.PlayURL) && cur.RetryAt == 0 &&
		(cur.UpdatedAt == 0 || time.Since(time.Unix(cur.UpdatedAt, 0)) <= s.videoTTL) {
		delete(s.pending, key)
		s.mu.Unlock()
		return
	}
	s.inflight[key] = true
	s.mu.Unlock()

	p := s.provider(site)
	if p == nil {
		s.mu.Lock()
		delete(s.pending, key)
		delete(s.inflight, key)
		s.mu.Unlock()
		return
	}

	stale := cur.UpdatedAt > 0 && time.Since(time.Unix(cur.UpdatedAt, 0)) > s.videoTTL

	var out Item
	// 两种情况可以只打一次播放页就换到直链，省掉详情页请求：
	//   - 直链快过期，换新签名
	//   - 目前只有嵌入页地址（/embed/...），还没挖出里面的直链
	if cur.PlayPage != "" && (stale || !provider.IsMediaURL(cur.PlayURL)) {
		if v, ok := p.Refresh(cur); ok {
			out = v
		} else {
			out = p.Enrich(cur)
		}
	} else {
		out = p.Enrich(cur)
	}
	out.Site = site
	s.finish(key, out)
}

// finish 把解析结果写回映射并出队。
func (s *Store) finish(key string, out Item) {
	s.mu.Lock()
	if cur, ok := s.items[key]; ok {
		copyPlayback(cur, &out)
	}
	delete(s.pending, key)
	delete(s.inflight, key)
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
