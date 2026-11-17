package store

// 本文件是详情流水线：worker 从队列取 itemKey，解析播放地址并增量写回映射。
//
// 直链采用「懒加载」策略：
//   - 已经解析出直链的条目，后台不会因为时间久了就重新请求；
//   - 只有「还没拿到直链」的条目才会进队列；
//   - 播放时旧直链失效，由前端调用 Repair 按需重解析（见 repair.go）。

import (
	"context"
	"sync"

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
//
// 只要已经有直链就直接跳过：直链是否还有效，交给播放时的按需修复判断，
// 后台不再按 TTL 批量续签（那样会白白打上游，还会把失败当成功写回）。
// 正在解析的条目不会重复请求。
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
	if provider.IsMediaURL(it.PlayURL) {
		delete(s.pending, key)
		s.mu.Unlock()
		return
	}
	site, _ := splitKey(key)
	if site == "" {
		site = s.defSite
	}
	s.inflight[key] = true
	p := s.provider(site)
	s.mu.Unlock()

	if p == nil {
		s.mu.Lock()
		delete(s.pending, key)
		delete(s.inflight, key)
		s.mu.Unlock()
		return
	}

	// 条目级串行：按需修复（Repair）与后台解析不会同时对同一条发请求。
	lk := s.keyLock(key)
	lk.Lock()
	defer lk.Unlock()

	// 拿到锁后重新取一次：期间可能已经被按需修复完成。
	s.mu.Lock()
	cur, ok := s.items[key]
	if !ok || provider.IsMediaURL(cur.PlayURL) {
		delete(s.pending, key)
		delete(s.inflight, key)
		s.mu.Unlock()
		return
	}
	in := *cur
	s.mu.Unlock()

	var out Item
	// 只有网页播放页（/embed/...）时，只打一次播放页就能换到直链，省掉详情页请求。
	if in.PlayPage != "" && !provider.IsMediaURL(in.PlayURL) {
		if v, ok := p.Refresh(in); ok {
			out = v
		} else {
			out = p.Enrich(in)
		}
	} else {
		out = p.Enrich(in)
	}
	out.Site = site
	s.finish(key, out)
}

// needsResolve 判断条目是否仍缺少直链（已解析出的直链不再后台重抓）。
func (s *Store) needsResolve(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.items[key]
	if !ok {
		return false
	}
	return !provider.IsMediaURL(it.PlayURL)
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
