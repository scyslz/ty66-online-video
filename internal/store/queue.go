package store

// 本文件实现「补页队列」：把所有需要抓取的列表页排进一个去重的有序队列，
// 由固定数量的 worker 取出执行。
//
// 去重约定（关键）：
//
//	syncReq[key] 表示「该页已排队或正在抓取」，从入队那一刻置位，
//	直到抓取真正结束才清除。这样无论入口被调用多少次、并发多高，
//	同一个 站点/页号 在同一时刻最多只有一个请求在途，也不会重复排队。
//
// 之所以要「等到结束才清除」：早期实现在出队时就清除标记，出队与真正
// 开始抓取之间有一个短暂空窗，期间新的请求会把同一页再次入队，造成重复抓取。

import (
	"context"
	"log"
	"time"
)

// enqueuePage 把一页加入补页队列（去重）。返回是否真的入队。
// 调用方需持有 s.mu。
func (s *Store) enqueuePageLocked(key string) bool {
	if s.syncReq[key] {
		return false // 已排队或正在抓取
	}
	s.syncReq[key] = true
	s.syncQueue = append(s.syncQueue, key)
	return true
}

// donePage 标记某页处理完毕，释放去重标记。调用方需持有 s.mu。
func (s *Store) donePageLocked(key string) {
	delete(s.syncReq, key)
	delete(s.syncing, key)
}

// requestPageAsync 在后台请求抓取某一页（去重、非阻塞）。
// 返回是否新入队。
func (s *Store) requestPageAsync(key string) bool {
	s.mu.Lock()
	if !s.enqueuePageLocked(key) {
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()
	s.broadcastWake(1)
	return true
}

// broadcastWake 投放 n 个唤醒信号，让最多 n 个 worker 同时开始干活。
//
// 只发一个信号的话，任务会被同一个 worker 逐个处理（因为其它 worker 还在睡），
// 多站点就退化成了串行抓取。
func (s *Store) broadcastWake(n int) {
	for i := 0; i < n; i++ {
		select {
		case s.wake <- struct{}{}:
		default:
			return // 缓冲满说明已有很多 worker 待命
		}
	}
}

// takeQueuedPage 取出一个待抓页键。调用方需持有 s.mu。
func (s *Store) takeQueuedPageLocked() (string, bool) {
	for len(s.syncQueue) > 0 {
		key := s.syncQueue[0]
		s.syncQueue = s.syncQueue[1:]
		// 期间可能被取消（例如站点已判定到底），此时跳过。
		if s.syncReq[key] {
			return key, true
		}
	}
	return "", false
}

// listWorkers 启动固定数量的抓取 worker，消费补页队列。
//
// 用固定 worker 而不是「一个请求一个 goroutine」：并发上限可控，
// 不会因为前端反复轮询而堆出成百上千个 goroutine。
func (s *Store) listWorkers(ctx context.Context) {
	n := s.listWorkersN
	if n < 1 {
		// 至少覆盖所有站点，保证多站点能真正并行；
		// 上限避免站点很多时开出过多 goroutine。
		n = len(s.sites)
		if n < 2 {
			n = 2
		}
		if n > 8 {
			n = 8
		}
	}
	for i := 0; i < n; i++ {
		go s.listWorker(ctx)
	}
}

// listWorker 是单个抓取 worker。
func (s *Store) listWorker(ctx context.Context) {
	for {
		key, ok := s.claimQueuedPage()
		if !ok {
			// 队列空：等唤醒信号或退出。
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
				continue
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		s.runSyncKey(key)
	}
}

// claimQueuedPage 取出一页。
//
// 注意：这里不再设置 syncing —— 那是 SyncPageSite 内部的职责。
// 去重标记 syncReq 保持置位直到 runSyncKey 结束，这就是「不重复请求」的保证。
func (s *Store) claimQueuedPage() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.takeQueuedPageLocked()
}

// runSyncKey 抓一页，结束后才释放去重标记。
func (s *Store) runSyncKey(key string) {
	site, page := splitPageKey(key)
	defer func() {
		// 无论成功失败，都在这里释放标记——保证不会永久占位，
		// 也不会在抓取过程中被重复入队。
		s.mu.Lock()
		s.donePageLocked(key)
		s.mu.Unlock()
	}()

	if p := s.provider(site); p == nil {
		return
	}
	if n := s.SyncPageSite(site, page, false); n > 0 {
		return
	}
	// 抓不到内容（空页或失败）：由 SyncPageSite 内部维护状态，
	// 这里只需唤醒可能已经空转的其它 worker。
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// queuedCount 返回仍在排队（尚未被 worker 取走）的页数。
func (s *Store) queuedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, key := range s.syncQueue {
		if s.syncReq[key] && !s.syncing[key] {
			n++
		}
	}
	return n
}

// pendingCountTotal 返回「排队 + 在抓」的独立任务数（同一页只算一次）。
func (s *Store) pendingPageCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]bool, len(s.syncReq))
	for key := range s.syncReq {
		seen[key] = true
	}
	return len(seen)
}

// inflightCount 返回正在抓取的页数。
func (s *Store) inflightCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.syncing)
}

// logQueue 输出队列概况（仅在真的发生了变化时打印，避免刷屏）。
func (s *Store) logQueue(reason string) {
	q, f := s.queuedCount(), s.inflightCount()
	if q > 0 || f > 0 {
		log.Printf("[Queue] %s：排队 %d 页，在抓 %d 页", reason, q, f)
	}
}
