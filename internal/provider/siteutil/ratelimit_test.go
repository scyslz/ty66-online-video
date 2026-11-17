package siteutil

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 窗口内放行次数不应超过配额，超出部分立即被拒绝（不阻塞）。
func TestSlidingWindowLimit(t *testing.T) {
	w := NewSlidingWindow(5, 200*time.Millisecond)

	start := time.Now()
	for i := 0; i < 5; i++ {
		if !w.Allow("example.com") {
			t.Fatalf("第 %d 次应在配额内", i+1)
		}
	}
	// 前 5 次应几乎立即通过。
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Errorf("配额内应快速通过，实际耗时 %s", el)
	}

	// 第 6 次必须立即被拒绝，而不是阻塞。
	start = time.Now()
	if w.Allow("example.com") {
		t.Error("超出配额应被拒绝")
	}
	if el := time.Since(start); el > 50*time.Millisecond {
		t.Errorf("拒绝应立即返回，实际耗时 %s", el)
	}
}

// 不同主机互不影响。
func TestSlidingWindowPerHost(t *testing.T) {
	w := NewSlidingWindow(2, time.Minute)
	w.Allow("a.example.com")
	w.Allow("a.example.com")

	if w.Allow("a.example.com") {
		t.Error("a 用满后应拒绝")
	}
	if !w.Allow("b.example.com") {
		t.Error("b 不受 a 影响，应放行")
	}
	if got := w.Usage("a.example.com"); got != 2 {
		t.Errorf("a 用量应为 2，实际 %d", got)
	}
	if got := w.Usage("b.example.com"); got != 1 {
		t.Errorf("b 用量应为 1，实际 %d", got)
	}
}

// 主机归一化：端口、大小写、www. 前缀都应视为同一主机。
func TestSlidingWindowHostNormalize(t *testing.T) {
	w := NewSlidingWindow(2, time.Minute)
	w.Allow("Example.COM:443")
	w.Allow("www.example.com")
	if w.Allow("example.com") {
		t.Error("归一化后应视为同一主机，第三次应被拒绝")
	}
	if got := w.Usage("example.com"); got != 2 {
		t.Errorf("归一化后用量应为 2，实际 %d", got)
	}
}

// 并发调用下，放行总数严格不超过配额，且不会阻塞。
func TestSlidingWindowConcurrentNoBlock(t *testing.T) {
	const limit = 10
	w := NewSlidingWindow(limit, time.Minute)

	var passed int64
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w.Allow("burst.example.com") {
				atomic.AddInt64(&passed, 1)
			}
		}()
	}
	wg.Wait()

	if el := time.Since(start); el > 200*time.Millisecond {
		t.Errorf("非阻塞语义下应立即返回，实际 %s", el)
	}
	if got := atomic.LoadInt64(&passed); got != limit {
		t.Errorf("100 次并发应恰好放行 %d 次，实际 %d", limit, got)
	}
}

// limit<=0 表示不限流。
func TestSlidingWindowDisabled(t *testing.T) {
	w := NewSlidingWindow(0, time.Minute)
	for i := 0; i < 500; i++ {
		if !w.Allow("example.com") {
			t.Fatalf("关闭限流后第 %d 次仍应放行", i)
		}
	}
}

// SetLimit 动态生效。
func TestSlidingWindowSetLimit(t *testing.T) {
	w := NewSlidingWindow(100, time.Minute)
	w.SetLimit(3)
	if got := w.Limit(); got != 3 {
		t.Fatalf("限额应为 3，实际 %d", got)
	}
	for i := 0; i < 3; i++ {
		if !w.Allow("example.com") {
			t.Fatalf("第 %d 次应在限额内", i+1)
		}
	}
	if w.Allow("example.com") {
		t.Error("超出新限额应被拒绝")
	}
}

// 窗口滑过后应重新获得配额。
func TestSlidingWindowRecovers(t *testing.T) {
	w := NewSlidingWindow(2, 100*time.Millisecond)
	w.Allow("example.com")
	w.Allow("example.com")
	if w.Allow("example.com") {
		t.Fatal("窗口内应拒绝")
	}
	time.Sleep(120 * time.Millisecond)
	if !w.Allow("example.com") {
		t.Error("窗口滑过后应放行")
	}
}

// RetryAfter 应给出准确的等待时间。
func TestSlidingWindowRetryAfter(t *testing.T) {
	w := NewSlidingWindow(1, 100*time.Millisecond)
	if got := w.RetryAfter("example.com"); got != 0 {
		t.Errorf("未用配额时应为 0，实际 %s", got)
	}
	w.Allow("example.com")
	got := w.RetryAfter("example.com")
	if got <= 0 || got > 100*time.Millisecond {
		t.Errorf("应返回 (0,100ms] 之间的等待时间，实际 %s", got)
	}
}

// Gate：间隔未到时不阻塞，直接拒绝。
func TestGateNonBlocking(t *testing.T) {
	g := NewGate(100 * time.Millisecond)
	if !g.Allow() {
		t.Fatal("首次应放行")
	}
	start := time.Now()
	if g.Allow() {
		t.Error("间隔未到应拒绝")
	}
	if el := time.Since(start); el > 20*time.Millisecond {
		t.Errorf("拒绝应立即返回，实际 %s", el)
	}
	if got := g.RetryAfter(); got <= 0 {
		t.Errorf("应返回正数等待时间，实际 %s", got)
	}
	time.Sleep(110 * time.Millisecond)
	if !g.Allow() {
		t.Error("间隔满足后应放行")
	}
}

// 并发下 Gate 只放行一次。
func TestGateConcurrent(t *testing.T) {
	g := NewGate(time.Minute)
	var passed int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.Allow() {
				atomic.AddInt64(&passed, 1)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(&passed); got != 1 {
		t.Errorf("50 次并发应只放行 1 次，实际 %d", got)
	}
}
