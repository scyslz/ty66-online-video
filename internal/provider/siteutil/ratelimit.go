package siteutil

// 本文件实现「每主机 N 次 / 周期」的滑动窗口限流。
//
// 为什么按主机而不是按站点：同一个站点可能同时有列表页、详情页、嵌入播放页
// 三类请求打到同一台服务器，只有按主机计数才能真实反映对上游的压力。
//
// 语义：不阻塞、不排队。配额够就放行，不够就立刻返回失败——
// 调用方据此放弃本次请求（或稍后再试），而不是把 goroutine 挂在那里等。
// 这样既守住了速率上限，也不会在高频轮询下堆积等待中的请求。

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ErrRateLimited 表示该主机当前配额已用尽，请求被拒绝。
var ErrRateLimited = errors.New("已达到该主机的请求速率上限")

// SlidingWindow 是按主机计数的滑动窗口限流器。
type SlidingWindow struct {
	mu     sync.Mutex
	hits   map[string][]time.Time
	limit  int
	window time.Duration
}

// NewSlidingWindow 创建限流器；limit 为每 window 允许的请求数，limit<=0 表示不限流。
func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		hits:   make(map[string][]time.Time),
		limit:  limit,
		window: window,
	}
}

// Default 是全部出站请求共用的限流器（每个主机 60 次/分钟）。
var Default = NewSlidingWindow(60, time.Minute)

// SetLimit 调整限流阈值（limit<=0 表示关闭限流）。
func (w *SlidingWindow) SetLimit(limit int) {
	w.mu.Lock()
	w.limit = limit
	w.mu.Unlock()
}

// Limit 返回当前阈值。
func (w *SlidingWindow) Limit() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.limit
}

// Window 返回窗口长度。
func (w *SlidingWindow) Window() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.window
}

// Allow 尝试占用一个配额。有配额返回 true 并记账；否则返回 false（不阻塞）。
func (w *SlidingWindow) Allow(host string) bool {
	return w.AllowAt(host, time.Now())
}

// AllowAt 与 Allow 相同，但显式传入时间（便于测试）。
func (w *SlidingWindow) AllowAt(host string, now time.Time) bool {
	host = normalizeHost(host)
	if host == "" {
		return true
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.limit <= 0 {
		return true // 未启用限流
	}

	cutoff := now.Add(-w.window)
	hits := w.hits[host]

	// 裁掉窗口外的记录（原地复用底层数组）。
	keep := hits[:0]
	for _, t := range hits {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	hits = keep

	if len(hits) >= w.limit {
		w.hits[host] = hits
		return false
	}

	w.hits[host] = append(hits, now)
	return true
}

// AllowURL 按地址所在主机尝试占用配额。
func (w *SlidingWindow) AllowURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return true // 解析失败交给后续请求去报错
	}
	return w.Allow(u.Host)
}

// RetryAfter 返回该主机还需要等多久才有配额（0 表示现在就可以）。
func (w *SlidingWindow) RetryAfter(host string) time.Duration {
	host = normalizeHost(host)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.limit <= 0 {
		return 0
	}
	now := time.Now()
	cutoff := now.Add(-w.window)
	var live []time.Time
	for _, t := range w.hits[host] {
		if t.After(cutoff) {
			live = append(live, t)
		}
	}
	if len(live) < w.limit {
		return 0
	}
	return live[0].Add(w.window).Sub(now)
}

// normalizeHost 归一化主机名：小写、去端口、去 www. 前缀。
func normalizeHost(hostport string) string {
	h := hostport
	if hh, _, err := net.SplitHostPort(hostport); err == nil {
		h = hh
	}
	h = strings.ToLower(strings.TrimSuffix(h, "."))
	return strings.TrimPrefix(h, "www.")
}

// Usage 返回某主机在窗口内已用的配额（用于观测）。
func (w *SlidingWindow) Usage(host string) int {
	host = normalizeHost(host)
	w.mu.Lock()
	defer w.mu.Unlock()
	cutoff := time.Now().Add(-w.window)
	n := 0
	for _, t := range w.hits[host] {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}

// UsageAll 返回所有主机的窗口内用量快照。
func (w *SlidingWindow) UsageAll() map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	cutoff := time.Now().Add(-w.window)
	out := make(map[string]int, len(w.hits))
	for host, hits := range w.hits {
		for _, t := range hits {
			if t.After(cutoff) {
				out[host]++
			}
		}
	}
	return out
}

// Reset 清空所有计数（测试或手动解限时使用）。
func (w *SlidingWindow) Reset() {
	w.mu.Lock()
	w.hits = make(map[string][]time.Time)
	w.mu.Unlock()
}
