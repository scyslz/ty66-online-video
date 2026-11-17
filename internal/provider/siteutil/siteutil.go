// Package siteutil 提供各站点实现共用的 HTTP 抓取与节流工具。
package siteutil

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// UserAgent 是抓取上游时统一使用的 UA。
const UserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"

var (
	PageClient = &http.Client{Timeout: 20 * time.Second}
	APIClient  = &http.Client{Timeout: 15 * time.Second}
)

// FetchBytes 发起一次 GET 请求（带 UA），返回响应体。
//
// 所有出站请求都经过这里，因此这里统一做「每主机 N 次/周期」的滑动窗口限流。
// 配额不足时立即返回 ErrRateLimited，不阻塞、不排队——
// 调用方应据此放弃本次抓取（等下一轮或由用户重试），
// 避免高频轮询把 goroutine 堆在等待上、同时持续推高上游请求量。
func FetchBytes(client *http.Client, rawURL, accept string) ([]byte, error) {
	if !Default.AllowURL(rawURL) {
		return nil, ErrRateLimited
	}

	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("%s: %s", rawURL, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 16<<20))
}

// FetchHTML 抓取网页并解码为 UTF-8（非法字节丢弃）。
func FetchHTML(rawURL string) (string, error) {
	b, err := FetchBytes(PageClient, rawURL, "")
	if err != nil {
		return "", err
	}
	return strings.ToValidUTF8(string(b), ""), nil
}

// Gate 是「两次请求之间至少间隔 gap」的闸门。
// 多个 goroutine 共享同一个 Gate 即可实现该站点/该类型的请求间隔。
//
// 语义是不阻塞：间隔未到时直接拒绝，由调用方放弃本次请求。
type Gate struct {
	mu   sync.Mutex
	last time.Time
	gap  time.Duration
}

// NewGate 创建一个最小间隔为 gap 的闸门。
func NewGate(gap time.Duration) *Gate {
	return &Gate{gap: gap}
}

// Allow 尝试占用一次请求机会。间隔已满足返回 true 并记账，否则返回 false。
func (g *Gate) Allow() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if now.Sub(g.last) < g.gap {
		return false
	}
	g.last = now
	return true
}

// RetryAfter 返回距离下次可请求还需等待多久（0 表示现在就可以）。
func (g *Gate) RetryAfter() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if wait := g.gap - time.Since(g.last); wait > 0 {
		return wait
	}
	return 0
}

// IsHTTPURL 判断字符串是否为带主机名的 http(s) 绝对地址。
func IsHTTPURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
