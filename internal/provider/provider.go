// Package provider 定义「一个视频来源站」的抽象，以及站点无关的公共类型与工具。
//
// 新增一个站点的做法：在本包下新建子包（例如 provider/newsite），
// 实现 Provider 接口，再在 main.go 里注册。store 只认接口，
// 分页、去重、缓存、续播、代理等能力全部自动复用。
//
// 接口刻意保持很小：站点只需要回答两件事——
//
//	列表页第 N 页有哪些条目
//	某条目的播放地址怎么解析
//
// 其余状态（解析进度、重试时间、签名过期）都由 store 托管。
package provider

import (
	"regexp"
	"strings"
	"time"
)

// Item 是一条视频记录：既来自列表页，也承载详情解析结果。
//
// ID 是站点内部的条目编号（如帖子号），Site 是站点标识（Provider.Name）。
// 两者合起来才是全局唯一键，见 store 的 itemKey。
type Item struct {
	ID          string `json:"id,omitempty"`
	Site        string `json:"site,omitempty"`
	Title       string `json:"title"`
	ThreadURL   string `json:"thread_url"`
	Duration    string `json:"duration"`
	RedirectURL string `json:"redirect_url,omitempty"`
	PlayPage    string `json:"play_page,omitempty"`
	PlayURL     string `json:"play_url,omitempty"`
	Source      string `json:"source,omitempty"`
	Error       string `json:"error,omitempty"`
	Resolved    bool   `json:"resolved"`
	Page        int    `json:"page,omitempty"`
	UpdatedAt   int64  `json:"updated_at,omitempty"`
	Attempts    int    `json:"attempts,omitempty"`
	RetryAt     int64  `json:"retry_at,omitempty"`
}

// Provider 是一个视频来源站。实现必须是并发安全的（store 会多 goroutine 调用）。
type Provider interface {
	// Name 是站点的稳定标识，用作映射键前缀，不要随意改。
	Name() string
	// MaxPages 是本站最多翻多少页（防止无限翻页）。
	MaxPages() int
	// FetchList 抓取第 page 页列表（页码从 1 开始），返回尚未补全详情的条目。
	FetchList(page int) ([]Item, error)
	// Enrich 抓详情，解析出播放页与直链，并写回 Error/RetryAt/UpdatedAt。
	Enrich(it Item) Item
	// Refresh 不重抓详情页，只用已存的播放页地址换一条新直链。
	// 返回 false 表示无法仅凭缓存刷新，调用方应回退到 Enrich。
	Refresh(it Item) (Item, bool)
}

// NormalizeSource 归一化来源域名：小写并去掉 www. 前缀。
func NormalizeSource(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	return strings.TrimPrefix(s, "www.")
}

// IsMediaURL 判断地址是否可以直接当媒体播放（m3u8/mp4）。
//
// 注意 /get_file/.../169074.mp4/?embed=true 这种形式：".mp4" 后面跟的是斜杠，
// 所以不能只判断结尾。
func IsMediaURL(raw string) bool {
	low := strings.ToLower(raw)
	if !strings.Contains(low, ".m3u8") && !strings.Contains(low, ".mp4") {
		return false
	}
	return mediaURLRe.MatchString(low)
}

var mediaURLRe = regexp.MustCompile(`\.(m3u8|mp4)(\?|#|/|$)`)

// 各类失败的重试节奏。站点实现可直接复用，也可自定义。
const (
	RetryFetchFailed = 10 * time.Minute // 网络/HTTP 出错的初始间隔
	RetryNoVideo     = 5 * time.Minute  // 有播放页但没拿到直链
)

// RetryThrottled 是「本次被自身节流跳过」后的重试延迟：
// 只需略大于闸门间隔，不应像真实失败那样长退避。
const RetryThrottled = 400 * time.Millisecond

// RetryHTTP 针对抓取失败（403/429 多为上游限流）做指数退避：
// 10 分钟起步，逐次翻倍，最长 6 小时。
func RetryHTTP(attempts int) time.Duration {
	d := RetryFetchFailed
	for i := 1; i < attempts && d < 6*time.Hour; i++ {
		d *= 2
	}
	if d > 6*time.Hour {
		d = 6 * time.Hour
	}
	return d
}

// RetrySchedule 针对“详情页还没挂播放链接”的新帖：先密集重试几次，再退到小时级。
func RetrySchedule(attempts int) time.Duration {
	switch {
	case attempts <= 3:
		return 45 * time.Second
	case attempts <= 8:
		return 5 * time.Minute
	default:
		return 30 * time.Minute
	}
}

// FirstNonEmpty 返回第一个非空字符串。
func FirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
