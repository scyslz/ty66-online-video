// Package demo 是一个用于验证扩展性的示例站点：JSON 列表 + JSON 详情。
//
// 它演示了接入一个「列表加 detail」型新站需要写多少代码：
// 只需要实现 ListURL/ParseList 与 Enrich 两个方法，其余（分页、去重、
// 缓存、重试、续播、代理）全部由 store 复用。
//
// 真实站点的接入方式与此完全一致，只是把解析换成该站的规则。
package demo

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"paresevideo/internal/provider"
	"paresevideo/internal/provider/siteutil"
)

// Config 是示例站点的地址配置。
type Config struct {
	Name     string
	Base     string // 站点根，如 https://demo.example
	ListPath string // %d 为页码
}

// DefaultConfig 返回示例站点配置。
func DefaultConfig() Config {
	return Config{
		Name:     "demo",
		Base:     "https://demo.example",
		ListPath: "/api/videos?page=%d",
	}
}

// Site 演示一个 JSON 接口型站点。
type Site struct {
	cfg  Config
	gate *siteutil.Gate
}

// New 创建示例站点。
func New(cfg Config) *Site {
	if cfg.Name == "" {
		cfg = DefaultConfig()
	}
	return &Site{cfg: cfg, gate: siteutil.NewGate(100 * time.Millisecond)}
}

// Name 实现 provider.Provider。
func (s *Site) Name() string { return s.cfg.Name }

// MaxPages 实现 provider.Provider。
func (s *Site) MaxPages() int { return 200 }

// listURL 拼接列表接口地址。
func (s *Site) listURL(page int) string {
	return s.cfg.Base + fmt.Sprintf(s.cfg.ListPath, page)
}

// listResp 是列表接口的响应结构。
type listResp struct {
	Items []struct {
		ID       string `json:"id"`
		Title    string `json:"title"`
		URL      string `json:"url"`
		Duration string `json:"duration"`
	} `json:"items"`
}

// ParseList 把列表 JSON 转成条目。
func (s *Site) ParseList(body []byte) ([]provider.Item, error) {
	var r listResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	out := make([]provider.Item, 0, len(r.Items))
	for _, it := range r.Items {
		if it.ID == "" || it.Title == "" {
			continue
		}
		out = append(out, provider.Item{
			ID:        it.ID,
			Site:      s.cfg.Name,
			Title:     it.Title,
			ThreadURL: it.URL,
			Duration:  it.Duration,
		})
	}
	return out, nil
}

// FetchList 实现 provider.Provider。
func (s *Site) FetchList(page int) ([]provider.Item, error) {
	body, err := siteutil.FetchBytes(siteutil.PageClient, s.listURL(page), "application/json")
	if err != nil {
		return nil, err
	}
	return s.ParseList(body)
}

// detailResp 是详情接口的响应结构。
type detailResp struct {
	// VideoURL 直接给媒体地址时最简单；需要再跳一层时给出 PageURL。
	VideoURL string `json:"video_url"`
	PageURL  string `json:"page_url"`
	Title    string `json:"title"`
}

// Enrich 实现 provider.Provider：抓详情拿到媒体地址。
func (s *Site) Enrich(it provider.Item) provider.Item {
	now := time.Now().Unix()
	it.Attempts++

	if !s.gate.Allow() {
		it.Error = "请求过于频繁，稍后重试"
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(300 * time.Millisecond).Unix()
		it.Attempts--
		return it
	}
	body, err := siteutil.FetchBytes(siteutil.PageClient, it.ThreadURL, "application/json")
	if err != nil {
		it.Error = err.Error()
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(provider.RetryHTTP(it.Attempts)).Unix()
		return it
	}
	var d detailResp
	if err := json.Unmarshal(body, &d); err != nil {
		it.Error = err.Error()
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(provider.RetryHTTP(it.Attempts)).Unix()
		return it
	}

	it.Site = s.cfg.Name
	it.UpdatedAt = now
	switch {
	case provider.IsMediaURL(d.VideoURL):
		it.PlayURL = d.VideoURL
		it.PlayPage = provider.FirstNonEmpty(d.PageURL, d.VideoURL)
		it.Resolved = true
		it.Error = ""
		it.RetryAt = 0
	case d.PageURL != "":
		it.PlayPage = d.PageURL
		it.PlayURL = d.PageURL
		it.Error = "详情页只给了播放页"
		it.RetryAt = time.Now().Add(provider.RetryNoVideo).Unix()
	default:
		it.Error = "详情页暂无播放链接"
		it.RetryAt = time.Now().Add(provider.RetrySchedule(it.Attempts)).Unix()
	}
	if link := provider.FirstNonEmpty(it.PlayPage, it.PlayURL); link != "" {
		if u, err := url.Parse(link); err == nil {
			it.Source = provider.NormalizeSource(u.Host)
		}
	}
	return it
}

// Refresh 实现 provider.Provider：示例站点只能重抓详情。
func (s *Site) Refresh(it provider.Item) (provider.Item, bool) {
	return it, false
}
