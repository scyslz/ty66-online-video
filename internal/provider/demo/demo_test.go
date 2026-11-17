package demo

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"paresevideo/internal/provider"
)

// TestFetchAndEnrich 用一个假的 JSON 站点走通列表 → 详情 → 直链。
func TestFetchAndEnrich(t *testing.T) {
	var base string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/videos":
			json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]string{
					{"id": "a1", "title": "片段一", "url": base + "/api/videos/a1", "duration": "01:23"},
					{"id": "a2", "title": "片段二", "url": base + "/api/videos/a2", "duration": "02:00"},
				},
			})
		case "/api/videos/a1":
			json.NewEncoder(w).Encode(map[string]any{
				"video_url": "https://cdn.example.com/a1/list.m3u8?k=1",
			})
		case "/api/videos/a2":
			// 只给播放页，不给直链
			json.NewEncoder(w).Encode(map[string]any{
				"page_url": "https://play.example.com/embed/a2",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	base = srv.URL

	s := New(Config{Name: "demo", Base: base, ListPath: "/api/videos?page=%d"})
	items, err := s.FetchList(1)
	if err != nil {
		t.Fatalf("抓列表: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("应有 2 条，得到 %d", len(items))
	}
	if items[0].Site != "demo" || items[0].ID != "a1" {
		t.Errorf("条目字段错误: %+v", items[0])
	}

	got := s.Enrich(items[0])
	if !got.Resolved || !provider.IsMediaURL(got.PlayURL) {
		t.Errorf("应解析出直链: %+v", got)
	}
	if got.Source != "cdn.example.com" {
		t.Errorf("来源应为 CDN 域名，得到 %q", got.Source)
	}

	// 只有播放页的条目：保留地址并安排重试
	only := s.Enrich(items[1])
	if only.Resolved {
		t.Errorf("没有直链时不应标记已解析: %+v", only)
	}
	if only.RetryAt == 0 || only.Error == "" {
		t.Errorf("应记录失败原因与重试时间: %+v", only)
	}
}

// 编译期确认实现了接口。
var _ provider.Provider = (*Site)(nil)
