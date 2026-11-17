package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"paresevideo/internal/provider"
	"paresevideo/internal/provider/demo"
)

// waitTotal 反复读取窗口，直到总数达到 want 或超时。
// 分页读取是异步的：第一次调用只投递补页请求，随后由列表线程补齐。
func waitTotal(t *testing.T, st *Store, site string, want int) Window {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var win Window
	for time.Now().Before(deadline) {
		win = st.FillWindowQuery(0, 50, "", site)
		if win.Total >= want && !win.Loading {
			return win
		}
		time.Sleep(20 * time.Millisecond)
	}
	return win
}

// 端到端：两个真实实现（假论坛站 + demo JSON 站）同时接入，
// 验证列表合并、站点过滤、异步补页与播放地址解析。
func TestEndToEndTwoSites(t *testing.T) {
	// 第二个站点：JSON 接口型，共 2 页
	var base string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/videos":
			// 只有 2 页内容，第 3 页起返回空列表（模拟翻到底）。
			page := r.URL.Query().Get("page")
			var items []map[string]string
			switch page {
			case "1":
				items = []map[string]string{{"id": "b1", "title": "站点B视频", "url": base + "/api/videos/b1", "duration": "03:00"}}
			case "2":
				items = []map[string]string{{"id": "b2", "title": "站点B视频二", "url": base + "/api/videos/b2", "duration": "04:00"}}
			}
			json.NewEncoder(w).Encode(map[string]any{"items": items})
		case "/api/videos/b1", "/api/videos/b2":
			id := filepath.Base(r.URL.Path)
			json.NewEncoder(w).Encode(map[string]any{
				"video_url": "https://cdn-b.example.com/" + id + "/list.m3u8?k=1",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	base = srv.URL

	siteA := newFake("alpha", "100", "101")
	siteB := demo.New(demo.Config{Name: "demo", Base: base, ListPath: "/api/videos?page=%d"})

	dir := t.TempDir()
	st := New(Options{
		File:      filepath.Join(dir, "store.json"),
		ListTTL:   time.Minute,
		VideoTTL:  time.Hour,
		Keep:      time.Hour,
		Workers:   2,
		MaxItems:  100,
		Providers: []provider.Provider{siteA, siteB},
	})
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	t.Cleanup(cancel)

	if got := st.Providers(); len(got) != 2 || got[0] != "alpha" || got[1] != "demo" {
		t.Fatalf("站点注册顺序错误: %v", got)
	}

	st.SyncPageSite("alpha", 1, true)

	// demo 站点：读窗口会异步补页，等它补完 2 页。
	win := waitTotal(t, st, "demo", 2)
	if win.Total != 2 {
		t.Fatalf("demo 站点应有 2 条，得到 %d", win.Total)
	}
	for _, it := range win.Items {
		if it.Site != "demo" {
			t.Errorf("站点过滤错误: %+v", it)
		}
	}

	// 全站视图：两个站点的条目都在。
	all := waitTotal(t, st, "", 4)
	if all.Total != 4 {
		t.Fatalf("两站共 4 条，得到 %d", all.Total)
	}
	sites := map[string]int{}
	for _, it := range all.Items {
		sites[it.Site]++
	}
	if sites["alpha"] != 2 || sites["demo"] != 2 {
		t.Errorf("各站点条数错误: %v", sites)
	}

	// 直链解析（异步详情流水线）应在短时间内补齐。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, it := range st.FillWindowQuery(0, 50, "", "").Items {
			if !provider.IsMediaURL(it.PlayURL) {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, it := range st.FillWindowQuery(0, 50, "", "").Items {
		if !provider.IsMediaURL(it.PlayURL) {
			t.Errorf("%s:%s 未解析出直链: %q", it.Site, it.ID, it.PlayURL)
		}
	}

	// 落盘后再读回，键应带站点前缀且两站都能恢复。
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	st2 := New(Options{
		File: filepath.Join(dir, "store.json"), ListTTL: time.Minute, VideoTTL: time.Hour,
		Keep: time.Hour, Workers: 2, MaxItems: 100,
		Providers: []provider.Provider{siteA, siteB},
	})
	if err := st2.Load(); err != nil {
		t.Fatal(err)
	}
	if got := st2.ItemCount(); got != 4 {
		t.Errorf("重新载入应有 4 条，得到 %d", got)
	}
}

// 站点过滤：指定不存在的站点必须返回空，而不是退化成全部。
func TestUnknownSiteFilter(t *testing.T) {
	st := newTestStore(t, newFake("alpha", "1", "2"))
	st.SyncPageSite("alpha", 1, true)

	if win := st.FillWindowQuery(0, 10, "", "alpha"); win.Total != 2 {
		t.Errorf("指定默认站点应返回全部 2 条，得到 %d", win.Total)
	}
	if win := st.FillWindowQuery(0, 10, "", "nope"); win.Total != 0 {
		t.Errorf("未知站点应返回 0 条，得到 %d（退化成全部了）", win.Total)
	}
	if win := st.FillWindowQuery(0, 10, "", ""); win.Total != 2 {
		t.Errorf("不过滤应返回 2 条，得到 %d", win.Total)
	}
}
