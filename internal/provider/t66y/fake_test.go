package t66y

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"paresevideo/internal/provider"
)

// Fake 模拟上游站点：列表页、帖子页与播放器接口。
type Fake struct {
	mu      sync.Mutex
	pages   map[int][]string  // page -> detail ids（顺序即列表顺序）
	threads map[string]string // id -> 播放页 URL
	embeds  map[string]string // embed id -> 嵌入页直链（测试 /embed/<id>）
	lists   int               // 列表页请求次数
	details int               // 详情页请求次数
	api     int               // 播放器接口请求次数
	broken  map[string]bool   // 返回 403 的详情页

	base         string // 假站点根地址，Start 后可用
	embedsServed int    // 嵌入页请求次数
}

func NewFake() *Fake {
	return &Fake{
		pages:   make(map[int][]string),
		threads: make(map[string]string),
		embeds:  make(map[string]string),
		broken:  make(map[string]bool),
	}
}

func (f *Fake) listHTML(page int) string {
	var b strings.Builder
	b.WriteString(`<html><body><table>`)
	for _, id := range f.pages[page] {
		fmt.Fprintf(&b, `<tr class="tr3 t_one tac"><td><h3><a href="/htm_data/2611/22/%s.html">视频 %s [12:34]</a></h3></td></tr>`, id, id)
	}
	b.WriteString(`</table></body></html>`)
	return b.String()
}

func (f *Fake) threadHTML(id string) string {
	target := strings.ReplaceAll(f.threads[id], ".", "___")
	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}
	return fmt.Sprintf(`<html><body><a href="https://2023.redircdn.com/?%s%saff=caoliu&popup=1&z">點擊這里打開新視窗</a></body></html>`, target, sep)
}

func (f *Fake) Handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case strings.HasPrefix(r.URL.Path, "/thread0806.php"):
		f.lists++
		page := 1
		fmt.Sscanf(r.URL.Query().Get("page"), "%d", &page)
		if page == 0 {
			page = 1
		}
		fmt.Fprint(w, f.listHTML(page))
	case strings.HasPrefix(r.URL.Path, "/htm_data/"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/htm_data/2611/22/"), ".html")
		if f.broken[id] {
			f.details++
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		f.details++
		fmt.Fprint(w, f.threadHTML(id))
	case strings.HasPrefix(r.URL.Path, "/embed/"):
		f.embedsServed++
		id := strings.TrimPrefix(r.URL.Path, "/embed/")
		video, ok := f.embeds[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(w, `<html><head><script src="/player/kt_player.js"></script></head><body>
<div id="kt_player"></div>
<script type="text/javascript">
	var flashvars = { embed_mode: '1', video_id: '%s', video_url_hd: '1',
		video_url: '%s', postfix: '.mp4', video_url_text: '普通清晰度',
		video_alt_url: 'https://ads.example.com/' };
	kt_player('kt_player', '/player/kt_player.swf', '100%%', '100%%', flashvars);
</script></body></html>`, id, video)
	case r.URL.Path == "/videos/playerurl":
		f.api++
		id := r.URL.Query().Get("id")
		fmt.Fprintf(w, `{"error":0,"content":{"video_url":"https://videos.example.com/%s/list.m3u8?k=%d"}}`, id, f.api)
	default:
		http.NotFound(w, r)
	}
}

// SetPage 设置某一页的条目（并按需准备对应的播放页）。
func (f *Fake) SetPage(page int, ids ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pages[page] = ids
	for _, id := range ids {
		if _, ok := f.threads[id]; !ok {
			f.threads[id] = fmt.Sprintf("https://play.example.com/video.html?id=%s&time=1&sign=abc", id)
		}
	}
}

// SetThread 指定某个帖子的播放页地址（空串表示暂无播放链接）。
func (f *Fake) SetThread(id, playPage string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if playPage == "" {
		delete(f.threads, id)
		return
	}
	f.threads[id] = playPage
}

// SetEmbed 为某个帖子准备一个 /embed/<id> 网页播放页，并登记其直链。
func (f *Fake) SetEmbed(id, videoURL string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.embeds[id] = videoURL
	base := f.base
	if base == "" {
		// Start 之前调用：先登记，Start 时再补地址。
		f.threads[id] = "PLACEHOLDER/embed/" + id
		return
	}
	f.threads[id] = base + "/embed/" + id
}

// Break 让某个详情页返回 403。
func (f *Fake) Break(id string, broken bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.broken[id] = broken
}

// Counts 返回列表/详情/接口的请求次数。
func (f *Fake) Counts() (lists, details, api int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists, f.details, f.api
}

// EmbedCount 返回嵌入页请求次数。
func (f *Fake) EmbedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.embedsServed
}

// Start 启动假上游，返回指向它的 Site 实例。
func (f *Fake) Start(t *testing.T) *Site {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.Handler(w, r)
	}))
	t.Cleanup(srv.Close)

	cfg := DefaultConfig()
	cfg.SiteBase = srv.URL + "/"
	cfg.APIBase = srv.URL
	s := New(cfg)
	f.mu.Lock()
	f.base = srv.URL
	// 把 Start 前登记过的占位符换成真实地址。
	for id := range f.embeds {
		if strings.Contains(f.threads[id], "PLACEHOLDER") {
			f.threads[id] = srv.URL + "/embed/" + id
		}
	}
	f.mu.Unlock()
	return s
}

// 确保 Fake 构造出的 Site 满足接口约定。
var _ provider.Provider = (*Site)(nil)
