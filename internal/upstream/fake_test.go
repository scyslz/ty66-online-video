package upstream

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Fake 模拟上游站点：列表页、帖子页与播放器接口。
type Fake struct {
	mu      sync.Mutex
	pages   map[int][]string  // page -> detail ids（顺序即列表顺序）
	threads map[string]string // id -> 播放页 URL
	lists   int               // 列表页请求次数
	details int               // 详情页请求次数
	api     int               // 播放器接口请求次数
	broken  map[string]bool   // 返回 403 的详情页
}

func NewFake() *Fake {
	return &Fake{
		pages:   make(map[int][]string),
		threads: make(map[string]string),
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
	return fmt.Sprintf(`<html><body><a href="https://2023.redircdn.com/?%s&aff=caoliu&popup=1&z">點擊這里打開新視窗</a></body></html>`, target)
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

// Start 启动假上游，并把 upstream 的地址指向它。
func (f *Fake) Start(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(f.Handler))
	t.Cleanup(srv.Close)

	oldSite, oldAPI := SiteBase, APIBase
	SiteBase, APIBase = srv.URL+"/", srv.URL
	t.Cleanup(func() { SiteBase, APIBase = oldSite, oldAPI })
}
