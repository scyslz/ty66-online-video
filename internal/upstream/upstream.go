// Package upstream 负责与上游站点交互：抓取列表页、帖子页，并解析出可播放的直链。
//
// 本包只做单次请求与解析，不含并发调度、缓存与重试策略——
// 那些由 internal/store 决定；重试时间的计算放在这里，因为它取决于失败原因。
package upstream

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

const userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"

// SiteBase 与 APIBase 是上游地址，测试可替换。
var (
	SiteBase = "https://www.t66y.com/"
	APIBase  = "https://cdn.js-static.cc"
)

// Item 是一条视频记录：既来自列表页，也承载详情解析结果。
// ID 为上游帖子编号，作为唯一主键。
type Item struct {
	ID          string `json:"id,omitempty"`
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

// UnmarshalJSON 兼容旧数据里的 domain 字段（现统一叫 source）。
func (it *Item) UnmarshalJSON(b []byte) error {
	type plain Item
	var aux struct {
		plain
		LegacyDomain string `json:"domain"`
	}
	aux.plain = plain(*it)
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	*it = Item(aux.plain)
	if it.Source == "" && aux.LegacyDomain != "" {
		it.Source = aux.LegacyDomain
	}
	it.Source = NormalizeSource(it.Source)
	return nil
}

var (
	pageClient = &http.Client{Timeout: 20 * time.Second}
	apiClient  = &http.Client{Timeout: 15 * time.Second}

	rowRe      = regexp.MustCompile(`(?s)<tr class="tr3 t_one tac">.*?</tr>`)
	linkRe     = regexp.MustCompile(`(?s)<h3><a href="([^"]+)"[^>]*>(.*?)</a></h3>`)
	tagRe      = regexp.MustCompile(`<[^>]*>`)
	durationRe = regexp.MustCompile(`\[(\d{1,3}:\d{2}(?::\d{2})?)\]\s*$`)
	threadIDRe = regexp.MustCompile(`/(\d+)\.html?(?:$|[?#])`)
	tidRe      = regexp.MustCompile(`[?&]tid=(\d+)`)
	playRe     = regexp.MustCompile(`href="(https://2023\.redircdn\.com/\?[^"]+)"[^>]*>\s*點擊這里打開新視窗`)
	playAnyRe  = regexp.MustCompile(`href="(https://2023\.redircdn\.com/\?[^"]+)"`)
	tailRe     = regexp.MustCompile(`(&popup=\d+)?(&z)?$`)
)

// fetchBytes 发起一次 GET 请求（带 UA），返回响应体。
func fetchBytes(client *http.Client, rawURL, accept string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
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

// fetchHTML 抓取网页并解码为 UTF-8（非法字节丢弃）。
func fetchHTML(rawURL string) (string, error) {
	b, err := fetchBytes(pageClient, rawURL, "")
	if err != nil {
		return "", err
	}
	return strings.ToValidUTF8(string(b), ""), nil
}

// 详情页请求节流：多个 worker 共享，避免瞬时并发被上游限流（403）。
var (
	detailGateMu   sync.Mutex
	detailLastCall time.Time
)

const detailMinGap = 120 * time.Millisecond

func throttleDetail() {
	detailGateMu.Lock()
	if wait := detailMinGap - time.Since(detailLastCall); wait > 0 {
		time.Sleep(wait)
	}
	detailLastCall = time.Now()
	detailGateMu.Unlock()
}

// ThreadID 从帖子地址里取出编号：.../7513454.html 或 read.php?tid=7513557。
func ThreadID(threadURL string) string {
	if m := threadIDRe.FindStringSubmatch(threadURL); m != nil {
		return m[1]
	}
	if m := tidRe.FindStringSubmatch(threadURL); m != nil {
		return m[1]
	}
	return threadURL
}

// NormalizeSource 归一化来源域名：小写并去掉 www. 前缀。
func NormalizeSource(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	return strings.TrimPrefix(s, "www.")
}

// parseList 从列表页解析出带时长的条目（只含标题与详情地址）。
func parseList(pageHTML string) []Item {
	var items []Item
	base, err := url.Parse(SiteBase)
	if err != nil {
		return nil
	}
	for _, row := range rowRe.FindAllString(pageHTML, -1) {
		m := linkRe.FindStringSubmatch(row)
		if m == nil {
			continue
		}
		title := strings.TrimSpace(tagRe.ReplaceAllString(m[2], ""))
		title = html.UnescapeString(title)
		dur := durationRe.FindStringSubmatch(title)
		if dur == nil {
			continue
		}
		ref, err := url.Parse(m[1])
		if err != nil {
			continue
		}
		threadURL := base.ResolveReference(ref).String()
		items = append(items, Item{
			ID:        ThreadID(threadURL),
			Title:     title,
			ThreadURL: threadURL,
			Duration:  dur[1],
		})
	}
	return items
}

// listURL 构造某个上游列表页的地址。
func listURL(page int) string {
	if page <= 1 {
		return SiteBase + "thread0806.php?fid=22"
	}
	return fmt.Sprintf("%sthread0806.php?fid=22&search=&page=%d", SiteBase, page)
}

// FetchList 抓取一页列表，返回尚未补全详情的条目。
func FetchList(page int) ([]Item, error) {
	body, err := fetchHTML(listURL(page))
	if err != nil {
		return nil, err
	}
	return parseList(body), nil
}

// extractPlay 从帖子页找出跳转链接，并还原出真正的播放页地址。
// 还原结果必须是合法的 http(s) 地址，否则视为“还没有播放链接”。
func extractPlay(threadHTML string) (redirect, target string) {
	m := playRe.FindStringSubmatch(threadHTML)
	if m == nil {
		m = playAnyRe.FindStringSubmatch(threadHTML)
	}
	if m == nil {
		return "", ""
	}
	redirect = html.UnescapeString(m[1])
	query := ""
	if i := strings.IndexByte(redirect, '?'); i >= 0 {
		query = redirect[i+1:]
	}
	if unescaped, err := url.PathUnescape(query); err == nil {
		target = unescaped
	} else {
		target = query
	}
	target = strings.ReplaceAll(target, "______", ".")
	target = strings.ReplaceAll(target, "___", ".")
	target = tailRe.ReplaceAllString(target, "")

	if !isHTTPURL(target) {
		return "", ""
	}
	return redirect, target
}

// isHTTPURL 判断字符串是否为带主机名的 http(s) 绝对地址。
func isHTTPURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// resolvable 判断播放页地址是否带播放器接口需要的 id。
func resolvable(playPage string) bool {
	if playPage == "" {
		return false
	}
	u, err := url.Parse(playPage)
	if err != nil {
		return false
	}
	return u.Query().Get("id") != ""
}

// ResolveRealVideo 调用播放器接口，把播放页地址解析成 m3u8/mp4 直链。
func ResolveRealVideo(targetURL string) string {
	if targetURL == "" {
		return ""
	}
	u, err := url.Parse(targetURL)
	if err != nil {
		return ""
	}
	query := u.Query()
	id := query.Get("id")
	if id == "" {
		return ""
	}
	api := APIBase + "/videos/playerurl?id=" + url.QueryEscape(id) +
		"&t=" + url.QueryEscape(query.Get("time")) +
		"&h=" + url.QueryEscape(query.Get("sign"))
	body, err := fetchBytes(apiClient, api, "application/json")
	if err != nil {
		return ""
	}
	var data struct {
		Error   any `json:"error"`
		Content struct {
			VideoURL string `json:"video_url"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return ""
	}
	ok := false
	switch e := data.Error.(type) {
	case float64:
		ok = e == 0
	case string:
		ok = e == "0"
	}
	v := data.Content.VideoURL
	if ok && (strings.Contains(v, ".m3u8") || strings.Contains(v, ".mp4")) {
		return v
	}
	return ""
}

// Enrich 抓详情页，解析出播放页与直链。
// 新帖刚发布时详情页可能还没挂播放链接，这种情况会安排稍后重试。
func Enrich(it Item) Item {
	now := time.Now().Unix()
	it.Attempts++

	throttleDetail()
	threadHTML, err := fetchHTML(it.ThreadURL)
	if err != nil {
		it.Error = err.Error()
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(retryHTTP(it.Attempts)).Unix()
		return it
	}
	redirect, target := extractPlay(threadHTML)
	real := ""
	if target != "" {
		real = ResolveRealVideo(target)
	}

	it.RedirectURL = redirect
	it.PlayPage = target
	it.Resolved = real != ""
	switch {
	case real != "":
		it.PlayURL = real
	case target != "":
		it.PlayURL = target
	case redirect != "":
		it.PlayURL = redirect
	}
	it.Source = ""
	if link := firstNonEmpty(it.PlayPage, it.PlayURL); link != "" {
		if pu, err := url.Parse(link); err == nil {
			it.Source = NormalizeSource(pu.Host)
		}
	}
	it.UpdatedAt = now

	switch {
	case real != "":
		it.Error = ""
		it.RetryAt = 0
	case target != "":
		// 有播放页但拿不到直链：可能是签名失效，稍后用播放页重试。
		it.Error = "播放器接口未返回直链"
		it.RetryAt = time.Now().Add(retryNoVideo).Unix()
	default:
		// 详情页里没有播放跳转：新帖常见，短时间内多试几次后放慢。
		it.Error = "详情页暂无播放链接"
		it.RetryAt = time.Now().Add(retrySchedule(it.Attempts)).Unix()
	}
	return it
}

// Refresh 不重抓详情页，只用已存的播放页地址换一条新直链（1 次请求）。
// 返回 false 表示无法仅凭缓存刷新，需要回退到完整抓取。
func Refresh(it Item) (Item, bool) {
	if !resolvable(it.PlayPage) {
		return it, false
	}
	v := ResolveRealVideo(it.PlayPage)
	if v == "" {
		return it, false
	}
	it.PlayURL = v
	it.Resolved = true
	it.Error = ""
	it.RetryAt = 0
	it.UpdatedAt = time.Now().Unix()
	return it, true
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// 详情解析失败后的重试节奏。
const (
	retryFetchFailed = 10 * time.Minute // 网络/HTTP 出错的初始间隔
	retryNoVideo     = 5 * time.Minute  // 有播放页但没拿到直链
)

// retryHTTP 针对抓取失败（403/429 多为上游限流）做指数退避：
// 10 分钟起步，逐次翻倍，最长 6 小时。
func retryHTTP(attempts int) time.Duration {
	d := retryFetchFailed
	for i := 1; i < attempts && d < 6*time.Hour; i++ {
		d *= 2
	}
	if d > 6*time.Hour {
		d = 6 * time.Hour
	}
	return d
}

// retrySchedule 针对“详情页还没挂播放链接”的新帖：先密集重试几次，再退到小时级。
func retrySchedule(attempts int) time.Duration {
	switch {
	case attempts <= 3:
		return 45 * time.Second
	case attempts <= 8:
		return 5 * time.Minute
	default:
		return 30 * time.Minute
	}
}
