// Package t66y 是 t66y 系论坛（帖子列表 + 详情页跳转播放器）的实现。
//
// 这类站点的共同结构：
//
//	列表页：表格行 <tr class="tr3 t_one tac">，行内 <h3><a href="...">标题 [mm:ss]</a></h3>
//	详情页：一个跳到 2023.redircdn.com 的链接，域名里的 ______ 是 . 的混淆
//	播放页：两种形态
//	    1. 播放器接口页 ……/video.html?id=…&time=…&sign=… → 调 API 换 m3u8
//	    2. 第三方嵌入页 ……/embed/<id> → 抓 HTML，取 flashvars.video_url
package t66y

import (
	"encoding/json"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"

	"paresevideo/internal/provider"
	"paresevideo/internal/provider/siteutil"
)

// Config 描述一个同类站点。字段为零值时用 DefaultConfig 的值。
type Config struct {
	Name string // 站点标识，用作映射键前缀
	// SiteBase 是论坛根地址（带结尾斜杠），例如 https://www.t66y.com/
	SiteBase string
	// ListPath 是列表页模板，%d 会被替换成页码；第 1 页用 FirstListPath。
	FirstListPath string
	ListPath      string
	// APIBase 是播放器接口域名（换直链用）。
	APIBase string
	// EmbedPlay 为 true 时，详情页里找不到播放器接口就直接抓 /embed/ 页挖直链。
	EmbedPlay bool
}

// DefaultConfig 是当前线上使用的 t66y 配置。
func DefaultConfig() Config {
	return Config{
		Name:          "t66y",
		SiteBase:      "https://www.t66y.com/",
		FirstListPath: "thread0806.php?fid=22",
		ListPath:      "thread0806.php?fid=22&search=&page=%d",
		APIBase:       "https://cdn.js-static.cc",
		EmbedPlay:     true,
	}
}

// Site 是 provider.Provider 的实现。
type Site struct {
	cfg Config

	detailGate *siteutil.Gate
	embedGate  *siteutil.Gate
}

// 构造时补齐默认值，并给每个站点独立的节流闸（不同站互不影响）。
func New(cfg Config) *Site {
	def := DefaultConfig()
	if cfg.Name == "" {
		cfg.Name = def.Name
	}
	if cfg.SiteBase == "" {
		cfg.SiteBase = def.SiteBase
	}
	if cfg.FirstListPath == "" {
		cfg.FirstListPath = def.FirstListPath
	}
	if cfg.ListPath == "" {
		cfg.ListPath = def.ListPath
	}
	if cfg.APIBase == "" {
		cfg.APIBase = def.APIBase
	}
	if !strings.HasSuffix(cfg.SiteBase, "/") {
		cfg.SiteBase += "/"
	}
	return &Site{
		cfg:        cfg,
		detailGate: siteutil.NewGate(120 * time.Millisecond),
		embedGate:  siteutil.NewGate(250 * time.Millisecond),
	}
}

// Name 实现 provider.Provider。
func (s *Site) Name() string { return s.cfg.Name }

// MaxPages 实现 provider.Provider。
func (s *Site) MaxPages() int { return 2500 }

var (
	rowRe        = regexp.MustCompile(`(?s)<tr class="tr3 t_one tac">.*?</tr>`)
	linkRe       = regexp.MustCompile(`(?s)<h3><a href="([^"]+)"[^>]*>(.*?)</a></h3>`)
	tagRe        = regexp.MustCompile(`<[^>]*>`)
	durationRe   = regexp.MustCompile(`\[(\d{1,3}:\d{2}(?::\d{2})?)\]\s*$`)
	threadIDRe   = regexp.MustCompile(`/(\d+)\.html?(?:$|[?#])`)
	tidRe        = regexp.MustCompile(`[?&]tid=(\d+)`)
	playRe       = regexp.MustCompile(`href="(https://2023\.redircdn\.com/\?[^"]+)"[^>]*>\s*點擊這里打開新視窗`)
	playAnyRe    = regexp.MustCompile(`href="(https://2023\.redircdn\.com/\?[^"]+)"`)
	tailRe       = regexp.MustCompile(`(&popup=\d+)?(&z)?$`)
	embedVideoRe = regexp.MustCompile(`\bvideo_url\s*:\s*['"]([^'"]+)['"]`)
)

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

// parseList 从列表页解析出带时长的条目（只含标题与详情地址）。
func (s *Site) parseList(pageHTML string) []provider.Item {
	var items []provider.Item
	base, err := url.Parse(s.cfg.SiteBase)
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
		items = append(items, provider.Item{
			ID:        ThreadID(threadURL),
			Site:      s.cfg.Name,
			Title:     title,
			ThreadURL: threadURL,
			Duration:  dur[1],
		})
	}
	return items
}

// listURL 构造某个列表页的地址。
func (s *Site) listURL(page int) string {
	if page <= 1 {
		return s.cfg.SiteBase + s.cfg.FirstListPath
	}
	return s.cfg.SiteBase + fmt.Sprintf(s.cfg.ListPath, page)
}

// FetchList 实现 provider.Provider。
func (s *Site) FetchList(page int) ([]provider.Item, error) {
	body, err := siteutil.FetchHTML(s.listURL(page))
	if err != nil {
		return nil, err
	}
	return s.parseList(body), nil
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

	if !siteutil.IsHTTPURL(target) {
		return "", ""
	}
	return redirect, target
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

// isEmbedPage 判断播放页是否为嵌入型网页播放器（而不是播放器接口页）。
func isEmbedPage(playPage string) bool {
	if playPage == "" {
		return false
	}
	u, err := url.Parse(playPage)
	if err != nil {
		return false
	}
	return strings.Contains(u.Path, "/embed/")
}

// ResolvePlayerAPI 调用播放器接口，把播放页地址解析成 m3u8/mp4 直链。
func (s *Site) ResolvePlayerAPI(targetURL string) string {
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
	api := s.cfg.APIBase + "/videos/playerurl?id=" + url.QueryEscape(id) +
		"&t=" + url.QueryEscape(query.Get("time")) +
		"&h=" + url.QueryEscape(query.Get("sign"))
	body, err := siteutil.FetchBytes(siteutil.APIClient, api, "application/json")
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
	if ok && provider.IsMediaURL(v) {
		return v
	}
	return ""
}

// ResolveEmbed 从第三方嵌入播放页（kt_player 风格的 /embed/<id>）里取出直链。
//
// 这类页面本身是 HTML，视频地址写在 flashvars 的 video_url 里，
// 拿到的是一个 /get_file/... 地址，会 302 到 CDN 并支持 Range。
func (s *Site) ResolveEmbed(playPage string) string {
	if !isEmbedPage(playPage) {
		return ""
	}
	if !s.embedGate.Allow() {
		return "" // 节流中，本次放弃；由上层安排重试
	}
	body, err := siteutil.FetchHTML(playPage)
	if err != nil {
		return ""
	}
	return extractEmbedVideo(body, playPage)
}

// extractEmbedVideo 从嵌入页 HTML 中提取并校验直链。
func extractEmbedVideo(pageHTML, playPage string) string {
	m := embedVideoRe.FindStringSubmatch(pageHTML)
	if m == nil {
		return ""
	}
	raw := strings.TrimSpace(html.UnescapeString(m[1]))
	if raw == "" {
		return ""
	}
	// 少数站点给相对路径（如 /get_file/...），按播放页补齐。
	if strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "./") || strings.HasPrefix(raw, "../") {
		if base, err := url.Parse(playPage); err == nil {
			if ref, err := url.Parse(raw); err == nil {
				raw = base.ResolveReference(ref).String()
			}
		}
	}
	if !siteutil.IsHTTPURL(raw) {
		return ""
	}
	if !provider.IsMediaURL(raw) {
		return ""
	}
	return raw
}

// Enrich 实现 provider.Provider：抓详情页，解析出播放页与直链。
// 新帖刚发布时详情页可能还没挂播放链接，这种情况会安排稍后重试。
func (s *Site) Enrich(it provider.Item) provider.Item {
	now := time.Now().Unix()
	it.Attempts++

	// 节流闸未放行：不阻塞，直接安排稍后重试。
	if !s.detailGate.Allow() {
		// 站内节流：只是「暂时让一让」，几秒后即可重试，
		// 不要按普通失败那样退避（否则一条视频要等几十秒）。
		it.Error = ErrThrottled
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(provider.RetryThrottled).Unix()
		it.Attempts-- // 节流不算一次失败尝试
		return it
	}
	threadHTML, err := siteutil.FetchHTML(it.ThreadURL)
	if err != nil {
		it.Error = err.Error()
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(provider.RetryHTTP(it.Attempts)).Unix()
		return it
	}
	redirect, target := extractPlay(threadHTML)
	real := ""
	if target != "" {
		real = s.ResolvePlayerAPI(target)
		if real == "" && s.cfg.EmbedPlay {
			// 不是播放器接口页，可能是 /embed/<id> 这类网页播放器：从 HTML 里挖直链。
			real = s.ResolveEmbed(target)
		}
	}

	it.Site = s.cfg.Name
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
	if link := provider.FirstNonEmpty(it.PlayPage, it.PlayURL); link != "" {
		if pu, err := url.Parse(link); err == nil {
			it.Source = provider.NormalizeSource(pu.Host)
		}
	}
	it.UpdatedAt = now

	switch {
	case real != "":
		it.Error = ""
		it.RetryAt = 0
	case target != "":
		// 有播放页但拿不到直链：可能是签名失效，稍后用播放页重试。
		it.Error = ErrNoDirect
		it.RetryAt = time.Now().Add(provider.RetryNoVideo).Unix()
	default:
		// 详情页里没有播放跳转：新帖常见，短时间内多试几次后放慢。
		it.Error = "详情页暂无播放链接"
		it.RetryAt = time.Now().Add(provider.RetrySchedule(it.Attempts)).Unix()
	}
	return it
}

// ErrThrottled 表示本次因站内节流被跳过，应稍后重试。
const ErrThrottled = "请求过于频繁，稍后重试"

// ErrNoDirect 表示拿到了播放页，但没能换出直链。
// store 用它与其它失败原因区分，决定是否值得重试补解析。
const ErrNoDirect = "播放器接口未返回直链"

// Refresh 实现 provider.Provider：只打一次播放页就换到新直链，省掉详情页请求。
func (s *Site) Refresh(it provider.Item) (provider.Item, bool) {
	v := ""
	switch {
	case isEmbedPage(it.PlayPage):
		// 嵌入页：重抓一次 HTML 即可换到新的带签名直链。
		v = s.ResolveEmbed(it.PlayPage)
	case resolvable(it.PlayPage):
		v = s.ResolvePlayerAPI(it.PlayPage)
	default:
		return it, false
	}
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
