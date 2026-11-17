package t66y

import (
	"strings"
	"testing"
	"time"

	"paresevideo/internal/provider"
)

func TestThreadID(t *testing.T) {
	cases := map[string]string{
		"https://www.t66y.com/htm_data/2611/22/7513454.html": "7513454",
		"https://www.t66y.com/read.php?tid=7513557":          "7513557",
		"https://www.t66y.com/read.php?tid=7513557&page=2":   "7513557",
	}
	for in, want := range cases {
		if got := ThreadID(in); got != want {
			t.Errorf("ThreadID(%q) = %q, want %q", in, got, want)
		}
	}
}

// 列表页应解析出 id、标题与时长，并跳过无时长的行。
func TestParseList(t *testing.T) {
	html := `<table>
<tr class="tr3 t_one tac"><td><h3><a href="/htm_data/2611/22/101.html">普通视频 [12:34]</a></h3></td></tr>
<tr class="tr3 t_one tac"><td><h3><a href="/htm_data/2611/22/102.html">没有时长</a></h3></td></tr>
<tr class="tr3 t_one tac"><td><h3><a href="/htm_data/2611/22/103.html">带转义 &amp; 符号 [01:02:03]</a></h3></td></tr>
</table>`

	items := New(DefaultConfig()).parseList(html)
	if len(items) != 2 {
		t.Fatalf("应解析出 2 条，得到 %d: %+v", len(items), items)
	}
	if items[0].ID != "101" || items[0].Duration != "12:34" {
		t.Errorf("第一条解析错误: %+v", items[0])
	}
	if !strings.Contains(items[1].Title, "&") {
		t.Errorf("HTML 实体应被还原: %q", items[1].Title)
	}
	if items[1].Duration != "01:02:03" {
		t.Errorf("应支持时分秒时长: %q", items[1].Duration)
	}
	if !strings.HasPrefix(items[0].ThreadURL, "https://www.t66y.com/") {
		t.Errorf("应拼接为绝对地址: %q", items[0].ThreadURL)
	}
}

// 帖子页里的跳转链接应还原成播放页地址，非法地址应丢弃。
func TestExtractPlay(t *testing.T) {
	html := `<a href="https://2023.redircdn.com/?https://yuese99______com/video______html?id=83193&time=1&sign=abc&aff=caoliu&popup=1&z">點擊這里打開新視窗</a>`

	redirect, target := extractPlay(html)
	if !strings.Contains(redirect, "redircdn") {
		t.Fatalf("应提取到跳转链接: %q", redirect)
	}
	if target != "https://yuese99.com/video.html?id=83193&time=1&sign=abc&aff=caoliu" {
		t.Fatalf("播放页还原错误: %q", target)
	}

	// 没有跳转链接
	if r, tg := extractPlay("<html><body>空</body></html>"); r != "" || tg != "" {
		t.Errorf("无链接时应返回空: %q %q", r, tg)
	}

	// 还原失败（不是 http 地址）时应整体丢弃
	bad := `<a href="https://2023.redircdn.com/?not-a-url&aff=caoliu">點擊這里打開新視窗</a>`
	if r, tg := extractPlay(bad); r != "" || tg != "" {
		t.Errorf("无效地址应丢弃: %q %q", r, tg)
	}
}

// 详情页正常时应解析出直链；没有播放链接时应安排重试。
func TestEnrich(t *testing.T) {
	f := NewFake()
	f.SetPage(1, "201")
	site := f.Start(t)

	items, err := site.FetchList(1)
	if err != nil {
		t.Fatalf("抓列表: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("应有 1 条，得到 %d", len(items))
	}

	out := site.Enrich(items[0])
	if !out.Resolved {
		t.Fatalf("应解析出直链: %+v", out)
	}
	if !strings.Contains(out.PlayURL, "videos.example.com") {
		t.Errorf("直链错误: %q", out.PlayURL)
	}
	if out.Source != "play.example.com" {
		t.Errorf("来源应为播放页域名，得到 %q", out.Source)
	}
	if out.Error != "" || out.RetryAt != 0 {
		t.Errorf("成功时不应有错误与重试: %+v", out)
	}

	// 帖子页没有播放链接
	f.SetThread("201", "")
	noLink := site.Enrich(items[0])
	if noLink.PlayURL != "" {
		t.Errorf("无播放链接时不应有地址: %q", noLink.PlayURL)
	}
	if noLink.RetryAt == 0 {
		t.Error("应安排重试时间")
	}
	if noLink.Error == "" {
		t.Error("应记录失败原因")
	}
}

// 详情页 403 时应按指数退避安排重试。
func TestEnrichHTTPFailureBackoff(t *testing.T) {
	f := NewFake()
	f.SetPage(1, "301")
	f.Break("301", true)
	site := f.Start(t)

	items, err := site.FetchList(1)
	if err != nil {
		t.Fatal(err)
	}

	first := site.Enrich(items[0])
	if first.Error == "" {
		t.Fatal("403 应记录错误")
	}
	firstWait := first.RetryAt - first.UpdatedAt
	if firstWait < int64(9*60) || firstWait > int64(11*60) {
		t.Errorf("首次失败应约 10 分钟后重试，实际 %d 秒", firstWait)
	}

	// 多次失败后间隔应增长（等闸门放行，否则会被节流挡回）
	time.Sleep(150 * time.Millisecond)
	second := site.Enrich(first)
	secondWait := second.RetryAt - second.UpdatedAt
	if secondWait <= firstWait {
		t.Errorf("连续失败应退避增长：%d -> %d", firstWait, secondWait)
	}
}

// 只换签名时不应重抓详情页。
func TestRefreshUsesPlayPage(t *testing.T) {
	f := NewFake()
	f.SetPage(1, "401")
	site := f.Start(t)

	items, _ := site.FetchList(1)
	enriched := site.Enrich(items[0])
	_, detailsBefore, apiBefore := f.Counts()

	refreshed, ok := site.Refresh(enriched)
	if !ok {
		t.Fatal("应能用播放页换新签名")
	}

	_, detailsAfter, apiAfter := f.Counts()
	if detailsAfter != detailsBefore {
		t.Errorf("不应重抓详情页: %d -> %d", detailsBefore, detailsAfter)
	}
	if apiAfter-apiBefore != 1 {
		t.Errorf("应只请求 1 次播放器接口，实际 %d", apiAfter-apiBefore)
	}
	if refreshed.PlayURL == enriched.PlayURL {
		t.Error("直链应被换新")
	}

	// 没有播放页时无法只换签名
	if _, ok := site.Refresh(provider.Item{ID: "x"}); ok {
		t.Error("没有播放页时应返回 false")
	}
}

// 嵌入型播放页（/embed/<id>）应从 flashvars 的 video_url 里取出直链。
func TestEnrichEmbedPage(t *testing.T) {
	f := NewFake()
	f.SetPage(1, "401")
	site := f.Start(t)
	f.SetEmbed("401", "https://fccw55.com/get_file/1/abc/109000/109853/109853.mp4/?embed=true")

	items, err := site.FetchList(1)
	if err != nil {
		t.Fatalf("抓列表: %v", err)
	}
	out := site.Enrich(items[0])
	if !out.Resolved {
		t.Fatalf("嵌入页应解析出直链: %+v", out)
	}
	if !strings.Contains(out.PlayURL, "109853.mp4") {
		t.Errorf("直链错误: %q", out.PlayURL)
	}
	if out.Error != "" {
		t.Errorf("成功时不应有错误: %q", out.Error)
	}
}

// 嵌入页解析：只认 video_url，不能误取 video_url_hd / video_alt_url。
func TestExtractEmbedVideo(t *testing.T) {
	page := `<script>var flashvars = {
		video_url_hd: '1',
		video_url: 'https://cdn.example.com/get_file/1/x/1/2/2.mp4/?embed=true',
		video_alt_url: 'https://ads.example.com/',
		postfix: '.mp4' };</script>`
	got := extractEmbedVideo(page, "https://www.99jj23.com/embed/2")
	if got != "https://cdn.example.com/get_file/1/x/1/2/2.mp4/?embed=true" {
		t.Fatalf("提取错误: %q", got)
	}

	// 相对路径应按播放页补齐
	rel := `<script>video_url: '/get_file/1/x/1/2/2.mp4?embed=true'</script>`
	if got := extractEmbedVideo(rel, "https://fccw55.com/embed/109853"); got != "https://fccw55.com/get_file/1/x/1/2/2.mp4?embed=true" {
		t.Errorf("相对路径补齐错误: %q", got)
	}

	// 广告地址、缺字段都应为空
	if got := extractEmbedVideo(`<script>video_alt_url: 'https://ads.example.com/a.mp4'</script>`, "https://x.com/embed/1"); got != "" {
		t.Errorf("不应取广告地址: %q", got)
	}
	if got := extractEmbedVideo(`<html>nothing</html>`, "https://x.com/embed/1"); got != "" {
		t.Errorf("无字段时应为空: %q", got)
	}
}

// isEmbedPage 只认 /embed/ 路径。
func TestIsEmbedPage(t *testing.T) {
	cases := map[string]bool{
		"https://www.99jj23.com/embed/169074":     true,
		"https://fccw55.com/embed/109853?x=1":     true,
		"https://yuese99.com/video.html?id=83193": false,
		"https://www.99jj23.com/embedding/169074": false,
		"": false,
	}
	for in, want := range cases {
		if got := isEmbedPage(in); got != want {
			t.Errorf("isEmbedPage(%q) = %v, want %v", in, got, want)
		}
	}
}

// IsMediaURL（provider 包）要认得出 /get_file/.../169074.mp4/?embed=true 这类地址。
func TestIsMediaURL(t *testing.T) {
	yes := []string{
		"https://videos.js-static.cc/hls/contents/videos/89000/89686/89686.mp4/list.m3u8?k=1&t=2",
		"https://www.99jj23.com/get_file/3/abc/169000/169074/169074.mp4/?embed=true",
		"https://fccw55.com/get_file/1/x/109000/109853/109853.mp4",
		"https://cdn.example.com/a.mp4?token=1",
	}
	for _, u := range yes {
		if !provider.IsMediaURL(u) {
			t.Errorf("应识别为媒体地址: %s", u)
		}
	}
	no := []string{
		"https://www.99jj23.com/embed/169074",
		"https://yuese99.com/video.html?id=83193",
		"",
	}
	for _, u := range no {
		if provider.IsMediaURL(u) {
			t.Errorf("不应识别为媒体地址: %s", u)
		}
	}
}
