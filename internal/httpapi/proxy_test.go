package httpapi

import (
	"net"
	"net/url"
	"strings"
	"testing"
)

// TestRewriteManifest 校验清单改写：分片、子清单、密钥都要指回代理。
func TestRewriteManifest(t *testing.T) {
	base, err := url.Parse("https://cdn.example.com/hls/89686/list.m3u8?k=abc")
	if err != nil {
		t.Fatal(err)
	}
	p := newProxy(nil)
	in := "#EXTM3U\n" +
		"#EXT-X-KEY:METHOD=AES-128,URI=\"vidensc.key\"\n" +
		"#EXTINF:10.0,\n" +
		"cdn-segs-1-v1-a1.ts\n" +
		"#EXT-X-MAP:URI=\"init.mp4\"\n" +
		"https://other.example.net/abs.ts\n" +
		"#EXT-X-ENDLIST\n"
	out := rewriteManifest(in, base, p)

	wantPaths := []string{
		"https://cdn.example.com/hls/89686/vidensc.key",
		"https://cdn.example.com/hls/89686/cdn-segs-1-v1-a1.ts",
		"https://cdn.example.com/hls/89686/init.mp4",
		"https://other.example.net/abs.ts",
	}
	for _, want := range wantPaths {
		if !strings.Contains(out, proxyPath(want)) {
			t.Errorf("清单未改写 %s\n%s", want, out)
		}
	}
	// 注释与结束标记必须原样保留
	for _, keep := range []string{"#EXTM3U", "#EXTINF:10.0,", "#EXT-X-ENDLIST"} {
		if !strings.Contains(out, keep+"\n") {
			t.Errorf("丢失标签 %s\n%s", keep, out)
		}
	}
	// 清单里出现的主机应被记住，后续分片请求才能通过白名单
	if !p.allow.allows("other.example.net") {
		t.Error("清单里的新主机未被加入白名单")
	}
}

// TestProxyPathRoundTrip 校验代理地址可以解回原始地址。
func TestProxyPathRoundTrip(t *testing.T) {
	orig := "https://cdn.example.com/a/b.ts?x=1&y=2"
	u, err := url.Parse(proxyPath(orig))
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != proxyPrefix {
		t.Fatalf("路径 = %s", u.Path)
	}
	if got := u.Query().Get("url"); got != orig {
		t.Fatalf("还原 = %s, want %s", got, orig)
	}
}

// TestIsPublicIP 校验内网/回环/元数据地址会被拒绝。
func TestIsPublicIP(t *testing.T) {
	cases := map[string]bool{
		"8.8.8.8":         true,
		"1.1.1.1":         true,
		"127.0.0.1":       false,
		"10.0.0.5":        false,
		"192.168.1.1":     false,
		"172.16.0.1":      false,
		"169.254.169.254": false,
		"100.100.1.1":     false,
		"::1":             false,
		"fd00::1":         false,
	}
	for raw, want := range cases {
		ip := net.ParseIP(raw)
		if ip == nil {
			t.Fatalf("无法解析 %s", raw)
		}
		if got := isPublicIP(ip); got != want {
			t.Errorf("isPublicIP(%s) = %v, want %v", raw, got, want)
		}
	}
}

// TestHostKey 校验白名单键的归一化（端口、大小写、www）。
func TestHostKey(t *testing.T) {
	cases := map[string]string{
		"Videos.JS-Static.cc:443": "videos.js-static.cc",
		"www.99jj23.com":          "99jj23.com",
		"fccw55.com":              "fccw55.com",
	}
	for raw, want := range cases {
		if got := hostKey(raw); got != want {
			t.Errorf("hostKey(%s) = %s, want %s", raw, got, want)
		}
	}
}
