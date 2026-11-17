package httpapi

// 视频代理（前端称为“线路二”）。
//
// 浏览器直连（线路一）时，跨域、防盗链、运营商网络差异都可能导致播放失败；
// 本文件提供一个只读反向代理：服务端代取清单/分片/密钥，再以同源方式回给浏览器。
//
// 关键点：
//
//	1. m3u8 是相对路径的清单（分片 cdn-segs-1.ts、密钥 vidensc.key），
//	   如果原样返回，浏览器会按 /api/proxy 去拼地址而取错资源，
//	   所以清单必须逐行重写成指回本代理的绝对地址。
//	2. 只允许公网 http(s) 目标，避免服务被当成内网探测器（SSRF）。
//	   校验放在拨号阶段（拿到 IP 后、建连之前），可防 DNS 重绑定。
//	3. 目标主机白名单：来自映射里的播放地址，或在代理清单里出现过的主机。
//	   这样既允许 CDN 分片域，也不会变成对全网开放的公共代理。

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"paresevideo/internal/store"
)

const proxyPrefix = "/api/proxy"

// proxyAllow 维护允许被代理的主机白名单。
type proxyAllow struct {
	mu     sync.Mutex
	hosts  map[string]bool
	seeded time.Time
	store  *store.Store
}

func newProxyAllow(st *store.Store) *proxyAllow {
	return &proxyAllow{hosts: make(map[string]bool), store: st}
}

// hostKey 把主机名归一化为白名单键（小写、去端口、去 www.）。
func hostKey(hostport string) string {
	h := hostport
	if hh, _, err := net.SplitHostPort(hostport); err == nil {
		h = hh
	}
	h = strings.ToLower(strings.TrimSuffix(h, "."))
	return strings.TrimPrefix(h, "www.")
}

// seed 从映射里的播放地址收集主机（60 秒内只做一次）。
func (a *proxyAllow) seed() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if time.Since(a.seeded) < time.Minute {
		return
	}
	a.seeded = time.Now()
	if a.store == nil {
		return
	}
	for _, it := range a.store.AllItems() {
		for _, raw := range []string{it.PlayURL, it.PlayPage, it.RedirectURL} {
			if u, err := url.Parse(raw); err == nil && u.Host != "" {
				a.hosts[hostKey(u.Host)] = true
			}
		}
	}
}

// allows 判断主机是否在白名单内。
func (a *proxyAllow) allows(host string) bool {
	a.seed()
	key := hostKey(host)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hosts[key]
}

// learn 记住从清单/重定向里发现的主机（分片 CDN 常与清单不同域）。
func (a *proxyAllow) learn(host string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hosts[hostKey(host)] = true
}

// proxy 是线路二的处理核心。
type proxy struct {
	allow  *proxyAllow
	client *http.Client
}

func newProxy(st *store.Store) *proxy {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ip, err := publicIP(ctx, host)
			if err != nil {
				return nil, err
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 25 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return &proxy{
		allow: newProxyAllow(st),
		client: &http.Client{
			// 不设整体超时：视频分片是长流，靠 ResponseHeaderTimeout 兜住握手阶段。
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 6 {
					return fmt.Errorf("重定向次数过多")
				}
				if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
					return fmt.Errorf("不支持的重定向协议")
				}
				// 目标 IP 由 DialContext 统一校验，这里只需限制协议。
				return nil
			},
		},
	}
}

// publicIP 解析主机并返回第一个公网 IP，拒绝内网、回环、链路本地等地址。
func publicIP(ctx context.Context, host string) (string, error) {
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return "", err
	}
	for _, ip := range ips {
		if isPublicIP(ip) {
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("目标地址不在公网范围")
}

// isPublicIP 判断是否为可代理的公网单播地址。
func isPublicIP(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return false
	}
	// 云厂商常用的 100.64.0.0/10（CGNAT）也不应被访问。
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return false
	}
	return true
}

// proxyPath 生成指回本代理的目标地址。
func proxyPath(raw string) string {
	return proxyPrefix + "?url=" + url.QueryEscape(raw)
}

// handleProxy 处理 GET/HEAD /api/proxy?url=<目标地址>。
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw := r.URL.Query().Get("url")
	target, err := url.Parse(raw)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		http.Error(w, "缺少有效的 url 参数", http.StatusBadRequest)
		return
	}
	if target.User != nil {
		http.Error(w, "不支持带凭证的地址", http.StatusBadRequest)
		return
	}
	if !s.Proxy.allow.allows(target.Host) {
		http.Error(w, "该地址不在允许代理的范围内", http.StatusForbidden)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), nil)
	if err != nil {
		http.Error(w, "构造请求失败", http.StatusBadRequest)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	// 部分 CDN 校验 Referer，用目标自身的源做参照最接近播放器行为。
	req.Header.Set("Referer", target.Scheme+"://"+target.Host+"/")
	if rg := r.Header.Get("Range"); rg != "" {
		req.Header.Set("Range", rg)
	}

	resp, err := s.Proxy.client.Do(req)
	if err != nil {
		http.Error(w, "上游取流失败: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// 重定向落地的主机也纳入白名单，便于后续分片直取。
	s.Proxy.allow.learn(resp.Request.URL.Host)

	ctype := resp.Header.Get("Content-Type")
	if isManifest(target.Path, ctype) {
		s.writeManifest(w, resp, target)
		return
	}
	writeStream(w, resp)
}

// isManifest 判断响应是否为 HLS 清单。
func isManifest(p, ctype string) bool {
	if strings.Contains(strings.ToLower(ctype), "mpegurl") {
		return true
	}
	return strings.HasSuffix(strings.ToLower(p), ".m3u8")
}

// writeManifest 读取清单，把分片/子清单/密钥地址改写成代理地址后返回。
func (s *Server) writeManifest(w http.ResponseWriter, resp *http.Response, target *url.URL) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		http.Error(w, "读取清单失败", http.StatusBadGateway)
		return
	}
	out := rewriteManifest(string(body), resp.Request.URL, s.Proxy)
	h := w.Header()
	h.Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, out)
}

// rewriteManifest 逐行重写清单：
//   - 普通行（分片、子清单、以及 #EXT-X-BYTERANGE 之外的 URI）→ 代理地址
//   - 带 URI="..." 的标签（#EXT-X-KEY / #EXT-X-MAP / #EXT-X-MEDIA 等）→ 同样改写
//   - 其它注释与空行原样保留
func rewriteManifest(body string, base *url.URL, p *proxy) string {
	var b strings.Builder
	b.Grow(len(body) + 256)
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimRight(line, "\r")
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
			b.WriteString("\n")
		case strings.HasPrefix(trimmed, "#"):
			b.WriteString(rewriteTagURI(line, base, p))
			b.WriteString("\n")
		default:
			b.WriteString(rewriteURI(trimmed, base, p))
			b.WriteString("\n")
		}
	}
	return b.String()
}

// rewriteTagURI 处理 #EXT-X-KEY:METHOD=AES-128,URI="vidensc.key" 这类标签。
func rewriteTagURI(line string, base *url.URL, p *proxy) string {
	idx := strings.Index(line, `URI="`)
	if idx < 0 {
		return line
	}
	start := idx + len(`URI="`)
	end := strings.IndexByte(line[start:], '"')
	if end < 0 {
		return line
	}
	uri := line[start : start+end]
	return line[:start] + rewriteURI(uri, base, p) + line[start+end:]
}

// rewriteURI 把清单里的相对地址解析成绝对地址，再套上代理前缀。
func rewriteURI(uri string, base *url.URL, p *proxy) string {
	ref, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	abs := base.ResolveReference(ref)
	if abs.Host == "" {
		return uri
	}
	p.allow.learn(abs.Host)
	return proxyPath(abs.String())
}

// writeStream 透传分片/视频文件，保留状态码与 Range 相关头。
func writeStream(w http.ResponseWriter, resp *http.Response) {
	h := w.Header()
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		h.Set("Content-Type", ct)
	} else if ext := path.Ext(resp.Request.URL.Path); ext != "" {
		if t := mime.TypeByExtension(ext); t != "" {
			h.Set("Content-Type", t)
		}
	}
	for _, k := range []string{"Content-Length", "Content-Range", "Accept-Ranges", "Last-Modified", "ETag"} {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Cache-Control", "public, max-age=3600")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
