// Package httpapi 提供 HTTP 接口与静态页面服务。
//
// 本包只做参数解析与 JSON 序列化，所有数据与并发控制都在 store 中。
package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"paresevideo/internal/provider/siteutil"
	"paresevideo/internal/store"
)

// Server 是 HTTP 处理器集合。
type Server struct {
	Store *store.Store
	Root  string // 磁盘上的静态目录；为空或没有 index.html 时用内嵌页面
	Proxy *proxy // 线路二：视频同源代理（由 Routes 惰性初始化）
	// ProxyEnabled 为 false 时不注册 /api/proxy 路由，页面也不显示“线路二”。
	ProxyEnabled bool
}

// Routes 注册所有路由。
func (s *Server) Routes() http.Handler {
	if s.ProxyEnabled {
		if s.Proxy == nil {
			s.Proxy = newProxy(s.Store)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/scrape", s.handleScrape)
	mux.HandleFunc("/api/pages", s.handlePages)
	mux.HandleFunc("/api/map", s.handleMap)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/sources", s.handleSources)
	mux.HandleFunc("/api/items", s.handleItems)
	mux.HandleFunc("/api/resolve", s.handleResolve)
	if s.ProxyEnabled {
		mux.HandleFunc(proxyPrefix, s.handleProxy)
	}
	mux.Handle("/", assetHandler(s.Root))
	return mux
}

// handleScrape 返回按条数窗口切片的列表。
//
// 参数：offset（默认 0）、limit（默认 24，最大 500）、source（按来源域名过滤）、
// site（按站点过滤，多站部署时用）；
// refresh=1 强制先同步第一页。兼容旧参数 page：换算为 offset=(page-1)*24。
func (s *Server) handleScrape(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	limit := 24
	if v, err := strconv.Atoi(query.Get("limit")); err == nil && v > 0 {
		limit = v
	}

	offset := 0
	if v, err := strconv.Atoi(query.Get("offset")); err == nil && v >= 0 {
		offset = v
	} else if v, err := strconv.Atoi(query.Get("page")); err == nil && v > 1 {
		offset = (min(v, store.MaxPage) - 1) * 24
	}

	source := store.NormalizeSource(query.Get("source"))
	site := strings.TrimSpace(query.Get("site"))

	if query.Get("refresh") == "1" {
		s.Store.RequestSyncSite(site, 1)
	}

	win := s.Store.FillWindowQuery(offset, limit, source, site)
	items := win.Items
	if items == nil {
		items = []store.Item{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"offset":  offset,
		"limit":   limit,
		"source":  source,
		"site":    site,
		"total":   win.Total,
		"count":   len(items),
		"items":   items,
		"pending": win.Pending,
		// loading: 后台正在补页，前端可稍后重试一次
		// has_more: 还有站点没到最后一页，理论上能取到更多
		"loading":  win.Loading,
		"has_more": win.HasMore,
	})
}

// handlePages 返回已缓存的上游页号。
func (s *Server) handlePages(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"cached":  s.Store.CachedPages(),
		"by_site": s.Store.CachedPagesBySite(),
	})
}

// handleMap 输出完整的 id → 视频地址映射。
func (s *Server) handleMap(w http.ResponseWriter, _ *http.Request) {
	entries := s.Store.MapEntries()
	if entries == nil {
		entries = []store.MapEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(entries), "items": entries})
}

// handleSources 返回当前列表中各来源域名及其条目数（按数量倒序）。
func (s *Server) handleSources(w http.ResponseWriter, _ *http.Request) {
	list := s.Store.Sources()
	if list == nil {
		list = []store.SourceCount{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(list), "sources": list})
}

// handleStatus 返回运行概况，便于排查。
func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	items, pages, pending, lastList := s.Store.Stats()
	resp := map[string]any{
		"items":         items,
		"pages":         pages,
		"pending":       pending,
		"refresh_every": s.Store.ListTTL().String(),
		"proxy":         s.ProxyEnabled,
		"sites":         s.Store.Providers(),
		// 懒加载：默认不主动抓详情，直链等播放时按需解析
		"lazy":      s.Store.Lazy(),
		"site_sync": s.Store.SiteSyncs(),
		// 限流观测：每主机每分钟配额与当前窗口内用量
		"rate_limit": siteutil.Default.Limit(),
		"rate_usage": siteutil.Default.UsageAll(),
	}
	if !lastList.IsZero() {
		resp["last_list_sync"] = lastList.Unix()
		resp["last_list_age"] = time.Since(lastList).Round(time.Second).String()
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleItems 输出当前列表全部条目（受 max-items 限制），供导出或第三方使用。
func (s *Server) handleItems(w http.ResponseWriter, _ *http.Request) {
	items := s.Store.AllItems()
	if items == nil {
		items = []store.Item{}
	}
	writeJSON(w, http.StatusOK, items)
}

// handleResolve 按需重解析一条，供前端「直链播放失败」时调用。
//
// 参数：site（可省略，默认站点）、id（必填）。
// 返回 {item, changed, waited}：changed=true 表示换到了新地址，前端直接重播；
// changed=false 时前端应回落到网页播放页或提示失败。
//
// 这是懒加载策略的入口：后台不再按 TTL 批量续签，只有真的播不动才解析。
func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	query := r.URL.Query()
	id := strings.TrimSpace(query.Get("id"))
	if id == "" {
		http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
		return
	}
	site := strings.TrimSpace(query.Get("site"))

	res := s.Store.Repair(site, id)
	// 站点未知时条目为空：明确报错，避免前端以为解析成功。
	if res.Item.ID == "" && res.Item.Title == "" && res.Item.PlayURL == "" {
		http.Error(w, `{"error":"unknown item"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"item":    res.Item,
		"changed": res.Changed,
		"waited":  res.Waited,
	})
}

// writeJSON 输出 JSON 响应（不转义 HTML 字符）。
func writeJSON(w http.ResponseWriter, code int, v any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		http.Error(w, `{"error":"encode failed"}`, http.StatusInternalServerError)
		return
	}
	b := bytes.TrimRight(buf.Bytes(), "\n")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(code)
	_, _ = w.Write(b)
}
