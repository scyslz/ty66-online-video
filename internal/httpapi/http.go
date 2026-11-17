// Package httpapi 提供 HTTP 接口与静态页面服务。
//
// 本包只做参数解析与 JSON 序列化，所有数据与并发控制都在 store 中。
package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"paresevideo/internal/store"
)

// Server 是 HTTP 处理器集合。
type Server struct {
	Store *store.Store
	Root  string // 磁盘上的静态目录；为空或没有 index.html 时用内嵌页面
}

// Routes 注册所有路由。
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/scrape", s.handleScrape)
	mux.HandleFunc("/api/pages", s.handlePages)
	mux.HandleFunc("/api/map", s.handleMap)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/sources", s.handleSources)
	mux.HandleFunc("/api/items", s.handleItems)
	mux.Handle("/", assetHandler(s.Root))
	return mux
}

// handleScrape 返回按条数窗口切片的列表。
//
// 参数：offset（默认 0）、limit（默认 24，最大 500）、source（按来源过滤）；
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

	if query.Get("refresh") == "1" {
		s.Store.RequestSync(1)
	}

	win := s.Store.FillWindowSource(offset, limit, source)
	items := win.Items
	if items == nil {
		items = []store.Item{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"offset":  offset,
		"limit":   limit,
		"source":  source,
		"total":   win.Total,
		"count":   len(items),
		"items":   items,
		"pending": win.Pending,
	})
}

// handlePages 返回已缓存的上游页号。
func (s *Server) handlePages(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"cached": s.Store.CachedPages()})
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
		"video_ttl":     s.Store.VideoTTL().String(),
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
