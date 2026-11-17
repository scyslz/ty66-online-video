package ys

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"paresevideo/internal/provider"
	"paresevideo/internal/provider/siteutil"
)

const (
	defaultBase = "https://ys.6d6wcdx7.icu"
	// AES key/IV observed in the site's frontend (main.dart.js).
	aesKey = "LqzCOd16vnYhOHTz"
	aesIV  = "LqzCOd16vnYhOHTz"

	// cdnBase is the observed HLS CDN host for derived playback URLs.
	cdnBase = "https://yuese.b-cdn.net/hls/contents/videos"
)

// Config configures the ys (月色视频) provider.
type Config struct {
	Name     string
	Base     string
	CacheDir string
	// AllowDetail controls whether uncached detail requests may hit the network.
	// Detail is reported as rate-limited (once/day); keep false to rely on cache only
	// unless explicitly enabled.
	AllowDetail bool
}

func DefaultConfig() Config {
	return Config{
		Name:     "ys",
		Base:     defaultBase,
		CacheDir: "cache/ys",
		// Conservative default: do not burn the daily detail quota automatically.
		AllowDetail: false,
	}
}

// Site implements provider.Provider for ys.6d6wcdx7.icu.
type Site struct {
	cfg       Config
	detailMu  sync.Mutex
	detailHit map[string]bool // in-process guard: never fetch same detail twice
	metaMu    sync.RWMutex
	meta      map[string]int // video_id -> is_hd (from latest list data)
}

func New(cfg Config) *Site {
	def := DefaultConfig()
	if cfg.Name == "" {
		cfg.Name = def.Name
	}
	if cfg.Base == "" {
		cfg.Base = def.Base
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = def.CacheDir
	}
	cfg.Base = strings.TrimRight(cfg.Base, "/")
	return &Site{cfg: cfg, detailHit: make(map[string]bool), meta: make(map[string]int)}
}

func (s *Site) Name() string  { return s.cfg.Name }
func (s *Site) MaxPages() int { return 2000 }

type apiResp struct {
	Error   int             `json:"error"`
	Message string          `json:"message"`
	Content json.RawMessage `json:"content"`
}

type encContent struct {
	Encrypt     int    `json:"encrypt"`
	EncryptData string `json:"encryptData"`
}

func decrypt(data string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher([]byte(aesKey))
	if err != nil {
		return nil, err
	}
	if len(raw)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("ciphertext not multiple of block size")
	}
	dst := make([]byte, len(raw))
	cipher.NewCBCDecrypter(block, []byte(aesIV)).CryptBlocks(dst, raw)
	// PKCS7 unpad
	if len(dst) == 0 {
		return nil, fmt.Errorf("empty plaintext")
	}
	pad := int(dst[len(dst)-1])
	if pad == 0 || pad > aes.BlockSize || pad > len(dst) {
		return nil, fmt.Errorf("bad padding")
	}
	for i := len(dst) - pad; i < len(dst); i++ {
		if int(dst[i]) != pad {
			return nil, fmt.Errorf("bad padding")
		}
	}
	return dst[:len(dst)-pad], nil
}

func (s *Site) cachePath(kind, key string) string {
	safe := strings.NewReplacer("/", "_", "?", "_", "&", "_", "=", "_", ":", "_").Replace(key)
	if len(safe) > 180 {
		safe = safe[len(safe)-180:]
	}
	return filepath.Join(s.cfg.CacheDir, kind, safe)
}

func (s *Site) saveCache(kind, key string, body []byte) {
	p := s.cachePath(kind, key)
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	_ = os.WriteFile(p, body, 0o644)
}

func (s *Site) loadCache(kind, key string) ([]byte, bool) {
	b, err := os.ReadFile(s.cachePath(kind, key))
	if err == nil && len(b) > 0 {
		return b, true
	}
	return nil, false
}

// apiGet fetches an API path, decrypts content, and returns decrypted JSON.
// persist controls whether raw/decrypted responses are saved to disk (detail
// responses must be persisted due to daily rate limit).
func (s *Site) apiGet(path string, useCache bool, cacheKind string, persist bool) ([]byte, bool, error) {
	cacheKey := path
	if useCache {
		if b, ok := s.loadCache(cacheKind, cacheKey); ok {
			return b, true, nil
		}
	}
	full := s.cfg.Base + path
	body, err := siteutil.FetchBytes(siteutil.APIClient, full, "application/json")
	if err != nil {
		return nil, false, err
	}
	if persist {
		// Persist raw and decrypted response.
		s.saveCache(cacheKind, cacheKey+".raw", body)
	}

	var r apiResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, false, fmt.Errorf("decode api response: %w", err)
	}
	if r.Error != 0 {
		return nil, false, fmt.Errorf("api error %d: %s", r.Error, r.Message)
	}
	var ec encContent
	plain := []byte(r.Content)
	if err := json.Unmarshal(r.Content, &ec); err == nil && ec.Encrypt == 1 && ec.EncryptData != "" {
		p, err := decrypt(ec.EncryptData)
		if err != nil {
			return nil, false, fmt.Errorf("decrypt: %w", err)
		}
		plain = p
	}
	if persist {
		s.saveCache(cacheKind, cacheKey, plain)
	}
	return plain, false, nil
}

type listItem struct {
	VideoID     int    `json:"video_id"`
	Title       string `json:"title"`
	Duration    int    `json:"duration"` // seconds
	Rating      any    `json:"rating"`
	VideoImg    string `json:"video_img"`
	RatingRatio any    `json:"rating_ratio"`
	IsHD        int    `json:"is_hd"`
}

type indexResp struct {
	Total       int        `json:"total"`
	PerPage     int        `json:"per_page"`
	CurrentPage int        `json:"current_page"`
	LastPage    int        `json:"last_page"`
	Data        []listItem `json:"data"`
}

func fmtDur(sec int) string {
	if sec <= 0 {
		return ""
	}
	h := sec / 3600
	m := (sec % 3600) / 60
	s := sec % 60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d", m, s)
}

// FetchList implements provider.Provider.
func (s *Site) FetchList(page int) ([]provider.Item, error) {
	if page < 1 {
		page = 1
	}
	path := fmt.Sprintf("/api/videos/index?page=%d&size=24&sort=new", page)
	// Always fetch latest list data directly; do not persist list responses.
	plain, _, err := s.apiGet(path, false, "list", false)
	if err != nil {
		return nil, err
	}
	var lr indexResp
	if err := json.Unmarshal(plain, &lr); err != nil {
		return nil, err
	}
	// If requested page is beyond last_page, treat as end.
	if lr.LastPage > 0 && page > lr.LastPage {
		return nil, nil
	}
	items := make([]provider.Item, 0, len(lr.Data))
	for _, it := range lr.Data {
		if it.VideoID == 0 || it.Title == "" {
			continue
		}
		id := fmt.Sprintf("%d", it.VideoID)
		// Keep list metadata (is_hd) in memory for URL derivation.
		s.setMeta(id, it.IsHD)
		items = append(items, provider.Item{
			ID:        id,
			Site:      s.cfg.Name,
			Title:     it.Title,
			ThreadURL: s.cfg.Base + "/#/video/" + id,
			Duration:  fmtDur(it.Duration),
			Page:      page,
		})
	}
	// Deterministic order as returned.
	_ = sort.Slice
	return items, nil
}

func (s *Site) setMeta(id string, isHD int) {
	s.metaMu.Lock()
	s.meta[id] = isHD
	s.metaMu.Unlock()
}

func (s *Site) loadMetaIsHD(id string) (int, bool) {
	s.metaMu.RLock()
	v, ok := s.meta[id]
	s.metaMu.RUnlock()
	if ok {
		return v, true
	}
	// Backward-compat: read legacy on-disk meta if present.
	b, ok := s.loadCache("meta", id)
	if !ok {
		return 0, false
	}
	var m struct {
		IsHD int `json:"is_hd"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return 0, false
	}
	s.setMeta(id, m.IsHD)
	return m.IsHD, true
}

// deriveCandidates returns candidate master.m3u8 URLs inferred from video_id.
// Observed pattern: /{grp}/{id}/,{id}.mp4[,{id}_720p.mp4],.urlset/master.m3u8
// where grp = (id/1000)*1000. is_hd hint selects the preferred order.
func (s *Site) deriveCandidates(id string, isHD int, known bool) []string {
	n, err := strconv.Atoi(id)
	if err != nil || n <= 0 {
		return nil
	}
	grp := (n / 1000) * 1000
	base := fmt.Sprintf("%s/%d/%d", cdnBase, grp, n)
	std := fmt.Sprintf("%s/,%%d.mp4,%%d_720p.mp4,.urlset/master.m3u8", base)
	std = fmt.Sprintf(std, n, n)
	no720 := fmt.Sprintf("%s/,%d.mp4,.urlset/master.m3u8", base, n)
	switch {
	case known && isHD == 0:
		return []string{no720, std}
	case known && isHD == 1:
		return []string{std, no720}
	default:
		// Unknown: try both; std is common for HD, no720 for SD.
		return []string{std, no720}
	}
}

// probeDerived tries candidate HLS master URLs (CDN, no detail API).
// Returns the first URL whose body looks like an HLS playlist.
func (s *Site) probeDerived(id string, candidates []string) (string, bool) {
	for _, u := range candidates {
		body, err := siteutil.FetchBytes(siteutil.PageClient, u, "application/vnd.apple.mpegurl,*/*")
		if err != nil {
			continue
		}
		if isHLSManifest(body) {
			return u, true
		}
	}
	return "", false
}

func isHLSManifest(b []byte) bool {
	s := strings.TrimSpace(string(b))
	return strings.HasPrefix(s, "#EXTM3U")
}

// resolveByDerivation derives and probes a playable m3u8 URL from list info,
// avoiding the rate-limited detail API. Probe responses are cached.
func (s *Site) resolveByDerivation(it provider.Item, now int64) (provider.Item, bool) {
	id := it.ID
	isHD, known := s.loadMetaIsHD(id)
	cands := s.deriveCandidates(id, isHD, known)
	if len(cands) == 0 {
		return it, false
	}
	play, ok := s.probeDerived(id, cands)
	if !ok {
		return it, false
	}
	it.Site = s.cfg.Name
	it.UpdatedAt = now
	if it.PlayPage == "" {
		it.PlayPage = provider.FirstNonEmpty(it.ThreadURL, s.cfg.Base+"/#/video/"+id)
	}
	it.PlayURL = play
	it.Resolved = true
	it.Error = ""
	it.RetryAt = 0
	if link := provider.FirstNonEmpty(it.PlayPage, it.PlayURL, it.ThreadURL); link != "" {
		if u, err := url.Parse(link); err == nil {
			it.Source = provider.NormalizeSource(u.Host)
		}
	}
	return it, true
}

type detailResp struct {
	VideoID    int    `json:"video_id"`
	Title      string `json:"title"`
	Duration   int    `json:"duration"`
	VideoURL   string `json:"video_url"`
	WebLink    string `json:"web_link"`
	IsHD       int    `json:"is_hd"`
	LineIdx    int    `json:"line_idx"`
	VideoImg   string `json:"video_img"`
	CDNLines   any    `json:"cdn_lines"`
	ImageLines any    `json:"image_lines"`
}

// Enrich implements provider.Provider.
func (s *Site) Enrich(it provider.Item) provider.Item {
	now := time.Now().Unix()
	it.Attempts++
	id := it.ID
	if id == "" {
		it.Error = "missing id"
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(provider.RetrySchedule(it.Attempts)).Unix()
		return it
	}

	path := "/api/videos/detail?id=" + url.QueryEscape(id)

	// 1) Always prefer cached detail (authoritative; avoids daily rate limit).
	if plain, ok := s.loadCache("detail", path); ok {
		return s.applyDetail(it, plain, now)
	}

	// 2) Derive & probe HLS URL from list metadata (no detail API). This bypasses
	// the reported "once per day" detail limit; probe responses are saved.
	if resolved, ok := s.resolveByDerivation(it, now); ok {
		return resolved
	}

	// Ensure a playable/openable page is always kept even when resolution fails.
	if it.PlayPage == "" {
		it.PlayPage = provider.FirstNonEmpty(it.ThreadURL, s.cfg.Base+"/#/video/"+id)
	}

	// 3) In-process guard: never hit same detail twice.
	s.detailMu.Lock()
	if s.detailHit[id] {
		s.detailMu.Unlock()
		it.Error = "detail rate-limited/cached-miss; skipped duplicate fetch"
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(24 * time.Hour).Unix()
		return it
	}
	s.detailHit[id] = true
	s.detailMu.Unlock()

	// 4) Network detail fetch only if explicitly allowed (consumes daily quota).
	if !s.cfg.AllowDetail {
		it.Error = "detail not cached; derivation failed and network fetch disabled (daily rate limit)"
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(24 * time.Hour).Unix()
		return it
	}

	// Detail is rate-limited; always persist to disk.
	plain, _, err := s.apiGet(path, false, "detail", true)
	if err != nil {
		it.Error = err.Error()
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(provider.RetryHTTP(it.Attempts)).Unix()
		return it
	}
	return s.applyDetail(it, plain, now)
}

func (s *Site) applyDetail(it provider.Item, plain []byte, now int64) provider.Item {
	var d detailResp
	if err := json.Unmarshal(plain, &d); err != nil {
		it.Error = "decode detail: " + err.Error()
		it.UpdatedAt = now
		it.RetryAt = time.Now().Add(provider.RetryHTTP(it.Attempts)).Unix()
		return it
	}
	it.Site = s.cfg.Name
	it.UpdatedAt = now
	if d.Title != "" {
		it.Title = d.Title
	}
	if d.Duration > 0 {
		it.Duration = fmtDur(d.Duration)
	}
	if d.WebLink != "" {
		it.PlayPage = d.WebLink
	} else if it.ThreadURL != "" {
		it.PlayPage = it.ThreadURL
	}
	if provider.IsMediaURL(d.VideoURL) {
		it.PlayURL = d.VideoURL
		it.Resolved = true
		it.Error = ""
		it.RetryAt = 0
	} else if d.VideoURL != "" {
		// keep it anyway; front/proxy may handle
		it.PlayURL = d.VideoURL
		it.Resolved = provider.IsMediaURL(d.VideoURL)
		if !it.Resolved {
			it.Error = "detail returned non-media video_url"
			it.RetryAt = time.Now().Add(provider.RetryNoVideo).Unix()
		}
	} else {
		it.Error = "detail missing video_url"
		it.RetryAt = time.Now().Add(provider.RetrySchedule(it.Attempts)).Unix()
	}
	if link := provider.FirstNonEmpty(it.PlayPage, it.PlayURL, it.ThreadURL); link != "" {
		if u, err := url.Parse(link); err == nil {
			it.Source = provider.NormalizeSource(u.Host)
		}
	}
	return it
}

// Refresh implements provider.Provider. Detail play URLs (when fetched from API)
// include tokens and may expire; detail is rate-limited so we avoid network.
// Prefer cached detail; otherwise re-resolve via derivation (tokenless CDN URL).
func (s *Site) Refresh(it provider.Item) (provider.Item, bool) {
	id := it.ID
	if id == "" {
		return it, false
	}
	path := "/api/videos/detail?id=" + url.QueryEscape(id)
	if plain, ok := s.loadCache("detail", path); ok {
		before := it.PlayURL
		it = s.applyDetail(it, plain, time.Now().Unix())
		return it, it.PlayURL != "" && it.PlayURL != before
	}
	before := it.PlayURL
	resolved, ok := s.resolveByDerivation(it, time.Now().Unix())
	if !ok {
		return it, false
	}
	return resolved, resolved.PlayURL != "" && resolved.PlayURL != before
}
