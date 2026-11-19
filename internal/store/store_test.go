package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"paresevideo/internal/provider"
)

// fakeProvider 是最小可用的站点实现，用于测试多站点隔离。
type fakeProvider struct {
	name  string
	pages map[int][]provider.Item
	// exhaustive 为 true 时，未设置的页返回空列表（模拟翻到底）。
	exhaustive bool
}

func (f *fakeProvider) Name() string  { return f.name }
func (f *fakeProvider) MaxPages() int { return 10 }
func (f *fakeProvider) FetchList(page int) ([]provider.Item, error) {
	if items, ok := f.pages[page]; ok {
		return items, nil
	}
	if f.exhaustive {
		return nil, nil
	}
	return nil, nil
}
func (f *fakeProvider) Enrich(it provider.Item) provider.Item {
	it.PlayURL = "https://cdn.example.com/" + f.name + "/" + it.ID + ".m3u8"
	it.Resolved = true
	it.UpdatedAt = time.Now().Unix()
	it.Error = ""
	it.RetryAt = 0
	return it
}
func (f *fakeProvider) Refresh(it provider.Item) (provider.Item, bool) {
	return f.Enrich(it), true
}

func newFake(name string, ids ...string) *fakeProvider {
	items := make([]provider.Item, 0, len(ids))
	for _, id := range ids {
		items = append(items, provider.Item{ID: id, Title: name + "-" + id, ThreadURL: "https://" + name + ".example/" + id})
	}
	return &fakeProvider{name: name, pages: map[int][]provider.Item{1: items}, exhaustive: true}
}

func newTestStore(t *testing.T, providers ...provider.Provider) *Store {
	t.Helper()
	dir := t.TempDir()
	st := New(Options{
		File:      filepath.Join(dir, "store.json"),
		ListTTL:   time.Minute,
		Keep:      time.Hour,
		Workers:   2,
		MaxItems:  100,
		Providers: providers,
	})
	// 启动详情流水线，否则窗口读取会等满 20 秒超时。
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	t.Cleanup(cancel)
	return st
}

// 两个站点的同号帖子必须各自独立，不能互相覆盖。
func TestMultiSiteKeyIsolation(t *testing.T) {
	a := newFake("alpha", "100", "101")
	b := newFake("beta", "100", "102")
	st := newTestStore(t, a, b)

	if n := st.SyncPageSite("alpha", 1, true); n != 2 {
		t.Fatalf("alpha 第 1 页应有 2 条，得到 %d", n)
	}
	if n := st.SyncPageSite("beta", 1, true); n != 2 {
		t.Fatalf("beta 第 1 页应有 2 条，得到 %d", n)
	}
	if got := st.ItemCount(); got != 4 {
		t.Fatalf("两站共 4 条，实际 %d（键冲突）", got)
	}

	win := waitTotal(t, st, "", 4)
	if win.Total != 4 {
		t.Fatalf("窗口总数应为 4，得到 %d", win.Total)
	}
	// 站点顺序：注册顺序优先，alpha 在前。
	if win.Items[0].Site != "alpha" {
		t.Errorf("第一条应来自 alpha，得到 %q", win.Items[0].Site)
	}
	seen := map[string]bool{}
	for _, it := range win.Items {
		seen[it.Site+":"+it.ID] = true
	}
	for _, want := range []string{"alpha:100", "alpha:101", "beta:100", "beta:102"} {
		if !seen[want] {
			t.Errorf("缺少条目 %s", want)
		}
	}
}

// 各站点的页号互不影响：alpha 第 2 页不该把 beta 第 1 页视为已同步。
func TestMultiSitePageIsolation(t *testing.T) {
	a := newFake("alpha", "1")
	b := newFake("beta", "1")
	st := newTestStore(t, a, b)

	st.SyncPageSite("alpha", 1, true)
	// beta 尚未同步，第二次同步应真的再抓一次（这里用条目数间接验证）。
	if n := st.SyncPageSite("beta", 1, true); n != 1 {
		t.Fatalf("beta 第 1 页应有 1 条，得到 %d", n)
	}
	bySite := st.CachedPagesBySite()
	if len(bySite["alpha"]) != 1 || len(bySite["beta"]) != 1 {
		t.Fatalf("两站都应有 1 页缓存，实际 %+v", bySite)
	}
}

// 旧格式（v1：裸页号 + 裸 id）应能直接载入，并自动补上默认站点前缀。
func TestLoadLegacyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "store.json")
	legacy := map[string]any{
		"version": 1,
		"pages": map[string]any{
			"1": map[string]any{"ids": []string{"100", "101"}, "last_list": 1700000000},
		},
		"items": map[string]any{
			"100": map[string]any{"id": "100", "title": "旧条目", "play_url": "https://cdn.example.com/100.m3u8", "resolved": true},
			"101": map[string]any{"id": "101", "title": "旧条目2"},
		},
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	st := New(Options{
		File: file, ListTTL: time.Minute,
		Keep: time.Hour, Workers: 2, MaxItems: 100,
		Providers: []provider.Provider{newFake("alpha")},
	})
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	t.Cleanup(cancel)
	if err := st.Load(); err != nil {
		t.Fatalf("载入旧格式失败: %v", err)
	}
	if got := st.ItemCount(); got != 2 {
		t.Fatalf("应载入 2 条，得到 %d", got)
	}
	win := st.FillWindow(0, 10)
	if win.Total != 2 {
		t.Fatalf("旧数据的页应可用，窗口总数 %d", win.Total)
	}
	for _, it := range win.Items {
		if it.Site != "alpha" {
			t.Errorf("旧数据应归到默认站点，得到 %q", it.Site)
		}
	}
	if !provider.IsMediaURL(win.Items[0].PlayURL) {
		t.Errorf("旧数据的直链应保留: %q", win.Items[0].PlayURL)
	}

	// 落盘后应为 v2 格式，键带站点前缀。
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var p persisted
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Version != currentVersion {
		t.Errorf("落盘版本应为 %d，得到 %d", currentVersion, p.Version)
	}
	if _, ok := p.Items["alpha:100"]; !ok {
		t.Errorf("落盘键应带站点前缀，实际键: %v", keysOf(p.Items))
	}
	if _, ok := p.Pages["alpha/1"]; !ok {
		t.Errorf("页键应带站点前缀，实际键: %v", keysOf(p.Pages))
	}
}

// 两个站点标题相同（仅空格不同）时，只保留展示顺序靠前的一条。
func TestCrossSiteTitleDedup(t *testing.T) {
	a := &fakeProvider{name: "alpha", pages: map[int][]provider.Item{1: {
		{ID: "1", Title: "同一 视频", ThreadURL: "https://alpha.example/1"},
		{ID: "2", Title: "仅 alpha", ThreadURL: "https://alpha.example/2"},
	}}, exhaustive: true}
	b := &fakeProvider{name: "beta", pages: map[int][]provider.Item{1: {
		{ID: "9", Title: "同一视频", ThreadURL: "https://beta.example/9"},
		{ID: "8", Title: "同一　视频", ThreadURL: "https://beta.example/8"},
		{ID: "7", Title: "仅 beta", ThreadURL: "https://beta.example/7"},
	}}, exhaustive: true}
	st := newTestStore(t, a, b)

	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("beta", 1, true)

	// 底层映射仍保留全部条目，去重只发生在展示/读取层。
	if got := st.ItemCount(); got != 5 {
		t.Fatalf("映射应保留全部 5 条，实际 %d", got)
	}

	win := st.FillWindowQuery(0, 100, "", "")
	if win.Total != 3 {
		t.Fatalf("按标题去重后窗口应为 3 条，实际 %d", win.Total)
	}
	got := map[string]string{}
	for _, it := range win.Items {
		got[it.Site] += it.ID + ","
	}
	if got["alpha"] != "1,2," {
		t.Errorf("alpha 应保留全部条目（先展示），实际 %q", got["alpha"])
	}
	if got["beta"] != "7," {
		t.Errorf("beta 只应保留标题不同的 7，实际 %q", got["beta"])
	}
	for _, it := range win.Items {
		if it.ID == "8" || it.ID == "9" {
			t.Errorf("重复标题的 beta 条目不应展示: %+v", it)
		}
	}
}

// splitKey / pageKey 的往返。
func TestKeyHelpers(t *testing.T) {
	if k := itemKey("t66y", "123"); k != "t66y:123" {
		t.Errorf("itemKey = %q", k)
	}
	if site, id := splitKey("t66y:123"); site != "t66y" || id != "123" {
		t.Errorf("splitKey = %q,%q", site, id)
	}
	// 旧键（无前缀）站点为空，id 保持原样。
	if site, id := splitKey("123"); site != "" || id != "123" {
		t.Errorf("旧键拆分错误: %q,%q", site, id)
	}
	if k := pageKey("t66y", 7); k != "t66y/7" {
		t.Errorf("pageKey = %q", k)
	}
	if site, page := splitPageKey("t66y/7"); site != "t66y" || page != 7 {
		t.Errorf("splitPageKey = %q,%d", site, page)
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// 落盘文件里若存在未知站点的页，载入时应丢弃，避免污染状态。
func TestLoadDropsUnknownSitePages(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "store.json")
	raw := map[string]any{
		"version": 2,
		"pages": map[string]any{
			"alpha/1": map[string]any{"ids": []string{"alpha:1"}, "last_list": 1700000000, "site": "alpha", "page": 1},
			"ghost/1": map[string]any{"ids": []string{}, "last_list": 1700000000, "site": "ghost", "page": 1},
		},
		"items": map[string]any{
			"alpha:1": map[string]any{"id": "1", "site": "alpha", "title": "正常条目"},
		},
	}
	b, _ := json.Marshal(raw)
	if err := os.WriteFile(file, b, 0o644); err != nil {
		t.Fatal(err)
	}

	st := New(Options{
		File: file, ListTTL: time.Minute,
		Keep: time.Hour, Workers: 1, MaxItems: 100,
		Providers: []provider.Provider{newFake("alpha")},
	})
	if err := st.Load(); err != nil {
		t.Fatal(err)
	}
	bySite := st.CachedPagesBySite()
	if _, ok := bySite["ghost"]; ok {
		t.Errorf("未知站点的页应被丢弃，实际保留: %v", bySite)
	}
	if len(bySite["alpha"]) != 1 {
		t.Errorf("正常站点的页应保留: %v", bySite)
	}
}

// countingProvider 记录每个条目被 Enrich 了几次，用来验证「翻页不重复解析」。
type countingProvider struct {
	name   string
	pages  [][]string // 每页的 id 列表，可随时改（模拟上游插入新帖）
	mu     sync.Mutex
	enrich map[string]int
	lists  int
}

func newCounting(name string, pages ...[]string) *countingProvider {
	return &countingProvider{name: name, pages: pages, enrich: map[string]int{}}
}

func (p *countingProvider) Name() string  { return p.name }
func (p *countingProvider) MaxPages() int { return 100 }

func (p *countingProvider) FetchList(page int) ([]provider.Item, error) {
	p.mu.Lock()
	p.lists++
	p.mu.Unlock()
	if page < 1 || page > len(p.pages) {
		return nil, nil // 到底
	}
	ids := p.pages[page-1]
	out := make([]provider.Item, 0, len(ids))
	for _, id := range ids {
		out = append(out, provider.Item{
			ID:        id,
			Site:      p.name,
			Title:     p.name + "-" + id,
			ThreadURL: "https://" + p.name + ".example/" + id,
		})
	}
	return out, nil
}

func (p *countingProvider) Enrich(it provider.Item) provider.Item {
	p.mu.Lock()
	p.enrich[it.ID]++
	p.mu.Unlock()
	it.PlayURL = "https://cdn.example.com/" + it.ID + ".m3u8"
	it.Resolved = true
	it.Error = ""
	it.RetryAt = 0
	it.UpdatedAt = time.Now().Unix()
	return it
}

func (p *countingProvider) Refresh(it provider.Item) (provider.Item, bool) { return it, false }

func (p *countingProvider) count(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.enrich[id]
}

func (p *countingProvider) totalEnrich() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, v := range p.enrich {
		n += v
	}
	return n
}

// 第一页插入新帖、原有条目整体后移一格后：
//   - 新帖子要被解析一次
//   - 被挤到下一页的旧条目不得重新解析（身份是 站点:id，与页码无关）
func TestReinsertHeadNoReparse(t *testing.T) {
	p := newCounting("alpha",
		[]string{"a", "b", "c"}, // 第 1 页
		[]string{"d", "e"},      // 第 2 页
	)
	st := newTestStore(t, p)

	// 先抓两页并等解析完成。
	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("alpha", 2, true)
	waitParsed(t, st, 5)

	before := map[string]int{}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		before[id] = p.count(id)
		if before[id] != 1 {
			t.Fatalf("%s 首次应解析 1 次，实际 %d", id, before[id])
		}
	}

	// 上游第 1 页插入新帖 x：旧 a,b,c 被挤到第 2 页头部，d,e 再往后。
	p.mu.Lock()
	p.pages = [][]string{
		{"x", "a", "b"},
		{"c", "d", "e"},
	}
	p.mu.Unlock()

	// 重新同步这两页（force，绕过 TTL）。
	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("alpha", 2, true)
	waitParsed(t, st, 6)

	// 新帖解析一次。
	if got := p.count("x"); got != 1 {
		t.Errorf("新帖 x 应解析 1 次，实际 %d", got)
	}
	// 老条目一次都不该增加。
	for id, want := range before {
		if got := p.count(id); got != want {
			t.Errorf("%s 被移动了位置，不应重新解析：%d -> %d", id, want, got)
		}
	}
}

// waitParsed 等窗口内条目都解析出直链。
func waitParsed(t *testing.T, st *Store, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := 0
		for _, it := range st.FillWindowQuery(0, 500, "", "").Items {
			if provider.IsMediaURL(it.PlayURL) {
				ok++
			}
		}
		if ok >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等解析超时：目标是 %d 条", want)
}

// 展示顺序的核心保证：
// 第 1 页插入新帖后，全站序列必须「无重复、无遗漏、新帖在最前」。
func TestOrderNoDuplicateNoLoss(t *testing.T) {
	p := newCounting("alpha",
		[]string{"a", "b", "c"}, // 第 1 页
		[]string{"d", "e"},      // 第 2 页
	)
	st := newTestStore(t, p)

	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("alpha", 2, true)

	ids := func() []string {
		var out []string
		for _, it := range st.FillWindowQuery(0, 100, "", "alpha").Items {
			out = append(out, it.ID)
		}
		return out
	}

	if got := ids(); !equalStrings(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("初始顺序错误: %v", got)
	}

	// 上游第 1 页插入 x：旧 a,b,c 中的 c 被挤到第 2 页。
	p.mu.Lock()
	p.pages = [][]string{
		{"x", "a", "b"},
		{"c", "d", "e"},
	}
	p.mu.Unlock()

	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("alpha", 2, true)

	got := ids()
	want := []string{"x", "a", "b", "c", "d", "e"}
	if !equalStrings(got, want) {
		t.Errorf("插入新帖后顺序错误:\n got %v\nwant %v", got, want)
	}

	// 无重复
	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Errorf("出现重复条目 %s: %v", id, got)
		}
		seen[id] = true
	}
	// 无遗漏：每个入库的都是列表可见的
	for id := range seen {
		if id == "" {
			continue
		}
	}
}

// 第 1 页刷新两次（无新帖）不应打乱顺序，也不应产生重复。
func TestOrderIdempotentRefresh(t *testing.T) {
	p := newCounting("alpha", []string{"a", "b", "c"}, []string{"d", "e"})
	st := newTestStore(t, p)
	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("alpha", 2, true)

	for i := 0; i < 3; i++ {
		st.SyncPageSite("alpha", 1, true)
	}

	var got []string
	for _, it := range st.FillWindowQuery(0, 100, "", "alpha").Items {
		got = append(got, it.ID)
	}
	if !equalStrings(got, []string{"a", "b", "c", "d", "e"}) {
		t.Errorf("重复刷新后顺序应不变: %v", got)
	}
}

// 队列顺序需要落盘并在重启后恢复。
func TestOrderPersists(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "store.json")
	p := newCounting("alpha", []string{"a", "b"}, []string{"c", "d"})

	mk := func() *Store {
		return New(Options{
			File: file, ListTTL: time.Minute,
			Keep: time.Hour, Workers: 2, MaxItems: 100,
			Providers: []provider.Provider{p},
		})
	}

	st := mk()
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("alpha", 2, true)
	before := st.FillWindowQuery(0, 100, "", "").Items
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	cancel()

	st2 := mk()
	if err := st2.Load(); err != nil {
		t.Fatal(err)
	}
	after := st2.FillWindowQuery(0, 100, "", "").Items
	if len(before) != len(after) {
		t.Fatalf("恢复后条数不一致: %d vs %d", len(before), len(after))
	}
	for i := range before {
		if before[i].ID != after[i].ID {
			t.Errorf("第 %d 条顺序不一致: %s vs %s", i, before[i].ID, after[i].ID)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
