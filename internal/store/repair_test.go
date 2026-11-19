package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"paresevideo/internal/provider"
)

// signingProvider 每次解析都返回一条新签名地址，用来验证懒加载：
// 后台不会主动换签名，只有按需修复才会请求。
type signingProvider struct {
	name    string
	mu      sync.Mutex
	enrich  int
	refresh int
	sign    int
	fail    bool // 为 true 时拿不到直链（模拟上游限流）
}

func (p *signingProvider) Name() string  { return p.name }
func (p *signingProvider) MaxPages() int { return 10 }

func (p *signingProvider) FetchList(page int) ([]provider.Item, error) {
	if page != 1 {
		return nil, nil
	}
	return []provider.Item{{
		ID:        "a1",
		Site:      p.name,
		Title:     "测试条目",
		ThreadURL: "https://" + p.name + ".example/a1",
	}}, nil
}

func (p *signingProvider) next() string {
	p.sign++
	return fmt.Sprintf("https://cdn.example.com/a1/list.m3u8?k=%d", p.sign)
}

func (p *signingProvider) Enrich(it provider.Item) provider.Item {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enrich++
	it.PlayPage = "https://play.example.com/video.html?id=a1"
	it.UpdatedAt = time.Now().Unix()
	if p.fail {
		it.Error = "请求过于频繁，稍后重试"
		it.RetryAt = time.Now().Add(time.Minute).Unix()
		return it
	}
	it.PlayURL = p.next()
	it.Resolved = true
	it.Error = ""
	it.RetryAt = 0
	return it
}

func (p *signingProvider) Refresh(it provider.Item) (provider.Item, bool) {
	p.mu.Lock()
	p.refresh++
	p.mu.Unlock()
	return p.Enrich(it), true
}

func (p *signingProvider) counts() (enrich, refresh int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.enrich, p.refresh
}

// 后台不应因为「时间久了」就主动给已有直链换签名。
func TestNoProactiveRefresh(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	st := newTestStore(t, p)

	st.SyncPageSite("alpha", 1, true)
	waitParsed(t, st, 1)

	beforeEnrich, beforeRefresh := p.counts()

	// 连续多轮后台扫描：已解析出的直链必须一动不动。
	for i := 0; i < 5; i++ {
		st.RefreshStale()
	}
	time.Sleep(150 * time.Millisecond)

	if enrich, refresh := p.counts(); enrich != beforeEnrich || refresh != beforeRefresh {
		t.Errorf("后台不该重新解析已完成条目: enrich %d->%d, refresh %d->%d",
			beforeEnrich, enrich, beforeRefresh, refresh)
	}
}

// 播放失败时按需重解析：只打播放页、换到新地址，并写回映射。
func TestRepairOnDemand(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	st := newTestStore(t, p)

	st.SyncPageSite("alpha", 1, true)
	waitParsed(t, st, 1)

	old := st.FillWindowQuery(0, 10, "", "").Items[0].PlayURL
	if !provider.IsMediaURL(old) {
		t.Fatalf("前置条件不成立: %q", old)
	}

	res := st.Repair("alpha", "a1")
	if !res.Changed {
		t.Fatalf("按需修复应换到新地址: %+v", res)
	}
	if res.Item.PlayURL == old {
		t.Errorf("地址应被换新: %q", res.Item.PlayURL)
	}
	// 有播放页时应只打播放页，不重抓详情页。
	if _, refresh := p.counts(); refresh == 0 {
		t.Error("应走 Refresh（只打播放页）")
	}

	// 新地址要真的落到映射里，后续读取能拿到。
	got := st.FillWindowQuery(0, 10, "", "").Items[0].PlayURL
	if got != res.Item.PlayURL {
		t.Errorf("映射未更新: %q != %q", got, res.Item.PlayURL)
	}
}

// 上游仍限流、拿不到直链时不谎报成功。
func TestRepairReportsFailure(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	st := newTestStore(t, p)

	st.SyncPageSite("alpha", 1, true)
	waitParsed(t, st, 1)

	p.mu.Lock()
	p.fail = true
	p.mu.Unlock()

	res := st.Repair("alpha", "a1")
	if res.Changed {
		t.Errorf("没拿到新地址不该报 changed: %+v", res)
	}
	if res.Item.Error == "" {
		t.Error("应保留失败原因")
	}
}

// 同一条目并发按需修复只应发出一次上游请求。
func TestRepairDedup(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	st := newTestStore(t, p)

	st.SyncPageSite("alpha", 1, true)
	waitParsed(t, st, 1)

	enrichBefore, refreshBefore := p.counts()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st.Repair("alpha", "a1")
		}()
	}
	wg.Wait()

	enrich, refresh := p.counts()
	if enrich-refresh > 0 {
		t.Logf("额外详情请求 %d 次（并发落在不同阶段属正常）", enrich-enrichBefore)
	}
	// 最多允许 1 次真实解析（TryLock 抢到的那个），其余应直接复用结果。
	if got := refresh - refreshBefore; got > 1 {
		t.Errorf("并发修复应合并为 1 次请求，实际 %d 次", got)
	}
}

// 旧的「有直链但挂着限额错误」脏数据应被清理掉。
func TestRefreshStaleHealsThrottledErrors(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	st := newTestStore(t, p)

	st.SyncPageSite("alpha", 1, true)
	waitParsed(t, st, 1)

	// 人为制造旧版本留下的状态：有直链，但错误标记还在。
	st.mu.Lock()
	it := st.items[itemKey("alpha", "a1")]
	it.Error = "请求过于频繁，稍后重试"
	it.RetryAt = time.Now().Add(-time.Hour).Unix()
	st.mu.Unlock()

	st.RefreshStale()

	st.mu.Lock()
	it = st.items[itemKey("alpha", "a1")]
	err, retry := it.Error, it.RetryAt
	st.mu.Unlock()

	if err != "" || retry != 0 {
		t.Errorf("有直链的条目不该留错误标记: error=%q retry=%d", err, retry)
	}
}

// 懒加载：抓列表不主动抓详情，条目保持「无直链」，播放时才解析。
func TestLazyDoesNotFetchDetails(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	dir := t.TempDir()
	st := New(Options{
		File:      filepath.Join(dir, "store.json"),
		ListTTL:   time.Minute,
		Keep:      time.Hour,
		Workers:   2,
		MaxItems:  100,
		Lazy:      true,
		Providers: []provider.Provider{p},
	})
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	t.Cleanup(cancel)

	st.SyncPageSite("alpha", 1, true)
	win := waitTotal(t, st, "alpha", 1)

	// 列表已经抓到，但不该有任何详情请求。
	time.Sleep(200 * time.Millisecond)
	st.RefreshStale()
	time.Sleep(100 * time.Millisecond)
	if enrich, refresh := p.counts(); enrich != 0 || refresh != 0 {
		t.Errorf("懒加载模式不应主动抓详情: enrich=%d refresh=%d", enrich, refresh)
	}
	if win.Total != 1 {
		t.Fatalf("列表应有 1 条，得到 %d", win.Total)
	}
	if got := win.Items[0].PlayURL; got != "" {
		t.Errorf("懒加载下列表不应带直链，得到 %q", got)
	}
	if win.Pending != 0 {
		t.Errorf("懒加载下不应显示「解析中」，得到 pending=%d", win.Pending)
	}

	// 用户点播放：按需解析一次并写回。
	res := st.Repair("alpha", "a1")
	if !provider.IsMediaURL(res.Item.PlayURL) {
		t.Fatalf("按需解析应拿到直链: %+v", res)
	}
	if enrich, _ := p.counts(); enrich != 1 {
		t.Errorf("按需解析应只请求 1 次，实际 %d", enrich)
	}
	// 解析结果落库，列表页再读就有地址了。
	if got := st.FillWindowQuery(0, 10, "", "").Items[0].PlayURL; got != res.Item.PlayURL {
		t.Errorf("解析结果未写回映射: %q != %q", got, res.Item.PlayURL)
	}
}

// 列表同步状态应能看出「上游上次同步时间」，供前端判断有没有更新。
func TestSiteSyncs(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	st := newTestStore(t, p)
	st.SyncPageSite("alpha", 1, true)

	rows := st.SiteSyncs()
	if len(rows) != 1 {
		t.Fatalf("应有 1 个站点的同步状态，得到 %d", len(rows))
	}
	row := rows[0]
	if row.Site != "alpha" {
		t.Errorf("站点名错误: %q", row.Site)
	}
	if row.LastSync == 0 {
		t.Error("同步成功后应有 last_sync")
	}
	// 条数按展示队列（去重链表）统计，不再按页快照累加。
	if row.Items != 1 {
		t.Errorf("条目数错误: items=%d，应为展示队列的 1 条", row.Items)
	}
	// SyncPageSite 是手动同步，不算增量轮，Added 应为 0。
	if row.Added != 0 {
		t.Errorf("手动同步不应计入增量新增数，added=%d", row.Added)
	}
	if row.Breaker != "closed" {
		t.Errorf("正常站点熔断状态应为 closed，得到 %q", row.Breaker)
	}
}

// 同一条目同时挂在多个页快照里时，展示队列只算一次，不能按页累加重复计数。
func TestSiteSyncsItemsAreDeduplicated(t *testing.T) {
	// 第 1 页与新帖：两页都包含 "shared"，页快照里它会被记两次。
	p := newPageTracking("alpha", []string{"shared", "only1"}, []string{"shared", "only2"})
	st := newTestStore(t, p)
	st.SyncPageSite("alpha", 1, true)
	st.SyncPageSite("alpha", 2, true)

	rows := st.SiteSyncs()
	if len(rows) != 1 {
		t.Fatalf("应有 1 个站点的同步状态，得到 %d", len(rows))
	}
	if got := rows[0].Items; got != 3 {
		t.Errorf("去重后应有 3 条（shared/only1/only2），items=%d", got)
	}
}

// 增量同步后，面板应显示该轮真正新增的条数（对应日志「更新 N 条」）。
func TestSiteSyncsAddedFromIncremental(t *testing.T) {
	p := newPageTracking("alpha",
		[]string{"a1", "a2"},
		[]string{"b1", "b2"},
	)
	st := newSyncTestStore(t, p)

	// 首轮增量：两页都是新条目，新增 4 条。
	incremental(t, st, "alpha")
	rows := st.SiteSyncs()
	if len(rows) != 1 || rows[0].Added != 4 {
		t.Fatalf("首轮增量新增应为 4，得到 %+v", rows)
	}
	if rows[0].LastSync == 0 {
		t.Error("增量同步后 last_sync 应指向该轮更新时间")
	}

	// 上游第 1 页再插一条新帖：第二轮新增 1 条。
	p.setPages([]string{"new", "a1", "a2"}, []string{"b1", "b2"})
	incremental(t, st, "alpha")
	rows = st.SiteSyncs()
	if rows[0].Added != 1 {
		t.Errorf("第二轮增量新增应为 1，得到 added=%d", rows[0].Added)
	}
}

// pageTrackingProvider 记录每页被抓了几次，并允许改写页内容。
type pageTrackingProvider struct {
	name    string
	mu      sync.Mutex
	pages   [][]string
	fetched map[int]int
}

func newPageTracking(name string, pages ...[]string) *pageTrackingProvider {
	return &pageTrackingProvider{name: name, pages: pages, fetched: map[int]int{}}
}

func (p *pageTrackingProvider) Name() string  { return p.name }
func (p *pageTrackingProvider) MaxPages() int { return 100 }

func (p *pageTrackingProvider) FetchList(page int) ([]provider.Item, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fetched[page]++
	if page < 1 || page > len(p.pages) {
		return nil, nil
	}
	out := make([]provider.Item, 0, len(p.pages[page-1]))
	for _, id := range p.pages[page-1] {
		out = append(out, provider.Item{
			ID: id, Site: p.name, Title: p.name + "-" + id,
			ThreadURL: "https://" + p.name + ".example/" + id,
		})
	}
	return out, nil
}

func (p *pageTrackingProvider) Enrich(it provider.Item) provider.Item {
	it.PlayURL = "https://cdn.example.com/" + it.ID + ".m3u8"
	it.Resolved = true
	it.UpdatedAt = time.Now().Unix()
	return it
}

func (p *pageTrackingProvider) Refresh(it provider.Item) (provider.Item, bool) {
	return p.Enrich(it), true
}

func (p *pageTrackingProvider) setPages(pages ...[]string) {
	p.mu.Lock()
	p.pages = pages
	p.mu.Unlock()
}

// newSyncTestStore 用于增量同步测试：TTL 极短，方便连续触发抓取。
// 不启动 detailLoop（这些用例只关心列表合并与展示顺序）。
func newSyncTestStore(t *testing.T, providers ...provider.Provider) *Store {
	t.Helper()
	dir := t.TempDir()
	return New(Options{
		File:      filepath.Join(dir, "store.json"),
		ListTTL:   time.Millisecond,
		Keep:      time.Hour,
		Workers:   1,
		MaxItems:  1000,
		Providers: providers,
	})
}

// syncPage 同步一页并让 TTL 失效，便于下一步继续抓。
func syncPage(t *testing.T, st *Store, site string, page int) {
	t.Helper()
	st.SyncPageSite(site, page, true)
	time.Sleep(3 * time.Millisecond)
}

// incremental 跑一轮增量同步并等 TTL 失效。
func incremental(t *testing.T, st *Store, site string) {
	t.Helper()
	time.Sleep(3 * time.Millisecond)
	st.syncIncrementalSite(site)
	time.Sleep(3 * time.Millisecond)
}

// 增量同步：从第 1 页往下翻，撞上本地已有 ID 就停。
// 已缓存的深层页不该被反复抓。
func TestIncrementalSyncStopsAtKnownID(t *testing.T) {
	p := newPageTracking("alpha",
		[]string{"a1", "a2"},
		[]string{"b1", "b2"},
		[]string{"c1", "c2"},
	)
	st := newSyncTestStore(t, p)
	for page := 1; page <= 3; page++ {
		syncPage(t, st, "alpha", page)
	}

	// 清空计数，只统计这一轮。
	p.mu.Lock()
	p.fetched = map[int]int{}
	p.mu.Unlock()

	incremental(t, st, "alpha")

	p.mu.Lock()
	got := map[int]int{}
	for k, v := range p.fetched {
		got[k] = v
	}
	p.mu.Unlock()

	// 第 1 页全是老内容（a1/a2 都在本地）→ 只翻 1 页就停。
	if got[1] == 0 {
		t.Errorf("第 1 页必须被同步，计数 %v", got)
	}
	if got[2] != 0 || got[3] != 0 {
		t.Errorf("追平后就该停，不该继续翻深层页: %v", got)
	}
}

// 上游第 1 页全是新帖、老内容被挤到第 2 页时，应继续翻到第 2 页，
// 直到撞上老 ID，并把新增的插到最前。
func TestIncrementalSyncInsertsNewAtFront(t *testing.T) {
	p := newPageTracking("alpha",
		[]string{"a", "b", "c"},
		[]string{"d", "e"},
	)
	st := newSyncTestStore(t, p)
	syncPage(t, st, "alpha", 1)
	syncPage(t, st, "alpha", 2)

	ids := func() []string {
		var out []string
		for _, it := range st.FillWindowQuery(0, 100, "", "alpha").Items {
			out = append(out, it.ID)
		}
		return out
	}
	if base := ids(); !equalStrings(base, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("初始顺序错误: %v", base)
	}

	// 3 小时后：上游第 1 页插入 x,y，a 被挤到第 2 页。
	p.setPages([]string{"x", "y", "a", "b"}, []string{"c", "d", "e"})

	incremental(t, st, "alpha")

	got := ids()
	want := []string{"x", "y", "a", "b", "c", "d", "e"}
	if !equalStrings(got, want) {
		t.Errorf("增量同步后顺序错误:\n got %v\nwant %v", got, want)
	}

	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Errorf("出现重复条目 %s: %v", id, got)
		}
		seen[id] = true
	}
	if len(seen) != 7 {
		t.Errorf("条数不对，应 7 条，得到 %d: %v", len(seen), got)
	}
}

// 一直撞不到本地已有 ID（等于首次同步）时，最多翻 maxIncrementalPages 页。
func TestIncrementalSyncCapsPages(t *testing.T) {
	pages := make([][]string, 0, maxIncrementalPages+2)
	for i := 1; i <= maxIncrementalPages+2; i++ {
		pages = append(pages, []string{fmt.Sprintf("p%d-1", i), fmt.Sprintf("p%d-2", i)})
	}
	p := newPageTracking("alpha", pages...)
	st := newSyncTestStore(t, p)

	incremental(t, st, "alpha")

	p.mu.Lock()
	fetched := len(p.fetched)
	p.mu.Unlock()
	if fetched != maxIncrementalPages {
		t.Errorf("应最多翻 %d 页，实际翻了 %d 页", maxIncrementalPages, fetched)
	}
}

// 新增的条目必须排在最前（用户看到的顺序：最新的在最上面）。
func TestIncrementalSyncNewItemsGoFirst(t *testing.T) {
	p := newPageTracking("alpha", []string{"old1", "old2"})
	st := newSyncTestStore(t, p)
	syncPage(t, st, "alpha", 1)

	p.setPages([]string{"new1", "new2", "old1", "old2"})
	incremental(t, st, "alpha")

	var got []string
	for _, it := range st.FillWindowQuery(0, 100, "", "alpha").Items {
		got = append(got, it.ID)
	}
	if !equalStrings(got, []string{"new1", "new2", "old1", "old2"}) {
		t.Errorf("新帖应排在最前: %v", got)
	}
}

// 懒加载模式下，增量同步列表不该触发任何详情解析。
func TestIncrementalSyncDoesNotFetchDetailsInLazy(t *testing.T) {
	p := &signingProvider{name: "alpha"}
	dir := t.TempDir()
	st := New(Options{
		File: filepath.Join(dir, "store.json"), ListTTL: time.Minute,
		Keep: time.Hour, Workers: 2, MaxItems: 100, Lazy: true,
		Providers: []provider.Provider{p},
	})
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	t.Cleanup(cancel)

	st.SyncPageSite("alpha", 1, true)
	st.RefreshStale()
	st.syncIncrementalSite("alpha")
	time.Sleep(150 * time.Millisecond)

	if enrich, refresh := p.counts(); enrich != 0 || refresh != 0 {
		t.Errorf("懒加载下增量同步不应抓详情: enrich=%d refresh=%d", enrich, refresh)
	}
}
