package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"paresevideo/internal/provider"
)

// gauge 是跨站点共享的并发计数器，用来验证「多站点真的同时在抓」。
type gauge struct {
	mu     sync.Mutex
	active int
	peak   int
}

func (g *gauge) enter() {
	g.mu.Lock()
	g.active++
	if g.active > g.peak {
		g.peak = g.active
	}
	g.mu.Unlock()
}

func (g *gauge) leave() {
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
}

func (g *gauge) max() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peak
}

// pageProvider 是能在测试里精确控制页数与耗时的站点。
type pageProvider struct {
	name    string
	maxPage int
	delay   time.Duration
	shared  *gauge // 可选：跨站点并发计数

	mu      sync.Mutex
	fetched map[int]int // page -> 抓取次数
}

func newPageProvider(name string, maxPage int, delay time.Duration) *pageProvider {
	return &pageProvider{name: name, maxPage: maxPage, delay: delay, fetched: map[int]int{}}
}

func (p *pageProvider) withGauge(g *gauge) *pageProvider {
	p.shared = g
	return p
}

func (p *pageProvider) Name() string  { return p.name }
func (p *pageProvider) MaxPages() int { return 100 }

func (p *pageProvider) FetchList(page int) ([]provider.Item, error) {
	if p.shared != nil {
		p.shared.enter()
		defer p.shared.leave()
	}
	defer func() {
		p.mu.Lock()
		p.fetched[page]++
		p.mu.Unlock()
	}()

	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	if page > p.maxPage {
		return nil, nil // 到底了：返回空列表
	}
	return []provider.Item{{
		ID:        p.name + "-p" + itoa(page),
		Site:      p.name,
		Title:     p.name + " 第" + itoa(page) + "页",
		ThreadURL: "https://" + p.name + ".example/" + itoa(page),
	}}, nil
}

func (p *pageProvider) Enrich(it provider.Item) provider.Item {
	it.PlayURL = "https://cdn.example.com/" + it.ID + ".m3u8"
	it.Resolved = true
	it.Error = ""
	it.RetryAt = 0
	it.UpdatedAt = time.Now().Unix()
	return it
}

func (p *pageProvider) Refresh(it provider.Item) (provider.Item, bool) {
	return p.Enrich(it), true
}

func (p *pageProvider) count(page int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fetched[page]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func newPageStore(t *testing.T, providers ...provider.Provider) *Store {
	t.Helper()
	st := New(Options{
		ListGap:   -1, // 禁用列表节流，避免测试被节流挡住
		File:      filepath.Join(t.TempDir(), "store.json"),
		ListTTL:   time.Minute,
		Keep:      time.Hour,
		Workers:   2,
		MaxItems:  1000,
		Providers: providers,
	})
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	t.Cleanup(cancel)
	return st
}

// 缓存不足时，应同时向「所有站点」投递下一页请求（并发）。
func TestConcurrentNextPageAllSites(t *testing.T) {
	g := &gauge{}
	// 延迟拉长到 150ms：给 worker 足够的重叠窗口，避免偶发的时序错开。
	a := newPageProvider("alpha", 5, 150*time.Millisecond).withGauge(g)
	b := newPageProvider("beta", 5, 150*time.Millisecond).withGauge(g)
	c := newPageProvider("gamma", 5, 150*time.Millisecond).withGauge(g)
	st := newPageStore(t, a, b, c)

	// 首次读取：缓存为空，应给三个站点各投一次「第 1 页」。
	win := st.FillWindowQuery(0, 24, "", "")
	if win.Total != 0 {
		t.Fatalf("首次读取应为空，得到 %d", win.Total)
	}
	if !win.Loading {
		t.Error("应有 loading 标志（后台正在补页）")
	}
	if !win.HasMore {
		t.Error("还有未抓的页时 HasMore 应为 true")
	}

	// 等三个站点都抓完第 1 页。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.count(1) > 0 && b.count(1) > 0 && c.count(1) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, p := range []*pageProvider{a, b, c} {
		if p.count(1) != 1 {
			t.Errorf("%s 第 1 页应被抓 1 次，实际 %d", p.name, p.count(1))
		}
	}
	// 三站应并发抓取（峰值 >= 2，说明不是一个一个来）。
	if peak := g.max(); peak < 2 {
		t.Errorf("应并发抓取多个站点，峰值并发 %d", peak)
	}
}

// 已到最后一页的站点必须被跳过，不再产生请求。
func TestSkipExhaustedSite(t *testing.T) {
	// alpha 只有 1 页，beta 有很多页。
	a := newPageProvider("alpha", 1, 0)
	b := newPageProvider("beta", 10, 0)
	st := newPageStore(t, a, b)

	// 反复请求，让两个站点都翻到底。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		win := st.FillWindowQuery(0, 24, "", "")
		if !win.HasMore && !win.Loading {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// alpha 到第 1 页就到底：第 2 页应只被探一次（用于确认空页），之后不再请求。
	a.mu.Lock()
	alphaPage2 := a.fetched[2]
	a.mu.Unlock()
	if alphaPage2 > 1 {
		t.Errorf("alpha 已到底，第 2 页不应反复抓取，实际 %d 次", alphaPage2)
	}

	// 再读若干次，确认已到底的站点不再增长请求数。
	before := a.mu.Lock
	_ = before
	a.mu.Lock()
	beforeP1 := a.fetched[1]
	beforeP2 := a.fetched[2]
	a.mu.Unlock()

	for i := 0; i < 5; i++ {
		st.FillWindowQuery(0, 24, "", "")
		time.Sleep(20 * time.Millisecond)
	}

	a.mu.Lock()
	afterP1 := a.fetched[1]
	afterP2 := a.fetched[2]
	a.mu.Unlock()
	if afterP1 != beforeP1 || afterP2 != beforeP2 {
		t.Errorf("已到末尾的站点不应再被抓：p1 %d->%d, p2 %d->%d", beforeP1, afterP1, beforeP2, afterP2)
	}
}

// 缓存够用时不应发起任何补页请求。
func TestNoFetchWhenCacheEnough(t *testing.T) {
	a := newPageProvider("alpha", 10, 0)
	st := newPageStore(t, a)

	// 先让它抓满 2 页：请求 2 条 → 触发补页 → 等抓完。
	st.FillWindowQuery(0, 2, "", "alpha")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(st.CachedPagesBySite()["alpha"]) >= 2 {
			break
		}
		st.FillWindowQuery(0, 2, "", "alpha")
		time.Sleep(20 * time.Millisecond)
	}
	// 再读一次让 loading 收尾
	for time.Now().Before(time.Now().Add(300 * time.Millisecond)) {
		if w := st.FillWindowQuery(0, 2, "", "alpha"); !w.Loading {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	a.mu.Lock()
	before := len(a.fetched)
	a.mu.Unlock()

	// 只取 1 条，缓存远超需求。
	for i := 0; i < 3; i++ {
		st.FillWindowQuery(0, 1, "", "")
		time.Sleep(20 * time.Millisecond)
	}

	a.mu.Lock()
	after := len(a.fetched)
	a.mu.Unlock()
	if after != before {
		t.Errorf("缓存够用却继续抓页：页数 %d -> %d", before, after)
	}
}

// 未知站点：读窗口应返回空，且绝不能创建页状态或发起请求。
func TestUnknownSiteCreatesNoState(t *testing.T) {
	a := newPageProvider("alpha", 5, 0)
	st := newPageStore(t, a)

	win := st.FillWindowQuery(0, 24, "", "nope")
	if win.Total != 0 || len(win.Items) != 0 {
		t.Errorf("未知站点应返回空，得到 total=%d items=%d", win.Total, len(win.Items))
	}
	if win.Loading {
		t.Error("未知站点不应有 loading（没有站点可抓）")
	}
	if win.HasMore {
		t.Error("未知站点不应报告 has_more")
	}

	time.Sleep(150 * time.Millisecond)
	if got := st.CachedPagesBySite(); len(got) != 0 {
		t.Errorf("不应创建任何页状态，实际 %v", got)
	}
	if c := a.count(1); c != 0 {
		t.Errorf("不应向已注册站点发请求，实际 %d 次", c)
	}
}

// failingProvider 固定对某些页返回错误，模拟上游 403 限流。
type failingProvider struct {
	*pageProvider
	failFrom int
}

func (f *failingProvider) FetchList(page int) ([]provider.Item, error) {
	if page >= f.failFrom {
		f.pageProvider.mu.Lock()
		f.pageProvider.fetched[page]++
		f.pageProvider.mu.Unlock()
		return nil, errFake403
	}
	return f.pageProvider.FetchList(page)
}

var errFake403 = &fakeErr{"403 Forbidden"}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }

// 连续失败达到阈值后应判定为边界，之后不再无限重试（403 场景）。
func TestStopAfterRepeatedPageFailures(t *testing.T) {
	base := newPageProvider("alpha", 3, 0) // 前 3 页正常
	p := &failingProvider{pageProvider: base, failFrom: 4}
	st := newPageStore(t, p)

	// 反复请求深尾，触发补页。
	deadline := time.Now().Add(6 * time.Second)
	var win Window
	for time.Now().Before(deadline) {
		win = st.FillWindowQuery(0, 500, "", "alpha")
		if !win.HasMore && !win.Loading {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 熔断后不应再有新的补页请求（has_more 反映硬边界，可能仍为 true）。
	if win.Loading {
		t.Errorf("熔断后不应仍在加载，实际 loading=true")
	}

	// 第 4 页失败次数不应无限增长。
	base.mu.Lock()
	fails := base.fetched[4]
	base.mu.Unlock()
	if fails > siteFailsCooldown+2 {
		t.Errorf("第 4 页失败重试次数过多: %d", fails)
	}

	// 再读若干次，确认不再请求。
	before := fails
	for i := 0; i < 5; i++ {
		st.FillWindowQuery(0, 500, "", "alpha")
		time.Sleep(30 * time.Millisecond)
	}
	base.mu.Lock()
	after := base.fetched[4]
	base.mu.Unlock()
	if after != before {
		t.Errorf("判定边界后不应再请求：%d -> %d", before, after)
	}
}

// 深挖上限：即使上游还有内容，也不应超过 DeepPages。
func TestDeepPageLimit(t *testing.T) {
	p := newPageProvider("alpha", 100, 0) // 上游有 100 页
	st := New(Options{
		File:      filepath.Join(t.TempDir(), "store.json"),
		ListTTL:   time.Minute,
		Keep:      time.Hour,
		Workers:   2,
		MaxItems:  5000,
		DeepPages: 4, // 只允许挖 4 页
		Providers: []provider.Provider{p},
	})
	ctx, cancel := context.WithCancel(context.Background())
	st.Run(ctx)
	t.Cleanup(cancel)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st.FillWindowQuery(0, 5000, "", "alpha")
		if !st.HasMore("alpha") && !st.FillWindowQuery(0, 24, "", "alpha").Loading {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := len(st.CachedPagesBySite()["alpha"]); got != 4 {
		t.Errorf("应只抓 4 页，实际 %d 页", got)
	}
	if st.HasMore("alpha") {
		t.Error("达到深挖上限后 has_more 应为 false")
	}
	// 第 5 页一次都不该被请求。
	if c := p.count(5); c != 0 {
		t.Errorf("超过上限的页不应被抓，第 5 页被请求 %d 次", c)
	}
}

// 冷却期内不应再投递补页请求。
func TestCooldownStopsRequests(t *testing.T) {
	base := newPageProvider("alpha", 3, 0)
	p := &failingProvider{pageProvider: base, failFrom: 4}
	st := newPageStore(t, p)

	// 触发若干次失败，让站点进入冷却。
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		st.FillWindowQuery(0, 5000, "", "alpha")
		if st.BreakerState("alpha") == "open" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	base.mu.Lock()
	before := base.fetched[4]
	base.mu.Unlock()

	// 冷却期内：不应发起新请求（先让在途请求落地）。
	time.Sleep(300 * time.Millisecond)
	for i := 0; i < 5; i++ {
		st.FillWindowQuery(0, 5000, "", "alpha")
		time.Sleep(25 * time.Millisecond)
	}

	base.mu.Lock()
	after := base.fetched[4]
	base.mu.Unlock()
	if after != before {
		t.Errorf("冷却期内不应发起请求：%d -> %d", before, after)
	}
}

// 熔断后必须在冷却结束时自动恢复：这是「站点判断停止何时恢复」的核心保证。
func TestBreakerRecoversAfterCooldown(t *testing.T) {
	// 把冷却缩短到 300ms，避免测试真的等半小时。
	oldBase, oldMax := siteCooldownBase, maxSiteCooldown
	siteCooldownBase, maxSiteCooldown = 300*time.Millisecond, 2*time.Second
	t.Cleanup(func() { siteCooldownBase, maxSiteCooldown = oldBase, oldMax })

	base := newPageProvider("alpha", 3, 0)
	p := &failingProvider{pageProvider: base, failFrom: 4}
	st := newPageStore(t, p)

	// 1) 触发熔断
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		st.FillWindowQuery(0, 5000, "", "alpha")
		if st.BreakerState("alpha") == "open" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := st.BreakerState("alpha"); got != "open" {
		t.Fatalf("应进入熔断，实际状态 %q", got)
	}

	base.mu.Lock()
	before := base.fetched[4]
	base.mu.Unlock()

	// 2) 冷却期内不抓
	time.Sleep(200 * time.Millisecond)
	st.FillWindowQuery(0, 5000, "", "alpha")
	time.Sleep(50 * time.Millisecond)
	base.mu.Lock()
	duringCool := base.fetched[4]
	base.mu.Unlock()
	if duringCool != before {
		t.Errorf("冷却期内不应请求：%d -> %d", before, duringCool)
	}

	// 3) 上游恢复 + 冷却结束 => 应自动试探并解除熔断
	p.failFrom = 1000
	deadline2 := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline2) {
		st.FillWindowQuery(0, 50, "", "alpha")
		if st.BreakerState("alpha") == "closed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := st.BreakerState("alpha"); got != "closed" {
		t.Fatalf("上游恢复后应解除熔断，实际状态 %q", got)
	}

	// 4) 恢复后应能继续正常抓取（前 3 页有内容）
	for page := 1; page <= 3; page++ {
		if n := st.SyncPageSite("alpha", page, false); n == 0 {
			t.Errorf("恢复后第 %d 页应能抓到内容", page)
		}
	}
	if got := len(st.CachedPagesBySite()["alpha"]); got < 3 {
		t.Errorf("恢复后应有 3 页，实际 %d", got)
	}
}

// 去重核心：同一页在排队/抓取期间，无论被请求多少次都只抓一次。
func TestNoDuplicateQueueing(t *testing.T) {
	p := newPageProvider("alpha", 5, 80*time.Millisecond) // 加延迟放大竞态窗口
	st := newPageStore(t, p)

	// 并发触发 50 次补页请求，全部指向同一页。
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st.RequestNextPage("alpha")
		}()
	}
	wg.Wait()
	st.RequestNextPage("alpha")

	// 等第 1 页抓完。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(st.CachedPagesBySite()["alpha"]) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // 让潜在的重复请求也落地

	if c := p.count(1); c != 1 {
		t.Errorf("并发的 51 次请求只应抓 1 次，实际 %d 次", c)
	}
}

// 排队中的页不应被重复入队（直接检查队列长度）。
func TestQueueDedup(t *testing.T) {
	p := newPageProvider("alpha", 5, 60*time.Millisecond)
	st := newPageStore(t, p)

	// 只入队，不等待抓取完成。
	for i := 0; i < 20; i++ {
		st.RequestNextPage("alpha")
	}
	// 同一页只能占一个独立任务（排队或在抓，都只算一个）。
	if n := st.pendingPageCount(); n > 1 {
		t.Errorf("同一页应只占一个任务，实际 %d", n)
	}
}

// 多站点：每个站点各自一页，互不干扰且不重复。
func TestQueueDedupMultiSite(t *testing.T) {
	a := newPageProvider("alpha", 5, 300*time.Millisecond)
	b := newPageProvider("beta", 5, 300*time.Millisecond)
	st := newPageStore(t, a, b)

	// 先只抓一次，让两个站点各排一页（此时 worker 正在抓，尚未完成）。
	st.RequestNextPage("")
	if n := st.pendingPageCount(); n > 2 {
		t.Errorf("两个站点应最多占两个任务，实际 %d", n)
	}

	// 抓取期间反复请求：不应增加任务数。
	for i := 0; i < 10; i++ {
		st.RequestNextPage("")
	}
	if n := st.pendingPageCount(); n > 2 {
		t.Errorf("抓取中不应重复入队，实际 %d", n)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(st.CachedPagesBySite()["alpha"]) >= 1 && len(st.CachedPagesBySite()["beta"]) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if c := a.count(1); c != 1 {
		t.Errorf("alpha 第 1 页应抓 1 次，实际 %d", c)
	}
	if c := b.count(1); c != 1 {
		t.Errorf("beta 第 1 页应抓 1 次，实际 %d", c)
	}
}

// 抓取结束后标记必须释放，否则该页永远无法再次刷新。
func TestQueueFlagReleasedAfterSync(t *testing.T) {
	p := newPageProvider("alpha", 5, 0)
	st := newPageStore(t, p)

	st.RequestNextPage("alpha")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(st.CachedPagesBySite()["alpha"]) >= 1 && st.pendingPageCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 抓完后标记必须已释放，否则该页永远无法再次刷新。
	if n := st.pendingPageCount(); n != 0 {
		t.Errorf("抓取结束后不应残留标记，实际 %d", n)
	}
}

// 过滤某站点时，即使该站点内容很少（分页不足），也不应请求其它站点。
func TestFilteredSiteShortNeverTouchesOthers(t *testing.T) {
	a := newPageProvider("alpha", 20, 0)
	b := newPageProvider("beta", 2, 0) // beta 只有 2 页
	st := newPageStore(t, a, b)

	// beta 只有很少内容，请求 24 条永远"不足"，会反复触发补页。
	for i := 0; i < 10; i++ {
		st.FillWindowQuery(0, 24, "", "beta")
		time.Sleep(30 * time.Millisecond)
	}

	if c := a.count(1); c != 0 {
		t.Errorf("过滤 beta 时 alpha 不应被抓，实际 %d 次", c)
	}
	// beta 抓到末尾后应停止（第 3 页探一次即止）。
	if c := b.count(3); c > 1 {
		t.Errorf("beta 到末尾后不应反复抓第 3 页，实际 %d 次", c)
	}
}
