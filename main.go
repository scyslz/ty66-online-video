// Command ty66-online-video 抓取上游列表、解析视频直链，并以网页形式浏览。
//
// 典型的单二进制部署：直接运行即可，页面已嵌入二进制内。
//
//	./ty66-online-video -addr :8000
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"paresevideo/internal/httpapi"
	"paresevideo/internal/provider"
	"paresevideo/internal/provider/siteutil"
	"paresevideo/internal/provider/t66y"
	"paresevideo/internal/provider/ys"
	"paresevideo/internal/store"
)

// version 由构建时注入（-ldflags "-X main.version=..."）。
var version = "dev"

// sites 返回要接入的来源站点，按展示优先级排列。
//
// 新增站点的做法：
//  1. 在 internal/provider/ 下新建子包，实现 provider.Provider
//  2. 在这里追加上去（第一个站点同时作为旧数据的归属站点）
//
// 站点之间互不影响：各自的列表页、详情页、播放地址解析都在自己的包里。
func sites() []provider.Provider {
	return []provider.Provider{
		t66y.New(t66y.DefaultConfig()),
		ys.New(ys.DefaultConfig()),
	}
}

func main() {
	showVersion := flag.Bool("version", false, "打印版本号并退出")
	root := flag.String("root", ".", "静态文件目录（没有 index.html 时使用内嵌页面）")
	addr := flag.String("addr", ":8000", "监听地址")
	storePath := flag.String("store", "", "映射文件路径（默认 <root>/store.json）")
	listTTL := flag.Duration("refresh", 3*time.Hour, "上游列表同步间隔")
	// 已废弃：直链不再按时间批量续签，改为播放失败时按需重解析。
	// 仍保留该参数，避免旧启动脚本传参时报错。
	flag.Duration("video-ttl", 0, "（已废弃，忽略）直链有效期")
	lazy := flag.Bool("lazy", true, "懒加载：只抓列表，直链等播放时按需解析（减少 403 风控）")
	keep := flag.Duration("keep", 30*24*time.Hour, "下架映射保留时长（0 表示永久）")
	workers := flag.Int("workers", 8, "详情解析并发数")
	maxItems := flag.Int("max-items", store.DefaultMaxItems, "映射最大条数")
	maxPages := flag.Int("max-pages", 30, "单站点最多深挖到第几页（防止翻尾时把上游全站抓一遍）")
	rateLimit := flag.Int("rate-limit", 60, "每主机每分钟最多请求数（滑动窗口，0 表示不限）")
	proxyOn := flag.Bool("proxy", true, "是否启用视频代理（线路二）；关闭后页面不显示线路二")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	// 全局限流：所有出站请求都按「每主机每分钟 N 次」控制。
	siteutil.Default.SetLimit(*rateLimit)

	file := *storePath
	if file == "" {
		file = filepath.Join(*root, "store.json")
	}

	st := store.New(store.Options{
		File:      file,
		ListTTL:   *listTTL,
		Keep:      *keep,
		Workers:   *workers,
		MaxItems:  *maxItems,
		DeepPages: *maxPages,
		Lazy:      *lazy,
		Providers: sites(),
	})
	if err := st.Load(); err != nil {
		log.Fatalf("[Init] %v", err)
	}
	if err := st.Save(); err != nil {
		log.Printf("[Init] 写入 %s 失败: %v", file, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st.Run(ctx)

	srv := &httpapi.Server{Store: st, Root: *root, ProxyEnabled: *proxyOn}
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Printf("[Exit] 正在停止服务")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	log.Printf("serving on %s (version=%s, root=%s, store=%s, refresh=%s, workers=%d, max-items=%d, max-pages=%d, rate-limit=%d/min, proxy=%v, lazy=%v)",
		*addr, version, *root, file, *listTTL, *workers, *maxItems, *maxPages, *rateLimit, *proxyOn, *lazy)

	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	st.Wait(ctx)
}
