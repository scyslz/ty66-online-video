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
	"paresevideo/internal/store"
)

// version 由构建时注入（-ldflags "-X main.version=..."）。
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "打印版本号并退出")
	root := flag.String("root", ".", "静态文件目录（没有 index.html 时使用内嵌页面）")
	addr := flag.String("addr", ":8000", "监听地址")
	storePath := flag.String("store", "", "映射文件路径（默认 <root>/store.json）")
	listTTL := flag.Duration("refresh", 15*time.Minute, "上游列表同步间隔")
	videoTTL := flag.Duration("video-ttl", 6*time.Hour, "直链有效期，超时后换新签名")
	keep := flag.Duration("keep", 30*24*time.Hour, "下架映射保留时长（0 表示永久）")
	workers := flag.Int("workers", 8, "详情解析并发数")
	maxItems := flag.Int("max-items", store.DefaultMaxItems, "映射最大条数")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	file := *storePath
	if file == "" {
		file = filepath.Join(*root, "store.json")
	}

	st := store.New(store.Options{
		File:     file,
		ListTTL:  *listTTL,
		VideoTTL: *videoTTL,
		Keep:     *keep,
		Workers:  *workers,
		MaxItems: *maxItems,
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

	srv := &httpapi.Server{Store: st, Root: *root}
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

	log.Printf("serving on %s (version=%s, root=%s, store=%s, refresh=%s, workers=%d, max-items=%d)",
		*addr, version, *root, file, *listTTL, *workers, *maxItems)

	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	st.Wait(ctx)
}
