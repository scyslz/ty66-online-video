package httpapi

// 本文件把前端页面嵌入二进制，使发布包成为单文件、无需附带 index.html。

import (
	"embed"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
)

//go:embed index.html favicon.svg
var embedded embed.FS

// assetHandler 返回静态资源处理器。
// 磁盘上存在 <root>/index.html 时优先使用（便于改完即刷新预览），
// 否则回退到内嵌版本，保证单个二进制即可运行。
func assetHandler(root string) http.Handler {
	if root != "" {
		if _, err := os.Stat(filepath.Join(root, "index.html")); err == nil {
			return http.FileServer(http.Dir(root))
		}
	}
	sub, err := fs.Sub(embedded, ".")
	if err != nil {
		return http.NotFoundHandler()
	}
	return http.FileServer(http.FS(sub))
}
