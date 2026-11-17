#!/usr/bin/env bash
# 构建并启动服务。
#   ./run.sh                 # 监听 :8000
#   ADDR=:8080 ./run.sh      # 换端口
#   ./run.sh -workers 12     # 透传其它参数
set -euo pipefail
cd "$(dirname "$0")"
go build -o paresevideo .
exec ./paresevideo -addr "${ADDR:-:8000}" -root . "$@"
