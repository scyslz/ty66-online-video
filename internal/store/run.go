package store

// 本文件负责流水线的启动与定时任务。

import (
	"context"
	"log"
	"time"
)

// Run 启动两条流水线与后台保存，直到 ctx 取消。
// 返回时会把内存中的数据落盘。
func (s *Store) Run(ctx context.Context) {
	go s.listLoop(ctx)
	go s.detailLoop(ctx)
	go s.saveLoop(ctx)

	// 周期性检查：换掉快过期的直链签名，并重试此前的失败条目。
	go func() {
		ticker := time.NewTicker(s.listTTL)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.RefreshStale()
			}
		}
	}()
}

// Wait 等待 ctx 结束后保存一次数据。
func (s *Store) Wait(ctx context.Context) {
	<-ctx.Done()
	log.Printf("[Exit] 正在保存 %s", s.file)
	if err := s.Save(); err != nil {
		log.Printf("[Exit] 保存失败: %v", err)
	}
}
