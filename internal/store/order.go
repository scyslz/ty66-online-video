package store

// 本文件维护「每个站点的展示顺序」——去重的双向有序队列。
//
// 为什么需要它：
//   pages 只表示「抓取记录」（某页抓过哪些 id），而展示顺序需要的是
//   「一条无重复、能插入头部、能追加尾部的有序序列」。两者若共用一个字段，
//   上游在第 1 页插入新帖时整页覆盖会让后面的页错位，造成展示上重复或漏条。
//
// 所以展示顺序单独用队列维护，队列本身就是去重的（map + 双向链表）：
//   - 刷新第 1 页：按上游顺序把该页条目移到队首，旧条目自然顺移一格
//   - 尾部补页    ：新条目追加到队尾
//   - 详情解析    ：不动队列
//   - 下架清理    ：从队列删除
//
// 所有方法均自带锁，调用方无需额外加锁；批量操作使用一次锁完成。

import (
	"container/list"
	"sync"
)

// orderQueue 是单个站点的展示顺序。
type orderQueue struct {
	mu sync.Mutex
	l  *list.List
	el map[string]*list.Element
}

func newOrderQueue() *orderQueue {
	return &orderQueue{l: list.New(), el: make(map[string]*list.Element)}
}

// Len 返回队列长度。
func (q *orderQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.l.Len()
}

// Has 判断键是否在队列中。
func (q *orderQueue) Has(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.el[key]
	return ok
}

// Prepend 把键插入队首（已存在则移动到队首）。
func (q *orderQueue) Prepend(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.prependLocked(key)
}

func (q *orderQueue) prependLocked(key string) {
	if e, ok := q.el[key]; ok {
		q.l.MoveToFront(e)
		return
	}
	q.el[key] = q.l.PushFront(key)
}

// Append 把键追加到队尾（已存在则不动）。
func (q *orderQueue) Append(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.el[key]; ok {
		return
	}
	q.el[key] = q.l.PushBack(key)
}

// Remove 删除一个键。
func (q *orderQueue) Remove(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if e, ok := q.el[key]; ok {
		q.l.Remove(e)
		delete(q.el, key)
	}
}

// PrependPage 用「上游该页的顺序」重建列表头部：
// 先把该页所有键按顺序放到最前面，再保留其余条目（自然顺移）。
//
// 这是「新数据插到最前、旧数据整体后移」的正确实现：
// 该页的键若已存在于队列中会被移动而不是复制，所以不会重复也不会遗漏。
func (q *orderQueue) PrependPage(keys []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// 逆序逐个前插，最终顺序与 keys 一致。
	for i := len(keys) - 1; i >= 0; i-- {
		q.prependLocked(keys[i])
	}
}

// AppendKeys 按顺序把键追加到队尾（已存在的跳过）。
func (q *orderQueue) AppendKeys(keys []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, key := range keys {
		if _, ok := q.el[key]; ok {
			continue
		}
		q.el[key] = q.l.PushBack(key)
	}
}

// Keys 返回当前顺序的浅拷贝；limit>0 时最多返回 limit 个。
func (q *orderQueue) Keys(limit int) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := q.l.Len()
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]string, 0, n)
	for e := q.l.Front(); e != nil && len(out) < n; e = e.Next() {
		out = append(out, e.Value.(string))
	}
	return out
}

// Snapshot 导出完整顺序（用于落盘）。
func (q *orderQueue) Snapshot() []string { return q.Keys(0) }

// Reset 用给定顺序整体重建队列（载入时使用）。
func (q *orderQueue) Reset(keys []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.l.Init()
	q.el = make(map[string]*list.Element, len(keys))
	for _, key := range keys {
		if _, ok := q.el[key]; ok {
			continue
		}
		q.el[key] = q.l.PushBack(key)
	}
}
