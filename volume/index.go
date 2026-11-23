// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"container/list"
	"sync"
)

type cachedObject struct {
	key   Key
	value *object
	cost  uint64
}
type objectCache struct {
	mu           sync.Mutex
	items        map[Key]*list.Element
	lru          *list.List
	entries      int
	budget, used uint64
}

func newCache(n int, b uint64) *objectCache {
	if n == 0 {
		n = 256
	}
	if b == 0 {
		b = 16 << 20
	}
	return &objectCache{items: make(map[Key]*list.Element), lru: list.New(), entries: n, budget: b}
}
func (c *objectCache) get(k Key) *object {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.items[k]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(cachedObject).value
	}
	return nil
}
func (c *objectCache) put(k Key, o *object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.items[k]; e != nil {
		c.used -= e.Value.(cachedObject).cost
		c.lru.Remove(e)
		delete(c.items, k)
	}
	cost := uint64(128 + len(o.Spans)*80)
	if c.entries < 0 || cost > c.budget {
		return
	}
	e := c.lru.PushFront(cachedObject{k, o, cost})
	c.items[k] = e
	c.used += cost
	for len(c.items) > c.entries || c.used > c.budget {
		e := c.lru.Back()
		x := e.Value.(cachedObject)
		delete(c.items, x.key)
		c.used -= x.cost
		c.lru.Remove(e)
	}
}
func (c *objectCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.items)
	c.lru.Init()
	c.used = 0
}

func (c *objectCache) remove(k Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.items[k]; e != nil {
		c.used -= e.Value.(cachedObject).cost
		c.lru.Remove(e)
		delete(c.items, k)
	}
}

// Index pages have an independent bounded LRU; no full directory is retained.
// Decoded nodes cost more than their encoded 4-KiB page, budgeted at 8 KiB each.
type cachedNode struct {
	off  uint64
	node treeNode
}
type nodeCache struct {
	mu    sync.Mutex
	items map[uint64]*list.Element
	lru   list.List
	limit int
}

func newNodeCache(entries int, budget uint64) *nodeCache {
	if entries == 0 {
		entries = 256
	}
	if budget == 0 {
		budget = 16 << 20
	}
	entries = min(entries, int(budget/8192))
	return &nodeCache{items: map[uint64]*list.Element{}, limit: entries}
}
func (c *nodeCache) get(off uint64) (treeNode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.items[off]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(cachedNode).node, true
	}
	return treeNode{}, false
}
func (c *nodeCache) put(off uint64, n treeNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.limit <= 0 {
		return
	}
	if e := c.items[off]; e != nil {
		e.Value = cachedNode{off, n}
		c.lru.MoveToFront(e)
		return
	}
	c.items[off] = c.lru.PushFront(cachedNode{off, n})
	for len(c.items) > c.limit {
		e := c.lru.Back()
		delete(c.items, e.Value.(cachedNode).off)
		c.lru.Remove(e)
	}
}
func (c *nodeCache) clear() { c.mu.Lock(); defer c.mu.Unlock(); clear(c.items); c.lru.Init() }
