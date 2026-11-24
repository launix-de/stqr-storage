// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"container/list"
	"crypto/sha256"
	"sync"
)

// Directory bodies are immutable. Cache their decoded representation by the
// blob selected by the current inode snapshot, never by a time-based inode TTL.
// Small inline bodies use a content digest. Mutable callers receive a copy.
type directoryCacheKey struct {
	id     ObjectID
	length uint64
	depth  uint8
	inline bool
	stamp  uint64
}
type directoryCacheValue struct {
	key     directoryCacheKey
	entries []FSEntry
	cost    uint64
}
type filesystemDirectoryCache struct {
	mu      sync.Mutex
	entries map[directoryCacheKey]*list.Element
	lru     list.List
	used    uint64
}

const directoryCacheBudget = 4 << 20
const directoryCacheEntries = 1024

func newFilesystemDirectoryCache() *filesystemDirectoryCache {
	return &filesystemDirectoryCache{entries: map[directoryCacheKey]*list.Element{}}
}
func directoryKey(blob fsBlob) (directoryCacheKey, bool) {
	if blob.source != (ObjectID{}) {
		return directoryCacheKey{}, false
	}
	key := directoryCacheKey{id: blob.root, length: blob.length, depth: blob.depth, stamp: blob.stamp}
	if blob.root == (ObjectID{}) {
		key.id = sha256.Sum256(blob.inline)
		key.inline = true
	}
	return key, true
}
func (c *filesystemDirectoryCache) get(key directoryCacheKey) ([]FSEntry, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el := c.entries[key]
	if el == nil {
		return nil, false
	}
	c.lru.MoveToFront(el)
	return el.Value.(directoryCacheValue).entries, true
}
func (c *filesystemDirectoryCache) put(key directoryCacheKey, entries []FSEntry) {
	if c == nil {
		return
	}
	cost := uint64(128 + len(entries)*64)
	for _, e := range entries {
		cost += uint64(len(e.Name))
	}
	if cost > directoryCacheBudget {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; ok {
		return
	}
	c.entries[key] = c.lru.PushFront(directoryCacheValue{key, entries, cost})
	c.used += cost
	for c.used > directoryCacheBudget || len(c.entries) > directoryCacheEntries {
		last := c.lru.Back()
		entry := last.Value.(directoryCacheValue)
		delete(c.entries, entry.key)
		c.lru.Remove(last)
		c.used -= entry.cost
	}
}
