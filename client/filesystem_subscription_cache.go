// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"container/list"
	"context"
	"errors"
	"io"
	"sync"
	"weak"
)

// SubscriptionStore caches ordinary object snapshots only while an active
// subscription proves their freshness. It does not use a time-based validity TTL.
// Native CAS remains authoritative; mutations never trust a cached precondition.
// Payload range reads keep the streaming/range path rather than populating a
// second whole-file cache beside the kernel page cache.
type SubscriptionStore struct {
	accepted bool

	client          *Client
	life            context.Context
	cancel          context.CancelFunc
	loading         map[ObjectID]chan struct{}
	mu              sync.Mutex
	entries         map[ObjectID]*list.Element
	lru             list.List
	used, budget    uint64
	closed          bool
	observers       map[ID]func(ObjectID)
	updateObservers map[ID]func(ObjectID)
}
type subscriptionCacheEntry struct {
	store *SubscriptionStore
	id    ObjectID
	sub   *Subscription
	cost  uint64
}

const maxFilesystemSubscriptions = 1024
const subscriptionCacheOverhead = 512

func NewSubscriptionStore(c *Client, budget uint64) *SubscriptionStore {
	life, cancel := context.WithCancel(context.Background())
	return &SubscriptionStore{client: c, life: life, cancel: cancel, loading: map[ObjectID]chan struct{}{}, budget: budget, entries: map[ObjectID]*list.Element{}, observers: map[ID]func(ObjectID){}, updateObservers: map[ID]func(ObjectID){}}
}

// Backpressure is per routed transport, not a bucket-wide semaphore: a paused
// owner must not consume another owner's admission slots. Cache hits bypass it.
func (c *SubscriptionStore) backend(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if c.life.Err() != nil {
		return nil, nil, ErrClosed
	}
	merged, cancel := context.WithCancel(context.WithValue(ctx, filesystemWaitKey{}, true))
	stop := context.AfterFunc(c.life, cancel)
	return merged, func() { stop(); cancel() }, nil
}
func (c *SubscriptionStore) readUncached(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	ctx, release, err := c.backend(ctx)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	defer release()
	return c.client.Get(ctx, id)
}
func (c *SubscriptionStore) Get(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectInfo{}, err
	}
	if c == nil || c.client == nil {
		return nil, ObjectInfo{}, ErrClosed
	}
	c.mu.Lock()
	closed, disabled := c.closed, c.budget == 0
	c.mu.Unlock()
	if closed {
		return nil, ObjectInfo{}, ErrClosed
	}
	if disabled {
		return c.readUncached(ctx, id)
	}
	attempts := 0
	for attempts < 3 {
		c.mu.Lock()
		el := c.entries[id]
		var entry *subscriptionCacheEntry
		if el != nil {
			entry = el.Value.(*subscriptionCacheEntry)
			c.lru.MoveToFront(el)
		}
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return nil, ObjectInfo{}, ErrClosed
		}
		if entry != nil {
			data, info, _, err := entry.sub.Snapshot()
			if err == nil || errors.Is(err, ErrNotFound) {
				return data, info, err
			}
			c.remove(id, entry)
			continue
		}
		c.mu.Lock()
		// Another loader may have completed while the previous snapshot was checked.
		if c.entries[id] != nil {
			c.mu.Unlock()
			continue
		}
		if done := c.loading[id]; done != nil {
			c.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ObjectInfo{}, ctx.Err()
			case <-c.life.Done():
				return nil, ObjectInfo{}, ErrClosed
			}
		}
		done := make(chan struct{})
		c.loading[id] = done
		c.mu.Unlock()
		data, info, err := c.load(ctx, id)
		c.mu.Lock()
		delete(c.loading, id)
		close(done)
		c.mu.Unlock()
		if !errors.Is(err, ErrSubscriptionResync) {
			return data, info, err
		}
		attempts++
	}
	return c.readUncached(ctx, id)
}
func (c *SubscriptionStore) load(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	ctx, release, err := c.backend(ctx)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	defer release()
	sub, err := c.client.Subscribe(ctx, id, SubscribeOptions{})
	if err != nil {
		if errors.Is(err, ErrFull) || errors.Is(err, ErrNotImplemented) || errors.Is(err, ErrRange) {
			return c.client.Get(ctx, id)
		}
		return nil, ObjectInfo{}, err
	}
	entry := &subscriptionCacheEntry{store: c, id: id, sub: sub}
	weakEntry := weak.Make(entry)
	sub.SetObserver(func() {
		if e := weakEntry.Value(); e != nil {
			e.store.changed(e)
		}
	})
	data, info, _, err := sub.Snapshot()
	if err != nil {
		sub.discard()
		return nil, info, err
	}
	var evicted []*subscriptionCacheEntry
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		sub.discard()
		return nil, ObjectInfo{}, ErrClosed
	}
	sub.mu.RLock()
	entry.cost = uint64(len(sub.data)) + subscriptionCacheOverhead
	sub.mu.RUnlock()
	cached := entry.cost <= c.budget
	if cached {
		c.entries[id] = c.lru.PushFront(entry)
		c.used += entry.cost
		evicted = c.trimLocked()
	}
	c.mu.Unlock()
	for _, e := range evicted {
		c.emit(e.id)
		e.sub.discard()
	}
	if !cached {
		sub.discard()
	}
	return data, info, nil
}
func (c *SubscriptionStore) trimLocked() []*subscriptionCacheEntry {
	var out []*subscriptionCacheEntry
	for c.used > c.budget || len(c.entries) > maxFilesystemSubscriptions {
		el := c.lru.Back()
		if el == nil {
			break
		}
		e := el.Value.(*subscriptionCacheEntry)
		delete(c.entries, e.id)
		c.lru.Remove(el)
		c.used -= e.cost
		out = append(out, e)
	}
	return out
}
func (c *SubscriptionStore) remove(id ObjectID, expected *subscriptionCacheEntry) {
	c.mu.Lock()
	el := c.entries[id]
	var entry *subscriptionCacheEntry
	if el != nil && (expected == nil || el.Value == expected) {
		entry = el.Value.(*subscriptionCacheEntry)
		delete(c.entries, id)
		c.lru.Remove(el)
		c.used -= entry.cost
	}
	c.mu.Unlock()
	if entry != nil {
		c.emit(id)
		entry.sub.discard()
	}
}
func (c *SubscriptionStore) changed(entry *subscriptionCacheEntry) {
	entry.sub.mu.RLock()
	cost := uint64(len(entry.sub.data)) + subscriptionCacheOverhead
	valid := entry.sub.valid && !entry.sub.closed
	entry.sub.mu.RUnlock()
	var evicted []*subscriptionCacheEntry
	tracked := false
	c.mu.Lock()
	if el := c.entries[entry.id]; el != nil && el.Value == entry {
		tracked = true
		if valid {
			c.used = c.used - entry.cost + cost
			entry.cost = cost
			evicted = c.trimLocked()
		} else {
			delete(c.entries, entry.id)
			c.lru.Remove(el)
			c.used -= entry.cost
			evicted = append(evicted, entry)
		}
	}
	c.mu.Unlock()
	c.emit(entry.id)
	c.mu.Lock()
	updates := make([]func(ObjectID), 0, len(c.updateObservers))
	for _, fn := range c.updateObservers {
		updates = append(updates, fn)
	}
	c.mu.Unlock()
	if tracked {
		for _, fn := range updates {
			fn(entry.id)
		}
	}
	for _, e := range evicted {
		c.emit(e.id)
		e.sub.discard()
	}
}
func (c *SubscriptionStore) emit(id ObjectID) {
	c.mu.Lock()
	fns := make([]func(ObjectID), 0, len(c.observers))
	for _, fn := range c.observers {
		fns = append(fns, fn)
	}
	c.mu.Unlock()
	for _, fn := range fns {
		fn(id)
	}
}
func (c *SubscriptionStore) watchChanges(fn func(ObjectID)) func() {
	token := NewRequestID()
	c.mu.Lock()
	c.observers[token] = fn
	c.mu.Unlock()
	return func() { c.mu.Lock(); delete(c.observers, token); c.mu.Unlock() }
}

// WatchUpdates observes subscription updates and transport/resync loss. Cache
// capacity evictions do not trigger callbacks, so adapters may refresh views
// without causing an eviction/refresh notification loop. Callbacks must enqueue
// work and must not issue blocking RPCs. Only currently subscribed objects are
// observed; directory enumeration remains authoritative after cache eviction.
func (c *SubscriptionStore) WatchUpdates(fn func(ObjectID)) func() {
	token := NewRequestID()
	c.mu.Lock()
	c.updateObservers[token] = fn
	c.mu.Unlock()
	return func() { c.mu.Lock(); delete(c.updateObservers, token); c.mu.Unlock() }
}
func (c *SubscriptionStore) subscribed(id ObjectID, proof SnapshotProof) bool {
	c.mu.Lock()
	el := c.entries[id]
	var sub *Subscription
	if el != nil {
		sub = el.Value.(*subscriptionCacheEntry).sub
	}
	c.mu.Unlock()
	if sub == nil {
		return false
	}
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	if sub.source == nil {
		return false
	}
	select {
	case <-sub.source.done:
		return false
	default:
	}
	return sub.valid && !sub.closed && sub.err == nil && proof.Subscription == sub.id && proof.Cursor == sub.cursor
}
func (c *SubscriptionStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	ctx, release, err := c.backend(ctx)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer release()
	opt.Accepted = opt.Accepted || c.accepted
	info, err := c.client.CAS(ctx, id, off, expected, replacement, opt)
	// A failed precondition may race an update on another socket. Drop that
	// snapshot now, rather than spinning CAS retries against a stale local copy.
	if errors.Is(err, ErrPrecondition) {
		c.remove(id, nil)
	}
	return info, err
}
func (c *SubscriptionStore) GetRange(ctx context.Context, id ObjectID, off, n uint64) ([]byte, ObjectInfo, error) {
	ctx, release, err := c.backend(ctx)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	defer release()
	return c.client.GetRange(ctx, id, off, n)
}
func (c *SubscriptionStore) GetRangeToMatching(ctx context.Context, id ObjectID, off, n uint64, condition ReadCondition, dst io.Writer) (ObjectInfo, error) {
	ctx, release, err := c.backend(ctx)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer release()
	return c.client.GetRangeToMatching(ctx, id, off, n, condition, dst)
}
func (c *SubscriptionStore) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	entries := make([]*subscriptionCacheEntry, 0, len(c.entries))
	for _, el := range c.entries {
		entries = append(entries, el.Value.(*subscriptionCacheEntry))
	}
	c.entries = map[ObjectID]*list.Element{}
	c.lru.Init()
	c.used = 0
	c.mu.Unlock()
	c.cancel()
	for _, e := range entries {
		c.emit(e.id)
		e.sub.discard()
	}
}

func (s *Subscription) discard() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.valid = false
	s.data = nil
	s.mu.Unlock()
	s.cleanup.Stop()
	s.client.abandonSubscription(s.id)
	s.notify()
}

// The FUSE adapter uses these optional hooks to invalidate its page cache.
type filesystemChangeStore interface {
	watchChanges(func(ObjectID)) func()
	subscribed(ObjectID, SnapshotProof) bool
}

func (f *Filesystem) watchChanges(fn func(ObjectID)) (func(), bool) {
	if s, ok := f.store.(filesystemChangeStore); ok {
		return s.watchChanges(fn), true
	}
	return func() {}, false
}
func (f *Filesystem) subscribed(id ObjectID, proof SnapshotProof) bool {
	if s, ok := f.store.(filesystemChangeStore); ok {
		return s.subscribed(id, proof)
	}
	return false
}
func (o *OverlayFilesystem) watchChanges(fn func(ObjectID)) (func(), bool) {
	a, aa := o.upper.watchChanges(fn)
	b, bb := o.lower.watchChanges(fn)
	return func() { a(); b() }, aa && bb
}
func (o *OverlayFilesystem) subscribed(id ObjectID, proof SnapshotProof) bool {
	return o.upper.subscribed(id, proof) || o.lower.subscribed(id, proof)
}

// EnableAcceptedWrites opts this filesystem wrapper into weaker write ACKs.
// Unsupported nodes retain the durable path. Call Sync at filesystem barriers.
func (c *SubscriptionStore) EnableAcceptedWrites()          { c.accepted = true }
func (c *SubscriptionStore) Sync(ctx context.Context) error { return c.client.Sync(ctx) }
