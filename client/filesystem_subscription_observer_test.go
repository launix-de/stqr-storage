// SPDX-License-Identifier: GPL-3.0-only
package client

import "testing"

func TestSubscriptionUpdateObserversIgnoreEvictedEntries(t *testing.T) {
	store := NewSubscriptionStore(nil, 4096)
	defer store.cancel()
	id := ObjectID{1}
	entry := &subscriptionCacheEntry{store: store, id: id, sub: &Subscription{valid: true}, cost: subscriptionCacheOverhead}
	store.entries[id] = store.lru.PushFront(entry)
	store.used = entry.cost
	count := 0
	unwatch := store.WatchUpdates(func(got ObjectID) {
		if got != id {
			t.Fatal(got)
		}
		count++
	})
	store.changed(entry)
	if count != 1 {
		t.Fatalf("active update: %d", count)
	}
	delete(store.entries, id) // eviction occurs before discard's final callback
	store.changed(entry)
	if count != 1 {
		t.Fatalf("eviction triggered update/refresh loop: %d", count)
	}
	store.entries[id] = store.lru.Front()
	unwatch()
	store.changed(entry)
	if count != 1 {
		t.Fatalf("unwatch retained callback: %d", count)
	}
}
