// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type cacheOwnershipStamp byte

func (s cacheOwnershipStamp) OwnershipCheckpointKey() [32]byte { return sha256.Sum256([]byte{byte(s)}) }
func cacheInclude(k Key, imported bool) bool                   { return !imported && k.Object[31]%2 == 0 }
func accountingCacheFixture(t *testing.T) (*Volume, string, string, StorageSnapshot) {
	t.Helper()
	dir := t.TempDir()
	name := filepath.Join(dir, "data.vol")
	cache := filepath.Join(dir, "metadata", "counters.cache")
	v, err := New(name, 8<<20, HDD)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 32; i++ {
		k := testKey(i)
		k.Bucket = testID(i%3 + 1)
		if _, err = testPut(v, k, bytes.Repeat([]byte{byte(i)}, int(i%9)), Options{RequestID: testID(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err = v.SetOwnershipFilter(cacheOwnershipStamp(1), cacheInclude); err != nil {
		t.Fatal(err)
	}
	want := StorageSnapshot{All: v.usage(v.accounting, v.sb), Owned: v.ownedUsage(v.owned, v.accounting, v.sb)}
	ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: cache, Stamp: cacheOwnershipStamp(1), Include: cacheInclude})
	if err != nil || ready {
		t.Fatal(ready, err)
	}
	before := v.IOStats()
	if err = v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := v.IOStats()
	if after.SyncOps != before.SyncOps || after.WriteOps != before.WriteOps {
		t.Fatal("census synced/wrote user storage", before, after)
	}
	return v, name, cache, want
}
func accountingUsage(t *testing.T, v *Volume) StorageSnapshot {
	t.Helper()
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	all, e1 := h.StorageUsage()
	owned, e2 := h.OwnedStorageUsage()
	if e1 != nil || e2 != nil {
		t.Fatal(e1, e2)
	}
	return StorageSnapshot{all, owned}
}
func reopenAccountingFile(t *testing.T, name string) *Volume {
	t.Helper()
	v, err := New(name, 0, HDD)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	return v
}
func TestAccountingCheckpointCacheHitHasZeroIndexReads(t *testing.T) {
	v, name, cache, want := accountingCacheFixture(t)
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	v = reopenAccountingFile(t, name)
	initialObjects, initialNodes := len(v.cache.items), len(v.nodes.items)
	before := v.IOStats()
	observed := 0
	ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: cache, Stamp: cacheOwnershipStamp(1), Include: cacheInclude, Observer: func(got StorageSnapshot) {
		observed++
		if observed == 1 && !reflect.DeepEqual(got, want) {
			t.Error("restored observer differs", got, want)
		}
	}})
	if err != nil || !ready || !v.AccountingReady() {
		t.Fatal(ready, err)
	}
	if got := accountingUsage(t, v); !reflect.DeepEqual(got, want) {
		t.Fatal(got, want)
	}
	if err = v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := v.IOStats()
	if after != before {
		t.Fatal("cache hit touched record/index storage", before, after)
	}
	if observed != 1 || len(v.accounting) != 3 || len(v.owned) != 3 || len(v.cache.items) != initialObjects || len(v.nodes.items) != initialNodes {
		t.Fatal("cache restoration retained object metadata", observed, len(v.cache.items), len(v.nodes.items))
	}
	st, err := os.Stat(cache)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("private cache", st, err)
	}
	// Restored private allocation fields, not just usage summaries, must support
	// the exact same subsequent incremental mutation/admission accounting.
	key := testKey(2)
	key.Bucket = testID(3)
	if _, err = testPutRange(v, key, 1, []byte("updated"), Options{}); err != nil {
		t.Fatal(err)
	}
	got := accountingUsage(t, v)
	v.mu.RLock()
	all, owned, err := v.scanAccounting(cacheOwnershipStamp(1), cacheInclude, true)
	want = StorageSnapshot{v.usage(all, v.sb), v.ownedUsage(owned, all, v.sb)}
	v.mu.RUnlock()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("restored delta diverged from census", got, want, err)
	}
}
func TestAccountingCacheMissesNeverPublishZeroQuota(t *testing.T) {
	for _, kind := range []string{"missing", "checksum", "truncated", "wrong-identity", "wrong-schema", "oversized", "public-permissions", "stale-generation", "owner-key", "unkeyed-owner"} {
		t.Run(kind, func(t *testing.T) {
			v, name, cache, want := accountingCacheFixture(t)
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(cache)
			if err != nil {
				t.Fatal(err)
			}
			opts := AccountingOptions{CachePath: cache, Stamp: cacheOwnershipStamp(1), Include: cacheInclude}
			switch kind {
			case "missing":
				os.Remove(cache)
			case "checksum":
				data[len(data)-1] ^= 1
				os.WriteFile(cache, data, 0600)
			case "truncated":
				os.WriteFile(cache, data[:len(data)/2], 0600)
			case "wrong-identity":
				data[40] ^= 1
				sum := sha256.Sum256(data[:len(data)-32])
				copy(data[len(data)-32:], sum[:])
				os.WriteFile(cache, data, 0600)
			case "wrong-schema":
				data[7] = '2'
				sum := sha256.Sum256(data[:len(data)-32])
				copy(data[len(data)-32:], sum[:])
				os.WriteFile(cache, data, 0600)
			case "oversized":
				f, e := os.OpenFile(cache, os.O_WRONLY, 0600)
				if e != nil {
					t.Fatal(e)
				}
				f.Truncate(accountingCacheLimit + 1)
				f.Close()
			case "public-permissions":
				os.Chmod(cache, 0644)
			case "owner-key":
				opts.Stamp = cacheOwnershipStamp(2)
			case "unkeyed-owner":
				opts.Stamp = struct{ N int }{1}
			case "stale-generation":
				v = reopenAccountingFile(t, name)
				if _, err = testPut(v, testKey(999), []byte("later"), Options{}); err != nil {
					t.Fatal(err)
				}
				v.Close()
			}
			v = reopenAccountingFile(t, name)
			before := v.IOStats()
			ready, err := v.ConfigureAccounting(opts)
			if err != nil || ready || v.AccountingReady() {
				t.Fatal("cache miss was authoritative", ready, err)
			}
			after := v.IOStats()
			if after.ReadOps != before.ReadOps {
				t.Fatal("Configure scanned record data")
			}
			if h, err := v.Open(true); h != nil || !errors.Is(err, ErrAccountingNotReady) {
				t.Fatal("unknown quota admitted mutation", h, err)
			}
			h, err := v.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.OwnedStorageUsage()
			if !errors.Is(err, ErrAccountingNotReady) {
				t.Fatal("unknown ownership masquerades as zero/all", err)
			}
			if _, err = h.StorageUsageWhere(func(Key) bool { return true }, true); !errors.Is(err, ErrAccountingNotReady) {
				t.Fatal("filtered quota escaped cold admission", err)
			}
			if kind == "owner-key" || kind == "unkeyed-owner" {
				all, e := h.StorageUsage()
				if e != nil || !reflect.DeepEqual(all, want.All) {
					t.Fatal("valid all-counter cache lost with owner change", all, e)
				}
			}
			h.Close()
			if err = v.WarmAccounting(context.Background()); err != nil || !v.AccountingReady() {
				t.Fatal("background repair", err)
			}
			if kind != "stale-generation" {
				if got := accountingUsage(t, v); !reflect.DeepEqual(got, want) {
					t.Fatal(got, want)
				}
			}
		})
	}
}

type accountingReadGate struct {
	VolumeIO
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *accountingReadGate) ReadAt(p []byte, off int64) (int, error) {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
	return g.VolumeIO.ReadAt(p, off)
}
func TestAccountingWarmupAllowsReadsAndStatsAndCancel(t *testing.T) {
	_, disk, want := startupAccountingFixture(t, 64)
	gate := &accountingReadGate{VolumeIO: disk, entered: make(chan struct{}), release: make(chan struct{})}
	v, err := Open(gate, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := v.ConfigureAccounting(AccountingOptions{Stamp: cacheOwnershipStamp(1)})
	if err != nil || ready {
		t.Fatal(ready, err)
	}
	gate.armed.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- v.WarmAccounting(ctx) }()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("census did not start")
	}
	foreground := make(chan error, 1)
	go func() {
		_ = v.Stats()
		k := testKey(1)
		k.Bucket = testID(2)
		_, _, e := testGet(v, k)
		foreground <- e
	}()
	select {
	case e := <-foreground:
		if e != nil {
			t.Error(e)
		}
	case <-time.After(time.Second):
		t.Error("foreground reads/stats waited behind whole census")
	}
	if _, e := v.Open(true); !errors.Is(e, ErrAccountingNotReady) {
		t.Error("cold writer did not fail before lock", e)
	}
	cancel()
	close(gate.release)
	if e := <-done; !errors.Is(e, context.Canceled) || v.AccountingReady() {
		t.Fatal("cancelled census was published", e)
	}
	if err = v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := accountingUsage(t, v); !reflect.DeepEqual(got.All, want) || !reflect.DeepEqual(got.Owned, want) {
		t.Fatal(got, want)
	}
}
func TestAccountingCacheAcceptedWritesAndCrashOrdering(t *testing.T) {
	for _, crash := range []bool{true, false} {
		t.Run(map[bool]string{true: "crash", false: "clean-close"}[crash], func(t *testing.T) {
			v, name, cache, _ := accountingCacheFixture(t)
			old, err := os.ReadFile(cache)
			if err != nil {
				t.Fatal(err)
			}
			before := v.IOStats()
			if _, err = testPut(v, testKey(1000), []byte("accepted tail"), Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			if err = v.SaveAccountingCache(); err != nil {
				t.Fatal(err)
			}
			unchanged, _ := os.ReadFile(cache)
			if !bytes.Equal(old, unchanged) {
				t.Fatal("cache described undurable accepted generation")
			}
			after := v.IOStats()
			if after.SyncOps != before.SyncOps {
				t.Fatal("accepted increment/cache introduced sync")
			}
			if crash {
				if err = v.dev.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err = v.Close(); err != nil {
					t.Fatal(err)
				}
			}
			v = reopenAccountingFile(t, name)
			ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: cache, Stamp: cacheOwnershipStamp(1), Include: cacheInclude})
			if err != nil || !ready {
				t.Fatal("restart should restore exact durable generation", ready, err)
			}
			_, _, err = testGet(v, testKey(1000))
			if crash && !errors.Is(err, ErrNotFound) || !crash && err != nil {
				t.Fatal("wrong durable data after reopen", err)
			}
		})
	}
}
func TestAccountingCacheFailureDoesNotFailStorageOrClose(t *testing.T) {
	v, _, _, _ := accountingCacheFixture(t)
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-dir")
	os.WriteFile(blocker, []byte("x"), 0600)
	v.accountCachePath = filepath.Join(blocker, "cache")
	if _, err := testPut(v, testKey(700), []byte("durable despite derived-cache failure"), Options{}); err != nil {
		t.Fatal(err)
	}
	if err := v.SaveAccountingCache(); err == nil {
		t.Fatal("cache error not observable")
	}
	if err := v.Close(); err != nil {
		t.Fatal("advisory cache broke clean close", err)
	}
}

func TestAccountingEmptyBootstrapAndRepeatedConfiguration(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	path := filepath.Join(t.TempDir(), "ram.cache")
	for _, stamp := range []cacheOwnershipStamp{1, 2} {
		ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: path, Stamp: stamp, Include: cacheInclude})
		if err != nil || !ready {
			t.Fatal("empty canonical root must bootstrap without census", ready, err)
		}
		if _, err = testPut(v, testKey(uint64(stamp)*2), []byte("data"), Options{}); err != nil {
			t.Fatal(err)
		}
		if stamp == 1 {
			// A changed filter on a now nonempty volume must invalidate selected counters.
			ready, err = v.ConfigureAccounting(AccountingOptions{CachePath: path, Stamp: stamp + 1, Include: cacheInclude})
			if err != nil || ready {
				t.Fatal("changed nonempty predicate reused stale authority", ready, err)
			}
			if err = v.WarmAccounting(context.Background()); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if err := v.SaveAccountingCache(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("RAM volume wrote a persistent statistics cache", err)
	}
}

func TestAccountingWarmFailureIsUnknownAndRetryable(t *testing.T) {
	_, disk, _ := startupAccountingFixture(t, 32)
	v, err := Open(disk, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = v.ConfigureAccounting(AccountingOptions{Include: cacheInclude}); err != nil {
		t.Fatal(err)
	}
	// Corrupt one descriptor after Open so a genuine storage problem occurs during
	// census. It must remain an error; a missing advisory cache cannot hide it.
	off, err := v.lookup(objectKey(Key{Bucket: testID(2), Object: testKey(1).Object}))
	if err != nil || off == 0 {
		t.Fatal(off, err)
	}
	original := make([]byte, 8)
	if _, err = disk.ReadAt(original, int64(off)); err != nil {
		t.Fatal(err)
	}
	if _, err = disk.WriteAt(make([]byte, 8), int64(off)); err != nil {
		t.Fatal(err)
	}
	if err = v.WarmAccounting(context.Background()); err == nil || v.AccountingReady() {
		t.Fatal("corrupt canonical data became empty quota", err)
	}
	if _, err = disk.WriteAt(original, int64(off)); err != nil {
		t.Fatal(err)
	}
	if err = v.WarmAccounting(context.Background()); err != nil || !v.AccountingReady() {
		t.Fatal("repaired data cannot retry", err)
	}
}
