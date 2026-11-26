// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
)

type startupOwnershipStamp struct{ uniform, own bool }

func (s startupOwnershipStamp) OwnershipForEntireBucket(ID) (bool, bool) { return s.own, s.uniform }

// Each object has one receipt. Half are migration shadows in the same bucket,
// so a mixed authority test cannot pass by selecting buckets alone. Include
// empty objects too: their descriptors and receipts still consume space.
func startupAccountingFixture(t *testing.T, n int) (*Volume, *MemoryIO, []BucketStorage) {
	t.Helper()
	v, d := memoryVolume(t, 16<<20)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		key := testKey(uint64(i))
		key.Bucket = testID(uint64(i%2 + 1))
		data := bytes.Repeat([]byte{byte(i)}, i%17)
		opt := Options{RequestID: testID(uint64(i)), ImportShadow: i%3 == 0}
		if opt.ImportShadow {
			err = h.Restore(key, Info{Length: uint64(len(data)), Compression: Raw}, func(yield func(uint64, io.Reader, uint64) error) error {
				if len(data) == 0 {
					return nil
				}
				return yield(0, bytes.NewReader(data), uint64(len(data)))
			}, opt)
		} else {
			_, err = h.Put(key, data, opt)
		}
		if err != nil {
			h.Close()
			t.Fatal(err)
		}
	}
	all, err := h.StorageUsage()
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	return v, d, all
}

func TestStartupAccountingColdSinglePassReadBudget(t *testing.T) {
	const n = 512
	_, disk, want := startupAccountingFixture(t, n)
	v, err := Open(disk, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	predicateCalls := 0
	if err = v.SetOwnershipFilter(startupOwnershipStamp{uniform: true, own: true}, func(Key, bool) bool { predicateCalls++; return true }); err != nil {
		t.Fatal(err)
	}
	after := v.IOStats()
	// Two descriptor reads and one receipt read per object, with headroom for
	// page crossings and the tree. A second census/repeated receipt decoding
	// exceeds this bound even when the operating system has every page cached.
	budget := uint64(4*n + 64)
	if reads := after.ReadOps - before.ReadOps; reads > budget {
		t.Fatalf("startup exceeded single-pass read budget: reads=%d budget=%d", reads, budget)
	}
	if predicateCalls != 0 {
		t.Fatal("uniform ownership ignored per-bucket proof", predicateCalls)
	}
	if after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
		t.Fatal("read-only reconstruction wrote storage")
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	all, err1 := h.StorageUsage()
	owned, err2 := h.OwnedStorageUsage()
	h.Close()
	if err1 != nil || err2 != nil || !reflect.DeepEqual(all, want) || !reflect.DeepEqual(owned, want) {
		t.Fatal("cold all/owned counters changed", all, owned, want, err1, err2)
	}
	if len(v.accounting) != 2 || len(v.owned) != 2 || len(v.cache.items) > 256 || len(v.nodes.items) > 256 {
		t.Fatal("census retained an object-sized directory")
	}
	t.Logf("cold census: %d objects plus %d receipts, %d backend reads", n, n, after.ReadOps-before.ReadOps)
}

func TestStartupAccountingOwnershipModesAndMutation(t *testing.T) {
	for _, mode := range []string{"all", "none", "mixed", "imports", "empty"} {
		t.Run(mode, func(t *testing.T) {
			n := 384
			if mode == "empty" {
				n = 0
			}
			_, disk, wantAll := startupAccountingFixture(t, n)
			v, err := Open(disk, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			include := func(k Key, shadow bool) bool {
				switch mode {
				case "none":
					return false
				case "mixed":
					return k.Object[31]%2 == 0
				case "imports":
					return !shadow
				default:
					return true
				}
			}
			stamp := startupOwnershipStamp{uniform: mode == "all" || mode == "none" || mode == "empty", own: mode != "none"}
			calls := 0
			if err = v.SetOwnershipFilter(stamp, func(k Key, shadow bool) bool { calls++; return include(k, shadow) }); err != nil {
				t.Fatal(err)
			}
			if n > 0 && !stamp.uniform && calls != 2*n {
				t.Fatal("mixed census did not evaluate each object and receipt exactly once", calls, 2*n)
			}
			h, err := v.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			all, e1 := h.StorageUsage()
			owned, e2 := h.OwnedStorageUsage()
			h.Close()
			if e1 != nil || e2 != nil || !reflect.DeepEqual(all, wantAll) {
				t.Fatal("physical reconstruction changed", all, wantAll, e1, e2)
			}
			expectedObjects, expectedLogical := uint64(0), uint64(0)
			for i := 1; i <= n; i++ {
				if include(testKey(uint64(i)), i%3 == 0) {
					expectedObjects++
					expectedLogical += uint64(i % 17)
				}
			}
			var objects, logical uint64
			for _, row := range owned {
				objects += row.Objects
				logical += row.Logical
			}
			if objects != expectedObjects || logical != expectedLogical {
				t.Fatal("wrong authoritative totals", owned, expectedObjects, expectedLogical)
			}
			if mode == "all" && !reflect.DeepEqual(all, owned) {
				t.Fatal("uniform all totals differ")
			}
			if mode == "none" && len(owned) != 0 {
				t.Fatal("unowned records charged", owned)
			}
			// Physical accounting must agree with an independent exact filtered query,
			// including receipt and shared-page overhead, not just logical object sums.
			h, _ = v.Open(false)
			exact, err := h.StorageUsageWhere(func(k Key) bool {
				if mode == "imports" {
					// Import provenance applies to receipts too; Object IDs encode fixture i.
					return binary.BigEndian.Uint64(k.Object[24:])%3 != 0
				}
				return include(k, false)
			})
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			var physical uint64
			for _, row := range owned {
				physical += row.Physical
			}
			// The portable query rounds each bucket's shared fraction upward;
			// owner attribution rounds down. They may differ by one byte/bucket.
			if physical > exact || exact-physical > uint64(len(all)) {
				t.Fatal("owned physical accounting differs from exact selection", physical, exact)
			}
			before := v.IOStats()
			if err = v.SetOwnershipFilter(stamp, include); err != nil {
				t.Fatal(err)
			}
			if after := v.IOStats(); after.ReadOps != before.ReadOps {
				t.Fatal("unchanged ownership recounted records")
			}
			// The initial proof still installs the actual future-write predicate.
			key := testKey(10000)
			key.Bucket = testID(1)
			h, _ = v.Open(true)
			if _, err = h.Put(key, []byte("later"), Options{RequestID: testID(10000)}); err != nil {
				t.Fatal(err)
			}
			next, err := h.OwnedStorageUsage()
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			var nextObjects uint64
			for _, row := range next {
				nextObjects += row.Objects
			}
			if include(key, false) {
				expectedObjects++
			}
			if nextObjects != expectedObjects {
				t.Fatal("cold path failed to install mutation predicate", nextObjects, expectedObjects)
			}
			t.Logf("%s: objects=%d logical=%d", mode, objects, logical)
		})
	}
}

func TestStartupAccountingFirstUniformFilterUsesAlreadyLoadedTotals(t *testing.T) {
	_, disk, want := startupAccountingFixture(t, 384)
	for _, own := range []bool{false, true} {
		v, err := Open(disk, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err = v.SetAdmission(nil); err != nil {
			t.Fatal(err)
		}
		before := v.IOStats()
		calls := 0
		if err = v.SetOwnershipFilter(startupOwnershipStamp{uniform: true, own: own}, func(Key, bool) bool { calls++; return own }); err != nil {
			t.Fatal(err)
		}
		if after := v.IOStats(); after.ReadOps != before.ReadOps || calls != 0 {
			t.Fatal("first uniform filter reread initialized totals", calls, after.ReadOps-before.ReadOps)
		}
		h, _ := v.Open(false)
		got, err := h.OwnedStorageUsage()
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
		if own && !reflect.DeepEqual(got, want) || !own && len(got) != 0 {
			t.Fatal("wrong first-filter totals", got, want)
		}
	}
}

// A failed cold census must not publish a partial quota allowance. Restoring
// the bad bytes and retrying still starts from the canonical index.
func TestStartupAccountingFailedCensusPublishesNoPartialTotals(t *testing.T) {
	_, disk, want := startupAccountingFixture(t, 64)
	v, err := Open(disk, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ptr, err := v.lookup(requestKey(testID(40)))
	if err != nil || ptr == 0 {
		t.Fatal(ptr, err)
	}
	raw := make([]byte, receiptBytes)
	if _, err = disk.ReadAt(raw, int64(ptr)); err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(raw)
	bad[0] ^= 0xff
	if _, err = disk.WriteAt(bad, int64(ptr)); err != nil {
		t.Fatal(err)
	}
	notifications := 0
	v.observer = func(StorageSnapshot) { notifications++ }
	if err = v.SetOwnershipFilter(startupOwnershipStamp{uniform: true, own: true}, func(Key, bool) bool { return true }); !errors.Is(err, ErrCorrupt) {
		t.Fatal("corrupt receipt accepted", err)
	}
	if v.accounting != nil || v.owned != nil || v.ownerStamp != nil || notifications != 0 {
		t.Fatal("failed census published partial ownership")
	}
	if _, err = disk.WriteAt(raw, int64(ptr)); err != nil {
		t.Fatal(err)
	}
	if err = v.SetOwnershipFilter(startupOwnershipStamp{uniform: true, own: true}, func(Key, bool) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if notifications != 1 {
		t.Fatal("successful retry did not publish exactly once", notifications)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.OwnedStorageUsage()
	h.Close()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("retry used partial counters", got, want, err)
	}
}
