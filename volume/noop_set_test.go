package volume

import (
	"bytes"
	"testing"
)

func TestNoOpFixedMetadataCASIndexStability(t *testing.T) {
	v, e := NewRAM(16 << 20)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	h, e := v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	data := bytes.Repeat([]byte{'a'}, 210)
	for i := uint64(0); i < 200; i++ {
		if _, e = h.Put(testKey(i), data, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	k := testKey(50)
	old, e := v.loadObject(k)
	if e != nil {
		t.Fatal(e)
	}
	root := v.sb.ObjectRoot
	pages := map[uint64][]byte{}
	var save func(uint64)
	save = func(off uint64) {
		n, e := v.node(off)
		if e != nil {
			t.Fatal(e)
		}
		page, e := v.readOnlyPage(off)
		if e != nil {
			t.Fatal(e)
		}
		pages[off] = bytes.Clone(page)
		if n.level > 0 {
			for _, x := range n.entries {
				save(x.value)
			}
		}
	}
	save(root)
	hits := 0
	initialGeneration := v.sb.Generation
	before := v.ioStatsLocked().SyncOps
	for i := 0; i < 100; i++ {
		next := bytes.Clone(data)
		next[i%len(next)]++
		if _, e = h.CAS(k, 0, data, next, Options{Accepted: true, WholeObject: true}); e != nil {
			t.Fatal(e)
		}
		after, e := v.loadObject(k)
		if e != nil {
			t.Fatal(e)
		}
		if after.Base == old.Base && after.Size == old.Size {
			hits++
		}
		data = next
	}
	if v.sb.Generation <= initialGeneration {
		t.Fatal("CAS generation did not advance")
	}
	if hits != 100 {
		t.Fatalf("stable pointer hits%d/100", hits)
	}
	if v.sb.ObjectRoot != root {
		t.Fatal("root pointer changed")
	}
	for off, before := range pages {
		page, e := v.readOnlyPage(off)
		if e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(before, page) {
			t.Fatal("index page changed", off)
		}
	}
	if v.ioStatsLocked().SyncOps != before {
		t.Fatal("accepted CAS synced")
	}
	t.Logf("stable descriptor pointers %d/100, unchanged index pages%d, no implicit Sync", hits, len(pages))
}
func BenchmarkNoOpIndexSet(b *testing.B) {
	v, e := NewRAM(16 << 20)
	if e != nil {
		b.Fatal(e)
	}
	b.Cleanup(func() { v.Close() })
	h, e := v.Open(true)
	if e != nil {
		b.Fatal(e)
	}
	data := bytes.Repeat([]byte{'a'}, 210)
	for i := uint64(0); i < 200; i++ {
		if _, e = h.Put(testKey(i), data, Options{}); e != nil {
			b.Fatal(e)
		}
	}
	h.Close()
	v.mu.Lock()
	b.Cleanup(v.mu.Unlock)
	tx := v.transaction()
	b.Cleanup(tx.close)
	key := objectKey(testKey(50))
	ptr, e := v.lookup(key)
	if e != nil {
		b.Fatal(e)
	}
	tx.set(&tx.sb.ObjectRoot, key, ptr)
	b.Run("CurrentSet", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if e := tx.set(&tx.sb.ObjectRoot, key, ptr); e != nil {
				b.Fatal(e)
			}
		}
	})
	b.Run("ReadOnlyExactProbeWithAccountingMark", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx.changed[key] = true
			e, ok, err := tx.neighbor(tx.sb.ObjectRoot, key, false)
			if err != nil || !ok || e.key != key || e.value != ptr {
				b.Fatal("probe mismatch", err)
			}
		}
	})
}

func noOpSetFixture(t testing.TB) (*Volume, *transaction, indexKey, uint64) {
	t.Helper()
	v, e := NewRAM(16 << 20)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { v.Close() })
	h, e := v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	data := bytes.Repeat([]byte{'a'}, 210)
	for i := uint64(0); i < 200; i++ {
		if _, e = h.Put(testKey(i), data, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	h.Close()
	v.mu.Lock()
	t.Cleanup(v.mu.Unlock)
	tx := v.transaction()
	t.Cleanup(tx.close)
	key := objectKey(testKey(50))
	ptr, e := v.lookup(key)
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.set(&tx.sb.ObjectRoot, key, ptr); e != nil {
		t.Fatal(e)
	}
	return v, tx, key, ptr
}
func TestNoOpIndexSetAllocations(t *testing.T) {
	_, tx, key, ptr := noOpSetFixture(t)
	allocs := testing.AllocsPerRun(100, func() {
		if e := tx.set(&tx.sb.ObjectRoot, key, ptr); e != nil {
			panic(e)
		}
	})
	if allocs != 0 {
		t.Fatalf("unchanged committed set allocated %.0f times", allocs)
	}
	if len(tx.pages) != 0 {
		t.Fatal("unchanged pointer dirtied index pages")
	}
	if !tx.changed[key] {
		t.Fatal("accounting mark lost")
	}
}
func TestNoOpIndexSetBorrowedOwnershipSurvivesDescendant(t *testing.T) {
	v, tx, n := readNodeFixture(t, 3)
	root := tx.sb.FreeRoot
	left, right := uint64(2<<20), uint64(2<<20+PageSize)
	a, b := n, n
	a.entries = append([]treeEntry(nil), n.entries[:2]...)
	b.entries = append([]treeEntry(nil), n.entries[2:]...)
	parent := treeNode{kind: treeFree, level: 1, entries: []treeEntry{tx.summary(left, a), tx.summary(right, b)}}
	for off, node := range map[uint64]treeNode{root: parent, left: a, right: b} {
		if _, e := v.dev.WriteAt(encodeNode(node, v.sb.Identity.VolumeID, 1), int64(off)); e != nil {
			t.Fatal(e)
		}
	}
	v.nodes.clear()
	held, borrowed, e := tx.inspectNode(root)
	if e != nil || !borrowed {
		t.Fatal("expected committed borrowed ancestor", e)
	}
	original := encodeNode(held, v.sb.Identity.VolumeID, 1)
	// A real descendant free-range change propagates its maximum into the parent,
	// making the ancestor address dirty AFTER the older cached slice was loaded.
	if e = tx.set(&tx.sb.FreeRoot, a.entries[0].key, 4*PageSize); e != nil {
		t.Fatal(e)
	}
	if tx.pages[root] == nil {
		t.Fatal("fixture did not dirty ancestor")
	}
	ownNodeEntries(&held, borrowed)
	held.entries[0].maximum++
	cached, e := v.node(root)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(original, encodeNode(cached, v.sb.Identity.VolumeID, 1)) {
		t.Fatal("late ancestor edit mutated committed cache")
	}
}
func TestNoOpIndexSetAccountingOnChangedDescriptor(t *testing.T) {
	v, e := NewRAM(4 << 20)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	h, e := v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	k := testKey(1)
	if _, e = h.Put(k, []byte("abcdef"), Options{}); e != nil {
		t.Fatal(e)
	}
	before, e := v.loadObject(k)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.StorageUsage(); e != nil {
		t.Fatal(e)
	}
	if _, e = h.Trunc(k, 3, Options{Accepted: true}); e != nil {
		t.Fatal(e)
	}
	after, e := v.loadObject(k)
	if e != nil {
		t.Fatal(e)
	}
	if before.Base != after.Base {
		t.Fatal("fixture moved descriptor")
	}
	usage, e := h.StorageUsage()
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, u := range usage {
		if u.Bucket == k.Bucket {
			found = true
			if u.Logical != 3 {
				t.Fatal("stable-pointer accounting stale", u.Logical)
			}
		}
	}
	if !found {
		t.Fatal("bucket usage missing")
	}
}
