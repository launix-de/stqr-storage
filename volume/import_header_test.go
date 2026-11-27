// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

// A sparse terabyte object with independently stored tiny extents: ownership
// metadata work must depend on its descriptor, not payload length or segments.
func importHeaderFixture(t *testing.T, segments int, segmented, shadow bool) (*Volume, *MemoryIO, Key, uint64) {
	t.Helper()
	v, d := memoryVolume(t, 8<<20)
	t.Cleanup(func() { v.Close() })
	key := testKey(1)
	h, e := v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	e = h.Restore(key, Info{Length: (1 << 40) + uint64(segments*2), Compression: Raw}, func(yield func(uint64, io.Reader, uint64) error) error {
		for i := 0; i < segments; i++ {
			if e := yield(uint64(i*2), bytes.NewReader([]byte{byte(i)}), 1); e != nil {
				return e
			}
		}
		return nil
	}, Options{ImportShadow: shadow, restoreSegmented: segmented, RequestID: testID(21)})
	h.Close()
	if e != nil {
		t.Fatal(e)
	}
	ptr, e := v.lookup(objectKey(key))
	if e != nil {
		t.Fatal(e)
	}
	return v, d, key, ptr
}
func importHeaderView(v *Volume) *Volume {
	return &Volume{dev: v.dev, sb: v.sb, limit: v.limit, cache: newCache(-1, 1), nodes: newNodeCache(-1, 1)}
}
func TestImportHeaderFragmentedReadBudgetAndFullLoadControl(t *testing.T) {
	const count = 501
	v, _, key, ptr := importHeaderFixture(t, count, true, true)
	view := importHeaderView(v)
	before := v.IOStats()
	flag, e := view.recordImported(objectKey(key))
	after := v.IOStats()
	if e != nil || !flag {
		t.Fatal(flag, e)
	}
	fastOps := testReadWorkOps(after) - testReadWorkOps(before)
	fastBytes := testReadWorkBytes(after) - testReadWorkBytes(before)
	if fastOps > 8 || fastBytes > 2*PageSize+64 {
		t.Fatalf("header lookup walked layout: ops%d bytes%d", fastOps, fastBytes)
	}
	// The former path is an explicit positive control: a cold full load traverses
	// every linked SEG2 page. Both mmap/RAM and positioned reads count as work.
	before = v.IOStats()
	o, e := importHeaderView(v).loadObject(key)
	after = v.IOStats()
	if e != nil || len(o.Spans) != count {
		t.Fatal(e)
	}
	fullOps := testReadWorkOps(after) - testReadWorkOps(before)
	if fullOps <= fastOps+uint64(count/listEntries-1) {
		t.Fatalf("negative control failed: full%d fast%d", fullOps, fastOps)
	}
	t.Logf("501 segments: header%d ops/%d bytes; old full load%d ops", fastOps, fastBytes, fullOps)
	// A broken external list must not be followed for this header-only property,
	// but ordinary object loading must continue to reject it.
	page := make([]byte, listBytes)
	if e = v.readBytes(page, o.ListHead); e != nil {
		t.Fatal(e)
	}
	page[0] ^= 1
	if _, e = v.dev.WriteAt(page, int64(o.ListHead)); e != nil {
		t.Fatal(e)
	}
	flag, e = importHeaderView(v).objectImportedAt(ptr)
	if e != nil || !flag {
		t.Fatal("header unexpectedly followed SEG2", flag, e)
	}
	if _, e = importHeaderView(v).loadObject(key); !errors.Is(e, ErrCorrupt) {
		t.Fatal("full read skipped list validation", e)
	}
}
func TestImportHeaderDescriptorValidation(t *testing.T) {
	v, _, _, ptr := importHeaderFixture(t, 9, true, true)
	original := make([]byte, 64)
	if e := v.readBytes(original, ptr); e != nil {
		t.Fatal(e)
	}
	cases := map[string]func([]byte){"magic": func(b []byte) { b[4] ^= 1 }, "size": func(b []byte) { diskOrder.PutUint32(b, 65) }, "bounds": func(b []byte) { diskOrder.PutUint32(b, uint32(v.sb.Identity.Size)) }, "codec": func(b []byte) { b[24] = 255 }, "count": func(b []byte) { diskOrder.PutUint64(b[56:], 0) }, "head": func(b []byte) { diskOrder.PutUint64(b[40:], v.sb.Identity.Size) }, "tail": func(b []byte) { diskOrder.PutUint64(b[48:], 1) }}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b := bytes.Clone(original)
			mutate(b)
			diskOrder.PutUint32(b[32:], 0)
			diskOrder.PutUint32(b[32:], checksum(b))
			if _, e := v.dev.WriteAt(b, int64(ptr)); e != nil {
				t.Fatal(e)
			}
			if _, e := importHeaderView(v).objectImportedAt(ptr); !errors.Is(e, ErrCorrupt) {
				t.Fatal(e)
			}
		})
	}
	bad := bytes.Clone(original)
	bad[25] ^= 4
	if _, e := v.dev.WriteAt(bad, int64(ptr)); e != nil {
		t.Fatal(e)
	}
	if _, e := importHeaderView(v).objectImportedAt(ptr); !errors.Is(e, ErrCorrupt) {
		t.Fatal("flag changed without CRC", e)
	}
	if _, e := importHeaderView(v).objectImportedAt(v.sb.Identity.Size - 2); !errors.Is(e, ErrCorrupt) {
		t.Fatal("out of bounds header", e)
	}
}
func TestImportHeaderInlineCRCAndMissing(t *testing.T) {
	for _, shadow := range []bool{false, true} {
		v, _, key, ptr := importHeaderFixture(t, 90, false, shadow)
		view := importHeaderView(v)
		flag, e := view.objectImportedAt(ptr)
		if e != nil || flag != shadow {
			t.Fatal(flag, e)
		}
		b := make([]byte, 1)
		if e = v.readBytes(b, ptr+objectHeader+spanBytes*89); e != nil {
			t.Fatal(e)
		}
		b[0] ^= 1
		if _, e = v.dev.WriteAt(b, int64(ptr+objectHeader+spanBytes*89)); e != nil {
			t.Fatal(e)
		}
		if _, e = view.objectImportedAt(ptr); !errors.Is(e, ErrCorrupt) {
			t.Fatal("inline CRC tail skipped", e)
		}
		if flag, e = view.recordImported(objectKey(testKey(99))); e != nil || flag {
			t.Fatal("missing object", flag, e)
		}
		_ = key
	}
}
func TestImportHeaderAccountingTransitionsAndReceipts(t *testing.T) {
	v, _, key, _ := importHeaderFixture(t, 120, true, true)
	if e := v.SetOwnershipFilter("exclude shadows", func(_ Key, shadow bool) bool { return !shadow }); e != nil {
		t.Fatal(e)
	}
	compare := func() {
		t.Helper()
		h, e := v.Open(false)
		if e != nil {
			t.Fatal(e)
		}
		got, e := h.OwnedStorageUsage()
		h.Close()
		if e != nil {
			t.Fatal(e)
		}
		all, owned, e := v.scanAccounting("exclude shadows", func(_ Key, shadow bool) bool { return !shadow }, true)
		if e != nil {
			t.Fatal(e)
		}
		want := v.ownedUsage(owned, all, v.sb)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("incremental owned mismatch: got%+v want%+v", got, want)
		}
	}
	compare()
	h, _ := v.Open(true)
	if _, e := h.Put(key, []byte("new"), Options{Accepted: true, RequestID: testID(22)}); e != nil {
		t.Fatal(e)
	}
	h.Close()
	compare()
	h, _ = v.Open(true)
	if e := h.Delete(key, Options{Accepted: true, RequestID: testID(23)}); e != nil {
		t.Fatal(e)
	}
	h.Close()
	compare()
	h, _ = v.Open(true)
	if e := h.Restore(testKey(2), Info{Deleted: true, Compression: Raw}, func(func(uint64, io.Reader, uint64) error) error { return nil }, Options{Accepted: true, ImportShadow: true, RequestID: testID(24)}); e != nil {
		t.Fatal(e)
	}
	h.Close()
	compare()
	for _, tc := range []struct {
		k    indexKey
		want bool
	}{{requestKey(testID(21)), true}, {requestKey(testID(22)), false}, {objectKey(testKey(2)), true}} {
		flag, e := v.recordImported(tc.k)
		if e != nil || flag != tc.want {
			t.Fatal(tc.k[0], flag, e)
		}
	}
	replicated := testKey(3)
	h, _ = v.Open(true)
	if e := h.Restore(replicated, Info{Length: 240, Compression: Raw}, func(yield func(uint64, io.Reader, uint64) error) error {
		for i := 0; i < 120; i++ {
			if e := yield(uint64(i*2), bytes.NewReader([]byte{1}), 1); e != nil {
				return e
			}
		}
		return nil
	}, Options{ImportShadow: true, restoreSegmented: true, RequestID: testID(25)}); e != nil {
		t.Fatal(e)
	}
	h.Close()
	compare()
	// Pending and committed replica records retain their existing non-shadow
	// ownership semantics while the canonical object changes independently.
	binding := ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}}
	rid := testID(90)
	h, _ = v.Open(true)
	if _, e := h.PrepareMutation(opPut, replicated, 0, 3, bytes.NewBufferString("rep"), nil, Options{RequestID: rid}, binding); e != nil {
		t.Fatal(e)
	}
	h.Close()
	compare()
	if flag, e := v.recordImported(replicaKey(rid)); e != nil || flag {
		t.Fatal("replica marker changed", flag, e)
	}
	h, _ = v.Open(true)
	if e := h.DecideReplica(rid, binding); e != nil {
		t.Fatal(e)
	}
	h.Close()
	compare()
	h, _ = v.Open(true)
	if _, e := h.PublishReplica(rid, binding); e != nil {
		t.Fatal(e)
	}
	h.Close()
	compare()
	for _, kind := range []byte{2, 3} {
		var k indexKey
		k[0] = kind
		if flag, e := v.recordImported(k); e != nil || flag {
			t.Fatal("nonobject flag", kind, flag, e)
		}
	}
}

func TestImportHeaderMetadataCanReuseRetiredInitialIndexPage(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	defer v.Close()
	relocatePayloadTestRoot(t, v)
	key := testKey(500)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Restore(key, Info{Length: 1 << 40, Compression: Raw}, func(yield func(uint64, io.Reader, uint64) error) error {
		for i := uint64(0); i < 9; i++ {
			if err := yield(i*2, bytes.NewReader([]byte{byte(i)}), 1); err != nil {
				return err
			}
		}
		return nil
	}, Options{ImportShadow: true, restoreSegmented: true})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	o, err := v.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the same valid descriptor/list bytes at the earliest reusable
	// metadata addresses, independent of free-list allocation order. The full
	// decoder is the positive control for this metadata placement contract.
	disk, err := NewMemory(int64(v.sb.Identity.Size))
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	base := treeStart
	lists := make([]uint64, len(o.ListBlocks))
	for i := range lists {
		lists[i] = base + 64 + uint64(i)*listBytes
	}
	for i, old := range o.ListBlocks {
		block := make([]byte, listBytes)
		if err = v.readBytes(block, old); err != nil {
			t.Fatal(err)
		}
		next := uint64(0)
		if i+1 < len(lists) {
			next = lists[i+1]
		}
		diskOrder.PutUint64(block[8:], next)
		diskOrder.PutUint32(block[508:], checksum(block[:508]))
		if _, err = disk.WriteAt(block, int64(lists[i])); err != nil {
			t.Fatal(err)
		}
	}
	header := make([]byte, 64)
	if err = v.readBytes(header, o.Base); err != nil {
		t.Fatal(err)
	}
	diskOrder.PutUint64(header[40:], lists[0])
	diskOrder.PutUint64(header[48:], lists[len(lists)-1])
	diskOrder.PutUint32(header[32:], 0)
	diskOrder.PutUint32(header[32:], checksum(header))
	if _, err = disk.WriteAt(header, int64(base)); err != nil {
		t.Fatal(err)
	}
	view := &Volume{dev: &meteredIO{VolumeIO: disk}, sb: v.sb, limit: v.limit}
	if _, err = view.loadObjectAt(base); err != nil {
		t.Fatal("full decoder rejects legal metadata fixture", err)
	}
	if flag, err := view.objectImportedAt(base); err != nil || !flag {
		t.Fatal("legal retired-page metadata rejected", flag, err)
	}
	if _, err := view.objectImportedAt(treeStart - 1); !errors.Is(err, ErrCorrupt) {
		t.Fatal("reserved-region descriptor accepted", err)
	}
	for _, span := range o.Spans {
		if span.Location < dataStart {
			t.Fatal("payload below dataStart", span)
		}
	}
	// Corrupt only the external list. Header flag reads must not follow it;
	// complete descriptor loading must continue checking the external CRC.
	block := make([]byte, listBytes)
	if _, err = disk.ReadAt(block, int64(lists[0])); err != nil {
		t.Fatal(err)
	}
	block[508] ^= 1
	if _, err = disk.WriteAt(block, int64(lists[0])); err != nil {
		t.Fatal(err)
	}
	before := view.IOStats()
	if flag, err := view.objectImportedAt(base); err != nil || !flag {
		t.Fatal("header lookup followed external metadata list", flag, err)
	}
	after := view.IOStats()
	if reads := testReadWorkOps(after) - testReadWorkOps(before); reads > 2 {
		t.Fatal("header lookup walked external lists", reads)
	}
	if _, err := view.loadObjectAt(base); !errors.Is(err, ErrCorrupt) {
		t.Fatal("full decoder ignored external list CRC", err)
	}
}
