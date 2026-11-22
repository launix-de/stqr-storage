// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestTreeWalkBoundsSeeksWithoutReadingPrecedingLeaves(t *testing.T) {
	key := func(i uint64) indexKey { var k indexKey; binary.BigEndian.PutUint64(k[41:], i); return k }
	nodes := map[uint64]treeNode{}
	root := treeNode{kind: treeObjects, level: 1}
	for i := uint64(0); i < 50; i++ {
		leaf := treeNode{kind: treeObjects}
		for j := uint64(0); j < 20; j++ {
			leaf.entries = append(leaf.entries, treeEntry{key: key(i*20 + j), value: i*20 + j})
		}
		nodes[i+2] = leaf
		root.entries = append(root.entries, treeEntry{key: key(i * 20), value: i + 2})
	}
	nodes[1] = root
	reads := 0
	read := func(off uint64) (treeNode, error) {
		reads++
		n, ok := nodes[off]
		if !ok {
			return n, ErrCorrupt
		}
		return n, nil
	}
	var got []uint64
	if err := treeWalkBounds(read, 1, key(801), key(804), 0, func(e treeEntry) error { got = append(got, e.value); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 801 || got[2] != 803 || reads != 2 {
		t.Fatal("bounded walk scanned unrelated leaves", got, reads)
	}
	cycle := func(uint64) (treeNode, error) {
		return treeNode{kind: treeObjects, level: 1, entries: []treeEntry{{key: key(0), value: 1}}}, nil
	}
	if err := treeWalkBounds(cycle, 1, key(1), key(4), 0, func(treeEntry) error { return nil }); !errors.Is(err, ErrCorrupt) {
		t.Fatal("unbounded corrupt cycle", err)
	}
}

func TestScanRequestPagesAndExtremeBucketBounds(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	for i := uint16(1); i <= 120; i++ {
		var id ID
		binary.BigEndian.PutUint16(id[14:], i)
		if _, err := h.Put(testKey(1), []byte{byte(i)}, Options{RequestID: id}); err != nil {
			t.Fatal(err)
		}
	}
	var cursor *ID
	count := 0
	var previous ID
	for {
		page, err := h.ScanRequestRecords(cursor, 13)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			if bytes.Compare(previous[:], r.ID[:]) >= 0 {
				t.Fatal("duplicate or out of order receipt")
			}
			previous = r.ID
			count++
		}
		next := page[len(page)-1].ID
		cursor = &next
	}
	if count != 120 {
		t.Fatal("lost retry records", count)
	}
	var bucket ID
	for i := range bucket {
		bucket[i] = 255
	}
	zero := Key{Bucket: bucket}
	last := zero
	for i := range last.Object {
		last.Object[i] = 255
	}
	for _, key := range []Key{zero, last} {
		if _, err := h.Put(key, nil, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := h.ScanBucketObjects(bucket, nil, 1)
	if err != nil || len(first) != 1 || first[0].Key != zero {
		t.Fatal("zero object excluded", first, err)
	}
	next, err := h.ScanBucketObjects(bucket, &zero.Object, 1)
	if err != nil || len(next) != 1 || next[0].Key != last {
		t.Fatal("maximum bucket/object excluded", next, err)
	}
	end, err := h.ScanBucketObjects(bucket, &last.Object, 1)
	if err != nil || len(end) != 0 {
		t.Fatal("bucket bounds leaked into request namespace", end, err)
	}
}
