// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func testMapPatch() MapPatch {
	return MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: ID{1}, ToEnd: true, Entries: []RangeEntry{{PolicyVersion: 1, OwnerEpoch: 1, Locations: []Location{{Node: ID{2}}}}}, Addresses: []NodeAddress{{Node: ID{2}, Addresses: []string{"http://node:7419"}}}, Policies: []PlacementPolicy{{Bucket: ID{1}, Version: 1, Name: "app", Replicas: 1, Unlimited: true}}}
}
func TestMapPatchCodecAndPartition(t *testing.T) {
	p := testMapPatch()
	left, right, e := SplitRange(p.Entries[0])
	if e != nil {
		t.Fatal(e)
	}
	p.Entries = []RangeEntry{left, right}
	b, e := p.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := DecodeMapPatch(b)
	if e != nil || !reflect.DeepEqual(decoded, p) {
		t.Fatal(decoded, e)
	}
	for n := 0; n < len(b); n++ {
		if _, e = DecodeMapPatch(b[:n]); e == nil {
			t.Fatal("accepted truncation", n)
		}
	}
	p.Entries[1].Start[0] = 0x81
	if p.Validate() == nil {
		t.Fatal("accepted gap/non-prefix range")
	}
	p = testMapPatch()
	p.Entries[0].Locations = append(p.Entries[0].Locations, p.Entries[0].Locations[0])
	if p.Validate() == nil {
		t.Fatal("accepted two primaries")
	}
	p = testMapPatch()
	p.Addresses = nil
	if p.Validate() == nil {
		t.Fatal("accepted owner without contact")
	}
}
func TestPartialMapBoundsStalePatchesAndMembership(t *testing.T) {
	p := testMapPatch()
	left, right, _ := SplitRange(p.Entries[0])
	p.Begin = right.Start
	p.Entries = []RangeEntry{right}
	m := &PartialMap{}
	if ok, e := m.Apply(p); !ok || e != nil {
		t.Fatal(ok, e)
	}
	if _, _, _, _, known := m.Lookup(p.Bucket, ObjectID{}); known {
		t.Fatal("unknown prefix became known")
	}
	max := ObjectID{}
	for i := range max {
		max[i] = 255
	}
	if _, _, _, _, known := m.Lookup(p.Bucket, max); !known {
		t.Fatal("last hash missing")
	}
	old := p
	p.Revision = 2
	p.Entries[0].OwnerEpoch = 2
	if _, e := m.Apply(p); e != nil {
		t.Fatal(e)
	}
	if ok, e := m.Apply(old); ok || e != nil {
		t.Fatal("stale patch applied", e)
	}
	middle := testMapPatch()
	middle.ToEnd = false
	middle.End = right.Start
	middle.Entries = []RangeEntry{left}
	middle.Revision = 2
	if _, e := m.Apply(middle); e != nil {
		t.Fatal(e)
	}
	p.Membership = 2
	p.Revision = 3
	if _, e := m.Apply(p); e != nil {
		t.Fatal(e)
	}
	if _, _, _, _, known := m.Lookup(p.Bucket, ObjectID{}); known {
		t.Fatal("old membership prefix retained")
	}
}
func TestPartialMapPreservesOutsideReplacementAndRejectsConflict(t *testing.T) {
	m := &PartialMap{}
	p := testMapPatch()
	m.Apply(p)
	left, right, _ := SplitRange(p.Entries[0])
	a, b, _ := SplitRange(right)
	update := p
	update.Revision = 2
	update.Begin = a.Start
	update.End = b.Start
	update.ToEnd = false
	update.Entries = []RangeEntry{a}
	update.Entries[0].OwnerEpoch = 2
	if _, e := m.Apply(update); e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		hash     ObjectID
		revision uint64
	}{{left.Start, 1}, {a.Start, 2}, {b.Start, 1}} {
		_, revision, _, _, known := m.Lookup(p.Bucket, test.hash)
		if !known || revision != test.revision {
			t.Fatal("bad segment bounds", revision, known)
		}
	}
	conflict := update
	conflict.Entries = append([]RangeEntry(nil), update.Entries...)
	conflict.Entries[0].OwnerEpoch = 3
	if _, e := m.Apply(conflict); e == nil {
		t.Fatal("accepted contradictory same-revision patch")
	}
	if _, _, _, _, known := m.Lookup(p.Bucket, a.Start); known {
		t.Fatal("conflict did not become unknown")
	}
	if _, _, _, _, known := m.Lookup(p.Bucket, b.Start); !known {
		t.Fatal("conflict erased unrelated suffix")
	}
}
func TestRangeSplitPartitionProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(53))
	p := testMapPatch()
	for step := 0; step < 200; step++ {
		index := rng.Intn(len(p.Entries))
		left, right, e := SplitRange(p.Entries[index])
		if e != nil {
			t.Fatal(e)
		}
		next := append([]RangeEntry(nil), p.Entries[:index]...)
		next = append(next, left, right)
		next = append(next, p.Entries[index+1:]...)
		p.Entries = next
		if e = p.Validate(); e != nil {
			t.Fatalf("split %d: %v", step, e)
		}
	}
}
func FuzzMapPatchCodec(f *testing.F) {
	b, _ := testMapPatch().MarshalBinary()
	f.Add(b)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxMessage {
			return
		}
		if p, e := DecodeMapPatch(b); e == nil {
			encoded, e := p.MarshalBinary()
			if e != nil || !bytes.Equal(encoded, b) {
				t.Fatal("patch roundtrip", e)
			}
		}
	})
}
