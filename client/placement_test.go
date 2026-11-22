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
	conflict.Entries[0].Locations = []Location{{Node: ID{3}}}
	conflict.Addresses = []NodeAddress{{Node: ID{3}, Addresses: []string{"http://other:7419"}}}
	if _, e := m.Apply(conflict); e == nil {
		t.Fatal("accepted contradictory same-owner-epoch patch")
	}
	if _, _, _, _, known := m.Lookup(p.Bucket, a.Start); known {
		t.Fatal("conflict did not become unknown")
	}
	if _, _, _, _, known := m.Lookup(p.Bucket, b.Start); !known {
		t.Fatal("conflict erased unrelated suffix")
	}
}

func TestPartialMapMergesDisjointScopesWithoutGlobalRevisionOrder(t *testing.T) {
	base := testMapPatch()
	left, right, _ := SplitRange(base.Entries[0])
	base.Entries = []RangeEntry{left, right}
	a := testMapPatch()
	a.ToEnd, a.End, a.Entries = false, right.Start, []RangeEntry{left}
	a.Entries[0].OwnerEpoch = 8
	a.Revision = 80
	b := base
	b.Entries = append([]RangeEntry{}, base.Entries...)
	b.Entries[1].OwnerEpoch = 9
	b.Revision = 50 // Lower global hint, but a newer handoff on the right.
	for _, patches := range [][]MapPatch{{base, a, b}, {base, b, a}, {a, b}, {b, a}} {
		m := &PartialMap{}
		for _, p := range patches {
			if _, err := m.Apply(p); err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct {
			hash            ObjectID
			epoch, revision uint64
		}{{left.Start, 8, 80}, {right.Start, 9, 50}} {
			entry, revision, _, _, found := m.Lookup(base.Bucket, tc.hash)
			if !found || entry.OwnerEpoch != tc.epoch || revision != tc.revision {
				t.Fatal("disjoint update lost", entry.OwnerEpoch, revision, found)
			}
		}
		base.Revision = 1000
		if applied, err := m.Apply(base); applied || err != nil {
			t.Fatal("old checkpoint overwrote newer handoffs", applied, err)
		}
	}
}

func TestPartialMapIndependentRangesPermutation(t *testing.T) {
	rng := rand.New(rand.NewSource(771))
	base := testMapPatch()
	base.Entries = nil
	for i := 0; i < 256; i++ {
		e := testMapPatch().Entries[0]
		e.Start[0], e.PrefixBits = byte(i), 8
		base.Entries = append(base.Entries, e)
	}
	m := &PartialMap{}
	if _, err := m.Apply(base); err != nil {
		t.Fatal(err)
	}
	for _, i := range rng.Perm(256) {
		patch := testMapPatch()
		patch.Begin = base.Entries[i].Start
		patch.End, patch.ToEnd, _ = RangeEnd(patch.Begin, 8)
		patch.Revision = uint64(1 + rng.Intn(10))
		patch.Entries = []RangeEntry{base.Entries[i]}
		patch.Entries[0].OwnerEpoch = uint64(i + 2)
		if ok, err := m.Apply(patch); !ok || err != nil {
			t.Fatal("independent patch rejected", i, ok, err)
		}
	}
	base.Revision = 10000
	if ok, err := m.Apply(base); ok || err != nil {
		t.Fatal("stale full checkpoint accepted", ok, err)
	}
	for i, entry := range base.Entries {
		got, _, _, _, found := m.Lookup(base.Bucket, entry.Start)
		if !found || got.OwnerEpoch != uint64(i+2) {
			t.Fatal("range lost", i, got.OwnerEpoch, found)
		}
	}
}

func TestPartialMapSameRevisionHandoffAndPolicyAdvance(t *testing.T) {
	m := &PartialMap{}
	p := testMapPatch()
	if _, err := m.Apply(p); err != nil {
		t.Fatal(err)
	}
	p.Entries[0].OwnerEpoch++
	if ok, err := m.Apply(p); !ok || err != nil {
		t.Fatal("same revision hides newer handoff", ok, err)
	}
	p.Entries[0].PolicyVersion++
	if ok, err := m.Apply(p); !ok || err != nil {
		t.Fatal("same owner policy advance rejected", ok, err)
	}
	got, _, _, _, found := m.Lookup(p.Bucket, ObjectID{})
	if !found || got.OwnerEpoch != 2 || got.PolicyVersion != 2 {
		t.Fatal("new per-range versions missing")
	}
}

func TestPartialMapConflictOnlyForOverlappingSameAgeRange(t *testing.T) {
	m := &PartialMap{}
	base := testMapPatch()
	left, right, _ := SplitRange(base.Entries[0])
	base.Entries = []RangeEntry{left, right}
	if _, err := m.Apply(base); err != nil {
		t.Fatal(err)
	}
	patch := base
	patch.Revision = 90 // A high global hint cannot resolve conflicting authority.
	patch.Entries = append([]RangeEntry{}, base.Entries...)
	patch.Entries[0].Locations = []Location{{Node: ID{3}}}
	patch.Entries[1].OwnerEpoch = 2
	patch.Addresses = append(append([]NodeAddress{}, base.Addresses...), NodeAddress{Node: ID{3}, Addresses: []string{"http://other:7419"}})
	if _, err := m.Apply(patch); err == nil {
		t.Fatal("conflicting owner under unchanged epoch accepted")
	}
	if _, _, _, _, found := m.Lookup(base.Bucket, left.Start); found {
		t.Fatal("conflicting intersection remained known")
	}
	entry, _, _, _, found := m.Lookup(base.Bucket, right.Start)
	if !found || entry.OwnerEpoch != 2 {
		t.Fatal("unrelated valid part of patch discarded")
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

// Reference learning per hash byte deliberately ignores the implementation's
// segment representation. It catches interval clipping/gaps with overlapping
// snapshots, unequal revisions and contradictory same-epoch ownership.
func FuzzPartialMapMerge(f *testing.F) {
	f.Add([]byte{0, 0, 2, 0, 1, 128, 3, 1, 0, 0, 1, 0})
	f.Add([]byte{7, 255, 5, 1, 0, 0, 2, 0, 3, 128, 5, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			return
		}
		var expected [256]struct {
			known    bool
			entry    RangeEntry
			revision uint64
		}
		m := &PartialMap{}
		for at := 0; at+4 <= len(data); at += 4 {
			prefix := uint16(1 + data[at]%8)
			width := 1 << (8 - prefix)
			start := int(data[at+1]) / width * width
			patch := testMapPatch()
			patch.Policies = nil
			entry := patch.Entries[0]
			entry.Start[0], entry.PrefixBits = byte(start), prefix
			entry.OwnerEpoch = uint64(1 + data[at+2]%8)
			node := ID{2 + data[at+3]%2}
			entry.Locations = []Location{{Node: node}}
			patch.Addresses = []NodeAddress{{Node: node, Addresses: []string{"http://node:7419"}}}
			patch.Begin = entry.Start
			patch.End, patch.ToEnd, _ = RangeEnd(entry.Start, prefix)
			patch.Entries = []RangeEntry{entry}
			patch.Revision = 1 + uint64(data[at+2])*31 + uint64(data[at+3])
			conflict := false
			for i := start; i < start+width; i++ {
				x := &expected[i]
				switch {
				case !x.known || entry.OwnerEpoch > x.entry.OwnerEpoch:
					x.known, x.entry, x.revision = true, entry, patch.Revision
				case entry.OwnerEpoch < x.entry.OwnerEpoch:
					// Preserve the newer owner at this hash.
				case !reflect.DeepEqual(entry, x.entry):
					x.known = false
					conflict = true
				case patch.Revision >= x.revision:
					x.entry, x.revision = entry, patch.Revision
				}
			}
			if _, err := m.Apply(patch); (err != nil) != conflict {
				t.Fatal("conflict outcome differs from per-hash reference", at, err)
			}
			for i, want := range expected {
				got, revision, _, _, known := m.Lookup(patch.Bucket, ObjectID{byte(i)})
				if known != want.known || known && (revision != want.revision || !reflect.DeepEqual(got, want.entry)) {
					t.Fatal("learned range differs from per-hash reference", at, i, known, want.known)
				}
			}
		}
	})
}
