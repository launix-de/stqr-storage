// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func rangeWireFixture(t testing.TB) rangePlan {
	t.Helper()
	before := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID{3}, ToEnd: true, Entries: []client.RangeEntry{{PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{{Node: client.ID{1}, Volume: client.ID{4}}}}}}
	before.Addresses = []client.NodeAddress{{Node: client.ID{1}, Addresses: []string{"http://127.0.0.1:7419"}}, {Node: client.ID{2}, Addresses: []string{"http://127.0.0.1:7420"}}}
	_, right, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	p, err := planRangeMove(before, right, client.ID{2}, volume.ID{5}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestRangePlanAttemptAndLegacyWireIdentity(t *testing.T) {
	p := rangeWireFixture(t)
	if p.Attempt == (client.ID{}) {
		t.Fatal("offer omitted attempt identity")
	}
	for _, legacy := range []bool{false, true} {
		original := p
		if legacy {
			original.Attempt = client.ID{}
		}
		wire, err := original.marshal()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeRangePlan(wire)
		if err != nil || decoded.Attempt != original.Attempt || decoded.id() != original.id() {
			t.Fatal("decoder changed persisted offer identity", legacy, err)
		}
		again, err := decoded.marshal()
		if err != nil || !bytes.Equal(wire, again) {
			t.Fatal("noncanonical plan round trip", legacy, err)
		}
	}
	retry := p
	retry.Attempt = client.NewRequestID()
	if retry.id() == p.id() {
		t.Fatal("fresh offer reused cancelled plan identity")
	}
	for i := 0; i < 4; i++ {
		bad := p
		bad.After.Entries = append([]client.RangeEntry(nil), p.After.Entries...)
		bad.After.Entries[0].Locations = append([]client.Location(nil), p.After.Entries[0].Locations...)
		switch i {
		case 0:
			bad.After.Entries[0].Locations[0].Volume = client.ID{8}
		case 1:
			bad.After.Entries[0].Runtime = client.ID{8}
		case 2:
			bad.After.Entries[0].PolicyVersion++
		case 3:
			bad.After.Entries[0].OwnerEpoch++
		}
		if _, err := bad.marshal(); err == nil {
			t.Fatal("offer changed untouched sibling metadata", i)
		}
	}
}
func FuzzRangePlanDecoder(f *testing.F) {
	p := rangeWireFixture(f)
	wire, _ := p.marshal()
	f.Add(wire)
	p.Attempt = client.ID{}
	wire, _ = p.marshal()
	f.Add(wire)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := decodeRangePlan(data)
		if err != nil {
			return
		}
		wire, err := p.marshal()
		if err != nil {
			t.Fatal(err)
		}
		again, err := decodeRangePlan(wire)
		if err != nil || again.id() != p.id() {
			t.Fatal("canonical plan identity changed", err)
		}
	})
}
