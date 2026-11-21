// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"testing"
)

func TestPlacementUsesDetachedContiguousImage(t *testing.T) {
	location := client.Location{Node: client.ID{2}}
	r := client.RangeEntry{PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{location}}
	left, right, _ := client.SplitRange(r)
	p := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID{1}, ToEnd: true, Entries: []client.RangeEntry{left, right}, Addresses: []client.NodeAddress{{Node: client.ID{2}, Addresses: []string{"http://node:7419"}}}}
	image, e := buildPlacement([]client.MapPatch{p})
	if e != nil {
		t.Fatal(e)
	}
	if len(image.data) != 2*(placementHeaderBytes+placementLocationBytes) {
		t.Fatal("unexpected packed size")
	}
	p.Entries[0].Locations[0].Node = client.ID{8}
	p.Entries[1].Locations[0].Node = client.ID{9}
	for _, hash := range []client.ObjectID{{}, right.Start, {255, 255, 255}} {
		got, ok := image.lookup(p.Bucket, hash)
		if !ok || got.Locations[0].Node != location.Node {
			t.Fatal("shared caller memory or wrong lookup", got, ok)
		}
	}
	if _, ok := image.lookup(client.ID{3}, client.ObjectID{}); ok {
		t.Fatal("lookup crossed buckets")
	}
	if len(image.bucketEntries(p.Bucket)) != 2 {
		t.Fatal("bucket enumeration")
	}
}
