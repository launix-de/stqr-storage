// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestQuotaWholeBucketOwnershipClassification(t *testing.T) {
	node, disk, bucket := client.ID{1}, client.ID{2}, client.ID{3}
	entry := client.RangeEntry{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: node, Volume: disk}}}
	stampFor := func(entries ...client.RangeEntry) quotaOwnershipStamp {
		t.Helper()
		p := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: bucket, ToEnd: true, Entries: entries, Addresses: []client.NodeAddress{{Node: node, Addresses: []string{"http://one:7419"}}, {Node: client.ID{9}, Addresses: []string{"http://other:7419"}}}}
		image, err := buildPlacement([]client.MapPatch{p})
		if err != nil {
			t.Fatal(err)
		}
		return quotaOwnershipStamp{node: volume.ID(node), volume: volume.ID(disk), placement: image, general: true}
	}
	check := func(label string, stamp quotaOwnershipStamp, own, known bool) {
		t.Helper()
		got, exact := stamp.OwnershipForEntireBucket(volume.ID(bucket))
		if got != own || exact != known {
			t.Fatal(label, got, exact, "want", own, known)
		}
	}
	check("pinned primary", stampFor(entry), true, true)
	next := entry
	next.Locations = []client.Location{{Node: client.ID{9}, Volume: disk}}
	check("remote", stampFor(next), false, true)
	next.Locations = []client.Location{{Node: node, Volume: client.ID{8}}}
	check("other volume", stampFor(next), false, true)
	next.Locations = []client.Location{{Node: node}}
	check("unbound imports", stampFor(next), false, false)
	next = entry
	next.State = 4
	check("lost", stampFor(next), false, true)
	next = entry
	next.Locations = []client.Location{{Node: client.ID{9}, Volume: client.ID{8}}, {Node: node, Volume: disk, Role: 1}}
	check("replica", stampFor(next), true, true)
	left, right, err := client.SplitRange(entry)
	if err != nil {
		t.Fatal(err)
	}
	check("all local split", stampFor(left, right), true, true)
	right.Locations = []client.Location{{Node: client.ID{9}, Volume: disk}}
	check("mixed split", stampFor(left, right), false, false)
}

func TestQuotaWholeBucketTransitionKeepsExistingAccounting(t *testing.T) {
	r, v, _, patch, _ := placementInventoryFixture(t)
	s := r.server
	s.cfg.Node.ID = r.node.String()
	s.controller = &clusterController{}
	if err := s.refreshQuotaOwnership(v); err != nil {
		t.Fatal(err)
	}
	bid, vid := volume.ID(patch.Bucket), v.Identity().VolumeID
	if storagePhysical(s.quotaUsage.Load().volumes[vid], bid) == 0 {
		t.Fatal("initial physical ownership missing")
	}
	publish := func(p client.MapPatch) {
		t.Helper()
		p.Addresses = append(p.Addresses, client.NodeAddress{Node: client.ID{9}, Addresses: []string{"http://other:7419"}})
		image, err := buildPlacement([]client.MapPatch{p})
		if err != nil {
			t.Fatal(err)
		}
		s.placement.Store(image)
	}
	for i, own := range []bool{false, true} {
		next := patch
		next.Revision = uint64(i + 2)
		next.Entries = append([]client.RangeEntry(nil), patch.Entries...)
		next.Entries[0].OwnerEpoch = uint64(i + 2)
		loc := next.Entries[0].Locations[0]
		if !own {
			loc.Node = client.ID{9}
		}
		next.Entries[0].Locations = []client.Location{loc}
		publish(next)
		before := v.IOStats()
		if err := s.refreshQuotaOwnership(v); err != nil {
			t.Fatal(err)
		}
		if after := v.IOStats(); after.ReadOps != before.ReadOps || after.WriteOps != before.WriteOps {
			t.Fatal("whole-bucket server transition scanned storage", before, after)
		}
		if got := storagePhysical(s.quotaUsage.Load().volumes[vid], bid) > 0; got != own {
			t.Fatal("quota observer retained obsolete ownership", got, own)
		}
	}
	// The selected object lies in the left half. A mixed-owner transition must
	// retain the exact fallback and exclude that object's physical allocation.
	next := patch
	next.Revision = 4
	left, right, err := client.SplitRange(patch.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	left.OwnerEpoch = 4
	left.Locations = []client.Location{{Node: client.ID{9}, Volume: client.ID(vid)}}
	right.OwnerEpoch = 4
	next.Entries = []client.RangeEntry{left, right}
	publish(next)
	before := v.IOStats()
	if err := s.refreshQuotaOwnership(v); err != nil {
		t.Fatal(err)
	}
	if v.IOStats().ReadOps == before.ReadOps || storagePhysical(s.quotaUsage.Load().volumes[vid], bid) != 0 {
		t.Fatal("mixed ownership did not use exact recount")
	}
}
