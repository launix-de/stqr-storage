// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

// Physical pool measurements must apply the same PG and storage-class rules
// to allocated bytes as they apply to available bytes. In particular HDD may
// spill onto SSD, while SSD and RAM may not use the HDD pool.
func TestCOWStatFSPhysicalPoolFiltersPlacementAndClass(t *testing.T) {
	s := testServer(t)
	hdd := addTestVolume(t, s, "pool-hdd", "hdd", 4<<20)
	ssd := addTestVolume(t, s, "pool-ssd", "ssd", 6<<20)
	ram, err := s.AddVolume(newID(), AddVolumeRequest{Name: "pool-ram", Class: "ram", Source: 2, SizeBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	excluded := addTestVolume(t, s, "excluded-pool-hdd", "hdd", 10<<20)
	group := PlacementGroup{ID: pgID(69), Name: "statfs-pool", Revision: 1,
		Effective: PlacementMask{Version: 1, Nodes: []PlacementNode{{NodeID: s.running.Node.ID, Mode: VolumesWhitelist, Volumes: []string{hdd.VolumeID, ssd.VolumeID, ram.VolumeID}}}}}
	installTestPlacementGroup(t, s, group)
	draft := newTestBucket(t, "filtered-pool").Describe()
	draft.Policy.PlacementGroup = group.ID
	if _, err := s.buckets.Save(draft, 0); err != nil {
		t.Fatal(err)
	}
	b, err := s.BucketByName(draft.Name)
	if err != nil {
		t.Fatal(err)
	}
	var expectedUsed, expectedFree [3]uint64
	for class, ids := range [][]string{{hdd.VolumeID, ssd.VolumeID}, {ssd.VolumeID}, {ram.VolumeID}} {
		for _, id := range ids {
			st := s.volumes[id].store.Stats()
			expectedUsed[class] += st.AllocatedPhysical
			expectedFree[class] += st.AdmissionFreePhysical
		}
	}
	if s.volumes[excluded.VolumeID].store.Stats().AdmissionFreePhysical == 0 {
		t.Fatal("excluded-volume control has no free space")
	}
	for class := uint8(0); class < 3; class++ {
		c, err := s.bucketQuotaCapacityMode(b.ID(), class, true)
		if err != nil {
			t.Fatal(err)
		}
		if c.Used != expectedUsed[class] || c.Available != expectedFree[class] || c.Maximum != c.Used+c.Available {
			t.Fatalf("class %d: %+v want physical used=%d free=%d", class, c, expectedUsed[class], expectedFree[class])
		}
	}
	// A genuinely empty eligible mask must not silently fall back to all volumes.
	none := PlacementGroup{ID: pgID(70), Name: "empty-statfs-pool", Revision: 1,
		Effective: PlacementMask{Version: 1, Nodes: []PlacementNode{{NodeID: s.running.Node.ID, Mode: VolumesWhitelist}}}}
	installTestPlacementGroup(t, s, none)
	selected := draft.Policy
	selected.PlacementGroup = none.ID
	empty, err := s.bucketQuotaCapacityMode(volume.ID{}, 0, true, selected)
	if err != nil || empty.Used != 0 || empty.Available != 0 {
		t.Fatalf("empty pool: %+v %v", empty, err)
	}
}
