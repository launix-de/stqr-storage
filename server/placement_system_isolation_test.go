// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestDataPGFiltersNeverFallBackToSystemVolumes(t *testing.T) {
	for _, mode := range []VolumeFilterMode{VolumesAll, VolumesBlacklist, VolumesWhitelist} {
		t.Run(string(mode), func(t *testing.T) {
			s := testServer(t)
			data := addTestVolume(t, s, "data", "hdd", 2<<20)
			system := s.system.Identity().VolumeID.String()
			node := PlacementNode{NodeID: s.cfg.Node.ID, Mode: mode}
			if mode == VolumesWhitelist {
				node.Volumes = []string{system, data.VolumeID}
			}
			group := PlacementGroup{ID: client.NewRequestID().String(), Name: "data only", Revision: 1, Effective: PlacementMask{Version: 1, Nodes: []PlacementNode{node}}}
			installTestPlacementGroup(t, s, group)
			stores := s.placementVolumes(group.ID)
			if len(stores) != 1 || stores[0].Identity().VolumeID.String() != data.VolumeID {
				t.Fatal("data filter admitted a system device")
			}
			if !s.placementGroups.Load().allows(systemPlacementGroup.String(), s.cfg.Node.ID, system) {
				t.Fatal("metadata placement changed")
			}
		})
	}
}
