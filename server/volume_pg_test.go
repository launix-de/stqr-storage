// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestNewVolumeWaitsForSelectedPGLists(t *testing.T) {
	s := testServer(t)
	policy := storagePolicy(DefaultBucketPolicy())
	group := volume.ID(client.NewRequestID())
	mask := PlacementMask{Storage: &policy, Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesBlacklist}}}
	centralPGRequest(t, s, group, 0, mask)
	op, err := s.AddVolume(newID(), AddVolumeRequest{Name: "held", Class: "hdd", SizeBytes: 2 << 20, PlacementGroups: []string{group.String()}})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.placementVolumes(defaultPlacementGroup.String())) != 0 {
		t.Fatal("pending blacklist volume admitted to default placement")
	}
	stats := s.CollectStats(client.StatsFilter{})
	for _, v := range stats.Volumes {
		if v.ID.String() == op.VolumeID && v.State != 0 {
			t.Fatal("held volume advertised active")
		}
	}
	if _, err := s.releaseVolumePlacement(newID(), op.VolumeID); statusOf(err) != 409 {
		t.Fatal("released before blacklist commit", err)
	}
	// A restart retains the hold even though the physical volume is mounted.
	path := s.configPath
	s.Close()
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	s = reopened
	if len(s.placementVolumes(defaultPlacementGroup.String())) != 0 {
		t.Fatal("restart lost placement hold")
	}
	mask.Nodes[0].Volumes = []string{op.VolumeID}
	centralPGRequest(t, s, group, 1, mask)
	release, err := s.releaseVolumePlacement(newID(), op.VolumeID)
	if err != nil || release.State != "fulfilled" {
		t.Fatal("effective list did not permit release", err)
	}
	b := centralBucket(t, s, defaultPlacementGroup, "allowed", 1<<20)
	user, _ := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	if _, err := b.Put(context.Background(), principal{User: user}, volume.ObjectID{1}, []byte("works"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	excluded := centralBucket(t, s, group, "excluded", 1<<20)
	if _, err := excluded.Put(context.Background(), principal{User: user}, volume.ObjectID{1}, []byte("no"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); statusOf(err) != 507 {
		t.Fatal("released volume escaped selected blacklist", err)
	}
}
