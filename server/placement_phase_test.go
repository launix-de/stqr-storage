// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestInitialPlacementUsesRebuildPhase(t *testing.T) {
	s, b, volumes, key := capacitySelectionFixture(t, 2)
	s.scopes = &rangeManager{server: s}
	now := time.Now()
	seedStatisticsVolume(s, client.ID(volumes[0].Identity().VolumeID), now, 128<<20)
	resources := make([]placementResource, len(volumes))
	options := make([]placementOption, len(volumes))
	ids := make([]client.ID, len(volumes))
	for i, v := range volumes {
		stats := v.Stats()
		resources[i] = placementResource{Node: client.ID(v.Identity().NodeID), Capacity: stats.CapacityPhysical, Free: stats.AdmissionFreePhysical}
		options[i] = placementOption{Range: 0, Volume: i, Allocate: 1}
		ids[i] = client.ID(v.Identity().VolumeID)
	}
	input := inputForInitialPlacement(len(options))
	input.BackgroundRead, input.BackgroundWrite = s.placementBackgroundVolumes(ids, now)
	phase, result := rebuildPlacement(context.Background(), []placementDemand{{Bucket: client.ID(b.ID()), Copies: 1, Bytes: 1}}, resources, options, nil, input, nil, initialPlacementBudget, 1)
	target, _, found, err := b.selectVolume(b.Describe(), key, volume.ID{}, true)
	if err != nil || found || target != volumes[options[phase.Assignments[0][0]].Volume] {
		t.Fatal("initial and rebuild phase disagree", found, err, phase)
	}
	if target != volumes[1] || result.After.Traffic >= result.Before.Traffic {
		t.Fatal("new writer ignored existing reader pressure", phase, result)
	}
	for _, option := range options {
		if option.Move != 0 {
			t.Fatal("empty placement charged migration")
		}
	}
}

func TestInitialPlacementCancelledAndFull(t *testing.T) {
	s, b, volumes, key := capacitySelectionFixture(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.ctx = ctx
	if _, _, _, err := b.selectVolume(b.Describe(), key, volume.ID{}, true); err != context.Canceled {
		t.Fatal("cancelled planning proceeded", err)
	}
	phase, result := rebuildPlacement(ctx, []placementDemand{{Copies: 1}}, nil, nil, nil, placementImproveInput{}, nil, initialPlacementBudget, 1)
	if len(phase.Pending) != 1 || len(phase.Assignments[0]) != 0 || !result.Stopped {
		t.Fatal("cancelled rebuild selected a destination", phase, result)
	}
	s.ctx = context.Background()
	for _, v := range volumes {
		if err := v.SetGuaranteedReserve(v.AdmissionFreePhysical()); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, _, err := b.selectVolume(b.Describe(), key, volume.ID{}, true); err != volume.ErrFull {
		t.Fatal("unbacked empty work assigned to full disk", err)
	}
}

func TestInitialBucketPhasePreservesStripingAndReplicas(t *testing.T) {
	source, peer := startAutomaticDaemonPair(t, false)
	s := source.node
	addReplicaDataVolume(t, s, "initial-local-a", "hdd", 16<<20)
	addReplicaDataVolume(t, s, "initial-local-b", "hdd", 16<<20)
	addReplicaDataVolume(t, peer.node, "initial-peer", "hdd", 16<<20)
	reviewWaitDest(t, s, 3)
	b, _ := createProtocolBucket(t, s, "shared-initial-phase")
	patch, ok := s.routingPatch(b.ID(), volume.ObjectID{})
	if !ok || len(patch.Entries) != 1 {
		t.Fatal("missing initial map")
	}
	locations := patch.Entries[0].Locations
	if len(locations) != 1 || locations[0].Node != s.scopes.node || locations[0].Volume != (client.ID{}) {
		t.Fatal("healthy initial owner lost striped cohort", locations)
	}
	d := b.Describe()
	d.Policy.Redundancy.Mode = "replication"
	d.Policy.Redundancy.Replicas = 2
	membership := s.currentSystem.Load().manifest.Membership
	locations, ok = s.scopes.initialBucketLocations(d, membership, s.scopes.node)
	if !ok || len(locations) != 2 || locations[0].Node == locations[1].Node || locations[0].Role != 0 || locations[1].Role != 1 {
		t.Fatal("initial phase ignored replica/node constraints", locations, ok)
	}
	d.Policy.Redundancy.Replicas = 3
	if locations, ok = s.scopes.initialBucketLocations(d, membership, s.scopes.node); ok {
		t.Fatal("local disks counted as distinct replica nodes", locations)
	}
}

func TestInitialBucketCountsSingleOwnerPrimaries(t *testing.T) {
	s := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false).node
	addReplicaDataVolume(t, s, "initial-counts", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "single-owner-count")
	image := s.placement.Load()
	entry, ok := image.lookup(client.ID(b.ID()), client.ObjectID{})
	if !ok || image.primaries[entry.Locations[0].Node] == 0 {
		t.Fatal("shared role objective has no single-owner baseline")
	}
}

func TestInitialPlacementPrivateBacking(t *testing.T) {
	s, b, volumes, _ := capacitySelectionFixture(t, 1)
	disk := volumes[0]
	id := disk.Identity().VolumeID
	bucket := client.ID(b.ID())
	s.cfg.Node.ID = disk.Identity().NodeID.String()
	backing := disk.AdmissionFreePhysical()
	if err := disk.SetGuaranteedReserve(backing); err != nil {
		t.Fatal(err)
	}
	s.quotaUsage.Store(&quotaUsageImage{volumes: map[volume.ID][]volume.BucketStorage{id: nil}})
	s.currentSystem.Store(&systemView{manifest: systemManifest{QuotaReserves: []quotaPhysicalReservation{{Bucket: bucket, Node: client.ID(disk.Identity().NodeID), Volume: client.ID(id), Backing: backing}}}})
	if selected, err := s.initialVolumePlacement(bucket, volume.HDD, volumes); err != nil || selected != disk {
		t.Fatal("own unused backing unavailable", err)
	}
	if _, err := s.initialVolumePlacement(client.ID{98}, volume.HDD, volumes); err != volume.ErrFull {
		t.Fatal("foreign guarantee lent to new placement", err)
	}
	s.quotaImports.Store(&quotaImportImage{reserved: map[quotaImportKey]uint64{{Bucket: bucket, Volume: client.ID(id)}: backing}})
	if _, err := s.initialVolumePlacement(bucket, volume.HDD, volumes); err != volume.ErrFull {
		t.Fatal("in-flight import backing spent twice", err)
	}
}
