// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestPlacementTargetMapRetainsLargestExistingCohortCopy(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	hdd := addReplicaDataVolume(t, s, "hdd", "hdd", 8<<20)
	retained := addReplicaDataVolume(t, s, "retained", "ssd", 8<<20)
	bucket, _ := createProtocolBucket(t, s, "cohort-minimum-copy")
	small, large := volume.ObjectID{1}, volume.ObjectID{2}
	for _, record := range []struct {
		store  *volume.Volume
		object volume.ObjectID
		size   int
	}{{hdd, small, 16 << 10}, {retained, large, 512 << 10}} {
		h, err := record.store.Open(true)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.Put(volume.Key{Bucket: bucket.ID(), Object: record.object}, bytes.Repeat([]byte{'x'}, record.size), volume.Options{RequestID: volume.ID(client.NewRequestID())})
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	// A larger empty destination must not win over retaining the majority of
	// an unpinned cohort's payload on an already suitable SSD.
	addReplicaDataVolume(t, s, "empty", "ssd", 32<<20)
	storage := storagePolicy(DefaultBucketPolicy())
	storage.StorageStrategy = "ssd"
	centralPGRequest(t, s, defaultPlacementGroup, 1, PlacementMask{NodeSelection: "all", Storage: &storage})
	m := s.scopes.buildPlacementTargets(defaultPlacementGroup.String(), s.bucketSnapshots())
	p, ok := s.scopes.nextPlacementTarget(m)
	if !ok || p.Volume != retained.Identity().VolumeID {
		t.Fatal("copied existing SSD data to the empty volume", p)
	}
	if p.Physical >= 128<<10 {
		t.Fatal("budget includes payload already retained at the destination", p.Physical)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := s.scopes.move(ctx, p); err != nil {
		t.Fatal(err)
	}
	for _, object := range []volume.ObjectID{small, large} {
		data, _, err := bucket.Get(ctx, userPrincipal(t, s), object)
		want := 16 << 10
		if object == large {
			want = 512 << 10
		}
		if err != nil || len(data) != want {
			t.Fatal("cohort data after consolidation", object, len(data), err)
		}
	}
}

func TestPlacementTargetMapRelievesReplicaVolumePressure(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	old := addReplicaDataVolume(t, source.node, "old", "hdd", 8<<20)
	retained := addReplicaDataVolume(t, target.node, "retained", "hdd", 8<<20)
	awaitReplicaDestinations(t, source.node, 2)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	bucket := replicaTestBucket(t, source.node, group, "replica-pressure")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	actor := userPrincipal(t, source.node)
	object := volume.ObjectID{4}
	opt := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	original := retryReplicaMutation(t, ctx, func() (volume.Result, error) { return bucket.Append(ctx, actor, object, []byte("once"), opt) })
	// Occupy the source volume with unrelated bytes. The replica itself is
	// small; moving it must not require relocating the valid follower.
	filler := volume.ID(client.NewRequestID())
	for i := byte(0); ; i++ {
		stats := old.Stats()
		if stats.AdmissionFreePhysical < stats.CapacityPhysical/12 {
			break
		}
		if i == 100 {
			t.Fatal("failed to create real volume pressure")
		}
		h, err := old.Open(true)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.Put(volume.Key{Bucket: filler, Object: volume.ObjectID{i}}, bytes.Repeat([]byte{'x'}, 128<<10), volume.Options{RequestID: volume.ID(client.NewRequestID())})
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh := addReplicaDataVolume(t, source.node, "fresh", "hdd", 32<<20)
	m := source.node.scopes.buildPlacementTargets(group.String(), source.node.bucketSnapshots())
	p, ok := source.node.scopes.nextPlacementTarget(m)
	if !ok || !p.ReplicaRelocate || p.Target != source.node.scopes.node || p.Volume != fresh.Identity().VolumeID {
		t.Fatal("pressure did not relocate only the constrained copy", p)
	}
	if err := source.node.scopes.move(ctx, p); err != nil {
		t.Fatal(err)
	}
	entry, _ := source.node.placement.Load().lookup(client.ID(bucket.ID()), client.ObjectID(object))
	found := false
	for _, loc := range entry.Locations {
		if loc.Node == target.node.scopes.node && loc.Volume == client.ID(retained.Identity().VolumeID) {
			found = true
		}
	}
	if !found {
		t.Fatal("moved valid follower unnecessarily", entry)
	}
	if _, ok := source.node.scopes.nextPlacementTarget(m); ok {
		t.Fatal("extra move after solving pressure placement")
	}
	if result, err := bucket.Append(ctx, actor, object, []byte("once"), opt); err != nil || result != original {
		t.Fatal("pressure move changed receipt", result, err)
	}
}

func TestPlacementTargetMapExecutesStorageClassDiff(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	a := addReplicaDataVolume(t, source.node, "old", "hdd", 8<<20)
	b := addReplicaDataVolume(t, target.node, "old", "hdd", 8<<20)
	x := addReplicaDataVolume(t, source.node, "new", "ssd", 8<<20)
	y := addReplicaDataVolume(t, target.node, "new", "ssd", 8<<20)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, a), whitelistReplicaMask(target.node, b)}})
	bucket := replicaTestBucket(t, source.node, group, "solver-class-diff")
	actor := userPrincipal(t, source.node)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	object := volume.ObjectID{4}
	opt := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	original := retryReplicaMutation(t, ctx, func() (volume.Result, error) { return bucket.Append(ctx, actor, object, []byte("once"), opt) })
	storage.StorageStrategy = "ssd"
	centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, x), whitelistReplicaMask(target.node, y)}})
	// The plan is solved once. Executing the second difference must not
	// reconstruct or rescore the first range after its first copy has moved.
	m := source.node.scopes.buildPlacementTargets(group.String(), source.node.bucketSnapshots())
	if m == nil || m.pending != 0 || len(m.targets) != 1 {
		t.Fatalf("target map %+v", m)
	}
	moved := make(map[client.ID]bool)
	for i := 0; i < 2; i++ {
		p, ok := source.node.scopes.nextPlacementTarget(m)
		if !ok || !p.ReplicaRelocate {
			t.Fatal("missing required class move", i, p)
		}
		if moved[client.ID(p.Volume)] {
			t.Fatal("copied the same target twice")
		}
		moved[client.ID(p.Volume)] = true
		if err := source.node.scopes.move(ctx, p); err != nil {
			t.Fatal(err)
		}
		data, _, err := bucket.Get(ctx, actor, object)
		if err != nil || string(data) != "once" {
			t.Fatal("mixed-class map cannot serve data", string(data), err)
		}
	}
	if !moved[client.ID(x.Identity().VolumeID)] || !moved[client.ID(y.Identity().VolumeID)] {
		t.Fatal("mask not enforced", moved)
	}
	if p, ok := source.node.scopes.nextPlacementTarget(m); ok {
		t.Fatal("extra payload move after satisfying target", p)
	}
	// Placement is complete, but its quota/policy checkpoint may still reject
	// writes transiently. Reuse the original request ID while that barrier drains.
	result := retryReplicaMutation(t, ctx, func() (volume.Result, error) {
		return bucket.Append(ctx, actor, object, []byte("once"), opt)
	})
	if result != original {
		t.Fatal("append receipt changed", result)
	}
	for _, node := range []*Server{source.node, target.node} {
		copy, err := node.Bucket(bucket.ID())
		if err != nil {
			t.Fatal(err)
		}
		data, _, err := copy.Get(ctx, actor, object)
		if err != nil || string(data) != "once" {
			t.Fatal("replica content", string(data), err)
		}
	}
}
