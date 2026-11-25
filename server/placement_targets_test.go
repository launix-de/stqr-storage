// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

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
	if result, err := bucket.Append(ctx, actor, object, []byte("once"), opt); err != nil || result != original {
		t.Fatal("append receipt changed", result, err)
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
