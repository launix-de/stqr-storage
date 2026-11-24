// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestLazyFreeWatermarks(t *testing.T) {
	for _, tc := range []struct {
		name                                                  string
		capacity, free, poolCapacity, poolFree, start, target uint64
	}{
		{"nearly empty", 1000, 850, 2000, 1850, 0, 0},
		{"idle new peer", 1000, 500, 2000, 1500, 0, 0},
		{"healthy boundary", 1000, 150, 2000, 800, 0, 0},
		{"imbalanced", 1000, 100, 2000, 800, 150, 200},
		{"tight pool", 1000, 30, 2000, 160, 40, 60},
		{"no target capacity", 1000, 0, 2000, 0, 0, 0},
		{"zero size", 0, 0, 0, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start, target := lazyFreeWatermarks(tc.capacity, tc.free, tc.poolCapacity, tc.poolFree)
			if start != tc.start || target != tc.target {
				t.Fatalf("got %d/%d, want %d/%d", start, target, tc.start, tc.target)
			}
		})
	}
}

func TestLazyHealthyPlacementDoesNotMoveOrReadPayload(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "lazy-source", "hdd", 8<<20)
	addReplicaDataVolume(t, target.node, "lazy-target", "hdd", 16<<20)
	reviewWaitDest(t, source.node, 2)
	b, _ := createProtocolBucket(t, source.node, "lazy-healthy")
	if _, err := b.Put(context.Background(), userPrincipal(t, source.node), volume.ObjectID{3}, bytes.Repeat([]byte("p"), 1<<20), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	before := src.IOStats()
	if p, ok := source.node.scopes.planNext(); ok {
		t.Fatal("healthy placement scheduled a move", p)
	}
	after := src.IOStats()
	if after.ReadBytes != before.ReadBytes || after.WriteBytes != before.WriteBytes {
		t.Fatal("idle planner accessed volume data", before, after)
	}
}
