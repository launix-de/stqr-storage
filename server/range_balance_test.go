// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
	"time"
)

func TestLazyFreeWatermarks(t *testing.T) {
	for _, tc := range []struct {
		name                                                  string
		capacity, free, poolCapacity, poolFree, start, target uint64
	}{
		{"nearly empty", 1000, 850, 2000, 1850, 0, 0},
		{"idle new peer", 1000, 500, 2000, 1500, 0, 0},
		{"healthy boundary", 1000, 100, 2000, 800, 0, 0},
		{"imbalanced", 1000, 90, 2000, 800, 100, 360},
		{"tight pool", 1000, 30, 2000, 160, 40, 72},
		{"no target capacity", 1000, 0, 2000, 0, 0, 0},
		{"50 MiB near full", 50 << 20, 2 << 20, 64 << 30, 32 << 30, 3 << 20, (50 << 20) * 45 / 100},
		{"50 MiB boundary", 50 << 20, 3 << 20, 64 << 30, 32 << 30, 0, 0},
		{"large volume 2 percent", 1 << 30, 1 << 20, 2 << 30, 1 << 30, (1 << 30) / 50, (1 << 30) * 45 / 100},
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
	if testStorageReadBytes(after) != testStorageReadBytes(before) || after.WriteBytes != before.WriteBytes {
		t.Fatal("idle planner accessed volume data", before, after)
	}
}

func TestCapacityGoalHysteresisAndForecast(t *testing.T) {
	now := time.Unix(100, 0)
	v := rangeDestination{capacity: 1000, free: 90, volume: client.VolumeStats{AllocatedPhysical: 910}}
	g := capacityGoal{}
	if n := g.observe(v, 10000, 8000, now); n != 410 {
		t.Fatal("initial reserve", n)
	}
	v.free = 200
	v.volume.AllocatedPhysical = 800
	if n := g.observe(v, 10000, 8000, now.Add(time.Second)); n != 300 {
		t.Fatal("stopped after crossing start watermark", n)
	}
	v.free = 500
	v.volume.AllocatedPhysical = 500
	if n := g.observe(v, 10000, 8000, now.Add(2*time.Second)); n != 0 {
		t.Fatal("did not stop at reserve", n)
	}
	v.free = 200
	v.volume.AllocatedPhysical = 800
	if n := g.observe(v, 10000, 8000, now.Add(3*time.Second)); n != 0 {
		t.Fatal("ordinary writes restarted balancing", n)
	}
	v.free = 90
	v.volume.AllocatedPhysical = 910
	if n := g.observe(v, 10000, 8000, now.Add(4*time.Second)); n <= 410 || n > 630 {
		t.Fatal("forecast not bounded by pool reserve", n)
	}
}

func TestCapacityPlannerChoosesUsefulReliefAcrossBuckets(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "reserve-small", "hdd", 8<<20)
	addReplicaDataVolume(t, target.node, "reserve-large", "hdd", 64<<20)
	reviewWaitDest(t, source.node, 2)
	tiny, _ := createProtocolBucket(t, source.node, "reserve-tiny")
	bulk, _ := createProtocolBucket(t, source.node, "reserve-bulk")
	ctx := context.Background()
	actor := userPrincipal(t, source.node)
	if _, err := tiny.Put(ctx, actor, volume.ObjectID{1}, bytes.Repeat([]byte("t"), 4096), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	for i := byte(0); i < 60 && src.Stats().AdmissionFreePhysical > src.Stats().CapacityPhysical/12; i++ {
		if _, err := bulk.Put(ctx, actor, volume.ObjectID{i * 4}, bytes.Repeat([]byte("b"), 128<<10), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(i, err)
		}
	}
	stats := src.Stats()
	if stats.AdmissionFreePhysical > stats.CapacityPhysical/10 {
		t.Fatal("source not near full", stats.AdmissionFreePhysical)
	}
	p, ok := source.node.scopes.planNext()
	if !ok || p.Before.Bucket != client.ID(bulk.ID()) {
		t.Fatal("planner picked ineffective tiny bucket", p.Before.Bucket)
	}
	cost, _, err := source.node.scopes.rangeCost(p.Before, p.Moving)
	if err != nil || cost < 3<<20 {
		t.Fatal("insufficient recovery batch", cost, err)
	}
	if err = source.node.scopes.move(ctx, p); err != nil {
		t.Fatal(err)
	}
	stats = src.Stats()
	if stats.AdmissionFreePhysical < stats.CapacityPhysical*4/10 {
		t.Fatal("did not establish useful reserve", stats.AdmissionFreePhysical)
	}
	// Crossing the trigger again is required; continued small writes are not a
	// reason to rebalance the remaining objects back toward equal fullness.
	if p, ok := source.node.scopes.planNext(); ok {
		t.Fatal("healthy reserve still migrates", p.Physical)
	}
}
