// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestCapacityRetriesDoNotRescanPendingOrActiveHandoff(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "retry-pressure-source", "hdd", 16<<20)
	addReplicaDataVolume(t, target.node, "retry-pressure-target", "hdd", 32<<20)
	reviewWaitDest(t, source.node, 2)
	b, _ := createProtocolBucket(t, source.node, "retry-pressure")
	for i := 0; i < 256; i++ {
		id := volume.ObjectID{byte(i)}
		if _, err := b.Put(context.Background(), userPrincipal(t, source.node), id, []byte("x"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.CheckpointThrough(src.Stats().Generation); err != nil {
		t.Fatal(err)
	}
	src.SetCacheLimits(1, 4096)
	id := volume.ObjectID{0x40, 1}
	r := source.node.scopes
	if !r.requestSpace(b.ID(), id, 64<<10, src.Identity().VolumeID) {
		t.Fatal("no capacity wait")
	}
	check := func(name string) {
		t.Helper()
		before := src.IOStats().ReadBytes
		for i := 0; i < 100; i++ {
			if !r.requestSpace(b.ID(), id, 64<<10, src.Identity().VolumeID) {
				t.Fatal(name, "lost pending wait")
			}
		}
		if reads := src.IOStats().ReadBytes - before; reads != 0 {
			t.Errorf("%s retries reread %d index bytes", name, reads)
		}
	}
	check("queued")
	plan, ok := r.nextPressure()
	if !ok {
		t.Fatal("missing queued plan")
	}
	if err := r.reserve(plan); err != nil {
		t.Fatal(err)
	}
	check("active")
}
