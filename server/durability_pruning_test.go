// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestAcceptedCleanupReleasesClosedVolumesWhilePeerPending(t *testing.T) {
	s := testServer(t)
	bucket := volume.ID(client.NewRequestID())
	pending, err := volume.NewRAM(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	generation := connectionTestPut(t, pending, volume.Key{Bucket: bucket, Object: volume.ObjectID{1}}, "pending")
	s.recordAccepted(bucket, pending, generation)
	stats := make(map[*volume.Volume]volume.IOStats)
	for n := byte(2); n < 5; n++ {
		v, err := volume.NewRAM(16 << 20)
		if err != nil {
			t.Fatal(err)
		}
		g := connectionTestPut(t, v, volume.Key{Bucket: bucket, Object: volume.ObjectID{n}}, "closed")
		s.recordAccepted(bucket, v, g)
		// Closing is not a durability request. Establish the confirmed fixture
		// through an explicit fence before testing nonblocking prefix pruning.
		if err := v.CheckpointThrough(g); err != nil {
			t.Fatal(err)
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
		if !v.TryIsDurableThrough(g) {
			t.Fatal("explicitly fenced closed volume lost confirmation")
		}
		stats[v] = v.IOStats()
	}
	// A busy still-unflushed peer cannot retain the confirmed closed devices,
	// block cleanup, or cause the bucket's entire prefix to be confirmed.
	stats[pending] = pending.IOStats()
	hold, err := pending.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { s.pruneDurability(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		hold.Close()
		t.Fatal("cleanup waited for a busy peer")
	}
	hold.Close()
	s.durability.mu.Lock()
	state := s.durability.buckets[bucket]
	retained, pendingGeneration := len(state.volumes), state.volumes[pending]
	confirmed := state.durable
	s.durability.mu.Unlock()
	if retained != 1 || pendingGeneration != generation {
		t.Fatalf("retained %d volumes, want only pending generation %d (got %d)", retained, generation, pendingGeneration)
	}
	mark, ok := s.acceptedMark(bucket)
	if !ok || mark.Sequence != 4 || confirmed != 0 {
		t.Fatal("partial cleanup confirmed an unflushed bucket prefix", mark, ok, confirmed)
	}
	for v, before := range stats {
		if after := v.IOStats(); after != before {
			t.Fatalf("cleanup performed storage I/O: before=%+v after=%+v", before, after)
		}
	}
	if err := pending.CheckpointThrough(generation); err != nil {
		t.Fatal(err)
	}
	s.pruneDurability()
	if _, ok := s.acceptedMark(bucket); ok {
		t.Fatal("fully checkpointed prefix not confirmed")
	}
	s.durability.mu.Lock()
	retained = len(state.volumes)
	s.durability.mu.Unlock()
	if retained != 0 {
		t.Fatalf("retained %d checkpointed devices", retained)
	}
}

// A disconnected/unmounted device must never turn a dropped accepted tail into
// a false persistence confirmation just because its handle has been closed.
func TestAcceptedClosedUnconfirmedVolumeNeverConfirms(t *testing.T) {
	s := testServer(t)
	bucket := volume.ID(client.NewRequestID())
	v, err := volume.NewRAM(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	generation := connectionTestPut(t, v, volume.Key{Bucket: bucket, Object: volume.ObjectID{1}}, "unconfirmed")
	s.recordAccepted(bucket, v, generation)
	before := v.IOStats()
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != before.SyncOps {
		t.Fatal("Close issued an unsolicited Sync")
	}
	s.pruneDurability()
	if v.TryIsDurableThrough(generation) {
		t.Fatal("Close invented persistence")
	}
	if _, ok := s.acceptedMark(bucket); !ok {
		t.Fatal("dropped accepted generation falsely confirmed")
	}
}
