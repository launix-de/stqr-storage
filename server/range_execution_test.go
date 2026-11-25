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

func TestRangeExecutionSharesWorkerWithForegroundHelper(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	a := addReplicaDataVolume(t, s, "source", "hdd", 8<<20)
	b := addReplicaDataVolume(t, s, "target", "hdd", 8<<20)
	bucket, _ := createProtocolBucket(t, s, "stealing")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actor := userPrincipal(t, s)
	object := volume.ObjectID{4}
	opt := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	original, err := bucket.Append(ctx, actor, object, []byte("once"), opt)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := s.routingPatch(bucket.ID(), object)
	pin, err := planRangeMove(before, before.Entries[0], s.scopes.node, a.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.scopes.move(ctx, pin); err != nil {
		t.Fatal(err)
	}
	if helped, err := s.scopes.stealSpace(ctx, bucket.ID(), a.Identity().VolumeID); helped || err != nil {
		t.Fatal("helper invented an unplanned move", helped, err)
	}
	before, _ = s.routingPatch(bucket.ID(), object)
	p, err := planRangeMove(before, before.Entries[0], s.scopes.node, b.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.scopes.reserve(p); err != nil {
		t.Fatal(err)
	}
	// Block the real storage action, allowing a helper to find the running
	// worker and lend priority before either can finish the handoff.
	hold, err := b.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	worker := make(chan error, 1)
	go func() { worker <- s.scopes.move(ctx, p) }()
	helper := make(chan error, 1)
	go func() {
		helped, err := s.scopes.stealSpace(ctx, bucket.ID(), a.Identity().VolumeID)
		if !helped && err == nil {
			err = errControlConflict
		}
		helper <- err
	}()
	for {
		s.scopes.pgRuntime.executionsMu.Lock()
		e := s.scopes.pgRuntime.executions[p.id()]
		shared := e != nil && e.refs == 2 && e.urgent.Load() == 1
		s.scopes.pgRuntime.executionsMu.Unlock()
		if shared {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("helper failed to share running execution")
		case <-time.After(time.Millisecond):
		}
	}
	if err := hold.Close(); err != nil {
		t.Fatal(err)
	}
	for _, done := range []chan error{worker, helper} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	entry, _ := s.placement.Load().lookup(client.ID(bucket.ID()), client.ObjectID(object))
	if entry.Locations[0].Volume != client.ID(b.Identity().VolumeID) {
		t.Fatal("target not activated", entry)
	}
	if result, err := bucket.Append(ctx, actor, object, []byte("once"), opt); err != nil || result != original {
		t.Fatal("handoff changed original append receipt", result, err)
	}
	data, _, err := bucket.Get(ctx, actor, object)
	if err != nil || string(data) != "once" {
		t.Fatal("duplicate or missing copy", string(data), err)
	}
	s.scopes.pgRuntime.executionsMu.Lock()
	remaining := len(s.scopes.pgRuntime.executions)
	s.scopes.pgRuntime.executionsMu.Unlock()
	if remaining != 0 {
		t.Fatal("execution gate leaked", remaining)
	}
}

func TestRebuildYieldsWithoutStarvingUnderForegroundLoad(t *testing.T) {
	s := &Server{}
	s.ioPriority.users.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.waitForUserIO(ctx); err != nil {
		t.Fatal("foreground load starved maintenance", err)
	}
	cancel()
	if err := s.waitForUserIO(ctx); err != context.Canceled {
		t.Fatal("cancelled maintenance ignored context", err)
	}
}
