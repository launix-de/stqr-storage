// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"net/http/httptest"
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
	cancelled, stopHelper := context.WithCancel(ctx)
	withdrawn := make(chan error, 1)
	go func() { withdrawn <- s.scopes.executeMove(cancelled, p, true) }()
	for {
		s.scopes.pgRuntime.executionsMu.Lock()
		e := s.scopes.pgRuntime.executions[p.id()]
		shared := e != nil && e.refs == 3 && e.urgent.Load() == 2
		s.scopes.pgRuntime.executionsMu.Unlock()
		if shared {
			break
		}
		select {
		case <-ctx.Done():
			stopHelper()
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	stopHelper()
	if err := <-withdrawn; err != context.Canceled {
		t.Fatal("cancelled helper did not withdraw", err)
	}
	s.scopes.pgRuntime.executionsMu.Lock()
	e := s.scopes.pgRuntime.executions[p.id()]
	retained := e != nil && e.refs == 2 && e.urgent.Load() == 1
	s.scopes.pgRuntime.executionsMu.Unlock()
	if !retained {
		t.Fatal("cancelled helper damaged worker or retained priority")
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

// A helper can select a live capacity offer before the worker rejects it, but
// only acquire its execution gate after the rejection has been persisted.
func TestRangeCapacityHelperRetriesAbortedHandoff(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	a := addReplicaDataVolume(t, s, "source", "hdd", 8<<20)
	b := addReplicaDataVolume(t, s, "target", "hdd", 8<<20)
	bucket, _ := createProtocolBucket(t, s, "aborted-capacity-helper")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actor := userPrincipal(t, s)
	object := volume.ObjectID{4}
	if _, err := bucket.Put(ctx, actor, object, []byte("original"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.routingPatch(bucket.ID(), object)
	pin, err := planRangeMove(before, before.Entries[0], s.scopes.node, a.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.scopes.move(ctx, pin); err != nil {
		t.Fatal(err)
	}
	before, _ = s.routingPatch(bucket.ID(), object)
	source := a.Identity().VolumeID
	target := b
	p, err := planRangeMove(before, before.Entries[0], s.scopes.node, target.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.scopes.reserve(p); err != nil {
		t.Fatal(err)
	}
	// Hold the worker's gate at the exact selection/recheck boundary.
	r := s.scopes
	runtime := &r.pgRuntime
	e := &rangeExecution{gate: make(chan struct{}, 1), refs: 1}
	e.gate <- struct{}{}
	runtime.executionsMu.Lock()
	if runtime.executions == nil {
		runtime.executions = make(map[[32]byte]*rangeExecution)
	}
	runtime.executions[p.id()] = e
	runtime.executionsMu.Unlock()
	released := false
	defer func() {
		if !released {
			<-e.gate
		}
		runtime.executionsMu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(runtime.executions, p.id())
		}
		runtime.executionsMu.Unlock()
	}()
	done := make(chan error, 1)
	go func() {
		helped, err := r.stealSpace(ctx, bucket.ID(), source)
		if !helped && err == nil {
			err = errors.New("helper did not select the reserved capacity offer")
		}
		done <- err
	}()
	for e.urgent.Load() != 1 {
		select {
		case <-ctx.Done():
			t.Fatal("capacity helper did not wait for execution", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := r.cancelBeforeFence(ctx, p, volume.ErrFull); !errors.Is(err, volume.ErrFull) {
		t.Fatal("worker's capacity rejection did not abort cleanly", err)
	}
	<-e.gate
	released = true
	select {
	case err := <-done:
		response := httptest.NewRecorder()
		s3Failure(response, err)
		if objectStatus(err) != 503 || response.Code != 503 {
			t.Fatalf("aborted capacity helper must retry: native=%d S3=%d error=%v", objectStatus(err), response.Code, err)
		}
	case <-ctx.Done():
		t.Fatal("aborted capacity helper remained blocked", ctx.Err())
	}
	data, _, err := bucket.Get(ctx, actor, object)
	if err != nil || string(data) != "original" {
		t.Fatal("aborted helper changed source data", string(data), err)
	}
}
