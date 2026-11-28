// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Unlike a scheduling-delay assertion, the first attempt here MUST encounter a
// failed exclusive TryOpenBuffered: the test owns a read lease throughout that
// attempt. Initial shared inspection can proceed, and no CAS bytes are consumed
// until the caller has released admission and retried the complete operation.
func TestAcceptedCASMetadataRetryDoesNotConsumePayloadOrAwaitFence(t *testing.T) {
	s, v, probe, b, _ := authorityFenceFixture(t)
	gate := &probe.serverFlushGate
	p := quotaPrincipal(t, s)
	ctx := context.Background()
	id := volume.ObjectID{29}
	if _, err := b.Put(ctx, p, id, []byte("initial"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(ctx, p, id, []byte("before"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	s.opMu.Lock()
	err := s.prepareParallelMutation(b, p, id)
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	s.opMu.RLock()
	ready := s.parallelMutationReady(b, p, id)
	s.opMu.RUnlock()
	if !ready {
		t.Fatal("fixture must enable warm parallel admission before holding reader")
	}
	cut := v.Header().Generation
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	gate.mu.Lock()
	gate.entered, gate.release = entered, release
	gate.mu.Unlock()
	checkpoint := make(chan error, 1)
	go func() { checkpoint <- v.CheckpointThrough(cut) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit checkpoint did not reach controlled Sync")
	}
	reader, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	payload := bytes.NewReader([]byte("after"))
	opt := volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())}
	_, err = b.mutateAttempt(ctx, p, id, 0x0108, 0, 5, payload, []byte("before"), opt)
	var metadata *metadataAdmissionWait
	var persistence *acceptedCheckpoint
	if !errors.As(err, &metadata) || errors.As(err, &persistence) || metadata.volume != v {
		t.Fatalf("metadata collision misclassified as persistence: %T %v", err, err)
	}
	if payload.Len() != 5 {
		t.Fatal("failed first CAS attempt consumed source bytes", payload.Len())
	}
	reader.Close()
	admitted := make(chan struct{})
	go func() { metadata.volume.WaitForMetadata(); close(admitted) }()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("metadata wait included unrelated physical checkpoint")
	}
	before := v.IOStats()
	done := make(chan error, 1)
	go func() {
		_, err := b.mutateAttempt(ctx, p, id, 0x0108, 0, 5, payload, []byte("before"), opt)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("CAS retry failed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("accepted CAS retry awaited unrelated physical checkpoint")
	}
	if payload.Len() != 0 {
		t.Fatal("successful CAS did not consume replacement exactly once")
	}
	got, _, err := b.Get(ctx, p, id)
	if err != nil || string(got) != "after" {
		t.Fatalf("retried CAS not atomically visible: %q %v", got, err)
	}
	if after := v.IOStats(); after.SyncOps != before.SyncOps {
		t.Fatal("metadata retry requested another physical fence", before, after)
	}
	select {
	case err := <-checkpoint:
		t.Fatalf("explicit checkpoint escaped controlled gate: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-checkpoint:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("explicit checkpoint did not finish after release")
	}
}
