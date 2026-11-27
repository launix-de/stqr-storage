// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func referenceGateWaiters(t *testing.T, r *referenceCoordination, bucket volume.ID, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.mu.Lock()
		count := 0
		if g := r.gates[bucket]; g != nil {
			count = g.refs
		}
		r.mu.Unlock()
		if count == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reference waiters=%d, want%d", count, n)
		}
		time.Sleep(time.Millisecond)
	}
}
func TestObjectReferenceGateCancellationAndSeparateBuckets(t *testing.T) {
	var gates referenceCoordination
	bucket := volume.ID{1}
	release, err := gates.acquire(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		r, e := gates.acquire(ctx, bucket)
		if r != nil {
			r()
		}
		done <- e
	}()
	referenceGateWaiters(t, &gates, bucket, 2)
	independent, err := gates.acquire(context.Background(), volume.ID{2})
	if err != nil {
		t.Fatal(err)
	}
	independent()
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	referenceGateWaiters(t, &gates, bucket, 1)
	release()
	gates.mu.Lock()
	remaining := len(gates.gates)
	gates.mu.Unlock()
	if remaining != 0 {
		t.Fatal("gate leaked after cancellation", remaining)
	}
	if r, e := gates.acquire(ctx, bucket); !errors.Is(e, context.Canceled) || r != nil {
		t.Fatal("already cancelled request acquired gate", e)
	}
}
func TestObjectReferenceGateQueuedRequestsMakeProgress(t *testing.T) {
	var gates referenceCoordination
	bucket := volume.ID{1}
	hold, err := gates.acquire(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var active, completed atomic.Int32
	errors := make(chan error, 64)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := gates.acquire(ctx, bucket)
			if err != nil {
				errors <- err
				return
			}
			if active.Add(1) != 1 {
				t.Error("same bucket entered concurrently")
			}
			active.Add(-1)
			completed.Add(1)
			release()
		}()
	}
	referenceGateWaiters(t, &gates, bucket, 65)
	hold()
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if completed.Load() != 64 {
		t.Fatal("waiter starved", completed.Load())
	}
	gates.mu.Lock()
	remaining := len(gates.gates)
	gates.mu.Unlock()
	if remaining != 0 {
		t.Fatal("completed gates retained", remaining)
	}
}
func TestObjectReferenceGateRechecksAuthorizationAfterWait(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "gated", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "gated")
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketAllPermissions}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	ws.acceptedReferencesCap.Store(true)
	hold, err := s.referenceCoordination.acquire(context.Background(), b.ID())
	if err != nil {
		t.Fatal(err)
	}
	accepted := true
	message := referenceRequest(b, client.ObjectID{9}, client.NewRequestID(), &accepted)
	done := make(chan error, 1)
	p := quotaPrincipal(t, s)
	p.connection = ws.authorization
	go func() { _, err := s.executeObjectReference(context.Background(), message, ws, p); done <- err }()
	referenceGateWaiters(t, &s.referenceCoordination, b.ID(), 2)
	ws.authorization.invalid.Store(true)
	hold()
	if err = <-done; !errors.Is(err, ErrBucketForbidden) {
		t.Fatal("revoked waiter updated reference root", err)
	}
}

func TestObjectReferenceGateExternalCASRemainsVisible(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "external", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "external")
	p := quotaPrincipal(t, s)
	uid, err := bucketID(p.User.ID)
	if err != nil {
		t.Fatal(err)
	}
	root := volume.ObjectID(client.ObjectReferenceRootID())
	original := encodeObjectReferences(nil, b.ID())
	if _, err = b.Put(context.Background(), p, root, original, volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	external := objectReferenceEntry{object: volume.ObjectID{12}, account: uid, proof: [32]byte{7}}
	next := encodeObjectReferences([]objectReferenceEntry{external}, b.ID())
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketAllPermissions}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	ws.acceptedReferencesCap.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.opMu.Lock()
	externalDone := make(chan error, 1)
	go func() {
		_, e := b.CAS(ctx, p, root, 0, original, next, volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())})
		externalDone <- e
	}()
	deadline := time.Now().Add(time.Second)
	for s.ioPriority.users.Load() == 0 {
		if time.Now().After(deadline) {
			s.opMu.Unlock()
			t.Fatal("external CAS never reached admission")
		}
		time.Sleep(time.Millisecond)
	}
	refDone := make(chan error, 1)
	accepted := true
	go func() {
		_, e := s.executeObjectReference(ctx, referenceRequest(b, client.ObjectID{13}, client.NewRequestID(), &accepted), ws, p)
		refDone <- e
	}()
	// External CAS already owns ordinary mutation admission. The reference
	// request enters its independent gate and must honor the external root,
	// whether its first read races the external publication or observes it.
	referenceGateWaiters(t, &s.referenceCoordination, b.ID(), 1)
	s.opMu.Unlock()
	if err = <-externalDone; err != nil {
		t.Fatal(err)
	}
	if err = <-refDone; err != nil {
		t.Fatal(err)
	}
	raw, _, err := b.Get(ctx, p, root)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := decodeObjectReferences(raw, b.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || !bytes.Equal(entries[0].proof[:], external.proof[:]) {
		t.Fatal("reference overwrite lost external CAS", entries)
	}
}

func TestObjectReferenceGateRechecksOwnerEpochAfterWait(t *testing.T) {
	s := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false).node
	addTestVolume(t, s, "owner", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "owner")
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketAllPermissions}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	ws.acceptedReferencesCap.Store(true)
	hold, err := s.referenceCoordination.acquire(context.Background(), b.ID())
	if err != nil {
		t.Fatal(err)
	}
	accepted := true
	message := referenceRequest(b, client.ObjectID{9}, client.NewRequestID(), &accepted)
	done := make(chan error, 1)
	p := quotaPrincipal(t, s)
	go func() { _, err := s.executeObjectReference(context.Background(), message, ws, p); done <- err }()
	referenceGateWaiters(t, &s.referenceCoordination, b.ID(), 2)
	patches := append([]client.MapPatch{}, s.currentSystem.Load().patches...)
	for i := range patches {
		if patches[i].Bucket == client.ID(b.ID()) {
			patches[i].Entries = append([]client.RangeEntry{}, patches[i].Entries...)
			for j := range patches[i].Entries {
				patches[i].Entries[j].State = 5
				patches[i].Entries[j].OwnerEpoch++
			}
		}
	}
	image, err := buildPlacement(patches)
	if err != nil {
		hold()
		t.Fatal(err)
	}
	s.placement.Store(image)
	hold()
	if err = <-done; err == nil {
		t.Fatal("queued reference ignored new lost-owner placement")
	}
}

func TestObjectReferenceGateParallelBurstPreservesRoot(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "burst", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, s, "burst")
	p := quotaPrincipal(t, s)
	uid, err := bucketID(p.User.ID)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]objectReferenceEntry, 900)
	for i := range entries {
		entries[i] = objectReferenceEntry{object: volume.ObjectID{byte(i), byte(i >> 8)}, proof: sha256.Sum256([]byte{byte(i), byte(i >> 8)}), account: uid}
	}
	original := encodeObjectReferences(entries, b.ID())
	root := volume.ObjectID(client.ObjectReferenceRootID())
	if _, err = b.Put(context.Background(), p, root, original, volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	const clients = 19
	var sessions [clients]*wsSession
	var tokens [clients]client.ID
	for i := range sessions {
		ws := connectionTestSession(s)
		ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketAllPermissions}
		ws.authorization.objectReferences.Store(true)
		ws.durabilityCap.Store(true)
		ws.acceptedReferencesCap.Store(true)
		sessions[i] = ws
		tokens[i] = client.NewRequestID()
	}
	before := b.allVolumes()[0].IOStats()
	for _, action := range []byte{0, 1} {
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, clients)
		for i := range sessions {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				accepted := true
				message := referenceRequest(b, client.ObjectID{0xfa, byte(i)}, tokens[i], &accepted)
				message.Body[len(message.Body)-18] = action
				_, err := s.executeObjectReference(context.Background(), message, sessions[i], p)
				errs <- err
			}(i)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		raw, _, err := b.Get(context.Background(), p, root)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeObjectReferences(raw, b.ID())
		if err != nil {
			t.Fatal(err)
		}
		want := 900
		if action == 0 {
			want += clients
		}
		if len(got) != want {
			t.Fatal("parallel references lost entries", len(got), want)
		}
		if action == 1 && !bytes.Equal(raw, original) {
			t.Fatal("PIN/UNPIN burst changed preexisting root")
		}
	}
	if b.allVolumes()[0].IOStats().SyncOps != before.SyncOps {
		t.Fatal("accepted coordination forced Sync")
	}
	s.referenceCoordination.mu.Lock()
	remaining := len(s.referenceCoordination.gates)
	s.referenceCoordination.mu.Unlock()
	if remaining != 0 {
		t.Fatal("burst retained gate metadata", remaining)
	}
}
