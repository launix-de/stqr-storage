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

func referenceAdmissionPrincipal(t *testing.T, s *Server) principal {
	p := quotaPrincipal(t, s)
	p.internalCoordination = true
	p.frontendPermission = BucketRead
	return p
}

func referenceAdmissionLink(b, peer *Bucket) func() {
	b.mu.Lock()
	old := b.description
	linked := old
	linked.ClonePeers = []BucketClonePeer{{Bucket: peer.ID().String()}}
	b.setDescription(linked)
	b.mu.Unlock()
	return func() { b.mu.Lock(); b.setDescription(old); b.mu.Unlock() }
}

func TestReferenceSharedAdmissionColdCloneDoesNotBlockIndependentVolume(t *testing.T) {
	s, a, b, va, vb := parallelAdmissionFixture(t, false, "logical", 0)
	p := referenceAdmissionPrincipal(t, s)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	old := []byte("old holders")
	if _, err := a.Put(context.Background(), p, root, old, parallelOpts()); err != nil {
		t.Fatal(err)
	}
	unlink := referenceAdmissionLink(a, b)
	defer unlink()
	a.usage.mu.Lock()
	a.usage.valid = false
	a.usage.mu.Unlock()
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("new holders")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	beforeA, beforeB := va.IOStats(), vb.IOStats()
	first := make(chan error, 1)
	go func() {
		_, err := a.mutateUnmetered(context.Background(), p, root, 0x0108, 0, 11, gate, old, volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())})
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reference did not reach payload without a COW scan")
	}
	other := make(chan error, 1)
	otherPrincipal := quotaPrincipal(t, s)
	go func() {
		_, err := b.Put(context.Background(), otherPrincipal, volume.ObjectID{130}, []byte("independent"), parallelOpts())
		other <- err
	}()
	awaitParallel(t, other)
	a.usage.mu.Lock()
	admissions, tickets, reserved, valid := a.usage.activeAdmissions, a.usage.activeTickets, a.usage.reservedGrowth, a.usage.valid
	a.usage.mu.Unlock()
	if admissions != 1 || tickets != 1 || reserved != 0 || valid {
		t.Fatal("unknown usage was invented or admission untracked", admissions, tickets, reserved, valid)
	}
	s.parallelQuota.mu.Lock()
	ticket := s.parallelQuota.slots[va.Identity().VolumeID]
	s.parallelQuota.mu.Unlock()
	if ticket == nil || ticket.kind != mutationPhysicalOnlyQuota {
		t.Fatal("missing independent physical admission ticket")
	}
	stats := make(chan error, 1)
	go func() { _, err := a.logicalUsage(); stats <- err }()
	select {
	case err := <-stats:
		if statusOf(err) != 503 {
			t.Fatal("cold logical query did not report unknown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cold usage waited for a publisher's Volume lock")
	}

	// Shared admission still pins configuration/placement for the whole operation.
	admin := make(chan struct{})
	go func() { s.opMu.Lock(); s.opMu.Unlock(); close(admin) }()
	select {
	case <-admin:
		t.Fatal("exclusive admin crossed reference publisher")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	awaitParallel(t, first)
	select {
	case <-admin:
	case <-time.After(3 * time.Second):
		t.Fatal("admin did not resume")
	}
	assertParallelReservationsEmpty(t, s, a)
	if va.IOStats().SyncOps != beforeA.SyncOps || vb.IOStats().SyncOps != beforeB.SyncOps {
		t.Fatal("accepted references introduced a Sync")
	}
	a.usage.mu.Lock()
	valid = a.usage.valid
	a.usage.mu.Unlock()
	if valid {
		t.Fatal("unknown inherited usage became an invented total")
	}
	unlink()
	// Exact reconstruction is still available on an explicit subsequent query.
	got, err := a.logicalUsage()
	if err != nil || got != uint64(4+11) {
		t.Fatal("logical reconstruction lost root delta", got, err)
	}
}

func TestReferenceSharedAdmissionKnownUsageAndFailureSettlement(t *testing.T) {
	for _, cold := range []bool{false, true} {
		t.Run(map[bool]string{false: "known", true: "unknown"}[cold], func(t *testing.T) {
			s, a, b, va, _ := parallelAdmissionFixture(t, false, "logical", 0)
			p := referenceAdmissionPrincipal(t, s)
			root := volume.ObjectID(client.ObjectReferenceRootID())
			old := []byte("old")
			if _, err := a.Put(context.Background(), p, root, old, parallelOpts()); err != nil {
				t.Fatal(err)
			}
			restore := referenceAdmissionLink(a, b)
			defer restore()
			a.usage.mu.Lock()
			before := a.usage.bytes
			if cold {
				a.usage.valid = false
			}
			a.usage.mu.Unlock()
			syncs := va.IOStats().SyncOps
			_, err := a.CAS(context.Background(), p, root, 0, old, []byte("larger holders"), volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())})
			if err != nil {
				t.Fatal(err)
			}
			assertParallelReservationsEmpty(t, s, a)
			a.usage.mu.Lock()
			valid, used := a.usage.valid, a.usage.bytes
			a.usage.mu.Unlock()
			if valid == cold || !cold && used != before-uint64(len(old))+14 {
				t.Fatal("logical settlement", valid, used, before)
			}
			_, err = a.CAS(context.Background(), p, root, 0, old, []byte("rejected"), volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())})
			if !errors.Is(err, volume.ErrPrecondition) {
				t.Fatal("CAS mismatch", err)
			}
			assertParallelReservationsEmpty(t, s, a)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = a.CAS(ctx, p, root, 0, nil, nil, parallelOpts()); !errors.Is(err, context.Canceled) {
				t.Fatal("cancel", err)
			}
			assertParallelReservationsEmpty(t, s, a)
			if va.IOStats().SyncOps != syncs {
				t.Fatal("reference settlement started Sync")
			}
		})
	}
}

func TestReferenceSharedAdmissionEligibilityIsPrivateAndUnlimited(t *testing.T) {
	s, a, b, _, _ := parallelAdmissionFixture(t, false, "logical", 0)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	p := referenceAdmissionPrincipal(t, s)
	restore := referenceAdmissionLink(a, b)
	defer restore()
	if !s.parallelMutationCandidate(a, p, root) {
		t.Fatal("missing private positive control")
	}
	if s.parallelMutationCandidate(a, quotaPrincipal(t, s), root) || s.parallelMutationCandidate(a, p, volume.ObjectID{42}) {
		t.Fatal("external or user object entered clone exception")
	}
	a.mu.Lock()
	d := a.description
	finite := d
	finite.Policy.Limits.Unlimited = false
	finite.Policy.Limits.Absolute = 1 << 20
	a.setDescription(finite)
	a.mu.Unlock()
	if s.parallelMutationCandidate(a, p, root) {
		t.Fatal("finite clone quota bypassed inherited usage")
	}
	a.mu.Lock()
	physical := d
	physical.Policy.Limits.Accounting = "physical"
	a.setDescription(physical)
	a.mu.Unlock()
	if s.parallelMutationCandidate(a, p, root) {
		t.Fatal("physical quota entered logical-only exception")
	}
	a.mu.Lock()
	a.setDescription(d)
	a.mu.Unlock()
}

func TestReferenceSharedAdmissionStillUsesPhysicalGuaranteeAdmission(t *testing.T) {
	s, a, b, va, _ := parallelAdmissionFixture(t, false, "logical", 0)
	p := referenceAdmissionPrincipal(t, s)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	old := []byte("old holders")
	if _, err := a.Put(context.Background(), p, root, old, parallelOpts()); err != nil {
		t.Fatal(err)
	}
	restore := referenceAdmissionLink(a, b)
	defer restore()
	a.usage.mu.Lock()
	a.usage.valid = false
	a.usage.mu.Unlock()
	// Model another bucket's remaining guarantee using the engine's exact
	// admission contract. A physical-only ticket must never bypass this callback.
	protected := va.Stats().AdmissionFreePhysical
	actual := s.quotaAdmission(va, client.ID(va.Identity().VolumeID))
	called := false
	if err := va.SetAdmission(func(change volume.StorageChange) (uint64, error) {
		reserve, err := actual(change)
		if err != nil {
			return reserve, err
		}
		s.parallelQuota.mu.Lock()
		ticket := s.parallelQuota.slots[va.Identity().VolumeID]
		s.parallelQuota.mu.Unlock()
		if ticket == nil || ticket.kind != mutationPhysicalOnlyQuota {
			return 0, errors.New("physical-only mutation bypassed quota ticket")
		}
		called = true
		return max(reserve, protected), nil
	}); err != nil {
		t.Fatal(err)
	}
	before := va.IOStats()
	_, err := a.CAS(context.Background(), p, root, 0, old, bytes.Repeat([]byte{'x'}, 128<<10), volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())})
	// Existing range-capacity recovery can turn engine ENOSPC into a 503
	// handoff retry. Neither result may publish bytes or consume the guarantee.
	if (!errors.Is(err, volume.ErrFull) && statusOf(err) != 503) || !called {
		t.Fatal("reference consumed reserved physical space", called, err)
	}
	assertParallelReservationsEmpty(t, s, a)
	got, _, err := a.Get(context.Background(), p, root)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatal("rejected reference changed bytes", len(got), err)
	}
	if va.IOStats().SyncOps != before.SyncOps {
		t.Fatal("507 path started Sync")
	}
}
