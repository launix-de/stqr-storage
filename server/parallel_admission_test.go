// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type parallelPayloadGate struct {
	io.Reader
	entered, release chan struct{}
	once             sync.Once
}

func (r *parallelPayloadGate) Read(b []byte) (int, error) {
	r.once.Do(func() { close(r.entered); <-r.release })
	return r.Reader.Read(b)
}
func parallelOpts() volume.Options {
	return volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}
}

func parallelAdmissionFixture(t *testing.T, same bool, accounting string, limit uint64) (*Server, *Bucket, *Bucket, *volume.Volume, *volume.Volume) {
	t.Helper()
	n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := n.node
	va := addReplicaDataVolume(t, s, "parallel-a", "hdd", 16<<20)
	vb := addReplicaDataVolume(t, s, "parallel-b", "hdd", 16<<20)
	a := createQuotaBucket(t, s, "parallel-a", accounting, 0, limit)
	b := a
	if !same {
		b = createQuotaBucket(t, s, "parallel-b", accounting, 0, limit)
	}
	// Model committed single-copy ranges on independent physical volumes. Real
	// control/authority publication is exercised by the existing handoff suites.
	s.opMu.Lock()
	patches := append([]client.MapPatch{}, s.currentSystem.Load().patches...)
	for i := range patches {
		if patches[i].Bucket != client.ID(a.ID()) && patches[i].Bucket != client.ID(b.ID()) {
			continue
		}
		entry := patches[i].Entries[0]
		entry.Start = client.ObjectID{}
		entry.PrefixBits = 0
		entry.OwnerEpoch++
		device := va
		if patches[i].Bucket == client.ID(b.ID()) && !same {
			device = vb
		}
		entry.Locations = []client.Location{{Node: s.control.ledger.node, Volume: client.ID(device.Identity().VolumeID), Role: 0, Class: uint8(device.Identity().Class)}}
		patches[i].Entries = []client.RangeEntry{entry}
		if same {
			entry.PrefixBits = 1
			other := entry
			other.Start[0] = 128
			other.Locations = []client.Location{{Node: s.control.ledger.node, Volume: client.ID(vb.Identity().VolumeID), Role: 0, Class: uint8(vb.Identity().Class)}}
			patches[i].Entries = []client.RangeEntry{entry, other}
		}
	}
	image, err := buildPlacement(patches)
	if err != nil {
		s.opMu.Unlock()
		t.Fatal(err)
	}
	s.placement.Store(image)
	s.dirtyVolumeAuthority()
	err = s.refreshQuotaProtectionLocked()
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	p := quotaPrincipal(t, s)
	for _, x := range []struct {
		b  *Bucket
		id volume.ObjectID
	}{{a, volume.ObjectID{1}}, {b, volume.ObjectID{129}}} {
		if _, err = x.b.Put(context.Background(), p, x.id, []byte("seed"), parallelOpts()); err != nil {
			t.Fatal(err)
		}
	}
	s.opMu.Lock()
	err = s.prepareParallelMutation(a, p, volume.ObjectID{2})
	if err == nil {
		err = s.prepareParallelMutation(b, p, volume.ObjectID{130})
	}
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	s.opMu.RLock()
	readyA, readyB := s.parallelMutationReady(a, p, volume.ObjectID{2}), s.parallelMutationReady(b, p, volume.ObjectID{130})
	s.opMu.RUnlock()
	if !readyA || !readyB {
		t.Fatalf("fixture did not enable parallel admission: %v %v", readyA, readyB)
	}
	return s, a, b, va, vb
}

func awaitParallel(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("independent operation blocked")
	}
}

func TestParallelAdmissionIndependentVolumesFiniteLogicalQuota(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(map[bool]string{false: "different-buckets", true: "same-bucket"}[same], func(t *testing.T) {
			s, a, b, va, vb := parallelAdmissionFixture(t, same, "logical", 16<<30)
			p := quotaPrincipal(t, s)
			gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("staged payload")), entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(gate.release) }) }
			defer release()
			beforeA, beforeB := va.IOStats(), vb.IOStats()
			first := make(chan error, 1)
			go func() {
				_, err := a.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 14, gate, nil, parallelOpts())
				first <- err
			}()
			select {
			case <-gate.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("A did not reach its payload phase")
			}
			other := make(chan error, 1)
			go func() {
				_, err := b.Put(context.Background(), p, volume.ObjectID{130}, []byte("parallel B"), parallelOpts())
				other <- err
			}()
			awaitParallel(t, other)
			got, _, err := b.Get(context.Background(), p, volume.ObjectID{130})
			if err != nil || string(got) != "parallel B" {
				t.Fatal(got, err)
			}
			// Administrative state, new range binding and Close retain the exclusive
			// lifetime barrier, so they cannot invalidate A's admitted authority.
			admin := make(chan struct{})
			go func() { s.opMu.Lock(); s.opMu.Unlock(); close(admin) }()
			select {
			case <-admin:
				t.Fatal("admin barrier passed a live publisher")
			case <-time.After(20 * time.Millisecond):
			}
			release()
			awaitParallel(t, first)
			select {
			case <-admin:
			case <-time.After(3 * time.Second):
				t.Fatal("admin did not resume")
			}
			if va.IOStats().SyncOps != beforeA.SyncOps || vb.IOStats().SyncOps != beforeB.SyncOps {
				t.Fatal("accepted burst started a physical Sync")
			}
			for _, bucket := range []*Bucket{a, b} {
				bucket.usage.mu.Lock()
				active, reserved := bucket.usage.activeAdmissions, bucket.usage.reservedGrowth
				bucket.usage.mu.Unlock()
				if active != 0 || reserved != 0 {
					t.Fatal("logical reservations leaked", active, reserved)
				}
			}
			s.parallelQuota.mu.Lock()
			slots, physical := len(s.parallelQuota.slots), len(s.parallelQuota.physical)
			s.parallelQuota.mu.Unlock()
			if slots != 0 || physical != 0 {
				t.Fatal("physical reservations leaked", slots, physical)
			}
		})
	}
}

func assertParallelReservationsEmpty(t *testing.T, s *Server, b *Bucket) {
	t.Helper()
	b.usage.mu.Lock()
	active, tickets, reserved := b.usage.activeAdmissions, b.usage.activeTickets, b.usage.reservedGrowth
	b.usage.mu.Unlock()
	if active != 0 || tickets != 0 || reserved != 0 {
		t.Fatal("logical ticket leak", active, tickets, reserved)
	}
	s.parallelQuota.mu.Lock()
	slots, physical, quarantine := len(s.parallelQuota.slots), len(s.parallelQuota.physical), len(s.parallelQuota.quarantine)
	s.parallelQuota.mu.Unlock()
	if slots != 0 || physical != 0 || quarantine != 0 {
		t.Fatal("physical ticket leak", slots, physical, quarantine)
	}
}

func TestParallelLogicalQuotaReservesConcurrentGrowth(t *testing.T) {
	const limit = 128 << 10
	s, b, _, va, vb := parallelAdmissionFixture(t, true, "logical", limit)
	p := quotaPrincipal(t, s)
	gate := &parallelPayloadGate{Reader: bytes.NewReader(bytes.Repeat([]byte{'a'}, 80<<10)), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	beforeA, beforeB := va.IOStats(), vb.IOStats()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 80<<10, gate, nil, parallelOpts())
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("A did not reserve logical growth")
	}
	second := make(chan error, 1)
	go func() {
		_, err := b.Put(context.Background(), p, volume.ObjectID{130}, bytes.Repeat([]byte{'b'}, 80<<10), parallelOpts())
		second <- err
	}()
	select {
	case err := <-second:
		if !errors.Is(err, volume.ErrFull) {
			t.Fatal("simultaneous growth was not rejected", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("quota decision waited for unrelated volume")
	}
	release()
	awaitParallel(t, first)
	if used, err := b.logicalUsage(); err != nil || used != 80<<10+8 || used > limit {
		t.Fatal("wrong committed logical usage", used, err)
	}
	if _, err := b.Put(context.Background(), p, volume.ObjectID{130}, []byte("fits"), parallelOpts()); err != nil {
		t.Fatal(err)
	}
	if va.IOStats().SyncOps != beforeA.SyncOps || vb.IOStats().SyncOps != beforeB.SyncOps {
		t.Fatal("quota reservation started Sync")
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelPhysicalQuotaReservesBeforePublication(t *testing.T) {
	const limit = 128 << 10
	s, b, _, va, vb := parallelAdmissionFixture(t, true, "physical", limit)
	p := quotaPrincipal(t, s)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	actual := s.quotaAdmission(va, client.ID(va.Identity().VolumeID))
	var gated atomic.Bool
	if err := va.SetAdmission(func(change volume.StorageChange) (uint64, error) {
		protected, err := actual(change)
		if err == nil && gated.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return protected, err
	}); err != nil {
		t.Fatal(err)
	}
	beforeA, beforeB := va.IOStats(), vb.IOStats()
	first := make(chan error, 1)
	go func() {
		_, err := b.Put(context.Background(), p, volume.ObjectID{2}, bytes.Repeat([]byte{'a'}, 80<<10), parallelOpts())
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("A did not reserve exact physical plan")
	}
	s.parallelQuota.mu.Lock()
	reserved := s.parallelQuota.physical[b.ID()]
	s.parallelQuota.mu.Unlock()
	if reserved < 80<<10 {
		t.Fatal("physical growth was not reserved before publication", reserved)
	}
	second := make(chan error, 1)
	go func() {
		_, err := b.Put(context.Background(), p, volume.ObjectID{130}, bytes.Repeat([]byte{'b'}, 80<<10), parallelOpts())
		second <- err
	}()
	select {
	case err := <-second:
		if !errors.Is(err, volume.ErrFull) {
			t.Fatal("double physical spend was not rejected", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("physical quota decision waited for A")
	}
	unblock()
	awaitParallel(t, first)
	used, err := b.physicalUsage()
	if err != nil || used > limit || used < 80<<10 {
		t.Fatal("wrong physical usage", used, err)
	}
	if _, _, err = b.Get(context.Background(), p, volume.ObjectID{130}); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("rejected physical write published", err)
	}
	if va.IOStats().SyncOps != beforeA.SyncOps || vb.IOStats().SyncOps != beforeB.SyncOps {
		t.Fatal("physical reservation started Sync")
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelQuotaRefundsReaderAndCASFailures(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 128<<10)
	p := quotaPrincipal(t, s)
	// This source aborts after the logical reservation, before publication.
	_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 80<<10, bytes.NewReader([]byte("short")), nil, parallelOpts())
	if err == nil {
		t.Fatal("short source accepted")
	}
	assertParallelReservationsEmpty(t, s, b)
	// Invalid cache is reconstructed only after all publishers have left shared
	// admission. The next correct write can use the refunded capacity.
	if _, err = b.Put(context.Background(), p, volume.ObjectID{130}, bytes.Repeat([]byte{'x'}, 80<<10), parallelOpts()); err != nil {
		t.Fatal("aborted source leaked quota", err)
	}
	casOptions := parallelOpts()
	casOptions.WholeObject = true
	if _, err = b.CAS(context.Background(), p, volume.ObjectID{130}, 0, []byte("wrong"), bytes.Repeat([]byte{'y'}, 100<<10), casOptions); !errors.Is(err, volume.ErrPrecondition) {
		t.Fatal("CAS precondition not rejected", err)
	}
	assertParallelReservationsEmpty(t, s, b)
	if used, err := b.logicalUsage(); err != nil || used != 80<<10+8 {
		t.Fatal("failed operation changed logical count", used, err)
	}
}

func TestParallelSameObjectMutationsRetainOrder(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 16<<30)
	p := quotaPrincipal(t, s)
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 5, gate, nil, parallelOpts())
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first writer not active")
	}
	second := make(chan error, 1)
	go func() {
		_, err := b.Append(context.Background(), p, volume.ObjectID{2}, []byte("second"), parallelOpts())
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatal("same object bypassed its active mutation", err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	awaitParallel(t, first)
	awaitParallel(t, second)
	got, _, err := b.Get(context.Background(), p, volume.ObjectID{2})
	if err != nil || string(got) != "firstsecond" {
		t.Fatal(got, err)
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelPhysicalAdmissionFailureRefundsExactPlan(t *testing.T) {
	s, b, _, va, _ := parallelAdmissionFixture(t, true, "physical", 128<<10)
	p := quotaPrincipal(t, s)
	actual := s.quotaAdmission(va, client.ID(va.Identity().VolumeID))
	var rejected atomic.Bool
	if err := va.SetAdmission(func(change volume.StorageChange) (uint64, error) {
		protected, err := actual(change)
		if err == nil && rejected.CompareAndSwap(false, true) {
			return 0, volume.ErrPrecondition
		}
		return protected, err
	}); err != nil {
		t.Fatal(err)
	}
	before := va.IOStats()
	if _, err := b.Put(context.Background(), p, volume.ObjectID{2}, bytes.Repeat([]byte{'x'}, 80<<10), parallelOpts()); !errors.Is(err, volume.ErrPrecondition) {
		t.Fatal(err)
	}
	assertParallelReservationsEmpty(t, s, b)
	if _, err := b.Put(context.Background(), p, volume.ObjectID{2}, bytes.Repeat([]byte{'x'}, 80<<10), parallelOpts()); err != nil {
		t.Fatal("failed transaction leaked its exact physical plan", err)
	}
	if va.IOStats().SyncOps != before.SyncOps {
		t.Fatal("failure refund requested Sync")
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelAdmissionCloseWaitsForActivePublisher(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 16<<30)
	p := quotaPrincipal(t, s)
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 5, gate, nil, parallelOpts())
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.RLock()
		closing := s.closed
		s.mu.RUnlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not start")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatal("Close passed a live shared publisher", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	awaitParallel(t, first)
	awaitParallel(t, closed)
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelAdmissionLeaseWaitsAndRevokesLaterWrites(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 16<<30)
	p := quotaPrincipal(t, s)
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 5, gate, nil, parallelOpts())
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher did not start")
	}
	_, epoch, err := s.leaseMembers()
	if err != nil {
		t.Fatal(err)
	}
	request := leaseRequest{Action: 1, Lease: bucketLease{Bucket: b.ID(), Epoch: epoch, Sequence: 1, Request: client.NewRequestID(), Token: client.NewRequestID(), Owner: s.control.ledger.node, Boot: s.boot, User: p.User.ID, Username: p.User.Name, ObjectIO: true, ReadOnly: true}}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	leased := make(chan error, 1)
	go func() {
		_, err := s.receiveBucketLease(context.Background(), body, s.control.ledger.node)
		leased <- err
	}()
	select {
	case err := <-leased:
		t.Fatal("lease passed active shared publisher", err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	awaitParallel(t, first)
	awaitParallel(t, leased)
	if _, err = b.Put(context.Background(), p, volume.ObjectID{130}, []byte("forbidden"), parallelOpts()); err == nil {
		t.Fatal("new publisher ignored installed read-only lease")
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelAdmissionMapChangeWaitsAndRevalidatesAuthority(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 16<<30)
	p := quotaPrincipal(t, s)
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 5, gate, nil, parallelOpts())
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher did not start")
	}
	changed := make(chan error, 1)
	go func() {
		s.opMu.Lock()
		defer s.opMu.Unlock()
		patches := append([]client.MapPatch{}, s.currentSystem.Load().patches...)
		for i := range patches {
			if patches[i].Bucket == client.ID(b.ID()) {
				patches[i].Entries = append([]client.RangeEntry{}, s.placement.Load().bucketEntries(client.ID(b.ID()))...)
				for j := range patches[i].Entries {
					patches[i].Entries[j].OwnerEpoch++
				}
			}
		}
		image, err := buildPlacement(patches)
		if err == nil {
			s.placement.Store(image)
			s.dirtyVolumeAuthority()
		}
		changed <- err
	}()
	select {
	case err := <-changed:
		t.Fatal("map changed while old-scope publisher active", err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	awaitParallel(t, first)
	awaitParallel(t, changed)
	s.opMu.RLock()
	ready := s.parallelMutationReady(b, p, volume.ObjectID{130})
	s.opMu.RUnlock()
	if ready {
		t.Fatal("stale authority proof admitted new map")
	}
	// The cold retry leaves shared admission before staging/waiting for the new
	// header fence, then reauthorizes under exclusive admission.
	if _, err := b.Put(context.Background(), p, volume.ObjectID{130}, []byte("new epoch"), parallelOpts()); err != nil {
		t.Fatal(err)
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelInvalidUsageNeverScansUnderActiveVolumeHandle(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 16<<30)
	p := quotaPrincipal(t, s)
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 5, gate, nil, parallelOpts())
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher did not start")
	}
	b.usage.mu.Lock()
	b.usage.valid = false
	b.usage.mu.Unlock()
	stats := make(chan error, 1)
	go func() { _, err := b.logicalUsage(); stats <- err }()
	select {
	case err := <-stats:
		if statusOf(err) != 503 {
			t.Fatal("unknown usage must be explicit", err)
		}
	case <-time.After(time.Second):
		t.Fatal("usage scan inverted Volume and bucket locks")
	}
	release()
	awaitParallel(t, first)
	if _, err := b.Put(context.Background(), p, volume.ObjectID{130}, []byte("second"), parallelOpts()); err != nil {
		t.Fatal(err)
	}
	if used, err := b.logicalUsage(); err != nil || used != 8+5+6 {
		t.Fatal("cold reconstruction lost committed bytes", used, err)
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelAmbiguousPhysicalPlanRemainsConservativeUntilExactObserver(t *testing.T) {
	s, b, _, va, _ := parallelAdmissionFixture(t, true, "physical", 128<<10)
	p := quotaPrincipal(t, s)
	actual := s.quotaAdmission(va, client.ID(va.Identity().VolumeID))
	if err := va.SetAdmission(func(change volume.StorageChange) (uint64, error) {
		protected, err := actual(change)
		if err == nil {
			return 0, errors.Join(volume.ErrRecovery, volume.ErrFull)
		}
		return protected, err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(context.Background(), p, volume.ObjectID{2}, bytes.Repeat([]byte{'x'}, 80<<10), parallelOpts()); err == nil {
		t.Fatal("ambiguous failure ignored")
	}
	b.usage.mu.Lock()
	active, reserved := b.usage.activeAdmissions, b.usage.reservedGrowth
	b.usage.mu.Unlock()
	s.parallelQuota.mu.Lock()
	slots, held, quarantined := len(s.parallelQuota.slots), s.parallelQuota.physical[b.ID()], len(s.parallelQuota.quarantine)
	s.parallelQuota.mu.Unlock()
	if active != 0 || reserved != 0 || slots != 0 || held < 80<<10 || quarantined != 1 {
		t.Fatal("ambiguous state was treated as free capacity", active, reserved, slots, held, quarantined)
	}
	// A verified accounting publication replaces the uncertain charge. In real
	// backend failures SetAdmission itself is fenced until storage is recovered.
	id := va.Identity().VolumeID
	if err := va.SetAdmission(actual, func(usage volume.StorageSnapshot) { s.publishQuotaUsage(id, usage.Owned, usage.All) }); err != nil {
		t.Fatal(err)
	}
	assertParallelReservationsEmpty(t, s, b)
	if _, err := b.Put(context.Background(), p, volume.ObjectID{2}, bytes.Repeat([]byte{'x'}, 80<<10), parallelOpts()); err != nil {
		t.Fatal("verified recovery kept stale reservation", err)
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelAdmissionUnpinnedMultipleVolumeCohortUsesExclusiveFallback(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 16<<30)
	p := quotaPrincipal(t, s)
	s.opMu.Lock()
	patches := append([]client.MapPatch{}, s.currentSystem.Load().patches...)
	for i := range patches {
		if patches[i].Bucket == client.ID(b.ID()) {
			entries := append([]client.RangeEntry{}, s.placement.Load().bucketEntries(client.ID(b.ID()))...)
			for j := range entries {
				entries[j].Locations = append([]client.Location{}, entries[j].Locations...)
				entries[j].Locations[0].Volume = client.ID{}
			}
			patches[i].Entries = entries
		}
	}
	image, err := buildPlacement(patches)
	if err == nil {
		s.placement.Store(image)
		s.dirtyVolumeAuthority()
	}
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	s.opMu.RLock()
	ready := s.parallelMutationReady(b, p, volume.ObjectID{2})
	s.opMu.RUnlock()
	if ready {
		t.Fatal("absent/unknown physical lookup admitted parallel fast path")
	}
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{2}, 0x0102, 0, 5, gate, nil, parallelOpts())
		done <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("fallback never reached payload")
	}
	if s.opMu.TryRLock() {
		s.opMu.RUnlock()
		t.Fatal("cold/multi-cohort path did not retain exclusive admission")
	}
	release()
	awaitParallel(t, done)
	assertParallelReservationsEmpty(t, s, b)
}

func TestParallelQuotaRecoveryDominatesExpectedFailureCodes(t *testing.T) {
	for _, err := range []error{volume.ErrPrecondition, volume.ErrFull, volume.ErrRange, volume.ErrNotFound, nil} {
		if mutationQuotaAmbiguous(err) {
			t.Fatalf("ordinary abort classified ambiguous: %v", err)
		}
	}
	for _, err := range []error{volume.ErrRecovery, errors.Join(volume.ErrRecovery, volume.ErrFull), errors.Join(volume.ErrRecovery, volume.ErrPrecondition), io.ErrUnexpectedEOF} {
		if !mutationQuotaAmbiguous(err) {
			t.Fatalf("uncertain publication donated quota: %v", err)
		}
	}
}

func TestParallelQuotaLogicalPublicationSurvivesLaterPhysicalCleanupFailure(t *testing.T) {
	// A successful DELETE can be followed by a failed tombstone-reclamation
	// transaction. Its logical result is known, even if the later physical plan
	// is ambiguous. Exercise the independent settlement outcomes directly.
	s := &Server{}
	b := &Bucket{server: s}
	b.usage.valid = true
	b.usage.bytes = 20
	b.usage.activeTickets = 1
	device := volume.ID{1}
	bucket := volume.ID{2}
	ticket := &mutationQuotaTicket{bucket: b, volume: device, physical: map[volume.ID]uint64{bucket: 100}}
	s.parallelQuota.slots = map[volume.ID]*mutationQuotaTicket{device: ticket}
	s.parallelQuota.physical = map[volume.ID]uint64{bucket: 100}
	s.finishMutationQuota(ticket, 10, 0, false, nil, errors.Join(volume.ErrRecovery, volume.ErrFull))
	if b.usage.bytes != 10 || b.usage.activeTickets != 0 {
		t.Fatal("later cleanup lost known logical publication", b.usage.bytes, b.usage.activeTickets)
	}
	if len(s.parallelQuota.slots) != 0 || s.parallelQuota.physical[bucket] != 100 || len(s.parallelQuota.quarantine) != 1 {
		t.Fatal("uncertain cleanup donated physical quota")
	}
}

func TestParallelAdmissionLinkedCOWAlwaysUsesExclusiveFallback(t *testing.T) {
	s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 16<<30)
	p := quotaPrincipal(t, s)
	ids := []volume.ObjectID{{2}, volume.ObjectID(client.ObjectReferenceRootID())}
	s.opMu.Lock()
	for _, id := range ids {
		if !s.parallelMutationCandidate(b, p, id) {
			s.opMu.Unlock()
			t.Fatal("missing positive eligibility control")
		}
	}
	b.mu.Lock()
	before := b.description
	linked := before
	linked.ClonePeers = []BucketClonePeer{{}}
	b.setDescription(linked)
	b.mu.Unlock()
	eligible := false
	for _, id := range ids {
		eligible = eligible || s.parallelMutationCandidate(b, p, id)
	}
	b.mu.Lock()
	b.setDescription(before)
	b.mu.Unlock()
	s.opMu.Unlock()
	if eligible {
		t.Fatal("linked COW bucket assumed local metadata equals inherited logical usage")
	}
}
