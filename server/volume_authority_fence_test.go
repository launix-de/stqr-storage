// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type authorityProbe struct {
	serverFlushGate
	fail                     atomic.Bool
	syncs                    atomic.Uint64
	blockAt                  atomic.Uint64
	nextEntered, nextRelease chan struct{}
}

func (p *authorityProbe) Sync() error {
	if p.fail.Load() {
		return errors.New("injected authority persistence failure")
	}
	n := p.syncs.Add(1)
	if n == p.blockAt.Load() {
		p.mu.Lock()
		entered, release := p.nextEntered, p.nextRelease
		p.mu.Unlock()
		close(entered)
		<-release
	}
	return p.serverFlushGate.Sync()
}
func authorityCrashImage(t *testing.T, dev volume.VolumeIO) *volume.Volume {
	t.Helper()
	size, err := dev.Size()
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, int(size))
	if n, e := dev.ReadAt(data, 0); n != len(data) || e != nil {
		t.Fatal("canonical device image", n, e)
	}
	copy, err := volume.NewMemory(size)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = copy.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	restored, err := volume.Open(copy, volume.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restored.Close() })
	return restored
}

func authorityFenceFixture(t *testing.T) (*Server, *volume.Volume, *authorityProbe, *Bucket, *Bucket) {
	t.Helper()
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	addTestVolume(t, s, "authority-data", "hdd", 16<<20)
	var probe *authorityProbe
	v := replaceTestVolumeIO(t, s, "authority-data", func(dev volume.VolumeIO) volume.VolumeIO {
		probe = &authorityProbe{serverFlushGate: serverFlushGate{VolumeIO: dev}}
		return probe
	})
	a, _ := createProtocolBucket(t, s, "changed-authority")
	b, _ := createProtocolBucket(t, s, "unaffected-authority")
	if err := s.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	for _, bucket := range []*Bucket{a, b} {
		if _, err := bucket.Put(context.Background(), quotaPrincipal(t, s), volume.ObjectID{1}, []byte("durable original"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	return s, v, probe, a, b
}

// Model an already committed scoped map, as loss-map/recovery tests do. This
// publication is protected by admission; the header must follow before writers.
func changeAuthorityEpoch(t *testing.T, s *Server, b *Bucket) {
	t.Helper()
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
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	s.dirtyVolumeAuthority()
}
func TestAuthorityFenceOnlyChangedRangeWaits(t *testing.T) {
	s, v, probe, a, b := authorityFenceFixture(t)
	original, err := readVolumeAuthority(v)
	if err != nil {
		t.Fatal(err)
	}
	changeAuthorityEpoch(t, s, a)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	probe.mu.Lock()
	probe.entered = entered
	probe.release = release
	probe.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- s.ensureVolumeAuthority() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("header checkpoint not requested")
	}
	if !s.authorityPending(a.ID(), volume.ObjectID{1}) || s.authorityPending(b.ID(), volume.ObjectID{1}) {
		t.Fatal("pending header blocked wrong scopes")
	}
	if _, err = a.Put(context.Background(), quotaPrincipal(t, s), volume.ObjectID{1}, []byte("must not ACK"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err == nil {
		t.Fatal("changed authority acknowledged before header persisted")
	}
	unrelated := make(chan error, 1)
	go func() {
		_, err := b.Put(context.Background(), quotaPrincipal(t, s), volume.ObjectID{1}, []byte("accepted during header fence"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())})
		if err == nil {
			var data []byte
			data, _, err = b.Get(context.Background(), quotaPrincipal(t, s), volume.ObjectID{1})
			if err == nil && !bytes.Equal(data, []byte("accepted during header fence")) {
				err = errors.New("unrelated read lost accepted bytes")
			}
		}
		unrelated <- err
	}()
	select {
	case err := <-unrelated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("physical header fence held admission or volume writer lock")
	}
	select {
	case err := <-done:
		t.Fatal("header returned before physical fence", err)
	default:
	}
	s.opMu.Lock()
	confirmed := s.authority.records[v.Identity().VolumeID]
	s.opMu.Unlock()
	if !reflect.DeepEqual(confirmed, original) {
		t.Fatal("unpersisted header published as confirmed")
	}
	cold := authorityCrashImage(t, probe.VolumeIO)
	coldRecord, err := readVolumeAuthority(cold)
	if err != nil || !reflect.DeepEqual(coldRecord, original) {
		t.Fatal("unfenced canonical crash image lost old authority", err)
	}
	unblock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if s.authorityPending(a.ID(), volume.ObjectID{1}) {
		t.Fatal("durable header left range gated")
	}
	durable := authorityCrashImage(t, probe.VolumeIO)
	durableRecord, err := readVolumeAuthority(durable)
	if err != nil || reflect.DeepEqual(durableRecord, original) {
		t.Fatal("final authority fence did not survive crash", err)
	}
	h, err := durable.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	data, _, err := h.Get(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
	h.Close()
	if err != nil || string(data) != "durable original" {
		t.Fatal("header cut incorrectly included later unrequested user write", err)
	}
	if _, err = a.Put(context.Background(), quotaPrincipal(t, s), volume.ObjectID{1}, []byte("after authority"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityFenceFailureRetainsAffectedGate(t *testing.T) {
	s, v, probe, a, b := authorityFenceFixture(t)
	s.opMu.Lock()
	old := s.authority.records[v.Identity().VolumeID]
	s.opMu.Unlock()
	changeAuthorityEpoch(t, s, a)
	probe.fail.Store(true)
	if err := s.ensureVolumeAuthority(); err == nil {
		t.Fatal("failed header fence acknowledged authority")
	}
	if !s.authorityPending(a.ID(), volume.ObjectID{1}) || s.authorityPending(b.ID(), volume.ObjectID{1}) {
		t.Fatal("failure lost or widened scoped admission")
	}
	s.opMu.Lock()
	got := s.authority.records[v.Identity().VolumeID]
	s.opMu.Unlock()
	if !reflect.DeepEqual(got, old) {
		t.Fatal("failed persistence advanced confirmed authority")
	}
	if _, err := a.Put(context.Background(), quotaPrincipal(t, s), volume.ObjectID{1}, []byte("must not publish"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err == nil {
		t.Fatal("failed authority permitted affected write")
	}
}
func TestAuthorityFenceSupersedingMapKeepsNewCutPending(t *testing.T) {
	s, v, probe, a, _ := authorityFenceFixture(t)
	changeAuthorityEpoch(t, s, a)
	firstEntered, firstRelease := make(chan struct{}), make(chan struct{})
	secondEntered, secondRelease := make(chan struct{}), make(chan struct{})
	var once1, once2 sync.Once
	unblock1 := func() { once1.Do(func() { close(firstRelease) }) }
	defer unblock1()
	unblock2 := func() { once2.Do(func() { close(secondRelease) }) }
	defer unblock2()
	probe.mu.Lock()
	probe.entered = firstEntered
	probe.release = firstRelease
	probe.nextEntered = secondEntered
	probe.nextRelease = secondRelease
	probe.mu.Unlock()
	probe.blockAt.Store(probe.syncs.Load() + 5)
	done := make(chan error, 1)
	go func() { done <- s.ensureVolumeAuthority() }()
	select {
	case <-firstEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("first fence missing")
	}
	changeAuthorityEpoch(t, s, a)
	s.opMu.Lock()
	err := s.ensureVolumeAuthorityLocked()
	s.opMu.Unlock()
	var wait *authorityWait
	if !errors.As(err, &wait) {
		t.Fatal("new map forgot ongoing cut", err)
	}
	unblock1()
	select {
	case <-secondEntered:
	case err := <-done:
		t.Fatal("new map acknowledged old header", err)
	case <-time.After(3 * time.Second):
		t.Fatal("second header cut did not start")
	}
	if !s.authorityPending(a.ID(), volume.ObjectID{1}) {
		t.Fatal("old fence cleared superseding scope")
	}
	select {
	case err := <-done:
		t.Fatal("new authority ACK before second fence", err)
	default:
	}
	unblock2()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	record, err := readVolumeAuthority(v)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := s.routingPatch(a.ID(), volume.ObjectID{1})
	for _, entry := range record.Entries {
		if entry.Bucket == client.ID(a.ID()) && entry.Entry.OwnerEpoch != current.Entries[0].OwnerEpoch {
			t.Fatal("confirmed stale map authority", entry.Entry.OwnerEpoch, current.Entries[0].OwnerEpoch)
		}
	}
}

func TestAuthorityFenceCloseWaitsForAlreadyRequestedWorker(t *testing.T) {
	s, v, probe, a, _ := authorityFenceFixture(t)
	changeAuthorityEpoch(t, s, a)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	probe.mu.Lock()
	probe.entered = entered
	probe.release = release
	probe.mu.Unlock()
	before := v.IOStats()
	requested := make(chan error, 1)
	go func() { requested <- s.ensureVolumeAuthority() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("header worker did not enter")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		t.Fatal("Close abandoned explicit administrative fence", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not join authority worker")
	}
	<-requested // Cancellation may stop the caller; it must not stop the requested fence.
	if syncs := v.IOStats().SyncOps - before.SyncOps; syncs != 4 {
		t.Fatal("Close started an extra data Sync", syncs)
	}
}
func TestAuthorityFenceSourceInvalidationCannotBeAbortedWhilePending(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	s := source.node
	addTestVolume(t, s, "fencing-source", "hdd", 16<<20)
	dst := addReplicaDataVolume(t, target.node, "fencing-target", "hdd", 16<<20)
	var probe *authorityProbe
	replaceTestVolumeIO(t, s, "fencing-source", func(dev volume.VolumeIO) volume.VolumeIO {
		probe = &authorityProbe{serverFlushGate: serverFlushGate{VolumeIO: dev}}
		return probe
	})
	a, _ := createProtocolBucket(t, s, "fencing-bucket")
	if err := s.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	before, ok := s.routingPatch(a.ID(), volume.ObjectID{1})
	if !ok {
		t.Fatal("source map missing")
	}
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, dst.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.scopes.reserve(plan); err != nil {
		t.Fatal(err)
	}
	if _, err = s.scopes.call(context.Background(), plan.Target, peerRangeReserve, mustRangeWire(plan)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	probe.mu.Lock()
	probe.entered = entered
	probe.release = release
	probe.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- s.scopes.fence(plan.id()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("source authority invalidation never staged")
	}
	aborted := make(chan error, 1)
	go func() { aborted <- s.scopes.abort(plan) }()
	select {
	case err := <-aborted:
		if err == nil {
			t.Fatal("pending invalidation reopened source")
		}
	case <-time.After(time.Second):
		t.Fatal("pending invalidation held global admission/range mutex")
	}
	s.scopes.mu.Lock()
	txn := s.scopes.txns[plan.id()]
	fencing, fenced := txn.fencing, txn.fenced
	s.scopes.mu.Unlock()
	if !fencing || fenced {
		t.Fatal("source FENCE preceded durable header invalidation")
	}
	unblock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	s.scopes.mu.Lock()
	fencing, fenced = txn.fencing, txn.fenced
	s.scopes.mu.Unlock()
	if fencing || !fenced {
		t.Fatal("confirmed header did not finalize source FENCE")
	}
}

func TestAuthorityFenceRequiresEveryCohortHeader(t *testing.T) {
	s := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false).node
	var volumes []*volume.Volume
	var probes []*authorityProbe
	for _, name := range []string{"cohort-one", "cohort-two"} {
		addTestVolume(t, s, name, "hdd", 16<<20)
		var probe *authorityProbe
		v := replaceTestVolumeIO(t, s, name, func(dev volume.VolumeIO) volume.VolumeIO {
			probe = &authorityProbe{serverFlushGate: serverFlushGate{VolumeIO: dev}}
			return probe
		})
		volumes = append(volumes, v)
		probes = append(probes, probe)
	}
	a, _ := createProtocolBucket(t, s, "cohort-authority")
	if err := s.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	for _, v := range volumes {
		record, err := readVolumeAuthority(v)
		if err != nil || len(record.Entries) != 1 || len(record.Entries[0].Cohort) != 2 {
			t.Fatal("positive control: expected two-device cohort", record, err)
		}
	}
	if volumes[0].Identity().VolumeID.String() > volumes[1].Identity().VolumeID.String() {
		volumes[0], volumes[1] = volumes[1], volumes[0]
		probes[0], probes[1] = probes[1], probes[0]
	}
	changeAuthorityEpoch(t, s, a)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	probes[1].mu.Lock()
	probes[1].entered = entered
	probes[1].release = release
	probes[1].mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- s.ensureVolumeAuthority() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("second cohort header never reached fence")
	}
	s.opMu.Lock()
	first := s.authority.records[volumes[0].Identity().VolumeID]
	second := s.authority.records[volumes[1].Identity().VolumeID]
	s.opMu.Unlock()
	if first.Entries[0].Entry.OwnerEpoch <= second.Entries[0].Entry.OwnerEpoch {
		t.Fatal("positive control: first header not independently confirmed")
	}
	if !s.authorityPending(a.ID(), volume.ObjectID{1}) {
		t.Fatal("one confirmed cohort member cleared complete range gate")
	}
	select {
	case err := <-done:
		t.Fatal("partial cohort acknowledged authority", err)
	default:
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.authorityPending(a.ID(), volume.ObjectID{1}) {
		t.Fatal("complete cohort remains gated")
	}
}
