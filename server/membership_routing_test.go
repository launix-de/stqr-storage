// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func membershipFixture(t *testing.T) (*Server, *Bucket, *volume.Volume, *volume.Volume) {
	t.Helper()
	n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := n.node
	a := addReplicaDataVolume(t, s, "membership-a", "hdd", 16<<20)
	b := addReplicaDataVolume(t, s, "membership-b", "hdd", 16<<20)
	group := volume.ID{0x13}
	policy := storagePolicy(DefaultBucketPolicy())
	centralPGRequest(t, s, group, 0, PlacementMask{Storage: &policy, Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesWhitelist, Volumes: []string{a.Identity().VolumeID.String(), b.Identity().VolumeID.String()}}}})
	bid := volume.ID{0x12}
	e := client.Encoder{}
	e.U16(1)
	e.ID(client.ID(bid))
	e.Text("membership")
	e.ID(client.ID(group))
	e.U8(0)
	e.U64(0)
	e.Bool(false)
	e.U64(16 << 30)
	e.U32(540)
	_, status, reply := adminBucketMessage(t, s, client.BucketCreateInGroup, e.B)
	if status != 201 {
		t.Fatal(status, reply)
	}
	bucket, err := s.Bucket(bid)
	if err != nil {
		t.Fatal(err)
	}
	s.opMu.Lock()
	// Model the already-deployed unpinned cohort; new bucket allocation may
	// choose an explicit pin. The cohort still contains both actual PG stores.
	patches := append([]client.MapPatch{}, s.currentSystem.Load().patches...)
	for i := range patches {
		if patches[i].Bucket == client.ID(bucket.ID()) {
			patches[i].Entries = append([]client.RangeEntry{}, s.placement.Load().bucketEntries(client.ID(bucket.ID()))...)
			for j := range patches[i].Entries {
				patches[i].Entries[j].Locations = append([]client.Location{}, patches[i].Entries[j].Locations...)
				patches[i].Entries[j].Locations[0].Volume = client.ID{}
			}
		}
	}
	image, err := buildPlacement(patches)
	if err != nil {
		s.opMu.Unlock()
		t.Fatal(err)
	}
	s.placement.Store(image)
	s.dirtyVolumeAuthority()
	for i, v := range []*volume.Volume{a, b} {
		h, e := v.Open(true)
		if e != nil {
			s.opMu.Unlock()
			t.Fatal(e)
		}
		_, e = h.Put(volume.Key{Bucket: bucket.ID(), Object: volume.ObjectID{byte(i + 1)}}, []byte("seed"), volume.Options{Accepted: true})
		h.Close()
		if e != nil {
			s.opMu.Unlock()
			t.Fatal(e)
		}
	}
	err = s.refreshQuotaProtectionLocked()
	if err == nil {
		err = s.prepareParallelMutation(bucket, quotaPrincipal(t, s), volume.ObjectID{1})
	}
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []*volume.Volume{a, b} {
		deadline := time.Now().Add(3 * time.Second)
		for {
			err = v.WarmKeyMembership(context.Background())
			if !errors.Is(err, volume.ErrBusy) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	entry, ok := s.placement.Load().lookup(client.ID(bucket.ID()), client.ObjectID{1})
	if !ok || len(entry.Locations) != 1 || entry.Locations[0].Volume != (client.ID{}) {
		t.Fatal("fixture is not an unpinned cohort", entry)
	}
	return s, bucket, a, b
}

func TestMembershipExistingUnpinnedVolumeProgress(t *testing.T) {
	s, b, a, other := membershipFixture(t)
	p := quotaPrincipal(t, s)
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	beforeA, beforeB := a.IOStats(), other.IOStats()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{1}, 0x0102, 0, 5, gate, nil, parallelOpts())
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first writer did not enter")
	}
	update := make(chan error, 1)
	go func() {
		_, err := b.Put(context.Background(), p, volume.ObjectID{2}, []byte("second"), parallelOpts())
		update <- err
	}()
	awaitParallel(t, update)
	read := make(chan error, 1)
	go func() {
		got, _, err := b.Get(withMembershipRead(context.Background()), p, volume.ObjectID{2})
		if err == nil && string(got) != "second" {
			err = errors.New("wrong read data")
		}
		read <- err
	}()
	awaitParallel(t, read)
	release()
	awaitParallel(t, first)
	if a.IOStats().SyncOps != beforeA.SyncOps || other.IOStats().SyncOps != beforeB.SyncOps {
		t.Fatal("membership routing started Sync")
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestMembershipRequestIdentityGuardAcrossExistingObjects(t *testing.T) {
	s, b, _, _ := membershipFixture(t)
	p := quotaPrincipal(t, s)
	id := volume.ID(client.NewRequestID())
	opts := volume.Options{Accepted: true, RequestID: id}
	gate := &parallelPayloadGate{Reader: bytes.NewReader([]byte("first")), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	first := make(chan error, 1)
	go func() {
		_, err := b.mutateUnmetered(context.Background(), p, volume.ObjectID{1}, 0x0102, 0, 5, gate, nil, opts)
		first <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first writer not active")
	}
	second := make(chan error, 1)
	go func() {
		_, err := b.Put(context.Background(), p, volume.ObjectID{2}, []byte("forbidden"), opts)
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatal("same requestID bypassed active request", err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	awaitParallel(t, first)
	select {
	case err := <-second:
		if !errors.Is(err, volume.ErrRequestID) {
			t.Fatal("receipt identity not rejected", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request guard never released")
	}
	got, _, err := b.Get(context.Background(), p, volume.ObjectID{2})
	if err != nil || string(got) != "seed" {
		t.Fatal("reused request changed B", string(got), err)
	}
	s.requestCoordination.mu.Lock()
	left := len(s.requestCoordination.gates)
	s.requestCoordination.mu.Unlock()
	if left != 0 {
		t.Fatal("request registry leak", left)
	}
	assertParallelReservationsEmpty(t, s, b)
}

type membershipCopyGate struct {
	entered chan struct{}
	release <-chan struct{}
}

func (w membershipCopyGate) Write(p []byte) (int, error) {
	w.entered <- struct{}{}
	<-w.release
	return len(p), nil
}
func TestMembershipReadGuardsEndBefore32PayloadCopies(t *testing.T) {
	s, b, _, _ := membershipFixture(t)
	p := quotaPrincipal(t, s)
	entered, release := make(chan struct{}, 32), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan error, 32)
	for i := 0; i < 32; i++ {
		go func() {
			_, _, err := b.copyToChecked(withMembershipRead(context.Background()), p, volume.ObjectID{2}, 0, 4, membershipCopyGate{entered, release}, nil)
			done <- err
		}()
	}
	for i := 0; i < 32; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatalf("only%d payload readers overlapped", i)
		}
	}
	admin := make(chan struct{})
	go func() { s.opMu.Lock(); s.opMu.Unlock(); close(admin) }()
	select {
	case <-admin:
	case <-time.After(time.Second):
		t.Fatal("payload held administrative admission")
	}
	s.replicas.mu.Lock()
	held := s.replicas.locks[volume.Key{Bucket: b.ID(), Object: volume.ObjectID{2}}] != nil
	s.replicas.mu.Unlock()
	if held {
		t.Fatal("payload held object-key coordination")
	}
	unblock()
	for i := 0; i < 32; i++ {
		awaitParallel(t, done)
	}
}

func TestMembershipInternalReadDoesNotAcquireOuterLocks(t *testing.T) {
	s, b, _, _ := membershipFixture(t)
	p := quotaPrincipal(t, s)
	release := s.replicas.lock(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{2}})
	defer release()
	s.opMu.Lock()
	defer s.opMu.Unlock()
	for _, ctx := range []context.Context{context.Background(), withoutMembershipRead(withMembershipRead(context.Background()))} {
		done := make(chan error, 1)
		go func(ctx context.Context) { _, _, err := b.Get(ctx, p, volume.ObjectID{2}); done <- err }(ctx)
		awaitParallel(t, done)
	}
}

func TestMembershipRelevantNamespacesRetainExactProbes(t *testing.T) {
	s, b, a, _ := membershipFixture(t)
	s.opMu.Lock()
	defer s.opMu.Unlock()
	request := volume.ID{0x91}
	replica := volume.ID{0x92}
	pending := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x70}}
	h, err := a.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x71}}, []byte("receipt"), volume.Options{Accepted: true, RequestID: request})
	if err == nil {
		_, err = h.PurgeAccepted(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x71}})
	}
	if err == nil {
		_, err = h.PrepareMutation(1, pending, 0, 1, bytes.NewReader([]byte("p")), nil, volume.Options{Accepted: true, RequestID: replica}, volume.ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}})
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		key     volume.Key
		request volume.ID
	}{{"object", volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}, volume.ID{}}, {"receipt", volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x72}}, request}, {"replica", volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x73}}, replica}, {"pending", pending, volume.ID{0x93}}} {
		proof := b.membershipProof(b.storageDescription(), tc.key, tc.request)
		if proof.absent[a] {
			t.Fatal(tc.name, "was skipped")
		}
	}
}

func TestMembershipRealFalsepositiveCannotSelectCreateOwner(t *testing.T) {
	s, b, a, other := membershipFixture(t)
	p := quotaPrincipal(t, s)
	key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{2}}
	// Seed2 exists on B in the ordinary fixture. Use the same deterministic
	// bitmap target in a separately emptied slot, while sticky bits on B would
	// otherwise mask the sole-falsepositive case. Reopen B's canonical empty
	// backend before building a fresh filter.
	s.opMu.Lock()
	h, err := other.Open(true)
	if err != nil {
		s.opMu.Unlock()
		t.Fatal(err)
	}
	_, err = h.PurgeAccepted(key)
	h.Close()
	if err != nil {
		s.opMu.Unlock()
		t.Fatal(err)
	}
	s.opMu.Unlock()
	// Choose a missing query with precomputed projections: fixed bucket12/object2.
	// The counterpart is proven absent by an empty freshly formatted eligible
	// volume, swapped using the existing isolated fixture helper.
	other = replaceTestVolumeIO(t, s, "membership-b", func(dev volume.VolumeIO) volume.VolumeIO { return dev })
	s.opMu.Lock()
	delete(s.authority.records, other.Identity().VolumeID)
	s.dirtyVolumeAuthority()
	s.opMu.Unlock()
	if err = s.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	if err = other.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.MayContainObject(key) {
		t.Fatal("negative control already has a falsepositive")
	}
	s.opMu.Lock()
	for _, nonce := range []uint64{20375786, 2156205, 2438937, 12945727} {
		collision := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x44}}
		binary.LittleEndian.PutUint64(collision.Object[24:], nonce)
		h, err = a.Open(true)
		if err != nil {
			s.opMu.Unlock()
			t.Fatal(err)
		}
		_, err = h.Put(collision, []byte("x"), volume.Options{Accepted: true})
		h.Close()
		if err != nil {
			s.opMu.Unlock()
			t.Fatal(err)
		}
	}
	b.usage.mu.Lock()
	b.usage.valid = false
	b.usage.mu.Unlock()
	err = s.refreshQuotaProtectionLocked()
	if err == nil {
		err = s.prepareParallelMutation(b, p, key.Object)
	}
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !a.MayContainObject(key) || other.MayContainObject(key) {
		t.Fatal("fixture did not produce exactly one real bitmap falsepositive")
	}
	options := parallelOpts()
	s.opMu.RLock()
	proof := b.membershipProof(b.storageDescription(), key, options.RequestID)
	ready := s.parallelMutationReadyProof(b, p, key.Object, proof)
	s.opMu.RUnlock()
	if !ready || len(proof.possible) != 1 || proof.possible[0] != a {
		t.Fatal("falsepositive did not enable exactly one possible guarded target")
	}
	src := bytes.NewReader([]byte("created-on-free-B"))
	_, firstErr := b.mutateAttempt(context.Background(), p, key.Object, 0x0102, 0, uint64(src.Len()), src, nil, options)
	var retry *exclusiveMutationRetry
	if !errors.As(firstErr, &retry) || src.Len() != len("created-on-free-B") {
		t.Fatal("Bloom-positive absent object created under R admission", firstErr, src.Len())
	}
	if _, err = b.Put(context.Background(), p, key.Object, []byte("created-on-free-B"), options); err != nil {
		t.Fatal(err)
	}
	h, err = a.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.RecordInfo(key)
	h.Close()
	if !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("Bloom falsepositive selected create owner", err)
	}
	h, err = other.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := h.Get(key)
	h.Close()
	if err != nil || string(got) != "created-on-free-B" {
		t.Fatal("negative candidate was removed from capacity ranking", string(got), err)
	}
	assertParallelReservationsEmpty(t, s, b)
}

func TestMembershipDuplicateAndShadowCopiesKeepOriginalRules(t *testing.T) {
	s, b, a, other := membershipFixture(t)
	p := quotaPrincipal(t, s)
	key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}
	s.opMu.Lock()
	h, err := other.Open(true)
	if err != nil {
		s.opMu.Unlock()
		t.Fatal(err)
	}
	_, err = h.Put(key, []byte("duplicate"), volume.Options{Accepted: true})
	h.Close()
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Get(withMembershipRead(context.Background()), p, key.Object); statusOf(err) != 503 {
		t.Fatal("duplicate originals were silently resolved", err)
	}
	s.opMu.Lock()
	h, err = other.Open(true)
	if err != nil {
		s.opMu.Unlock()
		t.Fatal(err)
	}
	_, err = h.PurgeAccepted(key)
	if err == nil {
		err = h.Restore(key, volume.Info{Length: 6, Compression: volume.Raw}, func(yield func(uint64, io.Reader, uint64) error) error {
			return yield(0, bytes.NewReader([]byte("shadow")), 6)
		}, volume.Options{Accepted: true, ImportShadow: true})
	}
	if err == nil {
		shadow, e := h.ImportShadow(key)
		if e != nil || !shadow {
			err = errors.New("fixture did not create imported shadow")
		}
	}
	h.Close()
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Get(withMembershipRead(context.Background()), p, key.Object)
	if err != nil || string(got) != "seed" {
		t.Fatal("uncommitted shadow replaced original", string(got), err)
	}
	// Nonzero receipt guards must remain independent of this object's data key.
	release, err := s.lockRequestID(context.Background(), volume.ID{1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.lockRequestID(ctx, volume.ID{1}); !errors.Is(err, context.Canceled) {
		release()
		t.Fatal("request guard ignored cancellation", err)
	}
	release()
	_ = a
}
