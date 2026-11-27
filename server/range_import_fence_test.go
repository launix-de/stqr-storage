// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A real two-node transport/control setup, with only the target data device
// wrapped so a requested persistence barrier can be held or failed precisely.
func rangeImportFixture(t *testing.T) (*daemonTestNode, *daemonTestNode, *volume.Volume, *cleanupProbeIO, *Bucket, rangePlan) {
	t.Helper()
	source, target := startAutomaticDaemonPair(t, false)
	addReplicaDataVolume(t, source.node, "bulk-source", "hdd", 16<<20)
	old := addReplicaDataVolume(t, target.node, "bulk-target", "hdd", 16<<20)
	identity := old.Identity()
	memory, err := volume.NewMemory(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	probe := &cleanupProbeIO{VolumeIO: memory}
	dst, err := volume.Format(probe, volume.FormatOptions{ClusterID: identity.ClusterID, NodeID: identity.NodeID, VolumeID: identity.VolumeID, Class: identity.Class})
	if err != nil {
		t.Fatal(err)
	}
	target.node.opMu.Lock()
	target.node.mu.Lock()
	managed := *target.node.volumes[identity.VolumeID.String()]
	managed.store = dst
	target.node.volumes[identity.VolumeID.String()] = &managed
	target.node.mu.Unlock()
	if err = old.Close(); err == nil {
		err = target.node.refreshQuotaProtectionLocked()
	}
	target.node.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := createProtocolBucket(t, source.node, "bulk-fence")
	actor := userPrincipal(t, source.node)
	for i := byte(1); i <= 8; i++ {
		key := volume.ObjectID{i}
		if _, err = b.Put(context.Background(), actor, key, bytes.Repeat([]byte{i}, 32<<10), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	before, ok := source.node.routingPatch(b.ID(), volume.ObjectID{1})
	if !ok {
		t.Fatal("missing source map")
	}
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, dst.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := source.node.scopes
	if err = r.reserve(plan); err != nil {
		t.Fatal(err)
	}
	if _, err = r.call(context.Background(), plan.Target, peerRangeReserve, mustRangeWire(plan)); err != nil {
		t.Fatal(err)
	}
	return source, target, dst, probe, b, plan
}

// Reopen only canonical device bytes, without the process-private accepted
// spill. This is the receiver state available after an unrequested tail is lost.
func rangeImportCrashImage(t *testing.T, probe *cleanupProbeIO) *volume.Volume {
	t.Helper()
	size, err := probe.Size()
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, int(size))
	if n, e := probe.ReadAt(data, 0); n != len(data) || (e != nil && e != io.EOF) {
		t.Fatal("device image", n, e)
	}
	memory, err := volume.NewMemory(size)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = memory.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	reopened, err := volume.Open(memory, volume.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

func TestRangeImportBulkNoSyncAndFinalReadyOutsideAdmission(t *testing.T) {
	source, target, dst, probe, b, plan := rangeImportFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	before := dst.IOStats()
	journalBefore := target.node.scopes.journal.store.IOStats()
	if _, _, err := source.node.scopes.sendSnapshot(ctx, plan, false); err != nil {
		t.Fatal(err)
	}
	if diff := dst.IOStats().SyncOps - before.SyncOps; diff != 0 {
		t.Fatalf("bulk issued %d data Sync barriers", diff)
	}
	if diff := target.node.scopes.journal.store.IOStats().SyncOps - journalBefore.SyncOps; diff != 0 {
		t.Fatalf("bulk wrote per-object durable memos: %d barriers", diff)
	}
	cold := rangeImportCrashImage(t, probe)
	coldHandle, err := cold.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	_, coldErr := coldHandle.RecordInfo(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
	coldHandle.Close()
	if !errors.Is(coldErr, volume.ErrNotFound) {
		t.Fatal("unfenced bulk unexpectedly present in canonical crash image", coldErr)
	}
	generation := dst.Stats().Generation
	if dst.IsDurableThrough(generation) {
		t.Fatal("negative control: bulk already durable")
	}
	h, err := dst.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := h.Get(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
	h.Close()
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 32<<10)) {
		t.Fatal("accepted import unavailable", err)
	}
	if err = source.node.scopes.fence(plan.id()); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	probe.mu.Lock()
	probe.entered = entered
	probe.release = release
	probe.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, _, err := source.node.scopes.sendSnapshot(ctx, plan, true); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("READY never requested durability")
	}
	// A slow device fence must not own node-wide admission or the volume RW lock.
	admitted := make(chan struct{})
	go func() { target.node.opMu.Lock(); target.node.opMu.Unlock(); close(admitted) }()
	select {
	case <-admitted:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("physical Sync held opMu")
	}
	readable := make(chan error, 1)
	go func() {
		h, e := dst.Open(false)
		if e == nil {
			_, _, e = h.Get(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
			h.Close()
		}
		readable <- e
	}()
	select {
	case e := <-readable:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("physical Sync held volume write lock")
	}
	target.node.scopes.mu.Lock()
	ready := target.node.scopes.txns[plan.id()].targetReady.RecordSequence
	target.node.scopes.mu.Unlock()
	if ready != 0 {
		t.Fatal("READY published before data persistence")
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if !dst.IsDurableThrough(generation) {
		t.Fatal("READY failed to cover imported generation")
	}
	durable := rangeImportCrashImage(t, probe)
	durableHandle, err := durable.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	durableData, _, durableErr := durableHandle.Get(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
	durableHandle.Close()
	if durableErr != nil || !bytes.Equal(durableData, got) {
		t.Fatal("READY did not survive receiver crash", durableErr)
	}
	if diff := dst.IOStats().SyncOps - before.SyncOps; diff != 4 {
		t.Fatalf("expected one four-barrier data checkpoint, got %d", diff)
	}
}

func TestRangeImportFailedFenceCannotPublishReady(t *testing.T) {
	source, target, dst, probe, _, plan := rangeImportFixture(t)
	ctx := context.Background()
	if _, _, err := source.node.scopes.sendSnapshot(ctx, plan, false); err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.fence(plan.id()); err != nil {
		t.Fatal(err)
	}
	probe.mu.Lock()
	probe.fail = errors.New("injected final data fence failure")
	probe.mu.Unlock()
	if _, _, err := source.node.scopes.sendSnapshot(ctx, plan, true); err == nil {
		t.Fatal("failed data fence acknowledged READY")
	}
	target.node.scopes.mu.Lock()
	ready := target.node.scopes.txns[plan.id()].targetReady.RecordSequence
	target.node.scopes.mu.Unlock()
	if ready != 0 {
		t.Fatal("failed data fence persisted READY")
	}
	if dst.IsDurableThrough(dst.Stats().Generation) {
		t.Fatal("failed checkpoint recorded durable generation")
	}
	source.node.scopes.mu.Lock()
	txn := source.node.scopes.txns[plan.id()]
	committed := txn.decision != nil
	fenced := txn.fenced
	source.node.scopes.mu.Unlock()
	if committed || !fenced {
		t.Fatal("failed target fence changed source authority")
	}
}

func TestRangeImportProbeRejectsStaleDurableMemo(t *testing.T) {
	source, target, dst, _, b, plan := rangeImportFixture(t)
	ctx := context.Background()
	if _, _, err := source.node.scopes.sendSnapshot(ctx, plan, false); err != nil {
		t.Fatal(err)
	}
	key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}
	h, err := dst.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	info, err := h.RecordInfo(key)
	if err != nil {
		h.Close()
		t.Fatal(err)
	}
	sum, err := rangeObjectDigest(ctx, h, key, info)
	if err != nil {
		h.Close()
		t.Fatal(err)
	}
	// A durable journal can outlive newer accepted shadow content, or vice versa.
	// Same-sized content and a matching old memo must never imply HAVE.
	_, err = h.Put(key, bytes.Repeat([]byte{99}, int(info.Length)), volume.Options{Accepted: true, ImportShadow: true})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = target.node.scopes.storeObjectMemo(plan.id(), rangeObjectMemo{Key: key, Info: info, Codec: volume.Raw, Digest: sum}); err != nil {
		t.Fatal(err)
	}
	e := rangeMessage(peerRangeProbe, plan.id())
	encodeRangeInfo(&e, key, info)
	e.B = append(e.B, sum[:]...)
	raw, err := source.node.scopes.call(ctx, plan.Target, peerRangeProbe, e.B)
	if err != nil {
		t.Fatal(err)
	}
	d := client.Decoder{B: raw}
	if d.Bool() {
		t.Fatal("stale memo accepted different target bytes with identical shape")
	}
}
