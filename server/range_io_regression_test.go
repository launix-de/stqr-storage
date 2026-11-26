// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
	"time"
)

func reviewWaitDest(t *testing.T, s *Server, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		d, _ := s.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(d) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("missing volume stats")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
func TestPressureIsolatesBlockedObject(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "review-data", "hdd", 4<<20)
	addTestVolume(t, target.node, "review-data", "hdd", 16<<20)
	reviewWaitDest(t, source.node, 2)
	b, _ := createProtocolBucket(t, source.node, "review-pressure")
	ctx := context.Background()
	actor := userPrincipal(t, source.node)
	small, big := volume.ObjectID{1}, volume.ObjectID{0xf0}
	for _, x := range []struct {
		id volume.ObjectID
		n  int
	}{{small, 1024}, {big, 1 << 20}} {
		if _, e := b.Put(ctx, actor, x.id, bytes.Repeat([]byte{1}, x.n), volume.Options{RequestID: volume.ID(client.NewRequestID())}); e != nil {
			t.Fatal(e)
		}
	}
	plan, ok := source.node.scopes.pressurePlan(rangePressure{bucket: b.ID(), object: small, needed: 1 << 20, requested: time.Now()})
	if !ok {
		t.Fatal("no pressure plan")
	}
	if plan.contains(big) {
		t.Fatalf("1-KiB blocked object pulls unrelated 1-MiB object into migration: prefix=%d reserved=%d", plan.Moving.PrefixBits, plan.Physical)
	}
}
func TestReplicaReductionPromotesExistingCopy(t *testing.T) { testReplicaReductionPlacement(t, true) }
func TestReplicaReductionCopiesWhenNoExistingLocationRemainsAllowed(t *testing.T) {
	testReplicaReductionPlacement(t, false)
}
func testReplicaReductionPlacement(t *testing.T, keepExisting bool) {
	source, target := startAutomaticDaemonPair(t, false)
	old := addReplicaDataVolume(t, source.node, "old", "hdd", 8<<20)
	spare := addReplicaDataVolume(t, source.node, "spare", "hdd", 8<<20)
	peer := addReplicaDataVolume(t, target.node, "peer", "hdd", 8<<20)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, old), whitelistReplicaMask(target.node, peer)}})
	b := replicaTestBucket(t, source.node, group, "review-reduction")
	if _, e := b.Put(context.Background(), userPrincipal(t, source.node), volume.ObjectID{3}, bytes.Repeat([]byte{1}, 1<<20), volume.Options{RequestID: volume.ID(client.NewRequestID())}); e != nil {
		t.Fatal(e)
	}
	storage.Redundancy = BucketRedundancy{Mode: "single", Replicas: 1, ACK: "all"}
	nodes := []PlacementNode{whitelistReplicaMask(source.node, spare)}
	if keepExisting {
		nodes = append(nodes, whitelistReplicaMask(target.node, peer))
	}
	centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: nodes})
	plan := awaitReplicaPlan(t, source.node, b.ID())
	if keepExisting && !plan.ReplicaPromote {
		t.Fatalf("existing valid replica could remain alone, but planner copies payload: relocate=%v add=%v reserved=%d", plan.ReplicaRelocate, plan.ReplicaAdd, plan.Physical)
	}
	if !keepExisting && (!plan.ReplicaRelocate || plan.Volume != spare.Identity().VolumeID) {
		t.Fatal("missing required replacement copy", plan)
	}

}
func TestCatchupWritesOnlyChangedPages(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "review-src", "hdd", 8<<20)
	dst := addReplicaDataVolume(t, target.node, "review-dst", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, source.node, "review-catchup")
	ctx := context.Background()
	actor := userPrincipal(t, source.node)
	oid := volume.ObjectID{7}
	if _, err := b.Put(ctx, actor, oid, bytes.Repeat([]byte{1}, 1<<20), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(b.ID(), oid)
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, dst.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 3<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := source.node.scopes
	if err = r.reserve(plan); err != nil {
		t.Fatal(err)
	}
	if _, err = r.call(ctx, plan.Target, peerRangeReserve, mustRangeWire(plan)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.sendSnapshot(ctx, plan, false); err != nil {
		t.Fatal(err)
	}
	malformed := rangeMessage(peerRangeObject, plan.id())
	binary.LittleEndian.PutUint16(malformed.B[:2], 2)
	malformed.ID(client.NewRequestID())
	if _, e := target.node.scopes.receiveSnapshot(client.Message{Kind: client.Request, Opcode: peerRangeObject, Body: malformed.B}, plan.Source, nil); !errors.Is(e, client.ErrProtocol) {
		t.Fatal("truncated delta accepted", e)
	}

	if _, err = b.PutRange(ctx, actor, oid, 0, []byte{2}, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if err = r.fence(plan.id()); err != nil {
		t.Fatal(err)
	}
	rb, wb := testStorageReadBytes(src.IOStats()), dst.IOStats().WriteBytes
	if _, _, err = r.sendSnapshot(ctx, plan, true); err != nil {
		t.Fatal(err)
	}
	read, written := testStorageReadBytes(src.IOStats())-rb, dst.IOStats().WriteBytes-wb
	t.Logf("catch-up source reads=%d target writes=%d", read, written)
	h, e := dst.Open(false)
	if e != nil {
		t.Fatal(e)
	}
	got, info, e := h.Get(volume.Key{Bucket: b.ID(), Object: oid})
	h.Close()
	expected := bytes.Repeat([]byte{1}, 1<<20)
	expected[0] = 2
	if e != nil || !bytes.Equal(got, expected) {
		t.Fatal("delta lost bytes", info, e)
	}
	if written > 128<<10 {
		t.Fatalf("1-byte update after bulk copy caused full catch-up: source reads=%d bytes, target writes=%d bytes", read, written)
	}
}
