// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func cowStatFSRequest(b *Bucket) client.Message {
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	return client.Message{Kind: client.Request, Opcode: client.BucketStatFS, ID: client.NewRequestID(), Body: e.B}
}
func cowStatFSSetQuota(t *testing.T, s *Server, b *Bucket, limit uint64) {
	t.Helper()
	desc := b.Describe()
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.U8(0)
	e.U64(0)
	e.Bool(false)
	e.U64(limit)
	e.U32(desc.Policy.Limits.MetadataReservePPM)
	e.U64(desc.Policy.Version)
	if _, status, _ := adminBucketMessage(t, s, client.BucketSetQuota, e.B); status != 200 {
		t.Fatalf("set quota status %d", status)
	}
}
func cowStatFSResult(t *testing.T, raw []byte) client.BucketSpace {
	t.Helper()
	d := client.Decoder{B: raw}
	out := client.BucketSpace{Used: d.U64(), Total: d.U64(), Free: d.U64(), Available: d.U64(), Quota: d.U64(), QuotaLimited: d.Bool()}
	if d.Done() != nil || out.Total-out.Free != out.Used || out.Available > out.Free {
		t.Fatal("invalid STATFS result", out)
	}
	return out
}
func cowStatFSFixture(t *testing.T) (*Server, *Bucket, *Bucket, []*volume.Volume) {
	t.Helper()
	s := testServer(t)
	stores := []*volume.Volume{
		addReplicaDataVolume(t, s, "cow-statfs-hdd", "hdd", 16<<20),
		addReplicaDataVolume(t, s, "cow-statfs-ssd", "ssd", 16<<20),
	}
	ram, err := s.AddVolume(newID(), AddVolumeRequest{Name: "cow-statfs-ram", Source: 2, Class: "ram", SizeBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	stores = append(stores, s.volumes[ram.VolumeID].store)
	s.mu.RUnlock()
	source, _ := createProtocolBucket(t, s, "cow-statfs-source")
	target, _ := createProtocolBucket(t, s, "cow-statfs-target")
	ctx := context.Background()
	actor := userPrincipal(t, s)
	for i := byte(1); i <= 64; i++ {
		if _, err := source.Put(ctx, actor, volume.ObjectID{i}, []byte("data"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	if err := referenceAdminClone(t, s, source, target); err != nil {
		t.Fatal(err)
	}
	for _, b := range []*Bucket{source, target} {
		b.usage.mu.Lock()
		b.usage.valid = false
		b.usage.mu.Unlock()
	}
	return s, source, target, stores
}
func assertCOWStatFSNoReads(t *testing.T, stores []*volume.Volume, before []volume.IOStats) {
	t.Helper()
	for i, v := range stores {
		after := v.IOStats()
		if after.ReadOps != before[i].ReadOps || after.ViewReadOps != before[i].ViewReadOps || after.SyncOps != before[i].SyncOps {
			t.Fatalf("capacity probe touched object storage: before=%+v after=%+v", before[i], after)
		}
	}
}
func TestCOWStatFSUnlimitedUsesPhysicalPoolWithoutScans(t *testing.T) {
	s, _, target, stores := cowStatFSFixture(t)
	// Real RAM scalar reserves are protected; HDD can also consume SSD space.
	if err := stores[0].SetGuaranteedReserve(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := stores[1].SetGuaranteedReserve(2 << 20); err != nil {
		t.Fatal(err)
	}
	before := make([]volume.IOStats, len(stores))
	for i, v := range stores {
		before[i] = v.IOStats()
	}
	var expectedUsed, expectedFree uint64
	for _, v := range stores[:2] {
		st := v.Stats()
		expectedUsed += st.AllocatedPhysical
		expectedFree += st.AdmissionFreePhysical
	}
	raw, err := s.executeBucketStatFS(context.Background(), cowStatFSRequest(target), userPrincipal(t, s))
	if err != nil {
		t.Fatal(err)
	}
	got := cowStatFSResult(t, raw)
	assertCOWStatFSNoReads(t, stores, before)
	if got.Used != expectedUsed || got.Free != expectedFree || got.QuotaLimited {
		t.Fatalf("physical HDD+SSD pool %+v wantUsed=%d wantFree=%d", got, expectedUsed, expectedFree)
	}
	assertCOWStatFSNoReads(t, stores, before)
	if got.Used == 0 {
		t.Fatal("physical allocation was invented as zero")
	}
	t.Logf("cold unlimited COW: metadata reads=0 usedPhysical=%d freePhysical=%d", got.Used, got.Free)
}
func TestCOWStatFSColdFiniteAndBusyUsageAreUnknownWithoutScans(t *testing.T) {
	s, source, target, stores := cowStatFSFixture(t)
	cowStatFSSetQuota(t, s, target, 4096)
	target.usage.mu.Lock()
	target.usage.valid = false
	target.usage.mu.Unlock()
	before := make([]volume.IOStats, len(stores))
	for i, v := range stores {
		before[i] = v.IOStats()
	}
	if _, err := s.executeBucketStatFS(context.Background(), cowStatFSRequest(target), userPrincipal(t, s)); statusOf(err) != 503 {
		t.Fatal("cold logical COW usage must be unknown", err)
	}
	if _, err := source.administrativeUsage(context.Background(), userPrincipal(t, s)); statusOf(err) != 503 {
		t.Fatal("cold dashboard COW usage must be unknown", err)
	}
	source.usage.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := source.observedCOWLogicalUsage(); done <- err }()
	select {
	case err := <-done:
		if statusOf(err) != 503 {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		source.usage.mu.Unlock()
		t.Fatal("observation waited on publisher usage lock")
	}
	source.usage.mu.Unlock()
	assertCOWStatFSNoReads(t, stores, before)
}
func TestCOWStatFSKnownFiniteTracksCloneCASAndDelete(t *testing.T) {
	s, source, target, _ := cowStatFSFixture(t)
	cowStatFSSetQuota(t, s, target, 4096)
	ctx := context.Background()
	actor := userPrincipal(t, s)
	// An explicit bootstrap/admin census may establish the initial scalar. A
	// subsequent observational probe must use it, never initiate another census.
	for _, b := range []*Bucket{source, target} {
		if _, err := b.logicalUsage(); err != nil {
			t.Fatal(err)
		}
	}
	check := func(b *Bucket, want uint64) {
		t.Helper()
		raw, err := s.executeBucketStatFS(ctx, cowStatFSRequest(b), actor)
		if err != nil {
			t.Fatal(err)
		}
		got := cowStatFSResult(t, raw)
		if got.Used != want || got.Free != 4096-want || !got.QuotaLimited {
			t.Fatal("finite quota measurement", got, want)
		}
	}
	check(target, 256)
	if _, err := target.CAS(ctx, actor, volume.ObjectID{1}, 0, []byte("data"), []byte("data-more"), volume.Options{WholeObject: true, Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	check(target, 261)
	if _, err := source.CAS(ctx, actor, volume.ObjectID{2}, 0, []byte("data"), []byte("source-more"), volume.Options{WholeObject: true, Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	check(target, 261)
	if err := target.Delete(ctx, actor, volume.ObjectID{2}, volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	check(target, 257)
	data, _, err := source.Get(ctx, actor, volume.ObjectID{2})
	if err != nil || string(data) != "source-more" {
		t.Fatal("clone delete changed source", string(data), err)
	}
}

func TestCOWStatFSClusterUsesPeerStatsWithoutOwnerStorageRequest(t *testing.T) {
	first, second := startAutomaticDaemonPair(t, false)
	local := addReplicaDataVolume(t, first.node, "cow-statfs-local", "hdd", 16<<20)
	remote := addReplicaDataVolume(t, second.node, "cow-statfs-remote", "hdd", 16<<20)
	reviewWaitDest(t, first.node, 2)
	source, _ := createProtocolBucket(t, first.node, "cow-statfs-cluster-source")
	target, _ := createProtocolBucket(t, first.node, "cow-statfs-cluster-target")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	actor := userPrincipal(t, first.node)
	if _, err := source.Put(ctx, actor, volume.ObjectID{1}, []byte("cluster-source"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if err := referenceAdminClone(t, first.node, source, target); err != nil {
		t.Fatal(err)
	}
	second.node.publishStats()
	deadline := time.Now().Add(5 * time.Second)
	for {
		known := false
		first.node.peerStatsMu.RLock()
		p := first.node.peerStats[second.node.control.ledger.node]
		if p != nil && p.snapshot != nil && p.online {
			for _, v := range p.snapshot.Volumes {
				known = known || v.ID == client.ID(remote.Identity().VolumeID)
			}
		}
		first.node.peerStatsMu.RUnlock()
		if known {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer volume stats unavailable")
		}
		time.Sleep(10 * time.Millisecond)
	}
	before := []volume.IOStats{local.IOStats(), remote.IOStats()}
	// A peer storage lock positively distinguishes cached aggregate statistics
	// from a hidden owner inventory/usage RPC. No remote storage request can
	// complete until this handle is released.
	h, err := remote.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	type reply struct {
		raw    []byte
		status int
	}
	done := make(chan reply, 1)
	go func() {
		raw, status := first.node.dispatchAs(cowStatFSRequest(target), nil, actor)
		done <- reply{raw, status}
	}()
	select {
	case r := <-done:
		if r.status != 200 {
			t.Fatal("cluster physical pool status", r.status)
		}
		d := client.Decoder{B: r.raw}
		d.V()
		d.Blob()
		space := cowStatFSResult(t, d.B)
		if space.Used == 0 || space.Free == 0 || space.QuotaLimited {
			t.Fatal("invalid physical pool", space)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unlimited COW STATFS waited on remote object storage")
	}
	h.Close()
	assertCOWStatFSNoReads(t, []*volume.Volume{local, remote}, before)
}

func TestCOWStatFSCancelledRecoveryPageDoesNotReadStorage(t *testing.T) {
	s, source, target, stores := cowStatFSFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := cowGraph{source.ID(): {target.ID()}, target.ID(): {source.ID()}}
	before := make([]volume.IOStats, len(stores))
	for i, v := range stores {
		before[i] = v.IOStats()
	}
	if _, _, _, err := s.cowGraphPage(ctx, target.ID(), nil, 128, false, g, s.placement.Load()); err != context.Canceled {
		t.Fatal("cancelled census continued", err)
	}
	assertCOWStatFSNoReads(t, stores, before)
}
