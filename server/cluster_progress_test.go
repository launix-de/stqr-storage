// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestPGAcceptedRoundPrecedesUnacceptedOffer(t *testing.T) {
	accepted := rangeWireFixture(t)
	offered := accepted
	offered.Attempt = client.NewRequestID()
	group := client.ID{7}
	r := &rangeManager{node: accepted.Source, pgOffers: map[client.ID]rangePlan{group: offered}, pgStates: map[client.ID]*pgRebuildState{group: {Active: &pgRebuildWork{Round: pgRebuildRound{ID: accepted.id(), Plan: mustRangeWire(accepted)}}}}}
	got, ok := r.pendingPGPlan(group.String())
	if !ok || got.id() != accepted.id() {
		t.Fatal("accepted work was stranded behind a rejected proposal", ok)
	}
	r.node = client.ID{9}
	offered.Source = r.node
	r.pgOffers[group] = offered
	if _, ok := r.pendingPGPlan(group.String()); ok {
		t.Fatal("local proposal bypassed an active foreign source")
	}
}

func TestPinnedLogicalUsageUsesMaintainedCounters(t *testing.T) {
	r, v, _, patch, _ := placementInventoryFixture(t)
	s := r.server
	s.cfg.Node.ID = r.node.String()
	s.controller = &clusterController{}
	s.control = &controlRuntime{ledger: &controlLedger{node: r.node}}
	if err := s.refreshQuotaOwnership(v); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(volume.ID(patch.Bucket))
	if err != nil {
		t.Fatal(err)
	}
	v.SetCacheLimits(0, 0)
	before := testStorageReadOps(v.IOStats())
	used, err := b.logicalUsage()
	if err != nil || used != uint64(len("selected")) {
		t.Fatal(used, err)
	}
	var exact bool
	if testStorageReadOps(v.IOStats()) != before {
		t.Fatal("scalar accounting read object metadata")
	}
	reference, err := b.scanLogicalObjectUsage()
	if err != nil || reference != used || testStorageReadOps(v.IOStats()) == before {
		t.Fatal("object-scan positive control", reference, err)
	}
	t.Logf("maintained count: 0 storage reads; object-scan control: %d", testStorageReadOps(v.IOStats())-before)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(volume.Key{Bucket: volume.ID(patch.Bucket), Object: volume.ObjectID{1}}, bytes.Repeat([]byte("x"), 32), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	used, exact, err = b.pinnedLogicalUsage()
	if err != nil || !exact || used != 32 {
		t.Fatal("counter did not track mutation", used, exact, err)
	}
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Delete(volume.Key{Bucket: volume.ID(patch.Bucket), Object: volume.ObjectID{1}}, volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	used, exact, err = b.pinnedLogicalUsage()
	if err != nil || !exact || used != 0 {
		t.Fatal("tombstone counted as live bytes", used, exact, err)
	}
}

func TestRangeCleanupBatchesDurableWrites(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "cleanup-source", "hdd", 16<<20)
	dst := addReplicaDataVolume(t, target.node, "cleanup-target", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, source.node, "cleanup-batches")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	actor := userPrincipal(t, source.node)
	for i := 0; i < 64; i++ {
		if _, err := b.Put(ctx, actor, volume.ObjectID{0x80, byte(i)}, []byte("moving"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	keep := volume.ObjectID{1}
	if _, err := b.Put(ctx, actor, keep, []byte("untouched"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	before, known := source.node.routingPatch(b.ID(), volume.ObjectID{0x80})
	if !known {
		t.Fatal("missing placement")
	}
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planRangeMove(before, moving, target.node.control.ledger.node, dst.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	flushes := src.IOStats().SyncOps
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	delta := src.IOStats().SyncOps - flushes
	if delta >= 64 {
		t.Fatalf("per-object durable cleanup returned: %d syncs for 64 objects", delta)
	}
	t.Logf("source cleanup: %d syncs for 64 objects plus retry receipts", delta)
	got, _, err := b.Get(ctx, actor, keep)
	if err != nil || string(got) != "untouched" {
		t.Fatal("unrelated range changed", string(got), err)
	}
	for i := 0; i < 64; i++ {
		peer, err := target.node.Bucket(b.ID())
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := peer.Get(ctx, actor, volume.ObjectID{0x80, byte(i)})
		if err != nil || string(got) != "moving" {
			t.Fatal("moved data", i, string(got), err)
		}
	}
}

func TestRangeCleanupReleasesPriorityBetweenPages(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cleanup-priority", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "cleanup-priority")
	v := b.allVolumes()[0]
	before := rangeWireFixture(t).Before
	before.Bucket = client.ID(b.ID())
	before.Entries[0].Locations[0].Volume = client.ID(v.Identity().VolumeID)
	_, right, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	p, err := planRangeMove(before, right, client.ID{2}, volume.ID{5}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := &rangeManager{server: s, txns: map[[32]byte]*rangeTxn{p.id(): {plan: p, decision: &rangeCommit{}}}}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		if _, err = h.Put(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x80, byte(i)}}, []byte("move"), volume.Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	if err = v.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	flushes := v.IOStats().SyncOps
	s.ioPriority.users.Store(1)
	defer s.ioPriority.users.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.cleanup(ctx, p) }()
	observed := false
	for !observed {
		select {
		case err := <-done:
			t.Fatal("cleanup exposed no idle admission window before completion", err)
		default:
		}
		if v.IOStats().SyncOps > flushes && s.opMu.TryLock() {
			if s.ioPriority.rebuilds.Load() == 0 {
				h, got, e := v.TryOpen(false)
				if e != nil {
					t.Fatal(e)
				}
				if got {
					page, e := p.scanObjects(h, nil, 1)
					h.Close()
					if e != nil {
						t.Fatal(e)
					}
					observed = len(page) > 0
				}
			}
			s.opMu.Unlock()
		}
		if ctx.Err() != nil {
			t.Fatal("no between-page idle admission", ctx.Err())
		}
		time.Sleep(100 * time.Microsecond)
	}
	s.ioPriority.users.Store(0)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNativeStatFSColdQuotaUsesPinnedCounters(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "statfs-counters", "hdd", 16<<20)
	addReplicaDataVolume(t, target.node, "statfs-peer", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, source.node, "statfs-counters")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	before, known := source.node.routingPatch(b.ID(), volume.ObjectID{})
	if !known {
		t.Fatal("missing placement")
	}
	p, err := planRangeMove(before, before.Entries[0], source.node.control.ledger.node, src.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.move(ctx, p); err != nil {
		t.Fatal(err)
	}
	const objects = 1024
	h, err := src.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < objects; i++ {
		id := volume.ObjectID{0xf0, byte(i >> 8), byte(i)}
		if _, err = h.Put(volume.Key{Bucket: b.ID(), Object: id}, []byte("data"), volume.Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	if err = src.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = source.node.refreshQuotaOwnership(src); err != nil {
		t.Fatal(err)
	}
	src.SetCacheLimits(0, 0)
	b.usage.mu.Lock()
	b.usage.valid = false
	b.usage.mu.Unlock()
	reads := testStorageReadOps(src.IOStats())
	begin := time.Now()
	space, err := c.Space(ctx)
	elapsed := time.Since(begin)
	if err != nil || space.Used != objects*4 {
		t.Fatal(space, err)
	}
	delta := testStorageReadOps(src.IOStats()) - reads
	if delta >= objects {
		t.Fatalf("statfs scanned object count: %d reads for %d objects", delta, objects)
	}
	t.Logf("native STATFS with cold quota and %d objects: %d data-volume reads in %s", objects, delta, elapsed)
}

func TestRangeCleanupAdvancesUnfinishedBucketPurgeBetweenPages(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "interleaved-cleanup", "hdd", 16<<20)
	moving, _ := createProtocolBucket(t, s, "interleaved-moving")
	doomed, _ := createProtocolBucket(t, s, "interleaved-doomed")
	v := moving.allVolumes()[0]
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []*Bucket{moving, doomed} {
		for i := 0; i < 64; i++ {
			if _, err = h.Put(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x80, byte(i)}}, []byte("cleanup"), volume.Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
		}
	}
	h.Close()
	if err = v.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	before := rangeWireFixture(t).Before
	before.Bucket = client.ID(moving.ID())
	before.Entries[0].Locations[0].Volume = client.ID(v.Identity().VolumeID)
	p, err := planRangeMove(before, before.Entries[0], client.ID{2}, volume.ID{5}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := &rangeManager{server: s, txns: map[[32]byte]*rangeTxn{p.id(): {plan: p, decision: &rangeCommit{}}}}
	oldView := s.currentSystem.Load()
	description := doomed.Describe()
	description.State = BucketDeleting
	doomed.mu.Lock()
	oldDescription := doomed.description
	doomed.description = description
	doomed.mu.Unlock()
	defer func() { doomed.mu.Lock(); doomed.description = oldDescription; doomed.mu.Unlock() }()
	s.currentSystem.Store(&systemView{descriptions: []BucketDescription{description}})
	defer s.currentSystem.Store(oldView)
	var after *volume.ObjectID
	for page := 1; page <= 2; page++ {
		h, err = v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		records, err := p.scanObjects(h, after, 32)
		h.Close()
		if err != nil || len(records) != 32 {
			t.Fatalf("range cleanup page: count=%d, error=%v", len(records), err)
		}
		next := records[len(records)-1].Key.Object
		after = &next
		// Exercise the production page path: scan under a read handle, then
		// authority-checked buffered deletion and an unlocked physical fence.
		if err = r.cleanupBatch(context.Background(), p, v, records, nil); err != nil {
			t.Fatal(err)
		}
		h, err = v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range []*Bucket{moving, doomed} {
			records, err := h.ScanBucketObjects(b.ID(), nil, 128)
			if err != nil || len(records) != 64-page*32 {
				h.Close()
				t.Fatalf("bucket %s did not advance between range pages: count=%d, error=%v", b.Describe().Name, len(records), err)
			}
		}
		h.Close()
	}
}
