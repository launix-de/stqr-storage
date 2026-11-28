// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestPlacementImproveTelemetryBound(t *testing.T) {
	s := &Server{}
	r := &rangeManager{server: s, node: client.ID{1}}
	s.scopes = r
	bucket, disk := client.ID{2}, client.ID{3}
	entries := make([]client.RangeEntry, 8192)
	for i := range entries {
		entry := &entries[i]
		binary.BigEndian.PutUint16(entry.Start[:2], uint16(i<<3))
		entry.PrefixBits, entry.OwnerEpoch, entry.PolicyVersion = 13, 1, 1
		entry.Locations = []client.Location{{Node: r.node, Volume: disk}}
		meter := &rangeMetric{}
		meter.read.Store(1)
		meter.active.Store(time.Now().UnixNano())
		r.server.statistics.ranges.Store(rangeMetricKey{bucket, entry.Start, 13}, meter)
	}
	image, err := buildPlacement([]client.MapPatch{{Membership: 1, Generation: 1, Revision: 1, Bucket: bucket, ToEnd: true, Entries: entries, Addresses: []client.NodeAddress{{Node: r.node, Addresses: []string{"http://node:7419"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	snapshot := client.StatsSnapshot{Schema: 1, Node: r.node, BootID: client.ID{4}, Sequence: 1}
	r.collectRangeTelemetry(&snapshot)
	if len(snapshot.Ranges) != maxRangeTelemetry {
		t.Fatal("unbounded telemetry", len(snapshot.Ranges))
	}
	if _, err := client.DecodeStats(client.EncodeStats(snapshot)); err != nil {
		t.Fatal("invalid bounded snapshot", err)
	}
	count := 0
	r.server.statistics.ranges.Range(func(_, _ any) bool { count++; return true })
	if count != len(entries) {
		t.Fatal("sampling discarded local optimization counters", count)
	}
}

func TestPlacementImproveRangeTraffic(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	store := addReplicaDataVolume(t, s, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "range-traffic")
	before, _ := s.routingPatch(b.ID(), volume.ObjectID{})
	plan, err := planRangeMove(before, before.Entries[0], s.scopes.node, store.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.scopes.move(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	p := userPrincipal(t, s)
	ctx := context.Background()
	object := volume.ObjectID{3}
	if _, err := b.Put(ctx, p, object, make([]byte, 128), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.GetRange(ctx, p, object, 0, 64); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Append(ctx, p, object, make([]byte, 8), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, _, err := b.copyToChecked(ctx, p, object, 0, 136, &out, nil); err != nil {
		t.Fatal(err)
	}
	stats := s.CollectStats(client.StatsFilter{IncludeRanges: true})
	if _, err := client.DecodeStats(client.EncodeStats(stats)); err != nil {
		t.Fatal("invalid wire telemetry", err)
	}
	found := false
	for _, row := range stats.Ranges {
		if row.Bucket == client.ID(b.ID()) {
			found = true
			if row.IO.LogicalReadBytes != 200 || row.IO.LogicalWriteBytes != 136 {
				t.Fatal("range payload counters", row.IO)
			}
		}
	}
	if !found {
		t.Fatal("range telemetry was not published")
	}
}

func TestPlacementImproveUnboundTelemetryCodec(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	addTestVolume(t, s, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "cohort-telemetry")
	object := volume.ObjectID{1}
	if _, err := b.Put(context.Background(), userPrincipal(t, s), object, []byte("actual traffic"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	key, ok := s.placement.Load().metricKey(client.ID(b.ID()), client.ObjectID(object))
	if !ok {
		t.Fatal("missing cohort map")
	}
	value, ok := s.statistics.ranges.Load(key)
	if !ok || value.(*rangeMetric).write.Load() != 14 {
		t.Fatal("cohort traffic was not retained for local optimization")
	}
	stats := s.CollectStats(client.StatsFilter{IncludeRanges: true})
	if _, err := client.DecodeStats(client.EncodeStats(stats)); err != nil {
		t.Fatal("cohort telemetry breaks existing peers", err)
	}
}

func TestDaemonPlacementImprovement(t *testing.T) {
	source, peer := startAutomaticDaemonPair(t, false)
	hot := addReplicaDataVolume(t, source.node, "hot", "hdd", 16<<20)
	cold := addReplicaDataVolume(t, source.node, "cold", "hdd", 16<<20)
	addTestVolume(t, peer.node, "not-in-pg", "ssd", 32<<20)
	s := source.node
	storage := storagePolicy(DefaultBucketPolicy())
	group, foreign := volume.ID(client.NewRequestID()), volume.ID(client.NewRequestID())
	centralPGRequest(t, s, group, 0, PlacementMask{Storage: &storage, Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesWhitelist, Volumes: []string{hot.Identity().VolumeID.String(), cold.Identity().VolumeID.String()}}}})
	centralPGRequest(t, s, foreign, 0, PlacementMask{Storage: &storage, Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesWhitelist, Volumes: []string{hot.Identity().VolumeID.String()}}}})
	b := replicaTestBucket(t, s, group, "continuously-improved")
	fixed := replicaTestBucket(t, s, foreign, "fixed-other-pg")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pin := func(bucket *Bucket) {
		t.Helper()
		before, _ := s.routingPatch(bucket.ID(), volume.ObjectID{})
		plan, err := planRangeMove(before, before.Entries[0], s.scopes.node, hot.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.scopes.move(ctx, plan); err != nil {
			t.Fatal("initial pin", err)
		}
	}
	pin(b)
	pin(fixed)
	actor := userPrincipal(t, s)
	object := volume.ObjectID{1}
	payload := bytes.Repeat([]byte{0x59}, 1<<20)
	request := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	first, err := b.Put(ctx, actor, object, payload, request)
	if err != nil {
		t.Fatal(err)
	}
	// Initially both disks are healthy. Merely discovering an empty volume must
	// not move the old valid placement, and the desired policy is unchanged.
	m := s.scopes.buildPlacementTargets(group.String(), s.bucketSnapshots())
	if p, ok := s.scopes.nextPlacementTarget(m); ok {
		t.Fatal("idle cluster moved data", p)
	}
	beforePolicy := b.Describe().Policy
	for i := byte(0); i < 11; i++ {
		if _, err := fixed.Put(ctx, actor, volume.ObjectID{i}, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal("foreign workload", err)
		}
	}
	fixedBefore, _ := s.placement.Load().lookup(client.ID(fixed.ID()), client.ObjectID{})
	// The old near-full watermark planner must not be responsible for this
	// move: improvement is worthwhile well before that mandatory threshold.
	available, _ := s.scopes.destinations(b.Describe(), true)
	for _, destination := range available {
		if destination.volume.ID == client.ID(hot.Identity().VolumeID) && s.scopes.capacityNeed(destination, available) != 0 {
			t.Fatal("fixture already requires emergency rebalancing")
		}
	}
	s.scopes.start()
	peer.node.scopes.start()
	for {
		// Publishing the target map precedes the asynchronous authority cut.
		// Wait for automatic handoff completion and its confirmed header before
		// checking payloads and receipts directly through Bucket below.
		s.opMu.RLock()
		entry, _ := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(object))
		ready := !s.authorityPending(b.ID(), object)
		if a := s.authority; a != nil {
			ready = ready && a.wanted != nil && a.wantedRevision == a.revision.Load()
		}
		s.scopes.mu.Lock()
		for _, txn := range s.scopes.activeBucketLocked(b.ID()) {
			if s.scopes.active(txn) && txn.plan.contains(object) {
				ready = false
				break
			}
		}
		s.scopes.mu.Unlock()
		s.opMu.RUnlock()
		if len(entry.Locations) == 1 && entry.Locations[0].Volume == client.ID(cold.Identity().VolumeID) && ready {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("continuous worker did not finish unchanged-policy improvement", entry, "authority/handoff ready", ready)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if b.Describe().Policy != beforePolicy || b.Describe().DesiredPolicy != nil {
		t.Fatal("improvement changed bucket configuration")
	}
	fixedAfter, _ := s.placement.Load().lookup(client.ID(fixed.ID()), client.ObjectID{})
	if !reflect.DeepEqual(fixedBefore, fixedAfter) {
		t.Fatal("other PG moved", fixedBefore, fixedAfter)
	}
	data, _, err := b.Get(ctx, actor, object)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatal("payload after optional move", len(data), err)
	}
	if again, err := b.Put(ctx, actor, object, payload, request); err != nil || again != first {
		t.Fatal("move lost retry receipt", again, err)
	}
	for i := byte(0); i < 11; i++ {
		data, _, err := fixed.Get(ctx, actor, volume.ObjectID{i})
		if err != nil || !bytes.Equal(data, payload) {
			t.Fatal("other PG lost bytes", i, err)
		}
	}
}
