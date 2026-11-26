// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type placementReceiptProbe struct {
	volume.VolumeIO
	armed   atomic.Bool
	reads   atomic.Uint64
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (p *placementReceiptProbe) ReadAt(data []byte, offset int64) (int, error) {
	n, err := p.VolumeIO.ReadAt(data, offset)
	if p.armed.Load() {
		var pcs [16]uintptr
		frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, ".loadDedupe") {
				p.reads.Add(1)
				p.once.Do(func() { close(p.entered); <-p.resume })
				break
			}
			if !more {
				break
			}
		}
	}
	return n, err
}

func placementInventoryFixture(t *testing.T) (*rangeManager, *volume.Volume, *placementReceiptProbe, client.MapPatch, BucketDescription) {
	t.Helper()
	dev, err := volume.NewMemory(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	probe := &placementReceiptProbe{VolumeIO: dev, entered: make(chan struct{}), resume: make(chan struct{})}
	vid, bid, node := volume.ID{3}, volume.ID{2}, client.ID{1}
	v, err := volume.Format(probe, volume.FormatOptions{Class: volume.HDD, VolumeID: vid})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	h, _ := v.Open(true)
	if _, err = h.Put(volume.Key{Bucket: bid, Object: volume.ObjectID{1}}, []byte("selected"), volume.Options{}); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 2048; i++ {
		var request volume.ID
		binary.BigEndian.PutUint64(request[8:], i)
		if _, err = h.Put(volume.Key{Bucket: volume.ID{4}, Object: volume.ObjectID{1}}, []byte("other bucket"), volume.Options{RequestID: request, Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	if err = v.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	d := BucketDescription{Schema: 1, ID: bid.String(), Name: "measurement", State: BucketActive, Revision: 1, Policy: BucketPolicy{Version: 1, StorageStrategy: "hdd"}}
	s := &Server{ctx: context.Background(), volumes: map[string]*managedVolume{vid.String(): {config: VolumeConfig{ID: vid.String(), State: "active"}, store: v}}, bucketsByID: make(map[volume.ID]*Bucket)}
	s.bucketsByID[bid] = &Bucket{server: s, description: d}
	p := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID(bid), ToEnd: true, Entries: []client.RangeEntry{{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: node, Volume: client.ID(vid)}}}}, Addresses: []client.NodeAddress{{Node: node, Addresses: []string{"http://node:7419"}}}}
	image, err := buildPlacement([]client.MapPatch{p})
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	s.currentSystem.Store(&systemView{manifest: systemManifest{Membership: client.Membership{Epoch: 1, Members: []client.Member{{ID: node, Addresses: []string{"http://node:7419"}}}}, Generation: 1, Revision: 1}})
	if err = v.SetAdmission(nil, func(snapshot volume.StorageSnapshot) { s.publishQuotaUsage(vid, snapshot.Owned, snapshot.All) }); err != nil {
		t.Fatal(err)
	}
	return &rangeManager{server: s, node: node}, v, probe, p, d
}

func TestPlacementInventoryYieldsVolumeToQueuedWriter(t *testing.T) {
	r, v, probe, _, _ := placementInventoryFixture(t)
	var resume sync.Once
	defer resume.Do(func() { close(probe.resume) })
	probe.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		err := scanRangeRequests(r.server.ctx, v, func(volume.RequestRecord) error { return nil })
		done <- err
	}()
	select {
	case <-probe.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request inventory did not start")
	}
	writer := make(chan uint64, 1)
	go func() {
		h, err := v.Open(true)
		if err != nil {
			writer <- ^uint64(0)
			return
		}
		reads := probe.reads.Load()
		h.Close()
		writer <- reads
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h, acquired, err := v.TryOpen(false)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			break // RWMutex has queued the writer behind the paused reader.
		}
		h.Close()
		if time.Now().After(deadline) {
			resume.Do(func() { close(probe.resume) })
			t.Fatal("writer was not queued")
		}
		time.Sleep(time.Millisecond)
	}
	resume.Do(func() { close(probe.resume) })
	select {
	case reads := <-writer:
		if reads > 512 {
			t.Errorf("receipt census blocked writer through %d reads", reads)
		}
	case <-time.After(5 * time.Second):
		t.Error("writer did not acquire between inventory pages")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("inventory did not complete")
	}
}

func TestPlacementEstimatesUseRAMWithoutIO(t *testing.T) {
	r, v, _, p, d := placementInventoryFixture(t)
	weights := r.placementWeights(r.server.placement.Load(), map[client.ID]BucketDescription{p.Bucket: d})
	for _, bits := range []uint16{0, 1, 8, 32, 256} {
		shape := placementShape{bucket: p.Bucket, bits: bits, copies: 1}
		for _, change := range []bool{false, true} {
			next := d
			if change {
				goal := d.Policy
				goal.Version++
				goal.Compression = "gzip"
				next.DesiredPolicy = &goal
			}
			r.measurements.Store(rangeMetricKey{shape.bucket, shape.start, shape.bits}, placementMeasurement{at: time.Now().Add(-time.Hour)})
			before := v.IOStats()
			for i := 0; i < 20; i++ {
				m, ok := r.measuredPlacement(shape, p.Entries[0], next, weights)
				if !ok || m.cost == 0 || !m.estimateOnly {
					t.Fatal("missing RAM estimate", bits, change, m, ok)
				}
			}
			after := v.IOStats()
			if testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps {
				t.Fatal("balancing read storage", bits, change, before, after)
			}
		}
	}
	// Candidate costing also stays RAM-only: compiling a selected difference
	// must not restart an unrelated receipt census on every failed admission.
	before := v.IOStats()
	cost, _, err := r.policyPlacementCost(p, p.Entries[0], client.ID{}, volume.ID{}, volume.Raw)
	if err != nil || cost == 0 || testStorageReadOps(v.IOStats()) != testStorageReadOps(before) {
		t.Fatal("candidate costing touched storage", cost, err)
	}
}

func TestPlacementEstimatesRefreshFromMutationAccounting(t *testing.T) {
	r, v, _, p, d := placementInventoryFixture(t)
	weights := r.placementWeights(r.server.placement.Load(), map[client.ID]BucketDescription{p.Bucket: d})
	shape := placementShape{bucket: p.Bucket, copies: 1}
	first, ok := r.measuredPlacement(shape, p.Entries[0], d, weights)
	if !ok {
		t.Fatal("unknown initial accounting")
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(volume.Key{Bucket: volume.ID(p.Bucket), Object: volume.ObjectID{2}}, make([]byte, 128<<10), volume.Options{Accepted: true})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	second, ok := r.measuredPlacement(shape, p.Entries[0], d, weights)
	if !ok || second.cost <= first.cost {
		t.Fatal("ignored committed accounting", first.cost, second.cost, ok)
	}
	if after := v.IOStats(); testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps {
		t.Fatal("refresh read storage", before, after)
	}
	// No statistics is unknown; it must not become a zero-byte placement offer.
	r.server.quotaUsage.Store(nil)
	if _, ok = r.measuredPlacement(shape, p.Entries[0], d, weights); ok {
		t.Fatal("missing aggregate fabricated as zero")
	}
}

func TestPlacementEstimatesNormalizeSplitRanges(t *testing.T) {
	r, v, _, p, d := placementInventoryFixture(t)
	left, right := p.Entries[0], p.Entries[0]
	left.PrefixBits, right.PrefixBits = 1, 1
	right.Start[0] = 128
	p.Entries = []client.RangeEntry{left, right}
	image, err := buildPlacement([]client.MapPatch{p})
	if err != nil {
		t.Fatal(err)
	}
	r.server.placement.Store(image)
	weights := r.placementWeights(image, map[client.ID]BucketDescription{p.Bucket: d})
	before := v.IOStats()
	var sum uint64
	for _, entry := range p.Entries {
		m, ok := r.measuredPlacement(placementShape{bucket: p.Bucket, start: entry.Start, bits: 1, copies: 1}, entry, d, weights)
		if !ok || m.cost == 0 {
			t.Fatal("split lacks aggregate estimate", m, ok)
		}
		sum += m.cost
	}
	total := storagePhysical(r.server.quotaUsage.Load().volumes[v.Identity().VolumeID], volume.ID(p.Bucket))
	if sum < total || sum > total+1 {
		t.Fatal("split duplicated bucket footprint", sum, total)
	}
	if after := v.IOStats(); testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps {
		t.Fatal("split scanned metadata", before, after)
	}
}

func TestPlacementIdlePlanningDoesNotReadStorage(t *testing.T) {
	r, v, _, p, d := placementInventoryFixture(t)
	s := r.server
	s.cfg.Node.ID = r.node.String()
	s.scopes = r
	s.quotaPolicies.Store(&quotaPolicyImage{descriptions: []BucketDescription{d}})
	// A skewed split with many unrelated receipts exercises the production
	// target compiler, not just its byte-cost helper. Both healthy and newly
	// required storage-class changes must remain metadata-I/O free.
	left, right := p.Entries[0], p.Entries[0]
	left.PrefixBits, right.PrefixBits = 1, 1
	right.Start[0] = 128
	p.Entries = []client.RangeEntry{left, right}
	image, err := buildPlacement([]client.MapPatch{p})
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	for _, required := range []bool{false, true} {
		next := d
		if required {
			next.Policy.StorageStrategy = "ssd"
			goal := next.Policy
			goal.Version++
			next.DesiredPolicy = &goal
		}
		before := v.IOStats()
		for i := 0; i < 3; i++ {
			r.measurements.Range(func(key, value any) bool {
				m := value.(placementMeasurement)
				m.at = time.Now().Add(-time.Hour)
				r.measurements.Store(key, m)
				return true
			})
			m := r.buildPlacementTargets("", []BucketDescription{next})
			if m == nil {
				t.Fatal("target planner skipped fixture")
			}
		}
		if after := v.IOStats(); testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps {
			t.Fatal("idle target planning touched storage", required, before, after)
		}
	}
}

// A target that stays pending is retried every worker tick in production. The
// old idle-only test missed this path: makePlan recounted ALL retry receipts
// on each diff, including receipts belonging to unrelated buckets.
func TestPlacementPendingDiffUsesRAMWithoutIO(t *testing.T) {
	r, v, _, p, d := placementInventoryFixture(t)
	r.server.cfg.Node.ID = r.node.String()
	target := placementTarget{shape: placementShape{bucket: p.Bucket, copies: 1}, description: d, locations: []client.Location{{Node: client.ID{9}, Volume: client.ID{8}, Class: uint8(volume.HDD)}}}
	before := v.IOStats()
	for i := 0; i < 20; i++ {
		if _, ok := r.diffPlacementTarget(target, p); ok {
			t.Fatal("unavailable destination became executable")
		}
		retained := map[volume.ID]uint64{}
		cost, count, err := r.policyPlacementCost(p, p.Entries[0], client.ID{9}, volume.ID{8}, volume.Raw, retained)
		if err != nil || cost == 0 || count != 1 || retained[v.Identity().VolumeID] == 0 {
			t.Fatal("missing RAM cost", cost, count, err, retained)
		}
		if _, err = r.replicaAbortCost(p, p.Entries[0]); err != nil {
			t.Fatal(err)
		}
	}
	after := v.IOStats()
	if testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
		t.Fatal("pending difference read persistent metadata", before, after)
	}
	r.server.quotaUsage.Store(nil)
	if _, _, err := r.policyPlacementCost(p, p.Entries[0], client.ID{9}, volume.ID{8}, volume.Raw); err == nil {
		t.Fatal("missing counters fabricated as empty range")
	}
}

// In-place codec conversion retains the destination's existing allocation but
// still needs RAM-derived growth headroom before the transfer is attempted.
func TestPlacementInPlaceCodecUsesRetainedAccounting(t *testing.T) {
	r, v, _, p, d := placementInventoryFixture(t)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(volume.Key{Bucket: volume.ID(p.Bucket), Object: volume.ObjectID{1}}, bytes.Repeat([]byte("x"), 1<<20), volume.Options{Compression: volume.Codec(volume.Gzip), Accepted: true})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	d.Policy.Compression = "gzip"
	r.server.bucketsByID[volume.ID(p.Bucket)].description = d
	before := v.IOStats()
	cost, count, err := r.policyPlacementCost(p, p.Entries[0], r.node, v.Identity().VolumeID, volume.Raw)
	if err != nil || cost == 0 || count != 1 {
		t.Fatal("retained codec expansion was free", cost, count, err)
	}
	same, _, err := r.policyPlacementCost(p, p.Entries[0], r.node, v.Identity().VolumeID, volume.Gzip)
	if err != nil || same != 0 {
		t.Fatal("unchanged retained copy was charged again", same, err)
	}
	if after := v.IOStats(); testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
		t.Fatal("retained codec estimate touched storage", before, after)
	}
}

func TestPlacementMissingSourceIsUnknownButKnownEmptyIsZero(t *testing.T) {
	r, v, _, p, _ := placementInventoryFixture(t)
	bad := p
	bad.Entries = append([]client.RangeEntry(nil), p.Entries...)
	bad.Entries[0].Locations = []client.Location{{Node: r.node, Volume: client.ID{99}}}
	if _, _, err := r.policyPlacementCost(bad, bad.Entries[0], client.ID{9}, volume.ID{8}, volume.Raw); statusOf(err) != 503 {
		t.Fatal("missing authoritative source fabricated as empty", err)
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	key := volume.Key{Bucket: volume.ID(p.Bucket), Object: volume.ObjectID{1}}
	err = h.Delete(key, volume.Options{})
	if err == nil {
		err = h.Purge(key)
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range r.server.quotaUsage.Load().volumes[v.Identity().VolumeID] {
		if row.Bucket == key.Bucket {
			t.Fatal("empty fixture still has allocation", row)
		}
	}
	before := v.IOStats()
	cost, count, err := r.policyPlacementCost(p, p.Entries[0], client.ID{9}, volume.ID{8}, volume.Raw)
	if err != nil || cost != 0 || count != 0 {
		t.Fatal("known complete snapshot omitted an empty bucket", cost, count, err)
	}
	if after := v.IOStats(); testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps {
		t.Fatal("empty estimate read storage", before, after)
	}
}

func TestPlacementRangeCostBoundsAtCounterLimit(t *testing.T) {
	r, v, _, p, _ := placementInventoryFixture(t)
	vid := v.Identity().VolumeID
	// Exercise the arithmetic boundary of maintained uint64 counters directly;
	// allocating MaxUint64 bytes is neither necessary nor possible in a fixture.
	r.server.quotaUsage.Store(&quotaUsageImage{volumes: map[volume.ID][]volume.BucketStorage{vid: {{Bucket: volume.ID(p.Bucket), Physical: math.MaxUint64, Logical: math.MaxUint64, Objects: math.MaxUint64}}}})
	before := v.IOStats()
	cost, count, err := r.policyPlacementCost(p, p.Entries[0], client.ID{9}, volume.ID{8}, volume.Gzip)
	if err != nil || cost != math.MaxUint64 || count != math.MaxUint64 {
		t.Fatal("full-range arithmetic wrapped", cost, count, err)
	}
	retainedCost, _, err := r.policyPlacementCost(p, p.Entries[0], r.node, vid, volume.Gzip)
	if err != nil || retainedCost != math.MaxUint64 {
		t.Fatal("retained subtraction erased saturated codec growth", retainedCost, err)
	}
	var prior uint64
	for _, bits := range []uint16{256, 128, 64, 32, 8, 1, 0} {
		moving := p.Entries[0]
		moving.PrefixBits = bits
		physical, _, objects, err := r.placementCostTotals(p, moving)
		if err != nil || physical == 0 || objects == 0 || physical < prior {
			t.Fatal("hash width produced empty/nonmonotone estimate", bits, physical, objects, err)
		}
		prior = physical
	}
	if after := v.IOStats(); testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
		t.Fatal("counter bound estimate touched storage", before, after)
	}
}
