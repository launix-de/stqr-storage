// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
	"github.com/launix-de/stqr-storage/volume"
	"math"
	"testing"
	"time"
)

func seedStatisticsVolume(s *Server, id client.ID, now time.Time, read uint64) {
	s.statistics.mu.Lock()
	defer s.statistics.mu.Unlock()
	s.statistics.node, s.statistics.boot, s.statistics.observed = s.quotaNode, s.boot, now
	if s.statistics.volumes == nil {
		s.statistics.volumes = make(map[string]*statisticsIOCounters)
	}
	c := &statisticsIOCounters{}
	h := s.placementHalfLives()
	c.observe(now.Add(-time.Second), client.IOStats{}, false, h)
	c.observe(now, client.IOStats{PhysicalReadBytes: read}, true, h)
	s.statistics.volumes[id.String()] = c
}

func TestTelemetryDashboardPlacementShareFrames(t *testing.T) {
	start := time.Unix(100, 0)
	node, boot, disk := client.ID{1}, client.ID{2}, client.ID{3}
	s := &Server{quotaNode: node, boot: boot}
	h := decay.HalfLives{Short: 3 * time.Second, Long: decay.DefaultLongHalfLife}
	sample := func(seq uint64, dt time.Duration, total uint64) {
		io := client.IOStats{PhysicalReadBytes: total, Errors: total / 300}
		io.Ops[0] = total
		s.statistics.observe(client.StatsSnapshot{Node: node, BootID: boot, Sequence: seq, CapturedUnixNS: uint64(start.Add(dt).UnixNano()), Volumes: []client.VolumeStats{{ID: disk, IO: io}}}, nil, h)
	}
	sample(1, 0, 0)
	sample(2, 3*time.Second, 300)
	frame := s.statistics.frames(nil).Volumes[disk.String()].Read
	if frame == nil || frame.Last != 100 || frame.Average != 100 {
		t.Fatal(frame)
	}
	read, _ := s.placementBackgroundVolumes([]client.ID{disk}, start.Add(3*time.Second))
	if read[0] != frame.Average {
		t.Fatal("planner maintains another mean", read, frame)
	}
	c := s.statistics.volumes[disk.String()]
	if c.activity[0].window.Snapshot().Last != 300 || c.activity[6].window.Snapshot().Last != 1 {
		t.Fatal("operation/error counters bypassed common frame")
	}
	sample(3, 4*time.Second, 600)
	before := c.traffic[0].window.Snapshot()
	for i := 0; i < 100; i++ {
		s.placementBackgroundVolumes([]client.ID{disk}, start.Add(4*time.Second))
		s.statistics.frames(nil)
	}
	if after := c.traffic[0].window.Snapshot(); after != before {
		t.Fatal("consumer sampled or ticked", before, after)
	}
	if read, _ := s.placementBackgroundVolumes([]client.ID{disk}, start.Add(4*time.Second)); read[0] != 100 {
		t.Fatal("unfinished frame influenced planner", read)
	}
	sample(4, 6*time.Second, 600)
	frame = s.statistics.frames(nil).Volumes[disk.String()].Read
	if c.traffic[0].window.Forecast().Long != frame.Average {
		t.Fatal("weekly history duplicated")
	}
	if c.traffic[0].window.Forecast().Short != 100 {
		t.Fatal("interval delta counted twice")
	}
}
func TestTelemetryPlannerDoesNotSampleRange(t *testing.T) {
	_, r, _, p, key, start := placementReadersFixture(t)
	group := placementReaderKey{key.bucket, p.connection.accountID, p.connection.readerID}
	r.server.storePlacementHints(group, readerHints(key, p, 1), start)
	m := &rangeMetric{sampled: start}
	r.server.statistics.ranges.Store(key, m)
	recordPlacementReader(m, key, p, 1000, 0, start)
	m.read.Store(1000)
	m.tick(start.Add(10*time.Second), r.server.placementHalfLives())
	before := m.readCounter.window.Snapshot()
	traffic := m.readers[group]
	readerBefore := traffic.readCounter.window.Snapshot()
	for i := 0; i < 100; i++ {
		r.server.statistics.rangeWorkload(key)
		r.placementReaderCosts(key, []client.ID{{1}, {3}}, 10, start.Add(20*time.Second))
	}
	if m.readCounter.window.Snapshot() != before || traffic.readCounter.window.Snapshot() != readerBefore {
		t.Fatal("planning changed measurement history")
	}
}
func TestTelemetryGrowthIsCollectedOnce(t *testing.T) {
	r, v, _, patch, desc := placementInventoryFixture(t)
	start := time.Now().Add(-3 * time.Second)
	key := bucketVolumeKey{patch.Bucket, v.Identity().VolumeID}
	g := &statisticsCounter{}
	h := r.server.placementHalfLives()
	g.update(start, 0, false, h, true)
	g.update(start.Add(3*time.Second), 300, true, h, true)
	r.server.statistics.bucketGrowth = map[bucketVolumeKey]*statisticsCounter{key: g}
	before := g.window.Snapshot()
	weights := r.placementWeights(r.server.placement.Load(), map[client.ID]BucketDescription{patch.Bucket: desc})
	shape := placementShape{bucket: patch.Bucket, copies: 1}
	for i := 0; i < 20; i++ {
		m, ok := r.measuredPlacement(shape, patch.Entries[0], desc, weights)
		if !ok || m.growth != 100 {
			t.Fatal("planner did not use collected growth", m, ok)
		}
	}
	if g.window.Snapshot() != before {
		t.Fatal("range cost estimation resampled allocation")
	}
	// Deallocation is a normal gauge change; it preserves history and adds zero.
	g.update(start.Add(6*time.Second), 100, true, h, true)
	if !g.window.Snapshot().Known || g.window.Snapshot().Last != 0 {
		t.Fatal("deallocation reset a counter or became negative growth")
	}
}
func TestTelemetryResourceQueryDoesNotCollectActivity(t *testing.T) {
	s := &Server{}
	s.sequence.Store(17)
	for i := 0; i < 10; i++ {
		snapshot := s.resourceSnapshot()
		if snapshot.Sequence != 17 || snapshot.CPUTimeNS != 0 || snapshot.ResidentBytes != 0 {
			t.Fatal("resource query remeasured activity", snapshot)
		}
	}
	if s.sequence.Load() != 17 {
		t.Fatal("planner advanced telemetry sequence")
	}
}

func TestTelemetrySparseCountersPreserveKnownZeroHistory(t *testing.T) {
	start := time.Unix(100, 0)
	h := (decay.HalfLives{}).Normalized()
	var io statisticsIOCounters
	io.observe(start, client.IOStats{}, false, h)
	io.observe(start.Add(3*time.Second), client.IOStats{}, true, h)
	if io.activity[6] != nil {
		t.Fatal("zero-only metric allocated history")
	}
	io.observe(start.Add(6*time.Second), client.IOStats{Errors: 1}, true, h)
	got := io.activity[6].window.Snapshot()
	want := (1.0 / 3) * -math.Expm1(-math.Ln2*3/decay.DefaultLongHalfLife.Seconds())
	if got.Last != 1 || math.Abs(got.Average-want) > 1e-15 {
		t.Fatal("known zero history became unknown prehistory", got, want)
	}
}

func TestTelemetryNativeOperationsUseSameCapture(t *testing.T) {
	start := time.Unix(100, 0)
	h := (decay.HalfLives{}).Normalized()
	var meter statisticsIOCounters
	meter.observe(start, client.IOStats{}, false, h, volume.IOStats{})
	meter.observe(start.Add(3*time.Second), client.IOStats{PhysicalReadBytes: 300}, true, h, volume.IOStats{ReadOps: 2, ViewReadOps: 1, WriteOps: 4})
	if meter.backend == nil {
		t.Fatal("backend counters bypassed accumulator")
	}
	for i, want := range []int64{2, 1, 4} {
		if meter.backend[i].window.Snapshot().Last != want {
			t.Fatal(i, want)
		}
	}
	if meter.traffic[0].window.Snapshot().Last != 300 {
		t.Fatal("backend operations duplicated byte traffic")
	}
}
