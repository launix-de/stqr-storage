// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
	"github.com/launix-de/stqr-storage/volume"
)

const dashboardRateInterval = 3 * time.Second

// Rate frames are advisory RAM aggregates, bounded by volumes, buckets and
// accounts. They never enumerate object indexes. Lifetime counters remain the
// source for totals; only exact differences enter the common interval counter.
type statisticsCounter struct {
	previous uint64
	started  bool
	window   decay.Series
}

func (c *statisticsCounter) observe(now time.Time, total uint64, tick bool, windows ...decay.HalfLives) {
	h := (decay.HalfLives{}).Normalized()
	if len(windows) > 0 {
		h = windows[0]
	}
	c.update(now, total, tick, h, false)
}

// Counter resets invalidate a rate. Gauges may fall normally; growth records
// only their positive changes. Both kinds share one baseline and frame path.
func (c *statisticsCounter) update(now time.Time, total uint64, tick bool, h decay.HalfLives, positiveChanges bool) {
	delta := subtractCapacity(total, c.previous)
	if !c.started || !positiveChanges && total < c.previous || delta > math.MaxInt64 {
		*c = statisticsCounter{previous: total, started: true}
		c.window.Tick(now, h)
		return
	}
	c.window.Add(int64(delta))
	c.previous = total
	if tick {
		c.window.Tick(now, h)
	}
}

type dashboardRatePair struct {
	Last    float64 `json:"last"`
	Average float64 `json:"average"`
}

func (c *statisticsCounter) rate() *dashboardRatePair {
	snapshot := c.window.Snapshot()
	if !snapshot.Known || snapshot.Saturated {
		return nil
	}
	return &dashboardRatePair{Last: snapshot.LastPerSecond(), Average: snapshot.Average}
}

type dashboardIORates struct {
	Read         *dashboardRatePair `json:"read"`
	Write        *dashboardRatePair `json:"write"`
	LogicalRead  *dashboardRatePair `json:"logicalRead"`
	LogicalWrite *dashboardRatePair `json:"logicalWrite"`
}

type statisticsIOCounters struct {
	traffic [4]statisticsCounter
	// Operation/error/flush counters share the same class. Zero-only optional
	// families allocate no histories, keeping bucket/account records compact.
	activity [9]*statisticsCounter
	backend  *[3]*statisticsCounter
}

func (c *statisticsIOCounters) observe(now time.Time, io client.IOStats, tick bool, h decay.HalfLives, native ...volume.IOStats) {
	previousFrame := c.traffic[0].window.Snapshot()
	for i, total := range [4]uint64{io.PhysicalReadBytes, io.PhysicalWriteBytes, io.LogicalReadBytes, io.LogicalWriteBytes} {
		c.traffic[i].observe(now, total, tick, h)
	}
	totals := [9]uint64{io.Ops[0], io.Ops[1], io.Ops[2], io.Ops[3], io.Ops[4], io.Ops[5], io.Errors, io.Flushes, io.FlushNS}
	observeActivity(c.activity[:], totals[:], previousFrame, now, tick, h)
	if len(native) > 0 {
		ops := [3]uint64{native[0].ReadOps, native[0].ViewReadOps, native[0].WriteOps}
		if c.backend == nil && (ops[0] != 0 || ops[1] != 0 || ops[2] != 0) {
			c.backend = new([3]*statisticsCounter)
		}
		if c.backend != nil {
			observeActivity(c.backend[:], ops[:], previousFrame, now, tick, h)
		}
	}
}
func observeActivity(counters []*statisticsCounter, totals []uint64, previousFrame decay.Snapshot, now time.Time, tick bool, h decay.HalfLives) {
	for i, total := range totals {
		counter := counters[i]
		if counter == nil {
			if total == 0 {
				continue
			}
			counter = &statisticsCounter{}
			start := previousFrame.Switched
			if !start.IsZero() && start.Before(now) {
				// All earlier observations were zero; reuse the actual previous interval
				// instead of pretending this is unknown prehistory when the first event arrives.
				if previousFrame.Known {
					counter.observe(start.Add(-previousFrame.Interval), 0, false, h)
					counter.observe(start, 0, true, h)
				} else {
					counter.observe(start, 0, false, h)
				}
			}
			counters[i] = counter
		}
		counter.observe(now, total, tick, h)
	}
}
func (c *statisticsIOCounters) rates() dashboardIORates {
	return dashboardIORates{c.traffic[0].rate(), c.traffic[1].rate(), c.traffic[2].rate(), c.traffic[3].rate()}
}

// statisticsSource is one source incarnation, shared by every consumer.
// received time drives peer intervals; source sequence/time only reject replays.
type bucketVolumeKey struct {
	bucket client.ID
	volume volume.ID
}

type statisticsSource struct {
	node, boot              client.ID
	observed, tick          time.Time
	sequence, captured      uint64
	io, userIO              statisticsIOCounters
	volumes, buckets, users map[string]*statisticsIOCounters
	growth                  map[client.ID]*statisticsCounter
	bucketGrowth            map[bucketVolumeKey]*statisticsCounter
	cpu, netRead, netWrite  statisticsCounter
}
type statisticsBook struct {
	mu sync.Mutex
	statisticsSource
	peers             map[client.ID]*statisticsSource
	ranges            sync.Map
	paths             placementReaders
	moveDuration      decay.Mean
	moveDurationValue time.Duration
}

// The same keyed update/prune path serves every measurement dimension.
func observeStatisticsEntities[K comparable, V any, T any](records map[K]*V, totals map[K]T, observe func(*V, T)) map[K]*V {
	if records == nil {
		records = make(map[K]*V, len(totals))
	}
	for key, total := range totals {
		record := records[key]
		if record == nil {
			record = new(V)
			records[key] = record
		}
		observe(record, total)
	}
	for key := range records {
		if _, ok := totals[key]; !ok {
			delete(records, key)
		}
	}
	return records
}

func (b *statisticsBook) observe(snap client.StatsSnapshot, users map[string]client.IOStats, windows ...decay.HalfLives) {
	h := (decay.HalfLives{}).Normalized()
	if len(windows) > 0 {
		h = windows[0]
	}
	if snap.CapturedUnixNS > math.MaxInt64 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.statisticsSource.observe(snap, users, nil, time.Unix(0, int64(snap.CapturedUnixNS)), h)
}
func (source *statisticsSource) observe(snap client.StatsSnapshot, users map[string]client.IOStats, bucketTotals map[bucketVolumeKey]uint64, now time.Time, h decay.HalfLives, nativeMaps ...map[string]volume.IOStats) {
	var native map[string]volume.IOStats
	if len(nativeMaps) > 0 {
		native = nativeMaps[0]
	}
	if source.boot != snap.BootID || source.node != snap.Node {
		*source = statisticsSource{node: snap.Node, boot: snap.BootID}
	}
	if !source.observed.IsZero() && (!now.After(source.observed) || snap.Sequence != 0 && snap.Sequence <= source.sequence || snap.CapturedUnixNS <= source.captured) {
		return
	}
	tick := !source.tick.IsZero() && now.Sub(source.tick) >= dashboardRateInterval
	if source.tick.IsZero() || tick {
		source.tick = now
	}
	source.observed, source.sequence, source.captured = now, snap.Sequence, snap.CapturedUnixNS
	source.io.observe(now, snap.IO, tick, h)
	type volumeObservation struct {
		io        client.IOStats
		native    volume.IOStats
		hasNative bool
	}
	volumes := make(map[string]volumeObservation, len(snap.Volumes))
	growth := make(map[client.ID]uint64, len(snap.Volumes))
	for _, v := range snap.Volumes {
		id := v.ID.String()
		raw, known := native[id]
		volumes[id] = volumeObservation{v.IO, raw, known}
		growth[v.ID] = v.AllocatedPhysical
	}
	buckets := make(map[string]client.IOStats, len(snap.Buckets))
	for _, bucket := range snap.Buckets {
		buckets[bucket.ID.String()] = bucket.IO
	}
	source.volumes = observeStatisticsEntities(source.volumes, volumes, func(c *statisticsIOCounters, v volumeObservation) {
		if v.hasNative {
			c.observe(now, v.io, tick, h, v.native)
		} else {
			c.observe(now, v.io, tick, h)
		}
	})
	source.buckets = observeStatisticsEntities(source.buckets, buckets, func(c *statisticsIOCounters, io client.IOStats) { c.observe(now, io, tick, h) })
	source.growth = observeStatisticsEntities(source.growth, growth, func(c *statisticsCounter, total uint64) { c.update(now, total, tick, h, true) })
	if bucketTotals != nil {
		source.bucketGrowth = observeStatisticsEntities(source.bucketGrowth, bucketTotals, func(c *statisticsCounter, total uint64) { c.update(now, total, tick, h, true) })
	}
	if users != nil {
		source.users = observeStatisticsEntities(source.users, users, func(c *statisticsIOCounters, io client.IOStats) { c.observe(now, io, tick, h) })
		var total client.IOStats
		for _, io := range users {
			total.LogicalReadBytes = addCapacity(total.LogicalReadBytes, io.LogicalReadBytes)
			total.LogicalWriteBytes = addCapacity(total.LogicalWriteBytes, io.LogicalWriteBytes)
		}
		source.userIO.observe(now, total, tick, h)
	}
	source.cpu.observe(now, snap.CPUTimeNS, tick, h)
	source.netRead.observe(now, snap.NetReadBytes, tick, h)
	source.netWrite.observe(now, snap.NetWriteBytes, tick, h)
}
func (b *statisticsBook) observePeer(snap client.StatsSnapshot, received time.Time, h decay.HalfLives) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.peers == nil {
		b.peers = make(map[client.ID]*statisticsSource)
	}
	source := b.peers[snap.Node]
	if source == nil {
		source = &statisticsSource{}
		b.peers[snap.Node] = source
	}
	source.observe(snap, nil, nil, received, h)
}

// A read never ticks or derives a new rate. Boot identity and freshness are
// explicit so reconnects/offline owners cannot borrow another incarnation's data.
func (b *statisticsBook) volumeForecast(node, boot, id client.ID, now time.Time) (read, write, growth float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	source := &b.statisticsSource
	if node != source.node {
		source = b.peers[node]
	}
	if source == nil || source.boot != boot || now.Sub(source.observed) >= 10*time.Second {
		return
	}
	if c := source.volumes[id.String()]; c != nil {
		read = c.traffic[0].window.Forecast().Peak()
		write = c.traffic[1].window.Forecast().Peak()
	}
	if g := source.growth[id]; g != nil {
		growth = g.window.Forecast().Peak()
	}
	return
}
func (b *statisticsBook) bucketVolumeGrowth(key bucketVolumeKey) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if g := b.bucketGrowth[key]; g != nil {
		return g.window.Forecast().Peak()
	}
	return 0
}
func (b *statisticsBook) observeMove(now time.Time, elapsed time.Duration, h decay.HalfLives) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.moveDuration.Observe(now, float64(elapsed), 1, h)
	b.moveDurationValue = time.Duration(b.moveDuration.Values(now, h).Peak())
}
func (b *statisticsBook) moveTime() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.moveDurationValue
}

func (s *Server) observeStatistics(snap client.StatsSnapshot, native ...map[string]volume.IOStats) {
	users := make(map[string]client.IOStats)
	// The resident account index is immutable. Do not load persistent metadata
	// or retain deleted account histories as part of telemetry collection.
	if accounts := s.accounts.Load(); accounts != nil {
		for _, user := range accounts.image.Users {
			read, write := s.userTraffic(user.ID)
			users[user.ID] = client.IOStats{LogicalReadBytes: read, LogicalWriteBytes: write}
		}
	}
	h := s.placementHalfLives()
	totals := make(map[bucketVolumeKey]uint64)
	if usage := s.quotaUsage.Load(); usage != nil {
		for id, rows := range usage.volumes {
			for _, row := range rows {
				totals[bucketVolumeKey{client.ID(row.Bucket), id}] = row.Physical
			}
		}
	}
	if snap.CapturedUnixNS > math.MaxInt64 {
		return
	}
	now := time.Unix(0, int64(snap.CapturedUnixNS))
	s.statistics.mu.Lock()
	s.statistics.statisticsSource.observe(snap, users, totals, now, h, native...)
	s.statistics.mu.Unlock()
	s.statistics.tickRanges(s, now, h)
}
