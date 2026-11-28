// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
	"github.com/launix-de/stqr-storage/volume"
)

type rangeMetricKey struct {
	bucket client.ID
	start  client.ObjectID
	bits   uint16
}
type rangeMetric struct {
	readers                   map[placementReaderKey]*placementReaderTraffic
	read, write               atomic.Uint64
	active                    atomic.Int64
	mu                        sync.Mutex
	readCounter, writeCounter statisticsCounter
	sampled                   time.Time
}

// Range telemetry is advisory. Keep snapshots comfortably below the existing
// wire count/byte limits even when hundreds of thousands of ranges are active.
// sync.Map iteration changes the sample; local optimizer counters remain whole.
const maxRangeTelemetry = 4096

func (b *Bucket) recordRangeTraffic(p principal, id volume.ObjectID, read, write uint64) {
	if p.internalCoordination || b.server == nil || p.cowInternal || p.ClusterPeer || read == 0 && write == 0 {
		return
	}
	s := b.server
	image := s.placement.Load()
	if image == nil {
		return
	}
	key, ok := image.metricKey(client.ID(b.ID()), client.ObjectID(id))
	if !ok {
		return
	}
	now := time.Now()
	value, exists := s.statistics.ranges.Load(key)
	if !exists {
		value, _ = s.statistics.ranges.LoadOrStore(key, &rangeMetric{sampled: now})
	}
	m := value.(*rangeMetric)
	m.read.Add(read)
	m.write.Add(write)
	m.active.Store(now.UnixNano())
	recordPlacementReader(m, key, p, read, write, now)
}

// Range and reader sampling belong to central telemetry, never a planner read.
func (m *rangeMetric) tick(now time.Time, h decay.HalfLives) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sampled.IsZero() {
		m.sampled = now
	}
	for _, counter := range []*statisticsCounter{&m.readCounter, &m.writeCounter} {
		if !counter.started {
			counter.observe(m.sampled, 0, false, h)
		}
	}
	tick := now.Sub(m.readCounter.window.Snapshot().Switched) >= dashboardRateInterval
	m.readCounter.observe(now, m.read.Load(), tick, h)
	m.writeCounter.observe(now, m.write.Load(), tick, h)
	for _, traffic := range m.readers {
		if h.Long <= time.Duration((1<<63-1)/4) && now.Sub(traffic.active) > h.Long*4 {
			continue
		}
		if !traffic.readCounter.started {
			traffic.readCounter.observe(traffic.sampled, 0, false, h)
			traffic.writeCounter.observe(traffic.sampled, 0, false, h)
		}
		tick := now.Sub(traffic.readCounter.window.Snapshot().Switched) >= dashboardRateInterval
		traffic.readCounter.observe(now, traffic.read, tick, h)
		traffic.writeCounter.observe(now, traffic.write, tick, h)
	}
}
func (m *rangeMetric) workload() (float64, float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readCounter.window.Forecast().Peak(), m.writeCounter.window.Forecast().Peak()
}
func (b *statisticsBook) rangeWorkload(key rangeMetricKey) (float64, float64) {
	value, ok := b.ranges.Load(key)
	if !ok {
		return 0, 0
	}
	return value.(*rangeMetric).workload()
}
func (b *statisticsBook) tickRanges(s *Server, now time.Time, h decay.HalfLives) {
	s.prunePlacementReaders(now)
	image := s.placement.Load()
	if image == nil {
		return
	}
	b.mu.Lock()
	node := b.node
	b.mu.Unlock()
	b.ranges.Range(func(k, v any) bool {
		key := k.(rangeMetricKey)
		entry, ok := image.lookup(key.bucket, key.start)
		if !ok || entry.Start != key.start || entry.PrefixBits != key.bits {
			b.ranges.Delete(key)
			return true
		}
		local := false
		for _, location := range entry.Locations {
			local = local || location.Node == node
		}
		if !local {
			b.ranges.Delete(key)
			return true
		}
		v.(*rangeMetric).tick(now, h)
		return true
	})
}

func (r *rangeManager) collectRangeTelemetry(snap *client.StatsSnapshot) {
	image := r.server.placement.Load()
	if image == nil {
		return
	}
	r.server.statistics.ranges.Range(func(k, v any) bool {
		key, m := k.(rangeMetricKey), v.(*rangeMetric)
		entry, ok := image.lookup(key.bucket, key.start)
		if !ok || entry.Start != key.start || entry.PrefixBits != key.bits {
			return true
		}
		var local *client.Location
		for i := range entry.Locations {
			if entry.Locations[i].Node == r.node {
				local = &entry.Locations[i]
				break
			}
		}
		if local == nil {
			return true
		}
		// The existing wire schema requires a concrete volume. Cohort traffic is
		// still used locally by the optimizer; do not invent a volume assignment
		// or disconnect older peers by publishing Volume=0.
		if local.Volume == (client.ID{}) {
			return true
		}
		// Keep cumulative counters, but do not push permanently idle rows every
		// three seconds. Cold ranges have no invented traffic measurements.
		if time.Now().UnixNano()-m.active.Load() > int64(30*time.Second) {
			return true
		}
		if len(snap.Ranges) >= maxRangeTelemetry {
			return true
		}
		snap.Ranges = append(snap.Ranges, client.RangeStats{Bucket: key.bucket, Start: key.start, PrefixBits: key.bits, Volume: local.Volume, Role: local.Role, State: entry.State, ActualClass: local.Class, RuntimeID: entry.Runtime, OwnerEpoch: entry.OwnerEpoch, IO: client.IOStats{LogicalReadBytes: m.read.Load(), LogicalWriteBytes: m.write.Load()}})
		return true
	})
}
