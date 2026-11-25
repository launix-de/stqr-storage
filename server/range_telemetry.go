// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type rangeMetricKey struct {
	bucket client.ID
	start  client.ObjectID
	bits   uint16
}
type rangeMetric struct {
	read, write         atomic.Uint64
	active              atomic.Int64
	mu                  sync.Mutex
	sampled             time.Time
	lastRead, lastWrite uint64
	readRate, writeRate float64
}

// Range telemetry is advisory. Keep snapshots comfortably below the existing
// wire count/byte limits even when hundreds of thousands of ranges are active.
// sync.Map iteration changes the sample; local optimizer counters remain whole.
const maxRangeTelemetry = 4096

func (b *Bucket) recordRangeTraffic(p principal, id volume.ObjectID, read, write uint64) {
	if b.server == nil || b.server.scopes == nil || p.cowInternal || p.ClusterPeer || read == 0 && write == 0 {
		return
	}
	r := b.server.scopes
	image := b.server.placement.Load()
	if image == nil {
		return
	}
	key, ok := image.metricKey(client.ID(b.ID()), client.ObjectID(id))
	if !ok {
		return
	}
	now := time.Now()
	value, exists := r.metrics.Load(key)
	if !exists {
		value, _ = r.metrics.LoadOrStore(key, &rangeMetric{sampled: now})
	}
	m := value.(*rangeMetric)
	m.read.Add(read)
	m.write.Add(write)
	m.active.Store(now.UnixNano())
}

func (r *rangeManager) rangeWorkload(key rangeMetricKey) (float64, float64) {
	value, ok := r.metrics.Load(key)
	if !ok {
		return 0, 0
	}
	m := value.(*rangeMetric)
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	read, write := m.read.Load(), m.write.Load()
	if m.sampled.IsZero() {
		m.sampled = now
		m.lastRead, m.lastWrite = read, write
		return 0, 0
	}
	seconds := now.Sub(m.sampled).Seconds()
	if seconds >= 1 {
		m.readRate = float64(read-m.lastRead) / seconds
		m.writeRate = float64(write-m.lastWrite) / seconds
		m.sampled, m.lastRead, m.lastWrite = now, read, write
	}
	return m.readRate, m.writeRate
}

func (r *rangeManager) collectRangeTelemetry(snap *client.StatsSnapshot) {
	image := r.server.placement.Load()
	if image == nil {
		return
	}
	r.measurements.Range(func(k, v any) bool {
		key := k.(rangeMetricKey)
		entry, ok := image.lookup(key.bucket, key.start)
		if !ok || entry.Start != key.start || entry.PrefixBits != key.bits || len(entry.Locations) == 0 || entry.Locations[0].Node != r.node {
			r.measurements.Delete(key)
		}
		return true
	})
	r.metrics.Range(func(k, v any) bool {
		key, m := k.(rangeMetricKey), v.(*rangeMetric)
		entry, ok := image.lookup(key.bucket, key.start)
		if !ok || entry.Start != key.start || entry.PrefixBits != key.bits {
			r.metrics.Delete(key)
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
			r.metrics.Delete(key)
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
