// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
)

type placementVolumeRate struct {
	read, write             float64 // captured forecast; the underlying windows decay on use
	readWindow, writeWindow decay.Accumulator
	sampled                 time.Time // local receipt time of the last new source snapshot
}

func placementVolumeRates(snapshots []client.StatsSnapshot, previous *pgRebuildCatalog, windows ...decay.HalfLives) map[client.ID]placementVolumeRate {
	h := (decay.HalfLives{}).Normalized()
	if len(windows) != 0 {
		h = windows[0].Normalized()
	}
	return placementVolumeRatesAt(snapshots, previous, h, time.Time{})
}

// Production windows use local receipt time. Peer wall clocks may be skewed;
// their timestamps determine source deltas, never the local decay epoch.
func placementVolumeRatesAt(snapshots []client.StatsSnapshot, previous *pgRebuildCatalog, h decay.HalfLives, received time.Time) map[client.ID]placementVolumeRate {
	out := make(map[client.ID]placementVolumeRate)
	if previous == nil {
		return out
	}
	old := make(map[client.ID]client.StatsSnapshot, len(previous.snapshots))
	for _, snapshot := range previous.snapshots {
		old[snapshot.Node] = snapshot
	}
	for _, snapshot := range snapshots {
		prior, ok := old[snapshot.Node]
		if !ok || prior.BootID != snapshot.BootID {
			continue
		}
		if snapshot.Sequence <= prior.Sequence {
			for _, v := range snapshot.Volumes {
				out[v.ID] = previous.volumeRates[v.ID]
			}
			continue
		}
		if snapshot.CapturedUnixNS <= prior.CapturedUnixNS {
			continue
		}
		if snapshot.CapturedUnixNS > 1<<63-1 || prior.CapturedUnixNS > 1<<63-1 {
			continue
		}
		start, end := time.Unix(0, int64(prior.CapturedUnixNS)), time.Unix(0, int64(snapshot.CapturedUnixNS))
		volumes := make(map[client.ID]client.VolumeStats, len(prior.Volumes))
		for _, v := range prior.Volumes {
			volumes[v.ID] = v
		}
		for _, v := range snapshot.Volumes {
			p, ok := volumes[v.ID]
			if !ok {
				continue
			}
			read := float64(subtractCapacity(v.IO.PhysicalReadBytes, p.IO.PhysicalReadBytes))
			write := float64(subtractCapacity(v.IO.PhysicalWriteBytes, p.IO.PhysicalWriteBytes))
			last := previous.volumeRates[v.ID]
			sampleStart, sampleEnd := start, end
			if !received.IsZero() {
				sampleEnd = received
				sampleStart = received.Add(-end.Sub(start))
				if !last.sampled.IsZero() {
					sampleStart = last.sampled
				}
			}
			last.sampled = sampleEnd
			last.readWindow.AddInterval(sampleStart, sampleEnd, read, h)
			last.writeWindow.AddInterval(sampleStart, sampleEnd, write, h)
			last.read, last.write = last.readWindow.Rates(sampleEnd, h).Peak(), last.writeWindow.Rates(sampleEnd, h).Peak()
			out[v.ID] = last
		}
	}
	return out
}
func placementBackgroundRates(catalog *pgRebuildCatalog, ids []client.ID, h decay.HalfLives, now time.Time) ([]float64, []float64) {
	read, write := make([]float64, len(ids)), make([]float64, len(ids))
	if catalog != nil {
		for i, id := range ids {
			rate := catalog.volumeRates[id]
			read[i], write[i] = rate.readWindow.Rates(now, h).Peak(), rate.writeWindow.Rates(now, h).Peak()
		}
	}
	return read, write
}
func (r *rangeManager) placementBackground(destinations []rangeDestination) ([]float64, []float64) {
	ids := make([]client.ID, len(destinations))
	for i, d := range destinations {
		ids[i] = d.volume.ID
	}
	return placementBackgroundRates(r.pgCatalog.Load(), ids, r.server.placementHalfLives(), time.Now())
}
