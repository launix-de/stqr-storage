// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type placementVolumeRate struct {
	read, write float64
}

func placementVolumeRates(snapshots []client.StatsSnapshot, previous *pgRebuildCatalog, halfLife time.Duration) map[client.ID]placementVolumeRate {
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
		if prior.Sequence == snapshot.Sequence {
			for _, v := range snapshot.Volumes {
				if rate, known := previous.volumeRates[v.ID]; known {
					out[v.ID] = rate
				}
			}
			continue
		}
		if snapshot.Sequence < prior.Sequence || snapshot.CapturedUnixNS <= prior.CapturedUnixNS {
			for _, v := range snapshot.Volumes {
				if rate, known := previous.volumeRates[v.ID]; known {
					out[v.ID] = rate
				}
			}
			continue
		}
		seconds := float64(snapshot.CapturedUnixNS-prior.CapturedUnixNS) / 1e9
		volumes := make(map[client.ID]client.VolumeStats, len(prior.Volumes))
		for _, v := range prior.Volumes {
			volumes[v.ID] = v
		}
		for _, v := range snapshot.Volumes {
			p, ok := volumes[v.ID]
			if !ok || v.IO.PhysicalReadBytes < p.IO.PhysicalReadBytes || v.IO.PhysicalWriteBytes < p.IO.PhysicalWriteBytes {
				continue
			}
			read := float64(subtractCapacity(v.IO.PhysicalReadBytes, p.IO.PhysicalReadBytes)) / seconds
			write := float64(subtractCapacity(v.IO.PhysicalWriteBytes, p.IO.PhysicalWriteBytes)) / seconds
			last, known := previous.volumeRates[v.ID]
			if !known {
				// Seed with an actually observed interval, not a week-long ramp
				// from an invented zero. A reboot/counter reset starts afresh.
				out[v.ID] = placementVolumeRate{read, write}
				continue
			}
			elapsed := time.Duration(min(snapshot.CapturedUnixNS-prior.CapturedUnixNS, uint64(math.MaxInt64)))
			out[v.ID] = placementVolumeRate{statisticsRate(last.read, read, elapsed, halfLife), statisticsRate(last.write, write, elapsed, halfLife)}
		}
	}
	return out
}
func (r *rangeManager) placementBackground(destinations []rangeDestination) ([]float64, []float64) {
	read, write := make([]float64, len(destinations)), make([]float64, len(destinations))
	catalog := r.pgCatalog.Load()
	for i, d := range destinations {
		if catalog != nil {
			rate := catalog.volumeRates[d.volume.ID]
			read[i], write[i] = rate.read, rate.write
		}
	}
	return read, write
}
