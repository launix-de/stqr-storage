// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
)

type placementVolumeRate struct{ read, write float64 }

func placementVolumeRates(snapshots []client.StatsSnapshot, previous *pgRebuildCatalog) map[client.ID]placementVolumeRate {
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
				out[v.ID] = previous.volumeRates[v.ID]
			}
			continue
		}
		if snapshot.CapturedUnixNS <= prior.CapturedUnixNS {
			continue
		}
		seconds := float64(snapshot.CapturedUnixNS-prior.CapturedUnixNS) / 1e9
		volumes := make(map[client.ID]client.VolumeStats, len(prior.Volumes))
		for _, v := range prior.Volumes {
			volumes[v.ID] = v
		}
		for _, v := range snapshot.Volumes {
			p, ok := volumes[v.ID]
			if !ok {
				continue
			}
			read := float64(subtractCapacity(v.IO.PhysicalReadBytes, p.IO.PhysicalReadBytes)) / seconds
			write := float64(subtractCapacity(v.IO.PhysicalWriteBytes, p.IO.PhysicalWriteBytes)) / seconds
			last := previous.volumeRates[v.ID]
			out[v.ID] = placementVolumeRate{.75*last.read + .25*read, .75*last.write + .25*write}
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
