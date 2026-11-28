// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Existing unrestricted cohorts already stripe objects over eligible volumes.
// Add foreground load to their new-object ranking without relocating an existing
// object, changing a retry receipt's owner or pinning the cohort to one disk.
// The immutable catalog is maintained by the background worker; this lookup
// performs no storage I/O and retains no object-specific state.
func (s *Server) localPlacementLoads(volumes []*volume.Volume, now time.Time) map[volume.ID]float64 {
	if s.scopes == nil {
		return nil
	}
	catalog := s.scopes.pgCatalog.Load()
	if catalog == nil {
		return nil
	}
	h := s.placementHalfLives()
	loads := make(map[volume.ID]float64, len(volumes))
	total := 0.0
	for _, v := range volumes {
		id := v.Identity().VolumeID
		rate := catalog.volumeRates[client.ID(id)]
		load := rate.readWindow.Rates(now, h).Peak() + rate.writeWindow.Rates(now, h).Peak()
		loads[id] = load
		total += load
	}
	if total == 0 {
		return nil
	}
	// A tiny isolated request must not outweigh substantial usable headroom.
	scale := math.Max(1<<20, total/float64(max(1, len(volumes))))
	for id, load := range loads {
		loads[id] = load / scale
	}
	return loads
}

func localVolumePlacementScore(free, capacity uint64, pressure float64) float64 {
	if free == 0 {
		return 0
	}
	return float64(free) / float64(max(uint64(1), capacity)) / (1 + math.Max(0, pressure))
}
