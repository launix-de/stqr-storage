// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"time"
)

const defaultPlacementStatsHalfLife = 7 * 24 * time.Hour

func (s *Server) placementStatsHalfLife() time.Duration {
	s.mu.RLock()
	halfLife := s.cfg.PlacementStatsHalfLife
	s.mu.RUnlock()
	if halfLife <= 0 {
		return defaultPlacementStatsHalfLife
	}
	return halfLife
}

// Compute once per sampled frame, never per object operation. Expm1 preserves
// precision when a short frame is tiny relative to the configured half-life.
func statisticsRate(previous, sample float64, elapsed, halfLife time.Duration) float64 {
	if elapsed <= 0 {
		return previous
	}
	if halfLife <= 0 {
		halfLife = defaultPlacementStatsHalfLife
	}
	weight := -math.Expm1(-math.Ln2 * float64(elapsed) / float64(halfLife))
	return previous + weight*(sample-previous)
}
