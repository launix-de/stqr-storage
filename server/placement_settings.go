// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"time"

	"github.com/launix-de/stqr-storage/internal/decay"
)

// PlacementStatisticsConfig controls advisory, RAM-only history. Zero fields
// select defaults, so preexisting node configurations remain compatible.
type PlacementStatisticsConfig struct {
	ShortHalfLifeSeconds int64 `yaml:"short_half_life_seconds,omitempty" json:"short_half_life_seconds"`
	LongHalfLifeSeconds  int64 `yaml:"long_half_life_seconds,omitempty" json:"long_half_life_seconds"`
}

func (c PlacementStatisticsConfig) validate() error {
	const maxSeconds = int64((1<<63 - 1) / time.Second)
	if c.ShortHalfLifeSeconds < 0 || c.LongHalfLifeSeconds < 0 || c.ShortHalfLifeSeconds > maxSeconds || c.LongHalfLifeSeconds > maxSeconds {
		return errors.New("placement statistics half-lives must be positive durations representable in seconds")
	}
	h := c.halfLives()
	if h.Short > h.Long {
		return errors.New("placement statistics short half-life exceeds long half-life")
	}
	return nil
}
func (c PlacementStatisticsConfig) halfLives() decay.HalfLives {
	return (decay.HalfLives{Short: time.Duration(c.ShortHalfLifeSeconds) * time.Second, Long: time.Duration(c.LongHalfLifeSeconds) * time.Second}).Normalized()
}
func (s *Server) placementHalfLives() decay.HalfLives {
	if s == nil {
		return (decay.HalfLives{}).Normalized()
	}
	s.mu.RLock()
	c := s.cfg.PlacementStatistics
	legacy := s.cfg.PlacementStatsHalfLife
	s.mu.RUnlock()
	h := c.halfLives()
	if c.LongHalfLifeSeconds == 0 && legacy > 0 {
		h.Long = legacy
		h.Short = min(h.Short, h.Long)
	}
	return h
}
