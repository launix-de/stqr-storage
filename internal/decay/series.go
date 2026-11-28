// SPDX-License-Identifier: GPL-3.0-only
package decay

import "time"

// Series collects an amount once. All means consume that single completed frame;
// no horizon has a separate current counter, delta baseline or sampling clock.
// Add is concurrent. Callers serialize Tick/Forecast; Snapshot is concurrent.
type Series struct {
	current               Accumulator
	short, long           float64
	shortKnown, longKnown bool
	halfLives             HalfLives
}

func (s *Series) Add(amount int64) bool { return s.current.Add(amount) }
func (s *Series) Tick(now time.Time, h HalfLives) bool {
	h = h.Normalized()
	if s.halfLives != h {
		s.shortKnown, s.longKnown = false, false
		s.short, s.long = 0, 0
		s.halfLives = h
	}
	if !s.current.Tick(now, DefaultLongHalfLife) {
		return false
	}
	frame := s.current.Snapshot()
	sample := frame.LastPerSecond()
	if h.Short != DefaultLongHalfLife {
		s.short = blendRate(s.short, sample, s.shortKnown, frame.Interval, h.Short)
		s.shortKnown = true
	}
	if h.Long != DefaultLongHalfLife && h.Long != h.Short {
		s.long = blendRate(s.long, sample, s.longKnown, frame.Interval, h.Long)
		s.longKnown = true
	}
	return true
}
func (s *Series) Snapshot() Snapshot { return s.current.Snapshot() }
func (s *Series) Forecast() Values {
	weekly := s.current.AveragePerSecond()
	short := weekly
	if s.halfLives.Short != DefaultLongHalfLife {
		short = s.short
	}
	long := weekly
	if s.halfLives.Long == s.halfLives.Short {
		long = short
	} else if s.halfLives.Long != DefaultLongHalfLife {
		long = s.long
	}
	return Values{short, long}
}
