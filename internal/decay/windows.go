// SPDX-License-Identifier: GPL-3.0-only
package decay

import (
	"math"
	"math/bits"
	"time"
)

const (
	DefaultShortHalfLife = 10 * time.Second
	DefaultLongHalfLife  = 7 * 24 * time.Hour
)

type HalfLives struct{ Short, Long time.Duration }

func (h HalfLives) Normalized() HalfLives {
	if h.Short <= 0 {
		h.Short = DefaultShortHalfLife
	}
	if h.Long <= 0 {
		h.Long = DefaultLongHalfLife
	}
	return h
}

type Values struct{ Short, Long float64 }

// Peak lets brief pressure and established demand both influence a forecast.
// The windows stay separate so callers can instead require sustained demand.
func (v Values) Peak() float64 { return math.Max(v.Short, v.Long) }

// Windows applies two half-lives to the same integer interval observations.
// Both windows use Accumulator's Tick implementation. Callers synchronize all
// access. Only immutable frame snapshots are copied, never live atomics.
type Windows struct {
	short, long Snapshot
	halfLives   HalfLives
}

func (w *Windows) configure(h HalfLives) {
	if w.halfLives != h {
		*w = Windows{halfLives: h}
	}
}

// SeedRate imports a previously measured forecast. A changed kernel forgets
// advisory history instead of reinterpreting it under another half-life.
func (w *Windows) SeedRate(now time.Time, rate float64, h HalfLives) {
	if !valid(rate) || now.IsZero() {
		return
	}
	w.configure(h.Normalized())
	w.short = Snapshot{Average: rate, Switched: now, Known: true}
	w.long = w.short
}

// AddInterval records integer events distributed over [start,end]. Replayed or
// backwards frames cannot add load. For a partially overlapping source frame,
// only its new suffix is admitted; that prorating necessarily estimates where
// the frame's events occurred but uses integer arithmetic throughout.
func (w *Windows) AddInterval(start, end time.Time, amount int64, h HalfLives) {
	if amount < 0 || !end.After(start) || start.IsZero() || end.IsZero() {
		return
	}
	h = h.Normalized()
	w.configure(h)
	at := w.short.Switched
	if !at.IsZero() {
		if !end.After(at) {
			return
		}
		if start.Before(at) {
			hi, lo := bits.Mul64(uint64(amount), uint64(end.Sub(at)))
			scaled, _ := bits.Div64(hi, lo, uint64(end.Sub(start)))
			amount = int64(scaled)
			start = at
		}
	}
	w.short = intervalFrame(w.short, start, end, amount, h.Short)
	w.long = intervalFrame(w.long, start, end, amount, h.Long)
}

// Rates previews idle decay on a value copy. Querying later than the next real
// sample does not advance either stored window or drop pending observations.
func (w Windows) Rates(now time.Time, h HalfLives) Values {
	h = h.Normalized()
	w.configure(h)
	return Values{previewFrame(w.short, now, h.Short), previewFrame(w.long, now, h.Long)}
}

// Working counters are private to this call. Snapshot import/export preserves
// value semantics for immutable placement catalogs and speculative forecasts.
func intervalFrame(frame Snapshot, start, end time.Time, amount int64, half time.Duration) Snapshot {
	var counter Accumulator
	counter.restore(frame)
	counter.Tick(start, half)
	counter.Add(amount)
	counter.Tick(end, half)
	return counter.Snapshot()
}
func previewFrame(frame Snapshot, now time.Time, half time.Duration) float64 {
	var counter Accumulator
	counter.restore(frame)
	counter.Tick(now, half)
	return counter.AveragePerSecond()
}

func valid(v float64) bool { return v >= 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }
func finiteAdd(a, b float64) float64 {
	if b > math.MaxFloat64-a {
		return math.MaxFloat64
	}
	return a + b
}
func finiteProduct(a, b float64) float64 {
	if a != 0 && b > math.MaxFloat64/a {
		return math.MaxFloat64
	}
	return a * b
}
func factor(elapsed, halfLife time.Duration) float64 {
	return math.Exp2(-float64(elapsed) / float64(halfLife))
}

// weightedSum is private and distinct from event counters: fractional network
// distances and observation weights cannot be represented by integer units.
// It keeps discounted evidence for Mean, not an alternative throughput engine.
type weightedSum struct {
	value     Values
	at        time.Time
	halfLives HalfLives
}

func (a *weightedSum) advance(now time.Time, h HalfLives) {
	if a.halfLives != h {
		*a = weightedSum{halfLives: h}
	}
	if a.at.IsZero() {
		a.at = now
	} else if now.After(a.at) {
		dt := now.Sub(a.at)
		a.value.Short *= factor(dt, h.Short)
		a.value.Long *= factor(dt, h.Long)
		a.at = now
	}
}
func (a *weightedSum) add(now time.Time, amount float64, h HalfLives) {
	if !valid(amount) {
		return
	}
	a.advance(now, h.Normalized())
	a.value.Short = finiteAdd(a.value.Short, amount)
	a.value.Long = finiteAdd(a.value.Long, amount)
}
func (a weightedSum) values(now time.Time, h HalfLives) Values {
	a.advance(now, h.Normalized())
	return a.value
}

// Mean retains fractional observations using discounted evidence. Values
// returns both means and Known distinguishes missing data from a measured zero.
// Old means retain their value while their evidence Weight decays during idle.
type Mean struct{ sum, weight weightedSum }

func (m *Mean) Observe(now time.Time, value, weight float64, h HalfLives) {
	if !valid(value) || !valid(weight) || weight == 0 || value != 0 && weight > math.MaxFloat64/value {
		return
	}
	m.sum.add(now, value*weight, h)
	m.weight.add(now, weight, h)
}
func (m Mean) Weight(now time.Time, h HalfLives) Values { return m.weight.values(now, h) }
func (m Mean) Values(now time.Time, h HalfLives) Values {
	s, w := m.sum.values(now, h), m.weight.values(now, h)
	var v Values
	if w.Short > 0 {
		v.Short = min(math.MaxFloat64, s.Short/w.Short)
	}
	if w.Long > 0 {
		v.Long = min(math.MaxFloat64, s.Long/w.Long)
	}
	return v
}
func (m Mean) Known(now time.Time, h HalfLives) bool {
	w := m.Weight(now, h)
	return w.Short > 0 || w.Long > 0
}
