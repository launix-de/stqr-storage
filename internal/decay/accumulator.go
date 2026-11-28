// SPDX-License-Identifier: GPL-3.0-only
// Package decay implements bounded, time-based short and long memory for
// advisory telemetry. Accumulators are values; callers provide synchronization.
package decay

import (
	"math"
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

// Accumulator holds discounted amounts, not lifetime totals. Add records point
// events; AddInterval records a cumulative-counter delta uniformly distributed
// over an interval. Do not feed the same observation through both methods.
// Values does not mutate the accumulator, so polling frequency has no effect.
// A zero value is ready for use and no per-object/event history is retained.
type Accumulator struct {
	value     Values
	at        time.Time
	halfLives HalfLives
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

// A changed kernel forgets advisory history rather than reinterpreting amounts
// recorded under another half-life. Ownership and cumulative counters live elsewhere.
func (a *Accumulator) configure(h HalfLives) {
	if a.halfLives != h {
		a.value, a.at, a.halfLives = Values{}, time.Time{}, h
	}
}
func (a *Accumulator) advance(now time.Time, h HalfLives) {
	a.configure(h)
	if a.at.IsZero() {
		a.at = now
	} else if now.After(a.at) {
		dt := now.Sub(a.at)
		a.value.Short *= factor(dt, h.Short)
		a.value.Long *= factor(dt, h.Long)
		a.at = now
	}
}

// Add accepts only finite nonnegative amounts. Slightly out-of-order event
// timestamps cannot move the clock backwards and do not discard real traffic.
func (a *Accumulator) Add(now time.Time, amount float64, h HalfLives) {
	if !valid(amount) {
		return
	}
	h = h.Normalized()
	a.advance(now, h)
	a.value.Short = finiteAdd(a.value.Short, amount)
	a.value.Long = finiteAdd(a.value.Long, amount)
}

// AddInterval integrates a constant amount/second over [start,end]. Overlap
// with previously observed time is clipped, making repeated snapshots inert.
// Callers still distinguish cumulative-counter incarnations and sequence IDs.
func (a *Accumulator) AddInterval(start, end time.Time, amount float64, h HalfLives) {
	if !valid(amount) || !end.After(start) {
		return
	}
	h = h.Normalized()
	a.configure(h)
	if !a.at.IsZero() && !end.After(a.at) {
		return
	}
	seconds := end.Sub(start).Seconds()
	rate := amount / seconds
	if !valid(rate) {
		return
	}
	if !a.at.IsZero() && start.Before(a.at) {
		start = a.at
	}
	a.advance(start, h)
	dt := end.Sub(start)
	add := func(old float64, half time.Duration) float64 {
		lambda := math.Ln2 / half.Seconds()
		f := factor(dt, half)
		// Expm1 keeps sub-half-life intervals accurate without cancellation.
		integral := -math.Expm1(-lambda*dt.Seconds()) / lambda
		return finiteAdd(old*f, finiteProduct(rate, integral))
	}
	a.value.Short = add(a.value.Short, h.Short)
	a.value.Long = add(a.value.Long, h.Long)
	a.at = end
}

func (a Accumulator) Values(now time.Time, h HalfLives) Values {
	a.advance(now, h.Normalized())
	return a.value
}

// Rates returns the exponentially weighted amount/second. With a constant
// input rate, each window approaches that rate independent of sampling cadence.
func (a Accumulator) Rates(now time.Time, h HalfLives) Values {
	h = h.Normalized()
	v := a.Values(now, h)
	return Values{finiteProduct(v.Short, math.Ln2/h.Short.Seconds()), finiteProduct(v.Long, math.Ln2/h.Long.Seconds())}
}

// Mean retains weighted observations using the same decay kernel. Values
// returns both means and Known distinguishes missing data from a measured zero.
// Old means retain their value while their evidence Weight decays during idle.
type Mean struct{ sum, weight Accumulator }

func (m *Mean) Observe(now time.Time, value, weight float64, h HalfLives) {
	if !valid(value) || !valid(weight) || weight == 0 || value != 0 && weight > math.MaxFloat64/value {
		return
	}
	m.sum.Add(now, value*weight, h)
	m.weight.Add(now, weight, h)
}
func (m Mean) Weight(now time.Time, h HalfLives) Values { return m.weight.Values(now, h) }
func (m Mean) Values(now time.Time, h HalfLives) Values {
	s, w := m.sum.Values(now, h), m.weight.Values(now, h)
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
