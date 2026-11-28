// SPDX-License-Identifier: GPL-3.0-only
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
	retained, _ := decayWeights(elapsed, halfLife)
	return retained
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
