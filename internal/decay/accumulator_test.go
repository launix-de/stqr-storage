// SPDX-License-Identifier: GPL-3.0-only
package decay

import (
	"math"
	"testing"
	"time"
)

func closeValue(t *testing.T, got, want float64) {
	t.Helper()
	if math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-want) > 1e-10*math.Max(1, math.Abs(want)) {
		t.Fatalf("got %.15g, want %.15g", got, want)
	}
}

func TestAccumulatorHalfLifeAndIdle(t *testing.T) {
	h := HalfLives{time.Second, 10 * time.Second}
	now := time.Unix(100, 0)
	var a Accumulator
	a.Add(now, 100, h)
	v := a.Values(now.Add(time.Second), h)
	closeValue(t, v.Short, 50)
	closeValue(t, v.Long, 100*math.Exp2(-.1))
	v = a.Values(now.Add(10*time.Second), h)
	closeValue(t, v.Short, 100.0/1024)
	closeValue(t, v.Long, 50)
	// Queries are pure, including a query later than the next real event.
	a.Values(now.Add(time.Hour), h)
	a.Add(now.Add(time.Second), 25, h)
	closeValue(t, a.Values(now.Add(time.Second), h).Short, 75)
	closeValue(t, a.Rates(now.Add(time.Second), h).Short, 75*math.Ln2)
}

func TestAccumulatorIntervalCadenceInvariant(t *testing.T) {
	h := HalfLives{3 * time.Second, 30 * time.Second}
	now := time.Unix(100, 0)
	var single, many Accumulator
	single.AddInterval(now, now.Add(time.Minute), 6000, h)
	for i := 0; i < 60; i++ {
		many.AddInterval(now.Add(time.Duration(i)*time.Second), now.Add(time.Duration(i+1)*time.Second), 100, h)
		many.Values(now.Add(time.Hour), h)
	}
	a, b := single.Rates(now.Add(time.Minute), h), many.Rates(now.Add(time.Minute), h)
	closeValue(t, a.Short, b.Short)
	closeValue(t, a.Long, b.Long)
	closeValue(t, a.Short, 100*(1-math.Exp2(-20)))
	closeValue(t, a.Long, 75)
	// Replayed and backwards intervals cannot add load or rewind the clock.
	many.AddInterval(now, now.Add(time.Minute), 6000, h)
	many.AddInterval(now.Add(time.Second), now.Add(10*time.Second), 900, h)
	closeValue(t, many.Rates(now.Add(time.Minute), h).Short, a.Short)
	// A partially overlapping snapshot contributes only its new suffix.
	many.AddInterval(now.Add(59*time.Second), now.Add(62*time.Second), 300, h)
	single.AddInterval(now.Add(time.Minute), now.Add(62*time.Second), 200, h)
	closeValue(t, many.Rates(now.Add(62*time.Second), h).Long, single.Rates(now.Add(62*time.Second), h).Long)
}

func TestAccumulatorFiniteInputsAndClock(t *testing.T) {
	h := HalfLives{time.Nanosecond, time.Second}
	now := time.Unix(100, 0)
	var a Accumulator
	a.Add(now, 5, h)
	for _, v := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		a.Add(now, v, h)
		a.AddInterval(now, now.Add(time.Second), v, h)
	}
	closeValue(t, a.Values(now, h).Short, 5)
	a.Add(now.Add(-time.Second), 5, h)
	closeValue(t, a.Values(now, h).Short, 10)
	a.AddInterval(now, now, 100, h)
	closeValue(t, a.Values(now, h).Short, 10)
	a.Add(now, math.MaxFloat64, h)
	a.Add(now, math.MaxFloat64, h)
	for _, v := range []float64{a.Values(now, h).Short, a.Rates(now, h).Short, a.Values(now.Add(time.Hour), h).Long} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			t.Fatal("non-finite state", v)
		}
	}
	if got := (HalfLives{}).Normalized(); got.Short != DefaultShortHalfLife || got.Long != DefaultLongHalfLife {
		t.Fatal(got)
	}
}

func TestMeanWeightedAndEvidenceDecay(t *testing.T) {
	now := time.Unix(100, 0)
	h := HalfLives{time.Second, 10 * time.Second}
	var m Mean
	if m.Known(now, h) {
		t.Fatal("unobserved mean is known")
	}
	m.Observe(now, 10, 1, h)
	m.Observe(now, 30, 3, h)
	closeValue(t, m.Values(now, h).Short, 25)
	closeValue(t, m.Values(now.Add(time.Second), h).Long, 25)
	closeValue(t, m.Weight(now.Add(time.Second), h).Short, 2)
	m.Observe(now.Add(time.Second), 0, 2, h)
	closeValue(t, m.Values(now.Add(time.Second), h).Short, 12.5)
	m.Observe(now, math.Inf(1), 1, h)
	m.Observe(now, 10, math.NaN(), h)
	closeValue(t, m.Values(now.Add(time.Second), h).Short, 12.5)
}

func TestAccumulatorKernelChangeForgetsAdvisoryHistory(t *testing.T) {
	now := time.Unix(100, 0)
	old := HalfLives{time.Second, 10 * time.Second}
	changed := HalfLives{2 * time.Second, 20 * time.Second}
	var a Accumulator
	a.Add(now, 100, old)
	if v := a.Rates(now, changed); v.Short != 0 || v.Long != 0 {
		t.Fatal("old history reinterpreted", v)
	}
	a.Add(now, 10, changed)
	closeValue(t, a.Values(now, changed).Short, 10)
	var m Mean
	m.Observe(now, 20, 1, old)
	if m.Known(now, changed) {
		t.Fatal("old mean survived kernel change")
	}
}
