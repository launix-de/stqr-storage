// SPDX-License-Identifier: GPL-3.0-only
package decay

import (
	"math"
	"testing"
	"time"
)

func TestWindowsIntervalCadenceInvariant(t *testing.T) {
	h := HalfLives{3 * time.Second, 30 * time.Second}
	now := time.Unix(100, 0)
	var single, many Windows
	single.AddInterval(now, now.Add(time.Minute), 6000, h)
	for i := 0; i < 60; i++ {
		many.AddInterval(now.Add(time.Duration(i)*time.Second), now.Add(time.Duration(i+1)*time.Second), 100, h)
		many.Rates(now.Add(time.Hour), h) // pure preview must not advance stored frames
	}
	a, b := single.Rates(now.Add(time.Minute), h), many.Rates(now.Add(time.Minute), h)
	closeValue(t, a.Short, b.Short)
	closeValue(t, a.Long, b.Long)
	closeValue(t, a.Short, 100)
	closeValue(t, a.Long, 100)
	many.AddInterval(now, now.Add(time.Minute), 6000, h)
	many.AddInterval(now.Add(time.Second), now.Add(10*time.Second), 900, h)
	closeValue(t, many.Rates(now.Add(time.Minute), h).Short, a.Short)
	many.AddInterval(now.Add(59*time.Second), now.Add(62*time.Second), 300, h)
	single.AddInterval(now.Add(time.Minute), now.Add(62*time.Second), 200, h)
	closeValue(t, many.Rates(now.Add(62*time.Second), h).Long, single.Rates(now.Add(62*time.Second), h).Long)
}

func TestWindowsIdleAndKernelChange(t *testing.T) {
	now := time.Unix(100, 0)
	h := HalfLives{time.Second, 10 * time.Second}
	var w Windows
	w.AddInterval(now, now.Add(time.Second), 100, h)
	closeValue(t, w.Rates(now.Add(2*time.Second), h).Short, 50)
	closeValue(t, w.Rates(now.Add(11*time.Second), h).Long, 50)
	// Idle frames between actual samples are included by the same Tick kernel.
	w.AddInterval(now.Add(2*time.Second), now.Add(3*time.Second), 0, h)
	closeValue(t, w.Rates(now.Add(3*time.Second), h).Short, 25)
	changed := HalfLives{2 * time.Second, 20 * time.Second}
	if v := w.Rates(now.Add(3*time.Second), changed); v.Short != 0 || v.Long != 0 {
		t.Fatal("old history reinterpreted under changed kernel", v)
	}
	w.AddInterval(now.Add(3*time.Second), now.Add(4*time.Second), 10, changed)
	closeValue(t, w.Rates(now.Add(4*time.Second), changed).Long, 10)
}

func TestWindowsIntegerProratingAndSeed(t *testing.T) {
	now := time.Unix(100, 0)
	h := HalfLives{time.Second, time.Second}
	var w Windows
	w.AddInterval(now, now.Add(time.Second), 1, h)
	// Exercise multiplication above 64-bit range without a float conversion.
	w.AddInterval(now, now.Add(2*time.Second), math.MaxInt64, h)
	if got := w.short.Snapshot().Last; got != math.MaxInt64/2 {
		t.Fatalf("integer prorating lost precision: %d", got)
	}
	var seeded Windows
	seeded.SeedRate(now, 100, h)
	seeded.AddInterval(now, now.Add(time.Second), 0, h)
	closeValue(t, seeded.Rates(now.Add(time.Second), h).Short, 50)
	for _, invalid := range []float64{-1, math.NaN(), math.Inf(1)} {
		seeded.SeedRate(now, invalid, h)
	}
	closeValue(t, seeded.Rates(now.Add(time.Second), h).Short, 50)
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
	changed := HalfLives{2 * time.Second, 20 * time.Second}
	if m.Known(now, changed) {
		t.Fatal("old fractional evidence survived kernel change")
	}
}
