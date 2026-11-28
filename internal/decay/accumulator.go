// SPDX-License-Identifier: GPL-3.0-only
// Package decay implements bounded, time-based telemetry. Event accumulators
// retain exact integer interval totals and a time-normalized floating mean.
package decay

import (
	"math"
	"runtime"
	"sync/atomic"
	"time"
)

// Accumulator is a unit-neutral event counter. Add, Tick, Snapshot and the rate
// getters may run concurrently. The zero value is ready for use. The first Tick
// establishes the start of measurement; pending additions are retained until a
// later Tick closes the first interval. Call Tick even during idle intervals.
//
// Do not copy an accumulator after use. Forecast windows retain immutable
// snapshots instead. The typed sequence atomic enforces 8-byte alignment for all
// 64-bit fields, including when embedded in another struct on 32-bit systems.
type Accumulator struct {
	sequence  atomic.Uint64
	average   uint64 // float64 bits, units/second
	last      int64
	interval  int64 // nanoseconds in the last completed interval
	current   int64
	switched  int64 // Unix nanoseconds of the last Tick
	flags     uint32
	saturated uint32
}

const (
	accumulatorStarted = 1 << iota
	accumulatorKnown
)

// Snapshot is an immutable view of one completed frame and the current total.
// Average is already normalized to units/second, so changing the measurement
// interval does not change the meaning of the half-life history. Last and
// Current retain exact integer units. Known is false until an interval closes.
type Snapshot struct {
	Average   float64
	Last      int64
	Interval  time.Duration
	Current   int64
	Switched  time.Time
	Known     bool
	Saturated bool
}

// Add adds amount to the current interval using CAS. A concurrent Tick's atomic
// exchange assigns each successful addition to exactly one interval. Negative
// amounts are supported for units such as signed connection-count changes.
// Overflow saturates instead of wrapping; false and Snapshot.Saturated expose
// that the exact amount no longer fits in int64. Saturation remains observable
// until the accumulator is reset while quiescent.
func (a *Accumulator) Add(amount int64) bool {
	for {
		old := atomic.LoadInt64(&a.current)
		next, exact := old+amount, true
		if amount > 0 && old > math.MaxInt64-amount {
			next, exact = math.MaxInt64, false
		} else if amount < 0 && old < math.MinInt64-amount {
			next, exact = math.MinInt64, false
		}
		if atomic.CompareAndSwapInt64(&a.current, old, next) {
			if !exact {
				atomic.StoreUint32(&a.saturated, 1)
			}
			return exact
		}
	}
}

func (a *Accumulator) lock() uint64 {
	for {
		sequence := a.sequence.Load()
		if sequence&1 == 0 && a.sequence.CompareAndSwap(sequence, sequence+1) {
			return sequence
		}
		runtime.Gosched()
	}
}
func (a *Accumulator) unlock(sequence uint64) { a.sequence.Store(sequence + 2) }

// Tick closes the current interval and updates its exponentially weighted mean
// using alpha = 1-exp(-ln(2)*elapsed/halfLife). The first completed measured rate
// seeds the mean; unknown prehistory is not treated as measured zero traffic.
// Nonpositive halfLife selects one week. Duplicate/backwards timestamps leave
// both pending counts and completed measurements untouched. The return value is
// true only when an interval was completed, including an idle (zero) interval.
func (a *Accumulator) Tick(now time.Time, halfLife time.Duration) bool {
	if now.IsZero() {
		return false
	}
	sequence := a.lock()
	defer a.unlock(sequence)
	flags := atomic.LoadUint32(&a.flags)
	if flags&accumulatorStarted == 0 {
		atomic.StoreInt64(&a.switched, now.UnixNano())
		atomic.StoreUint32(&a.flags, flags|accumulatorStarted)
		return false
	}
	lastTime := atomic.LoadInt64(&a.switched)
	if now.UnixNano() <= lastTime {
		return false
	}
	elapsed := now.Sub(time.Unix(0, lastTime))
	amount := atomic.SwapInt64(&a.current, 0)
	rate := float64(amount) / elapsed.Seconds()
	average := blendRate(math.Float64frombits(atomic.LoadUint64(&a.average)), rate, flags&accumulatorKnown != 0, elapsed, halfLife)
	atomic.StoreInt64(&a.last, amount)
	atomic.StoreInt64(&a.interval, int64(elapsed))
	atomic.StoreUint64(&a.average, math.Float64bits(average))
	atomic.StoreInt64(&a.switched, now.UnixNano())
	atomic.StoreUint32(&a.flags, flags|accumulatorKnown)
	return true
}

// Snapshot reads a coherent completed frame. Add does not need the Tick lock;
// Current is an atomic observation from within the snapshot's read interval.
func (a *Accumulator) Snapshot() Snapshot {
	for {
		sequence := a.sequence.Load()
		if sequence&1 != 0 {
			runtime.Gosched()
			continue
		}
		flags := atomic.LoadUint32(&a.flags)
		s := Snapshot{
			Average: math.Float64frombits(atomic.LoadUint64(&a.average)),
			Last:    atomic.LoadInt64(&a.last), Interval: time.Duration(atomic.LoadInt64(&a.interval)),
			Current: atomic.LoadInt64(&a.current), Known: flags&accumulatorKnown != 0,
			Saturated: atomic.LoadUint32(&a.saturated) != 0,
		}
		if flags&accumulatorStarted != 0 {
			s.Switched = time.Unix(0, atomic.LoadInt64(&a.switched))
		}
		if a.sequence.Load() == sequence {
			return s
		}
	}
}

func (s Snapshot) LastPerSecond() float64 {
	if !s.Known || s.Interval <= 0 {
		return 0
	}
	return float64(s.Last) / s.Interval.Seconds()
}
func (a *Accumulator) LastPerSecond() float64    { return a.Snapshot().LastPerSecond() }
func (a *Accumulator) AveragePerSecond() float64 { return a.Snapshot().Average }
