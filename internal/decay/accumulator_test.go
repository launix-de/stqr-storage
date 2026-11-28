// SPDX-License-Identifier: GPL-3.0-only
package decay

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestAccumulatorEmbeddedAlignment(t *testing.T) {
	type embedded struct {
		prefix  byte
		counter Accumulator
	}
	if offset := unsafe.Offsetof(embedded{}.counter); offset%8 != 0 {
		t.Fatalf("embedded 64-bit atomics are not aligned: offset %d", offset)
	}
	var counters [2]embedded
	start := time.Unix(100, 0)
	for i := range counters {
		c := &counters[i].counter
		c.Tick(start, time.Second)
		c.Add(17)
		c.Tick(start.Add(time.Second), time.Second)
		if c.Snapshot().Last != 17 {
			t.Fatal("embedded accumulator lost events")
		}
	}
}

func closeValue(t *testing.T, got, want float64) {
	t.Helper()
	if math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-want) > 1e-10*math.Max(1, math.Abs(want)) {
		t.Fatalf("got %.15g, want %.15g", got, want)
	}
}

func TestAccumulatorExactCompletedFramesAndScaling(t *testing.T) {
	now := time.Unix(100, 0)
	var a Accumulator
	if a.Snapshot().Known || a.LastPerSecond() != 0 || a.AveragePerSecond() != 0 {
		t.Fatal("zero value invented a measurement")
	}
	// Preserve integer precision above the largest consecutive float64 integer.
	const first = int64(1<<53 + 3)
	a.Add(first)
	if a.Tick(now, time.Second) || a.Snapshot().Current != first {
		t.Fatal("first tick consumed counts or completed an unknown interval")
	}
	a.Add(17)
	if !a.Tick(now.Add(2*time.Second), time.Second) {
		t.Fatal("interval did not complete")
	}
	s := a.Snapshot()
	if !s.Known || s.Last != first+17 || s.Current != 0 || s.Interval != 2*time.Second || !s.Switched.Equal(now.Add(2*time.Second)) {
		t.Fatalf("bad completed frame: %+v", s)
	}
	closeValue(t, a.LastPerSecond(), float64(first+17)/2)
	closeValue(t, a.AveragePerSecond(), float64(first+17)/2)
	a.Add(42)
	if s = a.Snapshot(); s.Current != 42 || s.Last != first+17 {
		t.Fatalf("current additions changed the completed interval: %+v", s)
	}
}

func TestAccumulatorHalfLifeIdleAndChangingIntervals(t *testing.T) {
	now := time.Unix(100, 0)
	var a Accumulator
	a.Tick(now, time.Second)
	a.Add(300)
	a.Tick(now.Add(3*time.Second), time.Second)
	closeValue(t, a.AveragePerSecond(), 100)
	a.Tick(now.Add(4*time.Second), time.Second)
	closeValue(t, a.LastPerSecond(), 0)
	closeValue(t, a.AveragePerSecond(), 50)
	a.Add(200)
	a.Tick(now.Add(6*time.Second), time.Second)
	closeValue(t, a.LastPerSecond(), 100)
	closeValue(t, a.AveragePerSecond(), 87.5)
	before := a.Snapshot()
	a.Add(7)
	if a.Tick(now.Add(6*time.Second), time.Second) || a.Tick(now.Add(time.Second), time.Second) {
		t.Fatal("duplicate or backwards tick completed an interval")
	}
	after := a.Snapshot()
	if after.Last != before.Last || after.Average != before.Average || after.Interval != before.Interval || !after.Switched.Equal(before.Switched) || after.Current != 7 {
		t.Fatalf("backwards tick changed state: before=%+v after=%+v", before, after)
	}
	// The default half-life is exactly one week, independently of the unit.
	var weekly Accumulator
	weekly.Tick(now, 0)
	weekly.Add(10)
	weekly.Tick(now.Add(time.Second), 0)
	weekly.Tick(now.Add(time.Second+DefaultLongHalfLife), 0)
	closeValue(t, weekly.AveragePerSecond(), 5)
}

func TestAccumulatorSignedAmountsAndObservableSaturation(t *testing.T) {
	now := time.Unix(100, 0)
	var a Accumulator
	a.Tick(now, time.Second)
	a.Add(-4)
	a.Tick(now.Add(2*time.Second), time.Second)
	closeValue(t, a.LastPerSecond(), -2)
	closeValue(t, a.AveragePerSecond(), -2)
	if !a.Add(math.MaxInt64) || a.Add(1) || !a.Snapshot().Saturated || a.Snapshot().Current != math.MaxInt64 {
		t.Fatal("positive overflow wrapped or was not observable")
	}
	a.Tick(now.Add(3*time.Second), time.Second)
	if !a.Add(math.MinInt64) || a.Add(-1) || a.Snapshot().Current != math.MinInt64 {
		t.Fatal("negative overflow wrapped or was not observable")
	}
	if a.Tick(time.Time{}, time.Second) || a.Snapshot().Current != math.MinInt64 {
		t.Fatal("zero timestamp consumed pending counts")
	}
}

func TestAccumulatorConcurrentAddTickAndSnapshotConserveCounts(t *testing.T) {
	const workers, adds, amount = 8, 20000, int64(3)
	now := time.Unix(100, 0)
	var a Accumulator
	a.Tick(now, time.Second)
	var writers sync.WaitGroup
	writers.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer writers.Done()
			for j := 0; j < adds; j++ {
				if !a.Add(amount) {
					t.Error("unexpected saturation")
				}
			}
		}()
	}
	var finished atomic.Bool
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for !finished.Load() {
			s := a.Snapshot()
			if s.Known && (s.Interval != time.Microsecond || s.Last < 0 || s.Average < 0) {
				t.Errorf("incoherent completed frame: %+v", s)
				return
			}
			a.LastPerSecond()
			a.AveragePerSecond()
		}
	}()
	done := make(chan struct{})
	go func() { writers.Wait(); close(done) }()
	var total int64
	for frame := 1; ; frame++ {
		next := now.Add(time.Duration(frame) * time.Microsecond)
		a.Tick(next, time.Second)
		total += a.Snapshot().Last
		select {
		case <-done:
			a.Tick(next.Add(time.Microsecond), time.Second)
			total += a.Snapshot().Last
			finished.Store(true)
			<-readDone
			if total != workers*adds*amount || a.Snapshot().Current != 0 {
				t.Fatalf("concurrent interval exchange lost/duplicated events: %d, want %d", total, workers*adds*amount)
			}
			return
		default:
		}
	}
}

func TestAccumulatorConcurrentTicksCloseOnce(t *testing.T) {
	now := time.Unix(100, 0)
	var a Accumulator
	a.Tick(now, time.Second)
	a.Add(123)
	var completed atomic.Int64
	var ticks sync.WaitGroup
	for i := 0; i < 16; i++ {
		ticks.Add(1)
		go func() {
			defer ticks.Done()
			if a.Tick(now.Add(time.Second), time.Second) {
				completed.Add(1)
			}
		}()
	}
	ticks.Wait()
	if completed.Load() != 1 || a.Snapshot().Last != 123 || a.Snapshot().Current != 0 {
		t.Fatal("concurrent ticks closed the same interval more than once")
	}
}
