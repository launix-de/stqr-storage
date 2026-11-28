// SPDX-License-Identifier: GPL-3.0-only
package decay

import (
	"math"
	"testing"
	"time"
)

func TestSeriesOneFrameForAllConsumers(t *testing.T) {
	start := time.Unix(100, 0)
	h := HalfLives{time.Second, 10 * time.Second}
	var meter Series
	meter.Tick(start, h)
	meter.Add(100)
	meter.Tick(start.Add(time.Second), h)
	if s := meter.Snapshot(); s.Last != 100 || s.Average != 100 {
		t.Fatal(s)
	}
	before := meter.Snapshot()
	forecast := meter.Forecast()
	for i := 0; i < 100; i++ {
		if meter.Snapshot() != before || meter.Forecast() != forecast {
			t.Fatal("read changed measurement")
		}
	}
	meter.Tick(start.Add(2*time.Second), h)
	closeValue(t, meter.Forecast().Short, 50)
	closeValue(t, meter.Forecast().Long, 100*math.Exp2(-.1))
	closeValue(t, meter.Snapshot().Average, 100*math.Exp2(-1.0/DefaultLongHalfLife.Seconds()))
}
func TestSeriesCadenceAndExactAmounts(t *testing.T) {
	now := time.Unix(100, 0)
	h := (HalfLives{}).Normalized()
	var one, many Series
	one.Tick(now, h)
	many.Tick(now, h)
	one.Add(6000)
	one.Tick(now.Add(time.Minute), h)
	for i := 1; i <= 60; i++ {
		many.Add(100)
		many.Tick(now.Add(time.Duration(i)*time.Second), h)
	}
	closeValue(t, one.Forecast().Short, many.Forecast().Short)
	closeValue(t, one.Forecast().Long, many.Forecast().Long)
	var precise Series
	precise.Tick(now, h)
	precise.Add(1<<53 + 7)
	precise.Tick(now.Add(time.Second), h)
	if precise.Snapshot().Last != 1<<53+7 {
		t.Fatal("integer precision lost")
	}
	if precise.Tick(now, h) || precise.Tick(now.Add(time.Second), h) {
		t.Fatal("old frame completed again")
	}
}
func TestSeriesConfigurationDoesNotReinterpretHistory(t *testing.T) {
	start := time.Unix(100, 0)
	var meter Series
	meter.Tick(start, HalfLives{})
	meter.Add(100)
	meter.Tick(start.Add(time.Second), HalfLives{})
	before := meter.Snapshot()
	changed := HalfLives{2 * time.Second, 20 * time.Second}
	meter.Tick(start.Add(time.Second), changed)
	if meter.Forecast() != (Values{}) || meter.Snapshot() != before {
		t.Fatal("configuration reinterpreted history or erased weekly display mean")
	}
	meter.Add(100)
	meter.Tick(start.Add(2*time.Second), changed)
	if meter.Forecast() != (Values{100, 100}) {
		t.Fatal("new frame did not seed new horizons", meter.Forecast())
	}
}
