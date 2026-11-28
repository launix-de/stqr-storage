// SPDX-License-Identifier: GPL-3.0-only
package decay

import (
	"math"
	"testing"
	"time"
)

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
