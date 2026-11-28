// SPDX-License-Identifier: GPL-3.0-only
package decay

import (
	"math"
	"time"
)

// Both event-rate means and weighted observation means use this kernel.
// Expm1 preserves short-interval accuracy without losing tiny retained weights.
func decayWeights(elapsed, halfLife time.Duration) (retained, incoming float64) {
	exponent := -math.Ln2 * elapsed.Seconds() / halfLife.Seconds()
	return math.Exp(exponent), -math.Expm1(exponent)
}

func blendRate(previous, sample float64, known bool, elapsed, halfLife time.Duration) float64 {
	if !known {
		return sample
	}
	if halfLife <= 0 {
		halfLife = DefaultLongHalfLife
	}
	_, incoming := decayWeights(elapsed, halfLife)
	return previous + incoming*(sample-previous)
}
