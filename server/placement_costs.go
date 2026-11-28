// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"

	"github.com/launix-de/stqr-storage/client"
)

func validTransferValue(v float64) bool { return v >= 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }

// transferCost is shared by all network paths. There is no discount for client
// traffic, replication or migration: the cost is bytes multiplied by distance.
// An invalid observation never turns a nonempty transfer into a free path.
func transferCost(bytes, distance float64) float64 {
	if !validTransferValue(bytes) || !validTransferValue(distance) {
		return math.MaxFloat64
	}
	if bytes == 0 || distance == 0 {
		return 0
	}
	if distance > math.MaxFloat64/bytes {
		return math.MaxFloat64
	}
	return bytes * distance
}

// Compute once per immutable measured-path image, not once per trial or edge.
// Unknown paths cost at least as much as any observed path; a missing sample
// cannot create an apparent locality improvement.
func placementDistanceFallback(distances map[[2]client.ID]float64) float64 {
	fallback := 1.0
	for _, distance := range distances {
		if validTransferValue(distance) {
			fallback = math.Max(fallback, distance)
		}
	}
	return fallback
}

func placementPeerDistance(distances map[[2]client.ID]float64, from, to client.ID, unknown float64) float64 {
	if from == to {
		return 0
	}
	if distance, ok := distances[[2]client.ID{from, to}]; ok && validTransferValue(distance) {
		return distance
	}
	if !validTransferValue(unknown) {
		return 1
	}
	return math.Max(1, unknown)
}
