// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"math"
	"time"
)

func (r *rangeManager) placementReaderCosts(key rangeMetricKey, nodes []client.ID, horizon float64, now time.Time) []float64 {
	costs := make([]float64, len(nodes))
	if horizon <= 0 || math.IsNaN(horizon) || math.IsInf(horizon, 0) {
		return costs
	}
	demands := r.server.statistics.readerDemands(key, now, r.server.placementHalfLives())
	for _, d := range demands {
		group, known := r.server.statistics.readerProfile(d.key)
		if !known || now.Sub(group.at) > placementPathTTL {
			continue
		}
		worst := float64(0)
		for _, path := range group.paths {
			if now.Sub(path.at) <= placementPathTTL {
				worst = math.Max(worst, path.value)
			}
		}
		if worst == 0 {
			continue
		}
		for i, node := range nodes {
			distance := worst
			if path, ok := group.paths[node]; ok && now.Sub(path.at) <= placementPathTTL {
				distance = path.value
			}
			contribution := math.Min(math.MaxFloat64, math.Min(math.MaxFloat64, d.rate*horizon)*distance)
			if !math.IsNaN(contribution) {
				costs[i] = math.Min(math.MaxFloat64, costs[i]+contribution)
			}
		}
	}
	return costs
}

func (r *rangeManager) placementPeerDistances(now time.Time) map[[2]client.ID]float64 {
	return r.server.statistics.peerDistances(r.node, now)
}
