// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

// rebuildPlacement is the common planning phase for an empty placement and an
// existing one. Callers supply requirements, eligible resources and measured
// work, never a second ranking formula. An initial placement has no held bytes
// or migration cost. The phase does no I/O and grants no reservation/authority.
func rebuildPlacement(ctx context.Context, demands []placementDemand, resources []placementResource, options []placementOption, budgets map[client.ID]uint64, work placementImproveInput, primaries []client.ID, budget time.Duration, seed uint64) (placementSolution, placementImproveResult) {
	if ctx.Err() != nil {
		solution := placementSolution{Assignments: make([][]int, len(demands)), Pending: make([]int, len(demands))}
		for i := range demands {
			solution.Pending[i] = i
		}
		return solution, placementImproveResult{Stopped: true}
	}
	solution := solvePlacement(demands, resources, options, budgets)
	for i, ids := range solution.Assignments {
		if i >= len(primaries) {
			break
		}
		for slot, id := range ids {
			if resources[options[id].Volume].Node == primaries[i] {
				ids[0], ids[slot] = ids[slot], ids[0]
				break
			}
		}
	}
	result := improvePlacement(ctx, demands, resources, options, budgets, &solution, work, budget, seed)
	return solution, result
}

const initialPlacementBudget = time.Millisecond

// inputForInitialPlacement gives new work the same marginal resource costs as
// a rebuild candidate. A unit demand is a placement hint, not an estimate of
// compressed size or a promise that the following mutation will fit. Actual
// quota, backing and physical admission are checked by the unchanged writer.
func inputForInitialPlacement(count int) placementImproveInput {
	return placementImproveInput{
		Work: []placementWorkload{{Physical: 1, Write: 1.0 / 30}}, Horizon: 30,
		Held: make([]uint64, count), Network: make([]uint64, count),
	}
}
