// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

// An empty bucket enters the same planning phase as a later rebuild. There is
// no payload to transfer and no current assignment to protect. Prefer the
// coordinator only as the initial seed; the normal objective can improve it.
// A quota supplies an expected-growth hint, never an initial reservation.
func (r *rangeManager) initialBucketLocations(d BucketDescription, membership client.Membership, coordinator client.ID) ([]client.Location, bool) {
	destinations, _ := r.destinations(d)
	members := membershipIDs(membership)
	var eligible []rangeDestination
	var resources []placementResource
	var options []placementOption
	var ids []client.ID
	local := false
	growth := uint64(4 << 20)
	for _, destination := range destinations {
		if !hasControlNode(members, destination.node) || destination.free == 0 {
			continue
		}
		local = local || destination.node == coordinator
		growth = min(growth, destination.capacity/10)
		options = append(options, placementOption{Range: 0, Volume: len(resources), Preferred: destination.node == coordinator})
		resources = append(resources, placementResource{Node: destination.node, Capacity: destination.capacity, Free: destination.free})
		ids = append(ids, destination.volume.ID)
		eligible = append(eligible, destination)
	}
	need := r.server.requiredReplicas(d.Policy, membership)
	if len(resources) == 0 || need < 1 || d.Policy.StorageStrategy == "ram" && !local {
		return nil, false
	}
	if !d.Policy.Limits.Unlimited && d.Policy.Limits.Absolute > 0 {
		growth = d.Policy.Limits.Absolute
	}
	input := inputForInitialPlacement(len(options))
	input.Work[0] = placementWorkload{Growth: float64(growth) / input.Horizon}
	if d.Policy.StorageStrategy == "ram" {
		input.Work[0].Growth = 0
	}
	input.BackgroundRead, input.BackgroundWrite = r.server.placementBackgroundVolumes(ids, time.Now())
	ctx := r.server.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	solution, _ := rebuildPlacement(ctx, []placementDemand{{Bucket: client.ID(newVolumeID(d.ID)), Copies: need}}, resources, options, nil, input, []client.ID{coordinator}, initialPlacementBudget, 1)
	if len(solution.Pending) != 0 || len(solution.Assignments[0]) != need {
		return nil, false
	}
	locations := make([]client.Location, need)
	for slot, id := range solution.Assignments[0] {
		destination := eligible[options[id].Volume]
		locations[slot] = client.Location{Node: destination.node, Volume: destination.volume.ID, Class: destination.volume.Class, Role: 1}
	}
	locations[0].Role = 0
	if need == 1 && locations[0].Node == coordinator {
		// Preserve the existing unrestricted local cohort and its striping. The
		// phase selected an owner; first object placement selects a concrete disk.
		locations[0].Volume = client.ID{}
		locations[0].Class = uint8(bucketClass(d.Policy))
	}
	return locations, true
}
