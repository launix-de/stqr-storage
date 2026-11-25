// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Place primary work across existing copies, without copying their payload.
// All planners walk the same stable range order. Capacity-weighted assignment
// rotates equal-sized ranges across equal-sized nodes; a larger node receives
// proportionally more primary work. An offline copy freezes its whole group.
func (r *rangeManager) planPrimaryBalance() (rangePlan, bool) {
	s := r.server
	descriptions := s.bucketSnapshots()
	sort.Slice(descriptions, func(i, j int) bool { return descriptions[i].ID < descriptions[j].ID })
	assigned := make(map[client.ID]float64)
	for _, description := range descriptions {
		if description.ID == systemBucket.String() || description.State == BucketDeleting || description.DesiredPolicy != nil || description.Policy.StorageStrategy == "ram" {
			continue
		}
		destinations, capacities := r.destinations(description)
		for _, entry := range s.placement.Load().bucketEntries(client.ID(newVolumeID(description.ID))) {
			if entry.State != 0 || len(entry.Locations) < 2 || !r.replicaSetOnline(entry) || !s.replicaSetSatisfies(description.Policy, entry.Locations) {
				continue
			}
			best := -1
			for i, loc := range entry.Locations {
				if capacities[loc.Node] == 0 {
					continue
				}
				score := (assigned[loc.Node] + 1) / float64(capacities[loc.Node])
				if best < 0 {
					best = i
					continue
				}
				other := entry.Locations[best].Node
				bestScore := (assigned[other] + 1) / float64(capacities[other])
				if score < bestScore || score == bestScore && loc.Node.String() < other.String() {
					best = i
				}

			}
			if best < 0 {
				continue
			}
			assigned[entry.Locations[best].Node]++
			if best == 0 || entry.Locations[0].Node != r.node {
				continue
			}
			before, ok := s.routingPatch(newVolumeID(description.ID), volume.ObjectID(entry.Start))
			if !ok {
				continue
			}
			bad := make([]bool, len(entry.Locations))
			for i := range bad {
				bad[i] = i != best
			}
			if p, ok := r.planReplicaPromotion(description, before, entry, bad, destinations); ok {
				return p, true
			}
		}
	}
	return rangePlan{}, false
}
