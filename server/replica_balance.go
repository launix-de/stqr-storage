// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Balance primary work only inside this PG, using existing committed copies.
// A role swap must reduce capacity-normalized work; an already balanced map
// remains untouched when a bucket is added. No payload copy is needed.
func (r *rangeManager) planPrimaryBalance(group string, descriptions []BucketDescription) (rangePlan, bool) {
	s := r.server
	descriptions = append([]BucketDescription(nil), descriptions...)
	sort.Slice(descriptions, func(i, j int) bool { return descriptions[i].ID < descriptions[j].ID })
	type rangeKey struct {
		bucket client.ID
		start  [32]byte
	}
	weights := make(map[rangeKey]uint64)
	var snapshots []client.StatsSnapshot
	if catalog := r.pgCatalog.Load(); catalog != nil {
		snapshots = catalog.snapshots
	} else {
		snapshots = []client.StatsSnapshot{s.CollectStats(client.StatsFilter{})}
		s.peerStatsMu.RLock()
		for _, peer := range s.peerStats {
			if peer.online && peer.snapshot != nil {
				snapshots = append(snapshots, *peer.snapshot)
			}
		}
		s.peerStatsMu.RUnlock()
	}
	for _, snapshot := range snapshots {
		for _, entry := range snapshot.Ranges {
			key := rangeKey{entry.Bucket, entry.Start}
			weights[key] = max(weights[key], entry.LogicalLiveBytes)
		}
	}
	weightOf := func(bucket client.ID, entry client.RangeEntry) float64 {
		return float64(max(uint64(1<<20), weights[rangeKey{bucket, entry.Start}]))
	}
	eligible := func(d BucketDescription) bool {
		return (group == "*" || d.Policy.PlacementGroup == group) && d.ID != systemBucket.String() && !d.DeletionComplete && d.State != BucketDeleting && d.Policy.StorageStrategy != "ram"
	}
	assigned := make(map[client.ID]float64)
	for _, d := range descriptions {
		if !eligible(d) {
			continue
		}
		bucket := client.ID(newVolumeID(d.ID))
		for _, entry := range s.placement.Load().bucketEntries(bucket) {
			if entry.State == 0 && len(entry.Locations) > 1 {
				assigned[entry.Locations[0].Node] += weightOf(bucket, entry)
			}
		}
	}
	for _, d := range descriptions {
		if !eligible(d) || d.DesiredPolicy != nil || s.controlBlocksPG(client.ID(newVolumeID(d.ID))) {
			continue
		}
		destinations, capacities := r.destinations(d)
		bucket := client.ID(newVolumeID(d.ID))
		for _, entry := range s.placement.Load().bucketEntries(bucket) {
			if entry.State != 0 || len(entry.Locations) < 2 || entry.Locations[0].Node != r.node || !r.replicaSetOnline(entry) || !s.replicaSetSatisfies(d.Policy, entry.Locations) || capacities[r.node] == 0 {
				continue
			}
			weight := weightOf(bucket, entry)
			best, bestGain := -1, float64(0)
			for i := 1; i < len(entry.Locations); i++ {
				node := entry.Locations[i].Node
				if capacities[node] == 0 {
					continue
				}
				gain := weight * ((2*assigned[r.node]-weight)/float64(capacities[r.node]) - (2*assigned[node]+weight)/float64(capacities[node]))
				if gain > bestGain || gain > 0 && gain == bestGain && node.String() < entry.Locations[best].Node.String() {
					best, bestGain = i, gain
				}
			}
			if best < 0 {
				continue
			}
			before, ok := s.routingPatch(volume.ID(bucket), volume.ObjectID(entry.Start))
			if !ok {
				continue
			}
			bad := make([]bool, len(entry.Locations))
			for i := range bad {
				bad[i] = i != best
			}
			if p, ok := r.planReplicaPromotion(d, before, entry, bad, destinations); ok {
				return p, true
			}
		}
	}
	return rangePlan{}, false
}
