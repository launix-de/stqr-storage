// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"math"
	"sort"
)

// Every current primary advances only its own groups toward the already
// committed desired policy. Source/target proofs remain scoped; global metadata
// activation follows after every group reports that desired policy version.
func (r *rangeManager) planPolicyPlacement(description BucketDescription, drainingNode bool, draining map[volume.ID]bool, requirePin ...bool) (rangePlan, bool) {
	desired := description.DesiredPolicy
	if desired == nil || (description.Policy.StorageStrategy == "ram") != (desired.StorageStrategy == "ram") {
		return rangePlan{}, false
	}
	bucket, err := bucketID(description.ID)
	if err != nil {
		return rangePlan{}, false
	}
	goal := description
	goal.Policy = *desired
	destinations, _ := r.destinations(goal)
	for _, entry := range r.server.placement.Load().bucketEntries(client.ID(bucket)) {
		if len(entry.Locations) != 1 || entry.Locations[0].Node != r.node {
			continue
		}
		if entry.Runtime != (client.ID{}) && !r.ramAvailable(bucket, volume.ObjectID(entry.Start), entry) {
			continue
		}
		before, known := r.server.routingPatch(bucket, volume.ObjectID(entry.Start))
		if !known {
			continue
		}
		volumeDrain := r.rangeOnDraining(before, draining)
		if entry.PolicyVersion == desired.Version && !drainingNode && !volumeDrain && (len(requirePin) == 0 || !requirePin[0] || entry.Locations[0].Volume != (client.ID{})) {
			continue
		}
		type candidate struct {
			destination rangeDestination
			cost, count uint64
		}
		var candidates []candidate
		var remoteCost, remoteCount uint64
		var remoteErr error
		remoteKnown := false
		for _, destination := range destinations {
			if drainingNode && destination.node == r.node || destination.node == r.node && draining[volume.ID(destination.volume.ID)] {
				continue
			}
			var cost, count uint64
			var err error
			if destination.node != r.node && remoteKnown {
				cost, count, err = remoteCost, remoteCount, remoteErr
			} else {
				cost, count, err = r.policyPlacementCost(before, entry, destination.node, volume.ID(destination.volume.ID), bucketCodec(*desired))
				if destination.node != r.node {
					remoteCost, remoteCount, remoteErr, remoteKnown = cost, count, err, true
				}
			}
			if err == nil {
				candidates = append(candidates, candidate{destination, cost, count})
			}
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].cost != candidates[j].cost {
				return candidates[i].cost < candidates[j].cost
			}
			if (candidates[i].destination.node == r.node) != (candidates[j].destination.node == r.node) {
				return candidates[i].destination.node == r.node
			}
			return candidates[i].destination.free > candidates[j].destination.free
		})
		for _, candidate := range candidates {
			destination := candidate.destination
			if drainingNode && destination.node == r.node {
				continue
			}
			if destination.node == r.node && draining[volume.ID(destination.volume.ID)] {
				continue
			}
			moving := entry
			cost, count := candidate.cost, candidate.count
			for {
				var err error
				if moving.Start != entry.Start || moving.PrefixBits != entry.PrefixBits {
					cost, count, err = r.policyPlacementCost(before, moving, destination.node, volume.ID(destination.volume.ID), bucketCodec(*desired))
				}
				if err != nil {
					break
				}
				if cost <= destination.free {
					plan, err := planRangeMove(before, moving, destination.node, volume.ID(destination.volume.ID), destination.volume.Class, bucketCodec(*desired), cost)
					if err != nil {
						break
					}
					for i := range plan.After.Entries {
						if plan.After.Entries[i].Start == moving.Start && plan.After.Entries[i].PrefixBits == moving.PrefixBits {
							plan.After.Entries[i].PolicyVersion = desired.Version
						}
					}
					if plan.validate() == nil {
						return plan, true
					}
					break
				}
				if count <= 1 {
					break
				}
				left, right, err := client.SplitRange(moving)
				if err != nil {
					break
				}
				lc, _, le := r.policyPlacementCost(before, left, destination.node, volume.ID(destination.volume.ID), bucketCodec(*desired))
				rc, _, re := r.policyPlacementCost(before, right, destination.node, volume.ID(destination.volume.ID), bucketCodec(*desired))
				if le != nil || re != nil {
					break
				}
				moving = left
				if rc < lc {
					moving = right
				}
			}
		}
	}
	return rangePlan{}, false
}

// placementCostTotals is advisory RAM accounting, never a PREPARE proof.
// Physical already includes retry receipts, tombstones and shared index bytes.
// Splits without observed child totals are apportioned by owned hash width.
// No placement retry may reopen an object/request/replica index to refine this
// estimate. Only the selected transfer reads records as it actually copies them;
// destination admission remains authoritative and rejects insufficient storage.
func (r *rangeManager) placementCostTotals(before client.MapPatch, moving client.RangeEntry, retained ...map[volume.ID]uint64) (physical, logical, count uint64, err error) {
	b, e := r.server.Bucket(volume.ID(before.Bucket))
	if e != nil {
		return 0, 0, 0, e
	}
	image := r.server.placement.Load()
	usage := r.server.quotaUsage.Load()
	if image == nil || usage == nil {
		return 0, 0, 0, fail(503, "placement accounting is unavailable")
	}
	weights := r.placementWeights(image, map[client.ID]BucketDescription{before.Bucket: b.storageDescription()})
	// Account every authoritative source, including a retained destination.
	// Excluding that destination here would erase in-place codec growth and
	// make a wholly unavailable source indistinguishable from an empty range.
	p := rangePlan{Before: before, Moving: moving, Source: r.node}
	stores := r.sourceVolumes(p)
	if len(stores) == 0 {
		return 0, 0, 0, fail(503, "source storage accounting is unavailable")
	}
	for _, store := range stores {
		id := store.Identity().VolumeID
		rows, known := usage.volumes[id]
		if !known {
			return 0, 0, 0, fail(503, "volume accounting is unavailable")
		}
		weight := weights[placementWeightKey{before.Bucket, id}]
		if weight <= 0 {
			return 0, 0, 0, fail(503, "range accounting is unavailable")
		}
		share := math.Min(1, math.Ldexp(1, -int(moving.PrefixBits))/weight)
		// A complete known volume snapshot may omit an empty bucket.
		if len(retained) > 0 {
			retained[0][id] = 0
		}
		for _, row := range rows {
			if row.Bucket != volume.ID(before.Bucket) {
				continue
			}
			n := placementCostShare(row.Physical, share)
			physical = addCapacity(physical, n)
			logical = addCapacity(logical, placementCostShare(row.Logical, share))
			count = addCapacity(count, placementCostShare(row.Objects, share))
			if len(retained) > 0 {
				retained[0][id] = n
			}
			break
		}
	}
	return
}
func placementCostShare(total uint64, share float64) uint64 {
	if share >= 1 || total == 0 {
		return total
	}
	estimate := math.Ceil(float64(total) * share)
	if estimate >= float64(total) {
		return total
	}
	return uint64(estimate)
}

func (r *rangeManager) policyPlacementCost(before client.MapPatch, moving client.RangeEntry, target client.ID, destination volume.ID, codec volume.Compression, retained ...map[volume.ID]uint64) (uint64, uint64, error) {
	byVolume := make(map[volume.ID]uint64)
	physical, logical, count, err := r.placementCostTotals(before, moving, byVolume)
	if err != nil {
		return 0, 0, err
	}
	b, err := r.server.Bucket(volume.ID(before.Bucket))
	if err != nil {
		return 0, 0, err
	}
	codecChange := bucketCodec(b.storageDescription().Policy) != codec
	if codecChange {
		// A codec change may expand compressed data. Logical bytes provide a
		// conservative planning bound; sparse ranges may therefore be overestimated.
		physical = max(physical, addCapacity(addCapacity(logical, logical/16), saturatingProduct(count, 256)))
	}
	if len(retained) > 0 {
		for id, n := range byVolume {
			retained[0][id] = n
		}
	}
	// Reuse an existing local copy, but subtract its existing allocation only
	// after computing codec expansion. Retained bytes are not copied again;
	// extra encoding headroom still belongs in the candidate budget.
	if target == r.node && !(codecChange && physical == math.MaxUint64) {
		var copied uint64
		for id, n := range byVolume {
			if id != destination {
				copied = addCapacity(copied, n)
			}
		}
		// A saturated sum minus one retained component must not erase other
		// known copied components. Saturated codec growth stays unknown/max.
		physical = max(copied, subtractCapacity(physical, byVolume[destination]))
	}
	// Budget target index/record growth without enumerating records. The source
	// aggregate already accounts for retry/abort history, so it is not scanned
	// again or charged once per object by a second pass.
	if physical > 0 {
		physical = addCapacity(addCapacity(physical, saturatingProduct(count, 256)), 64<<10)
	}
	return physical, count, nil
}
func saturatingProduct(a, b uint64) uint64 {
	if b != 0 && a > math.MaxUint64/b {
		return math.MaxUint64
	}
	return a * b
}
