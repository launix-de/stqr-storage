// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"sort"
)

// Every current primary advances only its own groups toward the already
// committed desired policy. Source/target proofs remain scoped; global metadata
// activation follows after every group reports that desired policy version.
func (r *rangeManager) planPolicyPlacement(description BucketDescription, drainingNode bool, draining map[volume.ID]bool) (rangePlan, bool) {
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
		if entry.PolicyVersion == desired.Version && !drainingNode && !volumeDrain {
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

// A local target already holding authoritative bytes does not need another
// payload allocation. Changed encodings budget a conservative logical extent
// size (not sparse holes), so gzip->raw cannot be admitted at its old gzip size.
func (r *rangeManager) policyPlacementCost(before client.MapPatch, moving client.RangeEntry, target client.ID, destination volume.ID, codec volume.Compression) (uint64, uint64, error) {
	p := rangePlan{Before: before, Moving: moving, Source: r.node, Target: target, Volume: destination}
	var cost, count uint64
	b, err := r.server.Bucket(volume.ID(before.Bucket))
	if err != nil {
		return 0, 0, err
	}
	for _, store := range r.sourceVolumes(p) {
		var after *volume.ObjectID
		for {
			h, err := store.Open(false)
			if err != nil {
				return 0, 0, err
			}
			records, err := h.ScanBucketObjects(volume.ID(before.Bucket), after, 128)
			h.Close()
			if err != nil {
				return 0, 0, err
			}
			if len(records) == 0 {
				break
			}
			for _, record := range records {
				if !p.contains(record.Key.Object) {
					continue
				}
				selected, _, _, err := b.selectVolume(b.Describe(), record.Key, volume.ID{}, false)
				if err != nil {
					return 0, 0, err
				}
				if selected != store {
					continue
				}
				h, err := store.Open(false)
				if err != nil {
					return 0, 0, err
				}
				allocated, err := h.AllocatedPhysical(record.Key)
				if err == nil && !record.Info.Deleted && record.Info.Compression != codec {
					extents, extentErr := h.Extents(record.Key)
					if extentErr != nil {
						err = extentErr
					} else {
						var written uint64
						for _, extent := range extents {
							written = addCapacity(written, extent.Length)
						}
						allocated = max(allocated, addCapacity(addCapacity(written, written/16), 256))
					}
				}
				h.Close()
				if err != nil {
					return 0, 0, err
				}
				cost = addCapacity(cost, addCapacity(allocated, 256))
				count++
			}
			next := records[len(records)-1].Key.Object
			after = &next
		}
		h, err := store.Open(false)
		if err != nil {
			return 0, 0, err
		}
		err = h.ScanRequests(func(record volume.RequestRecord) error {
			if record.Key.Bucket == volume.ID(before.Bucket) && p.contains(record.Key.Object) {
				cost = addCapacity(cost, 512)
			}
			return nil
		})
		h.Close()
		if err != nil {
			return 0, 0, err
		}
	}
	abortCost, err := r.replicaAbortCost(before, moving)
	if err != nil {
		return 0, 0, err
	}
	cost = addCapacity(cost, abortCost)
	if target == r.node {
		r.server.mu.RLock()
		managed := r.server.volumes[destination.String()]
		r.server.mu.RUnlock()
		if managed != nil {
			var after *volume.ObjectID
			for {
				h, err := managed.store.Open(false)
				if err != nil {
					return 0, 0, err
				}
				records, err := h.ScanBucketObjects(volume.ID(before.Bucket), after, 128)
				h.Close()
				if err != nil {
					return 0, 0, err
				}
				if len(records) == 0 {
					break
				}
				for _, record := range records {
					if !p.contains(record.Key.Object) || record.Info.Deleted || record.Info.Compression == codec {
						continue
					}
					selected, _, _, err := b.selectVolume(b.Describe(), record.Key, volume.ID{}, false)
					if err != nil {
						return 0, 0, err
					}
					if selected != managed.store {
						continue
					}
					h, err := managed.store.Open(false)
					if err != nil {
						return 0, 0, err
					}
					allocated, err := h.AllocatedPhysical(record.Key)
					var written uint64
					if err == nil {
						extents, e := h.Extents(record.Key)
						if e != nil {
							err = e
						} else {
							for _, extent := range extents {
								written = addCapacity(written, extent.Length)
							}
						}
					}
					h.Close()
					if err != nil {
						return 0, 0, err
					}
					upper := addCapacity(addCapacity(written, written/16), 256)
					if upper > allocated {
						cost = addCapacity(cost, upper-allocated)
					}
				}
				next := records[len(records)-1].Key.Object
				after = &next
			}
		}
	}
	if cost != 0 {
		cost = addCapacity(cost, 64<<10)
	}
	return cost, count, nil
}
