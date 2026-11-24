// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Lazy placement is driven by remaining usable capacity, never by the fraction
// of the hash space a node owns. The distinct start/target watermarks avoid
// repeatedly shuffling small amounts of data to chase an exact distribution.
func lazyFreeWatermarks(capacity, free uint64, poolCapacity, poolFree uint64) (uint64, uint64) {
	if capacity == 0 || poolCapacity == 0 {
		return 0, 0
	}
	mean := float64(poolFree) / float64(poolCapacity)
	start := uint64(float64(capacity) * math.Min(.15, .5*mean))
	target := uint64(float64(capacity) * math.Min(.20, .75*mean))
	if free >= start {
		return 0, 0
	}
	return start, target
}

func (r *rangeManager) planLazyCapacity(d BucketDescription, before client.MapPatch, entry client.RangeEntry, destinations []rangeDestination) (rangePlan, bool) {
	var total, free uint64
	for _, v := range destinations {
		total = addCapacity(total, v.capacity)
		free = addCapacity(free, min(v.free, v.capacity))
	}
	// Every source volume must be individually healthy. An idle new node does not
	// justify draining a healthy old one; a hot pinned disk is not hidden by the
	// free space of other local disks.
	var hot volume.ID
	var need uint64
	var targetFraction float64
	for _, src := range r.sourceVolumes(rangePlan{Before: before, Moving: entry}) {
		id := src.Identity().VolumeID
		for _, candidate := range destinations {
			if candidate.node != r.node || candidate.volume.ID != client.ID(id) {
				continue
			}
			_, target := lazyFreeWatermarks(candidate.capacity, candidate.free, total, free)
			if target > candidate.free && target-candidate.free > need {
				hot, need = id, target-candidate.free
				targetFraction = float64(target) / float64(candidate.capacity)
			}
		}
	}
	if need == 0 {
		return rangePlan{}, false
	}
	costOf, closeCosts := r.rangeCosts(before)
	defer closeCosts()
	var best rangePlan
	known := false
	for _, destination := range destinations {
		if destination.node == r.node && destination.volume.ID == client.ID(hot) {
			continue
		}
		floor := uint64(float64(destination.capacity) * targetFraction)
		if destination.free <= floor {
			continue
		}
		budget := destination.free - floor
		moving := entry
		cost, _, err := costOf(moving, nil)
		if err != nil {
			continue
		}
		released, _, err := costOf(moving, &hot)
		if err != nil || released == 0 {
			continue
		}
		goal := min(need, released)
		for moving.PrefixBits < 256 {
			left, right, e := client.SplitRange(moving)
			if e != nil {
				break
			}
			type choice struct {
				entry         client.RangeEntry
				cost, release uint64
			}
			var next *choice
			for _, child := range []client.RangeEntry{left, right} {
				n, _, e := costOf(child, &hot)
				if e != nil || n < goal {
					continue
				}
				c, _, e := costOf(child, nil)
				if e != nil {
					continue
				}
				if next == nil || c < next.cost {
					next = &choice{child, c, n}
				}
			}
			if next == nil {
				break
			}
			moving, cost, released = next.entry, next.cost, next.release
		}
		if cost > budget || released == 0 {
			continue
		}
		plan, e := planRangeMove(before, moving, destination.node, volume.ID(destination.volume.ID), destination.volume.Class, bucketCodec(d.Policy), cost)
		if e == nil && (!known || plan.Physical < best.Physical) {
			best, known = plan, true
		}
	}
	return best, known
}
