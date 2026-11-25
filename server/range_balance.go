// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"time"

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
	// The absolute floor protects tiny data volumes; cap it at 10% for
	// deliberately smaller test/embedded volumes and at half the pool mean.
	start := min(max(capacity/50, uint64(3<<20)), capacity/10, uint64(float64(capacity)*.5*mean))
	target := uint64(float64(capacity) * math.Min(.50, .9*mean))
	if free >= start {
		return 0, 0
	}
	return start, target
}

// Scheduling hints only: restarts may forget the forecast, never ownership.
// Once below the start watermark, continue until the goal is reached, even
// after crossing the start watermark again. Ordinary writes do not start it.
type capacityGoal struct {
	sampled   time.Time
	allocated uint64
	growth    float64 // smoothed net allocation growth in bytes/second
	target    uint64
}

func (g *capacityGoal) observe(v rangeDestination, poolCapacity, poolFree uint64, now time.Time, horizon ...time.Duration) uint64 {
	if !g.sampled.IsZero() && now.Sub(g.sampled) >= time.Second {
		seconds := now.Sub(g.sampled).Seconds()
		growth := float64(0)
		if v.volume.AllocatedPhysical > g.allocated {
			growth = float64(v.volume.AllocatedPhysical-g.allocated) / seconds
		}
		alpha := 1 - math.Exp(-seconds/10)
		g.growth += alpha * (growth - g.growth)
		g.sampled, g.allocated = now, v.volume.AllocatedPhysical
	} else if g.sampled.IsZero() {
		g.sampled, g.allocated = now, v.volume.AllocatedPhysical
	}
	_, target := lazyFreeWatermarks(v.capacity, v.free, poolCapacity, poolFree)
	if g.target == 0 && target == 0 {
		return 0
	}
	if g.target != 0 && v.free >= g.target {
		g.target = 0
		if target == 0 {
			return 0
		}
	}
	if poolCapacity == 0 || v.capacity == 0 {
		return 0
	}
	// Forecast a 30-second burst, but never reserve most of an otherwise tight
	// pool. Existing guarantees/in-flight reservations are excluded from free.
	ceiling := uint64(float64(v.capacity) * math.Min(.75, .9*float64(poolFree)/float64(poolCapacity)))
	seconds := 30.0
	if len(horizon) > 0 {
		seconds = max(seconds, horizon[0].Seconds())
	}
	forecast := uint64(math.Min(float64(ceiling), g.growth*seconds))
	g.target = min(ceiling, max(g.target, target, forecast))
	if g.target <= v.free {
		g.target = 0
		return 0
	}
	return g.target - v.free
}

func (r *rangeManager) capacityNeed(v rangeDestination, destinations []rangeDestination) uint64 {
	var total, free uint64
	for _, d := range destinations {
		if d.volume.Class == v.volume.Class {
			total = addCapacity(total, d.capacity)
			free = addCapacity(free, min(d.free, d.capacity))
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capacityGoals == nil {
		r.capacityGoals = make(map[volume.ID]*capacityGoal)
	}
	id := volume.ID(v.volume.ID)
	g := r.capacityGoals[id]
	if g == nil {
		g = &capacityGoal{}
		r.capacityGoals[id] = g
	}
	return g.observe(v, total, free, time.Now(), 4*r.capacityMoveTime)
}

// Evaluate a whole candidate pool once, rather than summing the same pool for
// every range/replica. The resulting thresholds are advisory placement input.
func (r *rangeManager) capacityNeeds(destinations []rangeDestination) map[client.ID]uint64 {
	type totals struct{ capacity, free uint64 }
	pools := make(map[uint8]totals)
	for _, d := range destinations {
		p := pools[d.volume.Class]
		p.capacity = addCapacity(p.capacity, d.capacity)
		p.free = addCapacity(p.free, min(d.free, d.capacity))
		pools[d.volume.Class] = p
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capacityGoals == nil {
		r.capacityGoals = make(map[volume.ID]*capacityGoal)
	}
	needs := make(map[client.ID]uint64, len(destinations))
	now := time.Now()
	for _, d := range destinations {
		id := volume.ID(d.volume.ID)
		g := r.capacityGoals[id]
		if g == nil {
			g = &capacityGoal{}
			r.capacityGoals[id] = g
		}
		pool := pools[d.volume.Class]
		needs[d.volume.ID] = g.observe(d, pool.capacity, pool.free, now, 4*r.capacityMoveTime)
	}
	return needs
}

type capacityMove struct {
	plan   rangePlan
	relief uint64 // progress toward the actual reserve, not merely copied bytes
}

func (a capacityMove) betterThan(b capacityMove) bool {
	return a.relief > b.relief || a.relief == b.relief && a.plan.Physical < b.plan.Physical
}

func (r *rangeManager) planLazyCapacity(d BucketDescription, before client.MapPatch, entry client.RangeEntry, destinations []rangeDestination) (capacityMove, bool) {
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
			deficit := r.capacityNeed(candidate, destinations)
			if deficit > need {
				hot, need = id, deficit
				targetFraction = float64(addCapacity(candidate.free, deficit)) / float64(candidate.capacity)
			}
		}
	}
	if need == 0 {
		return capacityMove{}, false
	}
	costOf, closeCosts := r.rangeCosts(before)
	defer closeCosts()
	var best capacityMove
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
		candidate := capacityMove{plan: plan, relief: min(need, released)}
		if e == nil && (!known || candidate.betterThan(best)) {
			best, known = candidate, true
		}
	}
	return best, known
}
