// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"time"
)

type rangePressure struct {
	bucket    volume.ID
	object    volume.ObjectID
	needed    uint64
	failed    volume.ID
	requested time.Time
}

// Volatile scheduling hints are bounded; neither an admission promise nor an
// ownership decision is made here. A retry recreates a dropped hint. All actual
// movement still uses the normal durable source/target handoff barriers.
func (r *rangeManager) requestSpace(bucket volume.ID, object volume.ObjectID, needed uint64, failed ...volume.ID) bool {
	b, err := r.server.Bucket(bucket)
	if err != nil {
		return false
	}
	d := b.Describe()
	if d.Policy.StorageStrategy == "ram" || b.blocksPlacementChange() {
		return false
	}
	hint := rangePressure{bucket: bucket, object: object, needed: needed, requested: time.Now()}
	if len(failed) > 0 {
		hint.failed = failed[0]
	}
	r.mu.Lock()
	for _, prior := range r.pressures {
		if prior.bucket == bucket && prior.object == object {
			hint.requested = prior.requested
			hint.needed = max(needed, prior.needed)
			break
		}
	}
	r.mu.Unlock()
	_, ready := r.pressurePlan(hint)
	if !ready && !r.awaitPressureStats(hint) {
		r.mu.Lock()
		for i, prior := range r.pressures {
			if prior.bucket == bucket && prior.object == object {
				r.pressures = append(r.pressures[:i], r.pressures[i+1:]...)
				break
			}
		}
		r.mu.Unlock()
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, prior := range r.pressures {
		if prior.bucket == bucket && prior.object == object {
			r.pressures[i] = hint
			r.wakeCapacity()
			return true
		}
	}
	if len(r.pressures) >= 128 {
		r.pressures = r.pressures[1:]
	}
	r.pressures = append(r.pressures, hint)
	r.wakeCapacity()
	return true
}

func (r *rangeManager) wakeCapacity() {
	select {
	case r.capacityWake <- struct{}{}:
	default:
	}
}

func (r *rangeManager) pressurePlan(hint rangePressure) (rangePlan, bool) {
	s := r.server
	before, ok := s.routingPatch(hint.bucket, hint.object)
	if !ok || len(before.Entries) != 1 || len(before.Entries[0].Locations) != 1 || before.Entries[0].Locations[0].Node != r.node {
		return rangePlan{}, false
	}
	b, err := s.Bucket(hint.bucket)
	if err != nil {
		return rangePlan{}, false
	}
	destinations, _ := r.destinations(b.Describe(), true)
	old := before.Entries[0]
	failed := hint.failed
	selected, _, found, selectErr := b.selectVolume(b.Describe(), volume.Key{Bucket: hint.bucket, Object: hint.object}, volume.ID{}, true)
	existing := selectErr == nil && found
	if failed == (volume.ID{}) && selected != nil {
		failed = selected.Identity().VolumeID
	}
	costOf, closeCosts := r.rangeCosts(before)
	defer closeCosts()
	for _, destination := range destinations {
		if destination.node == r.node && (old.Locations[0].Volume == destination.volume.ID || failed == volume.ID(destination.volume.ID)) {
			continue
		}
		if destination.free < hint.needed {
			continue
		}
		// Capacity pressure blocks the foreground writer. An otherwise huge
		// destination must not make that writer wait for a whole bucket copy.
		// Reclaim a useful bounded batch; one indivisible object may exceed
		// this soft bound, but must still fit the destination's hard capacity.
		budget := min(destination.free-hint.needed, max(hint.needed, uint64(4<<20)))
		moving := old
		cost, count, err := costOf(moving, nil)
		if err != nil {
			continue
		}
		// Keep updates to existing objects narrow. For new objects, move the
		// largest containing range within the batch budget: isolating empty leaves
		// never frees the full source and creates a handoff for every new ID.
		for moving.PrefixBits < 256 && count > 0 && !(existing && count == 1) && (existing || cost > budget) && (count > 1 || cost > destination.free-hint.needed) {
			left, right, e := client.SplitRange(moving)
			if e != nil {
				break
			}
			child := left
			if bytes.Compare(hint.object[:], right.Start[:]) >= 0 {
				child = right
			}
			childCost, childCount, e := costOf(child, nil)
			if e != nil {
				break
			}
			moving, cost, count = child, childCost, childCount
		}
		if cost <= destination.free-hint.needed {
			plan, e := planRangeMove(before, moving, destination.node, volume.ID(destination.volume.ID), destination.volume.Class, bucketCodec(b.Describe().Policy), cost)
			if e == nil {
				return plan, true
			}
		}
	}
	return rangePlan{}, false
}

// A stale low-free snapshot must not manufacture a hard 507 immediately
// after cleanup freed the peer. Wait for a fresh capacity observation, not a
// failure timeout; if all eligible fresh views still cannot fit it, reject.
func (r *rangeManager) awaitPressureStats(hint rangePressure) bool {
	before, known := r.server.routingPatch(hint.bucket, hint.object)
	if !known || len(before.Entries[0].Locations) != 1 {
		return false
	}
	b, err := r.server.Bucket(hint.bucket)
	if err != nil {
		return false
	}
	destinations, _ := r.destinations(b.Describe(), true)
	old := before.Entries[0]
	for _, destination := range destinations {
		if destination.node == r.node && (old.Locations[0].Volume == destination.volume.ID || hint.failed == volume.ID(destination.volume.ID)) {
			continue
		}
		if destination.capacity >= addCapacity(hint.needed, 64<<10) && destination.observed.Before(hint.requested) {
			return true
		}
	}
	return false
}
func (r *rangeManager) nextPressure() (rangePlan, bool) { return r.nextPressureGroup("*") }
func (r *rangeManager) nextPressureGroup(group string) (rangePlan, bool) {
	r.mu.Lock()
	hints := append([]rangePressure(nil), r.pressures...)
	r.mu.Unlock()
	for _, hint := range hints {
		if group != "*" {
			b, err := r.server.Bucket(hint.bucket)
			if err != nil || b.Describe().Policy.PlacementGroup != group {
				continue
			}
		}
		plan, ok := r.pressurePlan(hint)
		if !ok {
			continue
		}
		r.mu.Lock()
		for i, prior := range r.pressures {
			if prior.bucket == hint.bucket && prior.object == hint.object {
				r.pressures = append(r.pressures[:i], r.pressures[i+1:]...)
				break
			}
		}
		r.mu.Unlock()
		return plan, true
	}
	return rangePlan{}, false
}
