// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"math"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type rangeDestination struct {
	node           client.ID
	volume         client.VolumeStats
	capacity, free uint64
	observed       time.Time
}

func (r *rangeManager) start() {
	s := r.server
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		ticker := time.NewTicker(750 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}
			if s.joinPending() || s.shortStopping.Load() || s.clusterPaused() || s.forceFrozen() {
				continue
			}
			_ = r.collectMemos()
			_ = r.collectAbortedImports()
			_ = r.finishEmptyDrains()
			r.work.Lock()
			var pending *rangePlan
			r.mu.Lock()
			for _, id := range r.order {
				t := r.txns[id]
				if t.plan.Source == r.node && r.active(t) {
					copy := t.plan
					pending = &copy
					break
				}
			}
			r.mu.Unlock()
			var stepErr error
			if pending != nil {
				stepErr = r.move(s.ctx, *pending)
			} else if plan, ok := r.nextPressure(); ok {
				stepErr = r.move(s.ctx, plan)
			} else if plan, ok := r.planNext(); ok {
				stepErr = r.move(s.ctx, plan)
			} else {
				stepErr = r.finishNodeDrain(s.ctx)
			}
			r.mu.Lock()
			r.lastError = stepErr
			r.mu.Unlock()
			r.work.Unlock()
		}
	}()
}

func (r *rangeManager) destinations(d BucketDescription) ([]rangeDestination, map[client.ID]uint64) {
	s := r.server
	local := s.CollectStats(client.StatsFilter{})
	snapshots := []client.StatsSnapshot{local}
	seen := map[client.ID]time.Time{local.Node: time.Now()}
	s.peerStatsMu.RLock()
	for _, observation := range s.peerStats {
		if observation.online && observation.snapshot != nil && observation.connection.Capabilities&client.CapabilityRangeHandoff != 0 {
			snapshots = append(snapshots, *observation.snapshot)
			seen[observation.snapshot.Node] = observation.lastSeen
		}
	}
	s.peerStatsMu.RUnlock()
	view := s.currentSystem.Load()
	if view == nil {
		return nil, nil
	}
	members := membershipIDs(view.manifest.Membership)
	groups := s.placementGroups.Load()
	var out []rangeDestination
	capacity := make(map[client.ID]uint64)
	for _, snapshot := range snapshots {
		if !hasControlNode(members, snapshot.Node) || snapshot.NodeState != 0 {
			continue
		}
		for _, v := range snapshot.Volumes {
			if v.State != 1 || !bucketClass(d.Policy).Allows(volume.Class(v.Class)) || !groups.allowsVersion(d.Policy.PlacementGroup, d.Policy.PlacementMaskVersion, snapshot.Node.String(), v.ID.String()) {
				continue
			}
			if d.Policy.PlacementGroup == "" && groups.allows(systemPlacementGroup.String(), snapshot.Node.String(), v.ID.String()) {
				continue
			}
			capacity[snapshot.Node] = addCapacity(capacity[snapshot.Node], v.CapacityPhysical)
			out = append(out, rangeDestination{node: snapshot.Node, volume: v, capacity: v.CapacityPhysical, free: v.AdmissionFreePhysical, observed: seen[snapshot.Node]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.volume.Class != b.volume.Class {
			return a.volume.Class < b.volume.Class
		}
		if a.free != b.free {
			return a.free > b.free
		}
		if a.node != b.node {
			return bytes.Compare(a.node[:], b.node[:]) < 0
		}
		return bytes.Compare(a.volume.ID[:], b.volume.ID[:]) < 0
	})
	return out, capacity
}

func (r *rangeManager) planNext() (rangePlan, bool) {
	s := r.server
	image := s.placement.Load()
	view := s.currentSystem.Load()
	if image == nil || view == nil {
		return rangePlan{}, false
	}
	l := s.control.ledger
	l.mu.Lock()
	busy := l.slot != nil || l.request != nil
	l.mu.Unlock()
	if busy {
		return rangePlan{}, false
	}
	s.mu.RLock()
	drainingNode := s.cfg.Node.Draining
	draining := make(map[volume.ID]bool)
	for _, managed := range s.volumes {
		if managed.config.State == "draining" {
			draining[managed.store.Identity().VolumeID] = true
		}
	}
	s.mu.RUnlock()
	for _, d := range s.bucketSnapshots() {
		if d.ID == systemBucket.String() || d.DeletionComplete || d.State == BucketDeleting {
			continue
		}
		if d.Policy.StorageStrategy == "ram" {
			if plan, ok := r.planRuntimeReset(d); ok {
				return plan, true
			}
		}
		if d.DesiredPolicy != nil {
			if !samePlacementPolicy(d.Policy, *d.DesiredPolicy) {
				if plan, ok := r.planPolicyPlacement(d, drainingNode, draining); ok {
					return plan, true
				}
			}
			continue
		}
		if d.Policy.StorageStrategy == "ram" {
			// Live RAM uses the same scoped handoff, with process-bound READY
			// and explicit runtime-loss recovery rather than durable-byte claims.
		}
		b, err := s.Bucket(newVolumeID(d.ID))
		if err != nil || b.underMaintenance() {
			continue
		}
		destinations, capacity := r.destinations(d)
		var total uint64
		for _, n := range capacity {
			total = addCapacity(total, n)
		}
		if total == 0 {
			continue
		}
		entries := image.bucketEntries(client.ID(b.ID()))
		fractions := make(map[client.ID]float64)
		for _, entry := range entries {
			if len(entry.Locations) == 1 {
				fractions[entry.Locations[0].Node] += math.Ldexp(1, -int(entry.PrefixBits))
			}
		}
		for _, entry := range entries {
			if len(entry.Locations) != 1 || entry.Locations[0].Node != r.node {
				continue
			}
			if entry.Runtime != (client.ID{}) && !r.ramAvailable(b.ID(), volume.ObjectID(entry.Start), entry) {
				continue
			}
			before, ok := s.routingPatch(b.ID(), volume.ObjectID(entry.Start))
			if !ok {
				continue
			}
			volumeDrain := r.rangeOnDraining(before, draining)
			for _, destination := range destinations {
				if destination.node == r.node && (!volumeDrain || draining[volume.ID(destination.volume.ID)]) {
					continue
				}
				if drainingNode && destination.node == r.node {
					continue
				}
				if !drainingNode && !volumeDrain {
					ideal := float64(capacity[r.node]) / float64(total)
					targetIdeal := float64(capacity[destination.node]) / float64(total)
					if fractions[r.node] <= ideal+1e-12 || fractions[destination.node] >= targetIdeal-1e-12 {
						continue
					}
				}
				moving := entry
				if !drainingNode && !volumeDrain && capacity[r.node] != 0 {
					left, right, err := client.SplitRange(entry)
					if err != nil {
						continue
					}
					leftCost, _, err := r.rangeCost(before, left)
					if err != nil {
						continue
					}
					rightCost, _, err := r.rangeCost(before, right)
					if err != nil {
						continue
					}
					moving = left
					if rightCost < leftCost {
						moving = right
					}
					// Never transfer more hash share than the current surplus and
					// recipient deficit. Overshooting makes empty groups bounce
					// repeatedly and needlessly delays global administration.
					share := min(fractions[r.node]-float64(capacity[r.node])/float64(total), float64(capacity[destination.node])/float64(total)-fractions[destination.node])
					for math.Ldexp(1, -int(moving.PrefixBits)) > share+1e-12 && moving.PrefixBits < 256 {
						left, right, err := client.SplitRange(moving)
						if err != nil {
							break
						}
						lc, _, le := r.rangeCost(before, left)
						rc, _, re := r.rangeCost(before, right)
						if le != nil || re != nil {
							break
						}
						moving = left
						if rc < lc {
							moving = right
						}
					}
				}
				for {
					cost, count, err := r.rangeCost(before, moving)
					if err != nil {
						break
					}
					if cost <= destination.free {
						plan, err := planRangeMove(before, moving, destination.node, volume.ID(destination.volume.ID), destination.volume.Class, bucketCodec(d.Policy), cost)
						if err == nil {
							return plan, true
						}
						break
					}
					if count <= 1 || moving.PrefixBits == 256 {
						break
					}
					left, right, err := client.SplitRange(moving)
					if err != nil {
						break
					}
					a, _, err := r.rangeCost(before, left)
					if err != nil {
						break
					}
					b, _, err := r.rangeCost(before, right)
					if err != nil {
						break
					}
					moving = left
					if b < a {
						moving = right
					}
				}
			}
		}
	}
	return rangePlan{}, false
}
func newVolumeID(raw string) volume.ID { id, _ := bucketID(raw); return id }

func (r *rangeManager) rangeCost(before client.MapPatch, moving client.RangeEntry) (uint64, uint64, error) {
	plan := rangePlan{Before: before, Moving: moving}
	var cost, count uint64
	for _, v := range r.sourceVolumes(plan) {
		h, err := v.Open(false)
		if err != nil {
			return 0, 0, err
		}
		var after *volume.ObjectID
		for {
			records, err := h.ScanBucketObjects(volume.ID(before.Bucket), after, 128)
			if err != nil {
				h.Close()
				return 0, 0, err
			}
			if len(records) == 0 {
				break
			}
			for _, record := range records {
				if !plan.contains(record.Key.Object) {
					continue
				}
				n, err := h.AllocatedPhysical(record.Key)
				if err != nil {
					h.Close()
					return 0, 0, err
				}
				cost = addCapacity(cost, addCapacity(n, 256))
				count++
			}
			last := records[len(records)-1].Key.Object
			after = &last
		}
		err = h.ScanRequests(func(record volume.RequestRecord) error {
			if record.Key.Bucket == volume.ID(before.Bucket) && plan.contains(record.Key.Object) {
				cost = addCapacity(cost, 512)
			}
			return nil
		})
		h.Close()
		if err != nil {
			return 0, 0, err
		}
	}
	return addCapacity(cost, 64<<10), count, nil
}

func (r *rangeManager) rangeOnDraining(before client.MapPatch, draining map[volume.ID]bool) bool {
	for _, location := range before.Entries[0].Locations {
		if location.Node == r.node && draining[volume.ID(location.Volume)] {
			return true
		}
	}
	p := rangePlan{Before: before, Moving: before.Entries[0]}
	for _, v := range r.sourceVolumes(p) {
		if !draining[v.Identity().VolumeID] {
			continue
		}
		h, err := v.Open(false)
		if err != nil {
			continue
		}
		var found bool
		_ = h.ScanRequests(func(record volume.RequestRecord) error {
			if record.Key.Bucket == volume.ID(before.Bucket) && p.contains(record.Key.Object) {
				found = true
			}
			return nil
		})
		if !found {
			var after *volume.ObjectID
			for {
				records, err := h.ScanBucketObjects(volume.ID(before.Bucket), after, 128)
				if err != nil || len(records) == 0 {
					break
				}
				for _, record := range records {
					if p.contains(record.Key.Object) {
						found = true
						break
					}
				}
				if found {
					break
				}
				last := records[len(records)-1].Key.Object
				after = &last
			}
		}
		h.Close()
		if found {
			return true
		}
	}
	return false
}

// A device claim may only be released after both the authoritative map and
// physical indexes no longer reference it. Empty/tombstoned objects and retry
// receipts count too; logical byte totals alone cannot establish emptiness.
func (r *rangeManager) finishEmptyDrains() error {
	s := r.server
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.RLock()
	var candidates []*managedVolume
	for _, v := range s.volumes {
		if v.config.State == "draining" && !v.config.System {
			candidates = append(candidates, v)
		}
	}
	s.mu.RUnlock()
	for _, v := range candidates {
		id := v.store.Identity().VolumeID
		r.mu.Lock()
		busy := false
		for _, txn := range r.txns {
			if !r.active(txn) {
				continue
			}
			if txn.plan.Target == r.node && txn.plan.Volume == id {
				busy = true
			}
			if txn.plan.Source == r.node {
				for _, location := range txn.plan.Before.Entries[0].Locations {
					if location.Volume == (client.ID{}) || volume.ID(location.Volume) == id {
						busy = true
					}
				}
			}
		}
		r.mu.Unlock()
		if busy {
			continue
		}
		image := s.placement.Load()
		if image == nil {
			continue
		}
		for _, bucket := range s.bucketSnapshots() {
			bid, err := bucketID(bucket.ID)
			if err != nil {
				return err
			}
			for _, entry := range image.bucketEntries(client.ID(bid)) {
				for _, location := range entry.Locations {
					if location.Node == r.node && volume.ID(location.Volume) == id {
						busy = true
					}
				}
			}
		}
		if busy {
			continue
		}
		h, err := v.store.Open(false)
		if err != nil {
			return err
		}
		records, err := h.ScanObjects(nil, 1)
		if err == nil && len(records) == 0 {
			err = h.ScanRequests(func(volume.RequestRecord) error { busy = true; return nil })
		} else if len(records) != 0 {
			busy = true
		}
		h.Close()
		if err != nil {
			return err
		}
		if busy {
			continue
		}
		if err := s.completeDetach(v.config.OperationID, v.config.ID, v); err != nil {
			return err
		}
		s.notifyStats()
	}
	return nil
}

// With the optional data-replication feature disabled, process death loses the
// sole RAM copy. A self-contained, journaled scope advances its runtime before
// any object can be acknowledged again. Disk ranges keep their ownership.
func (r *rangeManager) planRuntimeReset(d BucketDescription) (rangePlan, bool) {
	s := r.server
	id, err := bucketID(d.ID)
	if err != nil {
		return rangePlan{}, false
	}
	for _, entry := range s.placement.Load().bucketEntries(client.ID(id)) {
		if len(entry.Locations) != 1 || entry.Locations[0].Node != r.node || r.ramAvailable(id, volume.ObjectID(entry.Start), entry) {
			continue
		}
		before, ok := s.routingPatch(id, volume.ObjectID(entry.Start))
		if !ok {
			continue
		}
		effective := d
		if d.DesiredPolicy != nil && entry.PolicyVersion == d.DesiredPolicy.Version {
			effective.Policy = *d.DesiredPolicy
		}
		destinations, _ := r.destinations(effective)
		sort.SliceStable(destinations, func(i, j int) bool { return destinations[i].node == r.node && destinations[j].node != r.node })
		for _, target := range destinations {
			plan, err := planRangeMove(before, entry, target.node, volume.ID(target.volume.ID), uint8(volume.RAM), bucketCodec(effective.Policy), 0)
			if err != nil {
				continue
			}
			plan.After.Entries[0].Runtime = s.boot
			if plan.validate() == nil {
				return plan, true
			}
		}
		continue
	}
	return rangePlan{}, false
}
