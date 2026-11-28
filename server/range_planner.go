// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type rangeDestination struct {
	node           client.ID
	boot           client.ID
	volume         client.VolumeStats
	capacity, free uint64
	observed       time.Time
}

func (r *rangeManager) start() { r.startPGWorkers() }

func (r *rangeManager) destinations(d BucketDescription, fresh ...bool) ([]rangeDestination, map[client.ID]uint64) {
	s := r.server
	var snapshots []client.StatsSnapshot
	seen := map[client.ID]time.Time{}
	if catalog := r.pgCatalog.Load(); catalog != nil && len(fresh) == 0 && time.Since(catalog.captured) < 1500*time.Millisecond {
		snapshots = catalog.snapshots
		seen = catalog.seen
	} else {
		local := s.resourceSnapshot()
		snapshots = []client.StatsSnapshot{local}
		seen[local.Node] = time.Now()
		s.peerStatsMu.RLock()
		for _, observation := range s.peerStats {
			if observation.online && observation.snapshot != nil && observation.snapshot.BootID == observation.connection.Boot && observation.connection.Capabilities&client.CapabilityRangeHandoff != 0 {
				snapshots = append(snapshots, *observation.snapshot)
				seen[observation.snapshot.Node] = observation.lastSeen
			}
		}
		s.peerStatsMu.RUnlock()
	}
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
			if recoveredVolume(view, v.ID) {
				continue
			}
			if v.State != 1 || !bucketClass(d.Policy).Allows(volume.Class(v.Class)) || !groups.allowsVersion(d.Policy.PlacementGroup, d.Policy.PlacementMaskVersion, snapshot.Node.String(), v.ID.String()) {
				continue
			}
			if d.Policy.PlacementGroup == "" && groups.allows(systemPlacementGroup.String(), snapshot.Node.String(), v.ID.String()) {
				continue
			}
			capacity[snapshot.Node] = addCapacity(capacity[snapshot.Node], v.CapacityPhysical)
			own := uint64(0)
			for _, reserve := range view.manifest.QuotaReserves {
				if reserve.Bucket.String() == d.ID && reserve.Node == snapshot.Node && reserve.Volume == v.ID {
					own = addCapacity(own, reserve.Backing)
				}
			}
			out = append(out, rangeDestination{node: snapshot.Node, boot: snapshot.BootID, volume: v, capacity: v.CapacityPhysical, free: addCapacity(v.AdmissionFreePhysical, min(own, v.GuaranteedRemainingPhysical)), observed: seen[snapshot.Node]})
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

func (r *rangeManager) planNextGroup(group string, descriptions []BucketDescription, maintenanceOnly ...bool) (rangePlan, bool) {
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
	if busy && (group == "" || group == "*") {
		return rangePlan{}, false
	}
	s.mu.RLock()
	drainingNode := s.cfg.Node.Draining
	draining := make(map[volume.ID]bool)
	localVolumes := make(map[volume.ID]bool)
	for _, managed := range s.volumes {
		localVolumes[managed.store.Identity().VolumeID] = true
		if managed.config.State == "draining" {
			draining[managed.store.Identity().VolumeID] = true
		}
	}
	s.mu.RUnlock()
	r.mu.Lock()
	for id := range r.capacityGoals {
		if !localVolumes[id] {
			delete(r.capacityGoals, id)
		}
	}
	r.mu.Unlock()
	var balance capacityMove
	var balancing bool
	for _, d := range descriptions {
		if group != "*" && d.Policy.PlacementGroup != group {
			continue
		}
		if group != "" && s.controlBlocksPG(client.ID(newVolumeID(d.ID))) {
			continue
		}
		if d.ID == systemBucket.String() || d.DeletionComplete || d.State == BucketDeleting {
			continue
		}
		if len(maintenanceOnly) == 0 || !maintenanceOnly[0] {
			if plan, ok := r.planReplicaRelocation(d); ok {
				return plan, true
			}
			if plan, ok := r.planReplicaAdd(d); ok {
				return plan, true
			}
			if plan, ok := r.planReplicaRemove(d); ok {
				return plan, true
			}
		}
		if d.Policy.StorageStrategy == "ram" {
			if plan, ok := r.planRuntimeReset(d); ok {
				return plan, true
			}
		}
		if d.DesiredPolicy != nil {
			if len(maintenanceOnly) > 0 && maintenanceOnly[0] {
				continue
			}
			if group != "" && group != "*" {
				if plan, ok := r.planPGReplicaCodec(d); ok {
					return plan, true
				}
			}
			if !samePlacementPolicy(d.Policy, *d.DesiredPolicy) || group != "" && group != "*" {
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
		if err != nil || b.blocksPlacementChange() {
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
		for _, entry := range entries {
			if entry.State == 4 || len(entry.Locations) != 1 || entry.Locations[0].Node != r.node {
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
			if !drainingNode && !volumeDrain {
				if candidate, ok := r.planLazyCapacity(d, before, entry, destinations); ok && (!balancing || candidate.betterThan(balance)) {
					balance, balancing = candidate, true
				}
				continue
			}
			if plan, ok := r.planRequiredRangeMove(d, before, entry, destinations, draining, drainingNode, volumeDrain); ok {
				return plan, true
			}

		}
	}
	if !balancing {
		return rangePlan{}, false
	}
	return balance.plan, balancing
}

func (r *rangeManager) planRequiredRangeMove(d BucketDescription, before client.MapPatch, entry client.RangeEntry, destinations []rangeDestination, draining map[volume.ID]bool, drainingNode, volumeDrain bool) (rangePlan, bool) {
	costOf, closeCosts := r.rangeCosts(before)
	defer closeCosts()
	for _, destination := range destinations {
		if destination.node == r.node && (!volumeDrain || draining[volume.ID(destination.volume.ID)]) {
			continue
		}
		if drainingNode && destination.node == r.node {
			continue
		}
		moving := entry
		for {
			cost, count, err := costOf(moving, nil)
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
			a, _, err := costOf(left, nil)
			if err != nil {
				break
			}
			b, _, err := costOf(right, nil)
			if err != nil {
				break
			}
			moving = left
			if b < a {
				moving = right
			}
		}
	}
	return rangePlan{}, false
}
func newVolumeID(raw string) volume.ID { id, _ := bucketID(raw); return id }

func (r *rangeManager) rangeCost(before client.MapPatch, moving client.RangeEntry) (uint64, uint64, error) {
	costOf, closeCosts := r.rangeCosts(before)
	defer closeCosts()
	return costOf(moving, nil)
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
				records, err := p.scanObjects(h, after, 128)
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
	if err := s.refreshQuotaProtectionLocked(); err != nil {
		return err
	}
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
		if s.volumeBacksGuarantee(id) {
			continue
		}
		r.mu.Lock()
		busy := false
		for _, txn := range r.txns {
			if !r.active(txn) {
				continue
			}
			if txn.plan.Target == r.node && txn.plan.Volume == id {
				busy = true
			}
			if txn.plan.Source == r.node || txn.plan.ReplicaRelocate && txn.plan.Target == r.node {
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
		if v.config.RecoveryOnly {
			// Recovery media can also contain unrelated obsolete copies. Only
			// discard those after ALL-owned scopes have left this volume and no
			// pending recovery still needs its original witnesses.
			view := s.currentSystem.Load()
			if view == nil {
				continue
			}
			record, err := readVolumeAuthority(v.store)
			if err != nil {
				return err
			}
			for _, origin := range view.manifest.VolumeRecoveries {
				if origin.Complete {
					continue
				}
				for _, claim := range record.Entries {
					if claim.Bucket == origin.Before.Bucket && sameRangeAuthority(claim.Entry, origin.Before.Entries[0]) {
						busy = true
					}
				}
			}
			for _, bucket := range s.bucketSnapshots() {
				bid, err := bucketID(bucket.ID)
				if err != nil {
					return err
				}
				for _, entry := range image.bucketEntries(client.ID(bid)) {
					if len(entry.Locations) == 1 && entry.Locations[0].Node == r.node && s.volumeAllowedForEntry(client.ID(bid), entry, id, view) {
						busy = true
					}
				}
			}
			if busy {
				continue
			}
			h, err := v.store.Open(true)
			if err != nil {
				return err
			}
			records, err := h.ScanObjects(nil, 128)
			if err == nil {
				err = h.PurgeBatch(records)
			}
			if err == nil && len(records) == 0 {
				requests, readErr := h.ScanRequestRecords(nil, 128)
				err = readErr
				for _, request := range requests {
					if err == nil {
						err = h.ForgetRequest(request.ID)
					}
				}
			}
			h.Close()
			if err != nil {
				return err
			}
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
