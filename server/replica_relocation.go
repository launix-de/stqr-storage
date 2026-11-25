// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Role changes retain every copy. Volume relocation replaces exactly one
// pinned location on the same node; a node replacement uses add then remove.
func (p rangePlan) validateReplicaTransition(old client.RangeEntry) error {
	if len(old.Locations) < 2 || old.Locations[0].Role != 0 || old.Locations[0].Node != p.Source || old.State != 0 || old.Runtime != (client.ID{}) || len(p.After.Entries) != 1 || old.OwnerEpoch == ^uint64(0) || p.Moving.Start != old.Start || p.Moving.PrefixBits != old.PrefixBits {
		return client.ErrProtocol
	}
	next := p.After.Entries[0]
	if next.Start != old.Start || next.PrefixBits != old.PrefixBits || next.State != old.State || next.Runtime != old.Runtime || next.OwnerEpoch != old.OwnerEpoch+1 || next.PolicyVersion < old.PolicyVersion || len(next.Locations) != len(old.Locations) {
		return client.ErrProtocol
	}
	index := -1
	seen := make(map[client.ID]bool)
	for i, loc := range old.Locations {
		if loc.Volume == (client.ID{}) || loc.Role > 1 || seen[loc.Node] || loc.Class > uint8(volume.SSD) {
			return client.ErrProtocol
		}
		seen[loc.Node] = true
		if loc.Node == p.Target {
			index = i
		}
	}
	if index < 0 {
		return client.ErrProtocol
	}
	expected := append([]client.Location(nil), old.Locations...)
	if p.ReplicaPromote {
		if index == 0 || old.Locations[index].Volume != client.ID(p.Volume) {
			return client.ErrProtocol
		}
		expected[0], expected[index] = expected[index], expected[0]
		expected[0].Role, expected[index].Role = 0, 1
	} else {
		if old.Locations[index].Volume == client.ID(p.Volume) || next.Locations[index].Class > uint8(volume.SSD) {
			return client.ErrProtocol
		}
		expected[index].Volume = client.ID(p.Volume)
		expected[index].Class = next.Locations[index].Class
	}
	if !reflect.DeepEqual(next.Locations, expected) {
		return client.ErrProtocol
	}
	return nil
}

func replicaTopologyDigest(p rangePlan, used uint64) [32]byte {
	e := client.Encoder{B: mustRangeWire(p)}
	if p.ReplicaPromote {
		e.U64(used)
	}
	return sha256.Sum256(e.B)
}

func (r *rangeManager) replicaLogicalUsage(p rangePlan, store *volume.Volume) (uint64, error) {
	h, err := store.Open(false)
	if err != nil {
		return 0, err
	}
	defer h.Close()
	var total uint64
	var after *volume.ObjectID
	for {
		records, err := p.scanObjects(h, after, 128)
		if err != nil {
			return 0, err
		}
		if len(records) == 0 {
			return total, nil
		}
		for _, record := range records {
			if p.contains(record.Key.Object) && !record.Info.Deleted {
				total = addCapacity(total, record.Info.Length)
			}
		}
		last := records[len(records)-1].Key.Object
		after = &last
	}
}

func (s *Server) replicaLocationAllowed(goal BucketPolicy, loc client.Location) bool {
	return loc.Role <= 1 && loc.Volume != (client.ID{}) && bucketClass(goal).Allows(volume.Class(loc.Class)) && s.placementGroups.Load().allowsVersion(goal.PlacementGroup, goal.PlacementMaskVersion, loc.Node.String(), loc.Volume.String())
}

func (s *Server) replicaSetSatisfies(goal BucketPolicy, locations []client.Location) bool {
	view := s.currentSystem.Load()
	if view == nil || len(locations) != s.requiredReplicas(goal, view.manifest.Membership) {
		return false
	}
	for _, loc := range locations {
		if !s.replicaLocationAllowed(goal, loc) {
			return false
		}
	}
	return true
}

// Required policy changes and physical pressure use the same relocation path.
func (r *rangeManager) replicaLocationNeedsMove(goal BucketPolicy, loc client.Location, destinations []rangeDestination) bool {
	_, draining := r.replicaLocationState(loc)
	if draining || !r.server.replicaLocationAllowed(goal, loc) {
		return true
	}
	for _, destination := range destinations {
		if destination.node == loc.Node && destination.volume.ID == loc.Volume {
			return r.capacityNeed(destination, destinations) > 0
		}
	}
	return false
}

// Offline observations cannot justify a replacement. A handoff already bound
// while online must resume its exact durable decision after disconnect/restart.
func (r *rangeManager) replicaLocationState(loc client.Location) (online, draining bool) {
	s := r.server
	var snapshot *client.StatsSnapshot
	if loc.Node == r.node {
		s.mu.RLock()
		managed := s.volumes[loc.Volume.String()]
		if managed != nil {
			online = !s.closed && (managed.config.State == "active" || managed.config.State == "draining")
			draining = s.cfg.Node.Draining || managed.config.State == "draining"
		}
		s.mu.RUnlock()
		return online, draining
	} else {
		s.peerStatsMu.RLock()
		o := s.peerStats[loc.Node]
		if o != nil && o.online && o.snapshot != nil && o.connection.Capabilities&client.CapabilityReplicaTopology != 0 {
			copy := *o.snapshot
			snapshot = &copy
		}
		s.peerStatsMu.RUnlock()
	}
	if snapshot == nil || snapshot.NodeState > 1 {
		return false, false
	}
	for _, v := range snapshot.Volumes {
		if v.ID == loc.Volume {
			return v.State == 1 || v.State == 2, snapshot.NodeState == 1 || v.State == 2
		}
	}
	return false, false
}

func (r *rangeManager) replicaSetOnline(entry client.RangeEntry) bool {
	for _, loc := range entry.Locations {
		if online, _ := r.replicaLocationState(loc); !online {
			return false
		}
	}
	return true
}

// EVERYWHERE shrinks only as part of an explicitly requested membership leave.
// Until that ALL-member leave commits, writes still enforce the old membership
// count and therefore wait. No successful write gets a weaker ACK contract.
func (r *rangeManager) replicaRemovalCount(goal BucketPolicy) int {
	s := r.server
	view := s.currentSystem.Load()
	need := s.requiredReplicas(goal, view.manifest.Membership)
	if goal.PlacementMode != "everywhere" || goal.Redundancy.Mode != "replication" || view.manifest.ReplicationProtocol < 2 {
		return need
	}
	mask, ok := s.placementGroups.Load().mask(goal.PlacementGroup, goal.PlacementMaskVersion)
	if !ok {
		return need
	}
	draining := 0
	for _, member := range view.manifest.Membership.Members {
		admitted := mask.NodeSelection == "all"
		for _, node := range mask.Nodes {
			if node.NodeID == member.ID.String() {
				admitted = true
			}
		}
		if member.ID == r.node {
			s.mu.RLock()
			leaving := s.cfg.Node.Draining
			s.mu.RUnlock()
			if admitted && leaving {
				draining++
			}
			continue
		}
		s.peerStatsMu.RLock()
		o := s.peerStats[member.ID]
		online := o != nil && o.online && o.snapshot != nil && o.connection.Capabilities&client.CapabilityReplicaTopology != 0
		leaving := online && o.snapshot.NodeState == 1
		s.peerStatsMu.RUnlock()
		// The membership leave will require this member even outside the PG.
		if !online {
			return need
		}
		if admitted && leaving {
			draining++
		}
	}
	if need-draining < 1 {
		return need
	}
	return need - draining
}

// Keep valid copies. For an invalid primary, prefer changing roles to a valid
// existing replica. Otherwise add an allowed copy before retiring any old one.
func (r *rangeManager) planReplicaRelocation(d BucketDescription) (rangePlan, bool) {
	s := r.server
	view := s.currentSystem.Load()
	if view == nil || view.manifest.ReplicationProtocol < 2 || d.Policy.StorageStrategy == "ram" {
		return rangePlan{}, false
	}
	goal := d.Policy
	if d.DesiredPolicy != nil {
		goal = *d.DesiredPolicy
	}
	if goal.StorageStrategy == "ram" || d.Policy.StorageStrategy == "ram" {
		return rangePlan{}, false
	}
	id, err := bucketID(d.ID)
	if err != nil {
		return rangePlan{}, false
	}
	destinationDescription := d
	destinationDescription.Policy = goal
	destinations, _ := r.destinations(destinationDescription)
	need := s.requiredReplicas(goal, view.manifest.Membership)
	for _, entry := range s.placement.Load().bucketEntries(client.ID(id)) {
		if entry.State != 0 || len(entry.Locations) < 2 || entry.Locations[0].Node != r.node || !r.replicaSetOnline(entry) {
			continue
		}
		before, ok := s.routingPatch(id, volume.ObjectID(entry.Start))
		if !ok {
			continue
		}
		bad := make([]bool, len(entry.Locations))
		firstBad := -1
		for i, loc := range entry.Locations {
			bad[i] = r.replicaLocationNeedsMove(goal, loc, destinations)
			if bad[i] && firstBad < 0 {
				firstBad = i
			}
		}
		if firstBad < 0 {
			continue
		}
		// At excess count a bad follower can already be removed safely.
		if !bad[0] && len(entry.Locations) > need {
			continue
		}
		if bad[0] && len(entry.Locations) > need {
			if plan, ok := r.planReplicaPromotion(d, before, entry, bad, destinations); ok {
				return plan, true
			}
		}
		for _, destination := range destinations {
			if destination.node != entry.Locations[firstBad].Node || destination.volume.ID == entry.Locations[firstBad].Volume {
				continue
			}
			cost, _, err := r.policyPlacementCost(before, entry, destination.node, volume.ID(destination.volume.ID), bucketCodec(d.Policy))
			if err != nil || cost > destination.free {
				continue
			}
			next := entry
			next.OwnerEpoch++
			next.Locations = append([]client.Location(nil), entry.Locations...)
			next.Locations[firstBad].Volume, next.Locations[firstBad].Class = destination.volume.ID, destination.volume.Class
			if goal.Compression == d.Policy.Compression && s.replicaSetSatisfies(goal, next.Locations) {
				next.PolicyVersion = goal.Version
			}
			after := before
			after.Entries = []client.RangeEntry{next}
			p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Moving: entry, Source: r.node, Target: destination.node, Volume: volume.ID(destination.volume.ID), Compression: bucketCodec(d.Policy), Physical: cost, ReplicaRelocate: true}
			if p.validate() == nil {
				return p, true
			}
		}

		if bad[0] && len(entry.Locations) <= need {
			if plan, ok := r.planReplicaPromotion(d, before, entry, bad, destinations); ok {
				return plan, true
			}
		}

		for _, destination := range destinations {
			present := false
			for _, loc := range entry.Locations {
				if loc.Node == destination.node {
					present = true
				}
			}
			if present {
				continue
			}
			cost, _, err := r.policyPlacementCost(before, entry, destination.node, volume.ID(destination.volume.ID), bucketCodec(d.Policy))
			if err != nil || cost > destination.free {
				continue
			}
			next := entry
			next.OwnerEpoch++
			next.Locations = append(append([]client.Location(nil), entry.Locations...), client.Location{Node: destination.node, Volume: destination.volume.ID, Role: 1, Class: destination.volume.Class})
			after := before
			after.Entries = []client.RangeEntry{next}
			p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Moving: entry, Source: r.node, Target: destination.node, Volume: volume.ID(destination.volume.ID), Compression: bucketCodec(d.Policy), Physical: cost, ReplicaAdd: true}
			if p.validate() == nil {
				return p, true
			}
		}
	}
	return rangePlan{}, false
}

func (r *rangeManager) planReplicaPromotion(d BucketDescription, before client.MapPatch, entry client.RangeEntry, bad []bool, destinations []rangeDestination) (rangePlan, bool) {

	for i := 1; i < len(entry.Locations); i++ {
		if bad[i] {
			continue
		}
		abortCost, err := r.replicaAbortCost(before, entry)
		if err != nil {
			continue
		}
		capacity := false
		for _, destination := range destinations {
			if destination.node == entry.Locations[i].Node && destination.volume.ID == entry.Locations[i].Volume && destination.free >= abortCost {
				capacity = true
			}
		}
		if !capacity {
			continue
		}
		next := entry
		next.OwnerEpoch++
		next.Locations = append([]client.Location(nil), entry.Locations...)
		next.Locations[0], next.Locations[i] = next.Locations[i], next.Locations[0]
		next.Locations[0].Role, next.Locations[i].Role = 0, 1
		after := before
		after.Entries = []client.RangeEntry{next}
		p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Moving: entry, Source: r.node, Target: next.Locations[0].Node, Volume: volume.ID(next.Locations[0].Volume), Compression: bucketCodec(d.Policy), Physical: abortCost, ReplicaPromote: true}
		if p.validate() == nil {
			return p, true
		}
	}

	return rangePlan{}, false
}
