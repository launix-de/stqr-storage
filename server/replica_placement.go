// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (s *Server) requiredReplicas(p BucketPolicy, membership client.Membership) int {
	if p.Redundancy.Mode != "replication" {
		return 1
	}
	if p.PlacementMode != "everywhere" {
		return int(p.Redundancy.Replicas)
	}
	mask, ok := s.placementGroups.Load().mask(p.PlacementGroup, p.PlacementMaskVersion)
	if !ok {
		return len(membership.Members)
	}
	if mask.NodeSelection == "all" {
		return len(membership.Members)
	}
	n := 0
	for _, member := range membership.Members {
		for _, node := range mask.Nodes {
			if node.NodeID == member.ID.String() {
				n++
				break
			}
		}
	}
	return n
}

// A new PG version can keep an already valid set. Codec conversion then runs
// on every required copy under the bucket's control fence; no data handoff is
// needed merely because the central mask/version number changed.
func (s *Server) replicaPolicyInPlace(d BucketDescription) bool {
	if d.DesiredPolicy == nil || d.Policy.ReadAcceleration != d.DesiredPolicy.ReadAcceleration {
		return false
	}
	view := s.currentSystem.Load()
	if view == nil {
		return false
	}
	goal := *d.DesiredPolicy
	need := s.requiredReplicas(goal, view.manifest.Membership)
	if need < 2 {
		return false
	}
	id, err := bucketID(d.ID)
	if err != nil {
		return false
	}
	entries := s.placement.Load().bucketEntries(client.ID(id))
	if len(entries) == 0 {
		return false
	}
	for _, entry := range entries {
		if entry.State != 0 || len(entry.Locations) != need {
			return false
		}
		if !s.replicaSetSatisfies(goal, entry.Locations) {
			return false
		}
	}
	return true
}

// A learner uses the existing durable COPY -> final CATCH_UP -> ACTIVATE
// machinery. The primary keeps its data and retry receipts. The old set remains
// authoritative during bulk copy; a short range fence covers the final catch-up.
func (r *rangeManager) planReplicaAdd(d BucketDescription) (rangePlan, bool) {
	goal := d.Policy
	if d.DesiredPolicy != nil {
		goal = *d.DesiredPolicy
	}
	if goal.Redundancy.Mode != "replication" || goal.StorageStrategy == "ram" || d.Policy.StorageStrategy == "ram" {
		return rangePlan{}, false
	}
	need := r.server.requiredReplicas(goal, r.server.currentSystem.Load().manifest.Membership)
	if need < 2 {
		return rangePlan{}, false
	}
	bucket, err := bucketID(d.ID)
	if err != nil {
		return rangePlan{}, false
	}
	targetDescription := d
	targetDescription.Policy = goal
	destinations, _ := r.destinations(targetDescription)
	for _, entry := range r.server.placement.Load().bucketEntries(client.ID(bucket)) {
		if entry.State != 0 || len(entry.Locations) == 0 || len(entry.Locations) >= need || entry.Locations[0].Node != r.node {
			continue
		}
		if len(entry.Locations) == 1 && entry.Locations[0].Volume == (client.ID{}) {
			pinned := d
			pinned.DesiredPolicy = &pinned.Policy
			return r.planPolicyPlacement(pinned, false, nil, true)
		}
		before, ok := r.server.routingPatch(bucket, volume.ObjectID(entry.Start))
		if !ok {
			continue
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
			if goal.Compression == d.Policy.Compression && r.server.replicaSetSatisfies(goal, next.Locations) {
				next.PolicyVersion = goal.Version
			}
			after := before
			after.Entries = []client.RangeEntry{next}
			p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Source: r.node, Target: destination.node, Volume: volume.ID(destination.volume.ID), Moving: entry, Compression: bucketCodec(d.Policy), Physical: cost, ReplicaAdd: true}
			if p.validate() == nil {
				return p, true
			}
		}
	}
	return rangePlan{}, false
}
