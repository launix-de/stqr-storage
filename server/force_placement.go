// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (c *clusterController) markExcludedRangesLost(patches []client.MapPatch, descriptions []BucketDescription, x *controlExclusion, membership client.Membership) error {
	s := c.server
	for i := range patches {
		p := &patches[i]
		var description BucketDescription
		for _, d := range descriptions {
			if d.ID == p.Bucket.String() {
				description = d
				break
			}
		}
		if description.ID == "" {
			return volume.ErrCorrupt
		}
		for j := range p.Entries {
			entry := &p.Entries[j]
			removed := false
			for _, loc := range entry.Locations {
				if loc.Role == 0 && hasControlNode(x.Nodes, loc.Node) {
					removed = true
				}
			}
			if !removed {
				continue
			}
			if entry.OwnerEpoch == ^uint64(0) {
				return volume.ErrRange
			}
			goal := description
			if description.DesiredPolicy != nil && entry.PolicyVersion == description.DesiredPolicy.Version {
				goal.Policy = *description.DesiredPolicy
			}
			owner, class, pin := membership.Members[0].ID, uint8(bucketClass(goal.Policy)), volume.ID{}
			if s.scopes != nil {
				destinations, _ := s.scopes.destinations(goal)
				for _, destination := range destinations {
					if hasControlNode(membershipIDs(membership), destination.node) {
						owner, class, pin = destination.node, destination.volume.Class, volume.ID(destination.volume.ID)
						break
					}
				}
			}
			entry.OwnerEpoch++
			entry.State = 5
			entry.Locations = []client.Location{{Node: owner, Volume: client.ID(pin), Class: class}}
			if goal.Policy.StorageStrategy == "ram" {
				if owner == c.coordinator.ledger.node {
					entry.Runtime = s.boot
				} else {
					s.peerStatsMu.RLock()
					observation := s.peerStats[owner]
					if observation != nil && observation.snapshot != nil {
						entry.Runtime = observation.snapshot.BootID
					} else {
						entry.Runtime = client.ID{}
					}
					s.peerStatsMu.RUnlock()
					if entry.Runtime == (client.ID{}) {
						return fail(503, "RAM loss recovery needs the target process identity")
					}
				}
			}
		}
		if err := p.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Force PREPARE may remove only unowned stale data. Every survivor first checks
// that all of its current authoritative groups remain exactly present. Thus a
// stale coordinator cannot discard a committed survivor handoff as LOST.
func (s *Server) prepareExclusionImage(view *systemView, proposal controlProposal) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	l := s.control.ledger
	l.mu.Lock()
	valid := l.slot != nil && l.slot.ConstructionStarted && equalControlClaim(l.slot.Claim, proposal.Claim)
	l.mu.Unlock()
	if !valid || !validExclusionClaim(proposal.Claim) {
		return errControlConflict
	}
	current := s.placement.Load()
	if current == nil {
		return errControlConflict
	}
	for _, description := range s.bucketSnapshots() {
		if description.ID == systemBucket.String() || description.DeletionComplete {
			continue
		}
		bucket, _ := bucketID(description.ID)
		for _, entry := range current.bucketEntries(client.ID(bucket)) {
			owned := false
			for _, loc := range entry.Locations {
				if loc.Role == 0 && loc.Node == l.node {
					owned = true
				}
			}
			if !owned {
				continue
			}
			next, known := view.placement.lookup(client.ID(bucket), entry.Start)
			if !known || !reflect.DeepEqual(next, entry) {
				return fail(409, "exclusion would replace an authoritative survivor range")
			}
		}
	}
	for _, patch := range view.patches {
		for _, entry := range patch.Entries {
			if entry.State != 5 {
				old, known := current.lookup(patch.Bucket, entry.Start)
				if !known || !reflect.DeepEqual(old, entry) {
					for _, location := range entry.Locations {
						if location.Role == 0 && location.Node == l.node && !s.hasSealedExclusionRange(proposal.Claim, patch.Bucket, entry) {
							return fail(409, "new survivor ownership lacks a sealed recovery snapshot")
						}
					}
				}
			}
			if entry.State != 5 {
				continue
			}
			old, known := current.lookup(patch.Bucket, entry.Start)
			if known && reflect.DeepEqual(old, entry) {
				continue
			}
			if err := s.purgeUnownedLostRange(patch.Bucket, entry); err != nil {
				return err
			}
		}
	}
	return s.syncVolumes()
}

func (s *Server) hasSealedExclusionRange(claim controlClaim, bucket client.ID, entry client.RangeEntry) bool {
	if s.scopes == nil {
		return false
	}
	r := s.scopes
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.txns {
		if t.forcePromotion == nil || !equalControlClaim(*t.forcePromotion, claim) || t.plan.Before.Bucket != bucket || t.plan.Target != r.node || t.plan.ram() && t.targetBoot != s.boot {
			continue
		}
		for _, moved := range t.plan.After.Entries {
			if moved.Start == t.plan.Moving.Start && moved.PrefixBits == t.plan.Moving.PrefixBits && reflect.DeepEqual(moved, entry) {
				return true
			}
		}
	}
	return false
}

// Caller holds opMu under the exact force slot. No object mutation can create
// new records before ALL release. Retrying PREPARE after a crash is safe, and a
// delayed PREPARE after release returns its existing READY without purging again.
func (s *Server) purgeUnownedLostRange(bucket client.ID, entry client.RangeEntry) error {
	p := rangePlan{Before: client.MapPatch{Bucket: bucket}, Moving: entry}
	s.mu.RLock()
	var stores []*volume.Volume
	for _, managed := range s.volumes {
		stores = append(stores, managed.store)
	}
	s.mu.RUnlock()
	for _, store := range stores {
		var after *volume.ObjectID
		for {
			if err := s.ctx.Err(); err != nil {
				return err
			}
			h, err := store.Open(true)
			if err != nil {
				return err
			}
			records, err := h.ScanBucketObjects(volume.ID(bucket), after, 128)
			if err == nil {
				for _, record := range records {
					if p.contains(record.Key.Object) {
						err = h.Purge(record.Key, record.Info.Version)
						if err != nil {
							break
						}
					}
				}
			}
			h.Close()
			if err != nil {
				return err
			}
			if len(records) == 0 {
				break
			}
			next := records[len(records)-1].Key.Object
			after = &next
		}
		var cursor *volume.ID
		for {
			h, err := store.Open(true)
			if err != nil {
				return err
			}
			records, err := h.ScanRequestRecords(cursor, 128)
			if err == nil {
				for _, record := range records {
					if record.Key.Bucket == volume.ID(bucket) && p.contains(record.Key.Object) {
						err = h.ForgetRequest(record.ID)
						if err != nil {
							break
						}
					}
				}
			}
			h.Close()
			if err != nil {
				return err
			}
			if len(records) == 0 {
				break
			}
			next := records[len(records)-1].ID
			cursor = &next
		}
	}
	return nil
}

type forceRecoveredRange struct {
	Plan rangePlan
	Lost bool
}

func (c *clusterController) applyRecoveredRanges(patches []client.MapPatch, recovered []forceRecoveredRange) error {
	for _, recovery := range recovered {
		p := recovery.Plan
		applied := false
		for i := range patches {
			if patches[i].Bucket != p.Before.Bucket {
				continue
			}
			var entries []client.RangeEntry
			for _, entry := range patches[i].Entries {
				if reflect.DeepEqual(entry, p.Before.Entries[0]) {
					for _, next := range p.After.Entries {
						if recovery.Lost && next.Start == p.Moving.Start && next.PrefixBits == p.Moving.PrefixBits {
							next.State = 5
							if p.ram() {
								if p.Target == c.coordinator.ledger.node {
									next.Runtime = c.server.boot
								} else {
									c.server.peerStatsMu.RLock()
									observation := c.server.peerStats[p.Target]
									if observation != nil && observation.snapshot != nil {
										next.Runtime = observation.snapshot.BootID
									}
									c.server.peerStatsMu.RUnlock()
								}
							}
						}
						entries = append(entries, next)
					}
					applied = true
				} else {
					entries = append(entries, entry)
				}
			}
			patches[i].Entries = entries
		}
		if !applied {
			return fail(409, "sealed recovery conflicts with a newer range map")
		}
	}
	return nil
}
