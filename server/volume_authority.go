// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"reflect"
	"sort"
	"sync/atomic"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type volumeAuthorityEntry struct {
	Bucket client.ID
	Entry  client.RangeEntry
	Cohort []client.ID
	Active bool
	Fence  [32]byte
}
type volumeAuthorityRecord struct {
	Schema                uint16
	Cluster, Node, Volume client.ID
	Entries               []volumeAuthorityEntry
}
type volumeAuthorityState struct {
	dirty    atomic.Bool
	revision atomic.Uint64
	pending  atomic.Pointer[authorityPendingView]
	records  map[volume.ID]volumeAuthorityRecord // confirmed per-device headers; opMu
	wanted   map[volume.ID]volumeAuthorityRecord
	staged   map[volume.ID]authorityTarget
	batch    *authorityBatch
}

func readVolumeAuthority(store *volume.Volume) (volumeAuthorityRecord, error) {
	identity := store.Identity()
	h, err := store.Open(false)
	if err != nil {
		return volumeAuthorityRecord{}, err
	}
	defer h.Close()
	raw, err := h.LocalMetadata()
	if err != nil {
		return volumeAuthorityRecord{}, err
	}
	if len(raw) == 0 {
		return volumeAuthorityRecord{}, volume.ErrNotFound
	}
	var record volumeAuthorityRecord
	if decodeVolumeAuthority(raw, &record) != nil || record.Schema != 1 || record.Volume != client.ID(identity.VolumeID) || record.Cluster != client.ID(identity.ClusterID) {
		return record, volume.ErrCorrupt
	}
	canonicalAuthorityEntries(&record)
	return record, nil
}

func (s *Server) dirtyVolumeAuthority() {
	if s.authority != nil {
		s.authority.revision.Add(1)
		s.authority.dirty.Store(true)
	}
}

// Recovery-only volumes participate solely in their committed imported scopes,
// or in an explicit pinned placement. Unrelated unpinned ranges cannot see old
// copies, even if an old object's numeric version exceeds their current value.
func (s *Server) volumeAllowedForEntry(bucket client.ID, entry client.RangeEntry, id volume.ID, view *systemView) bool {
	if len(entry.Locations) == 0 {
		return false
	}
	loc := entry.Locations[0]
	if len(entry.Locations) > 1 {
		found := false
		for _, candidate := range entry.Locations {
			if candidate.Node == s.control.ledger.node && candidate.Role <= 1 {
				loc = candidate
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if loc.Volume != (client.ID{}) {
		return loc.Volume == client.ID(id)
	}
	if view != nil {
		for _, origin := range view.manifest.VolumeRecoveries {
			if origin.Complete && origin.Before.Bucket == bucket && sameRangeAuthority(origin.After, entry) {
				return hasControlNode(origin.Volumes, client.ID(id))
			}
		}
	}
	s.mu.RLock()
	managed := s.volumes[id.String()]
	allowed := managed != nil && !managed.config.RecoveryOnly
	s.mu.RUnlock()
	return allowed
}

func (s *Server) planVolumeAuthorityLocked(allowances ...map[volume.ID]uint64) ([]authorityTarget, error) {
	if s.controller == nil || s.authority == nil || !s.authority.dirty.Load() {
		return nil, nil
	}
	view := s.currentSystem.Load()
	image := s.placement.Load()
	if view == nil || image == nil {
		return nil, nil
	}
	s.mu.RLock()
	stores := make(map[volume.ID]*volume.Volume)
	for _, managed := range s.volumes {
		if !managed.config.System && managed.store.Identity().Class != volume.RAM {
			stores[managed.store.Identity().VolumeID] = managed.store
		}
	}
	s.mu.RUnlock()
	// Closed, detached devices cannot publish their discarded accepted tail.
	// Keep staged recovery devices, which have not joined s.volumes yet.
	for id := range s.authority.staged {
		if stores[id] == nil && s.recoveryStore(id) == nil {
			delete(s.authority.staged, id)
			delete(s.authority.records, id)
		}
	}
	for id := range s.authority.records {
		if stores[id] == nil && s.recoveryStore(id) == nil {
			delete(s.authority.records, id)
		}
	}
	desired := make(map[volume.ID]volumeAuthorityRecord)
	for id, store := range stores {
		identity := store.Identity()
		desired[id] = volumeAuthorityRecord{Schema: 1, Cluster: client.ID(identity.ClusterID), Node: client.ID(identity.NodeID), Volume: client.ID(id)}
	}
	type fence struct {
		bucket client.ID
		entry  client.RangeEntry
		id     [32]byte
	}
	var fences []fence
	if r := s.scopes; r != nil {
		r.mu.Lock()
		for _, txn := range r.txns {
			if txn.plan.Source == r.node && (txn.fenced || txn.fencing) && !txn.aborted && !txn.forceRetired {
				fences = append(fences, fence{txn.plan.Before.Bucket, txn.plan.Before.Entries[0], txn.plan.id()})
			}
		}
		r.mu.Unlock()
	}
	for _, description := range s.bucketSnapshots() {
		if description.ID == systemBucket.String() || description.DeletionComplete {
			continue
		}
		bid, err := bucketID(description.ID)
		if err != nil {
			return nil, err
		}
		b, err := s.Bucket(bid)
		if err != nil {
			return nil, err
		}
		for _, entry := range image.bucketEntries(client.ID(bid)) {
			if len(entry.Locations) != 1 || entry.Locations[0].Node != s.control.ledger.node || entry.State != 0 && entry.State != 5 || entry.Runtime != (client.ID{}) {
				continue
			}
			var cohort []client.ID
			for _, store := range b.volumesFor(description) {
				id := store.Identity().VolumeID
				if stores[id] != nil && s.rangeClass(description.Policy, entry).Allows(store.Identity().Class) && s.volumeAllowedForEntry(client.ID(bid), entry, id, view) {
					cohort = append(cohort, client.ID(id))
				}
			}
			if len(cohort) == 0 {
				continue
			}
			sort.Slice(cohort, func(i, j int) bool { return cohort[i].String() < cohort[j].String() })
			claim := volumeAuthorityEntry{Bucket: client.ID(bid), Entry: entry, Cohort: cohort, Active: true}
			for _, f := range fences {
				if f.bucket == claim.Bucket && sameRangeAuthority(f.entry, entry) {
					claim.Active = false
					claim.Fence = f.id
				}
			}
			for _, id := range cohort {
				record := desired[volume.ID(id)]
				record.Entries = append(record.Entries, claim)
				desired[volume.ID(id)] = record
			}
		}
	}
	ids := make([]volume.ID, 0, len(stores))
	for id := range stores {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	var targets []authorityTarget
	wanted := make(map[volume.ID]volumeAuthorityRecord, len(ids))
	for _, id := range ids {
		record := desired[id]
		sort.Slice(record.Entries, func(i, j int) bool {
			a, b := record.Entries[i], record.Entries[j]
			if a.Bucket != b.Bucket {
				return a.Bucket.String() < b.Bucket.String()
			}
			return string(a.Entry.Start[:]) < string(b.Entry.Start[:])
		})
		prior, known := s.authority.records[id]
		if !known {
			var err error
			prior, err = readVolumeAuthority(stores[id])
			if err != nil && !errors.Is(err, volume.ErrNotFound) {
				return nil, err
			}
		}

		for _, claim := range prior.Entries {
			preserve := false
			for _, origin := range view.manifest.VolumeRecoveries {
				if !origin.Complete && origin.Before.Bucket == claim.Bucket && sameRangeAuthority(origin.Before.Entries[0], claim.Entry) && hasControlNode(claim.Cohort, client.ID(id)) {
					preserve = true
				}
			}
			if preserve {
				record.Entries = append(record.Entries, claim)
			}
		}
		sort.Slice(record.Entries, func(i, j int) bool {
			a, b := record.Entries[i], record.Entries[j]
			if a.Bucket != b.Bucket {
				return a.Bucket.String() < b.Bucket.String()
			}
			if a.Entry.Start != b.Entry.Start {
				return string(a.Entry.Start[:]) < string(b.Entry.Start[:])
			}
			return a.Entry.OwnerEpoch < b.Entry.OwnerEpoch
		})
		s.authority.records[id] = prior
		wanted[id] = record
		staged, hasStaged := s.authority.staged[id]
		if !reflect.DeepEqual(prior, record) || hasStaged {
			budget := uint64(0)
			if len(allowances) > 0 {
				budget = allowances[0][id]
			}
			target := authorityTarget{store: stores[id], record: record, budget: budget}
			if hasStaged && reflect.DeepEqual(staged.record, record) {
				target.generation = staged.generation
			}
			targets = append(targets, target)
		}
	}
	s.authority.wanted = wanted
	return targets, nil
}
func writeVolumeAuthority(store *volume.Volume, record volumeAuthorityRecord) error {
	return writeVolumeAuthorityBudget(store, record, 0)
}
func writeVolumeAuthorityBudget(store *volume.Volume, record volumeAuthorityRecord, budget uint64, compact ...bool) error {
	h, err := store.Open(true)
	if err != nil {
		return err
	}
	defer h.Close()
	current, err := h.LocalMetadata()
	if err != nil {
		return err
	}
	useCompact := len(compact) > 0 && compact[0] || len(current) >= 4 && string(current[:4]) == "STVA"
	raw, err := marshalStableVolumeAuthority(record, useCompact, current)
	if err != nil {
		return err
	}
	return h.SetLocalMetadataWithOptions(raw, volume.Options{InFlightAllowance: budget})
}

// A committed location is the effective storage contract during a policy
// migration. The bucket's desired class cannot invalidate an already activated
// copy, nor can the old bucket class hide the replacement before finalization.
func (s *Server) rangeClass(policy BucketPolicy, entry client.RangeEntry) volume.Class {
	for _, loc := range entry.Locations {
		if loc.Node == s.control.ledger.node && loc.Role <= 1 && loc.Volume != (client.ID{}) {
			return volume.Class(loc.Class)
		}
	}
	return bucketClass(policy)
}
