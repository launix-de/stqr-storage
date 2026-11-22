// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
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
	dirty   atomic.Bool
	records map[volume.ID]volumeAuthorityRecord // protected by node opMu
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
	if decodeControlRecord(raw, &record) != nil || record.Schema != 1 || record.Volume != client.ID(identity.VolumeID) || record.Cluster != client.ID(identity.ClusterID) {
		return record, volume.ErrCorrupt
	}
	return record, nil
}

func (s *Server) dirtyVolumeAuthority() {
	if s.authority != nil {
		s.authority.dirty.Store(true)
	}
}

// Recovery-only volumes participate solely in their committed imported scopes,
// or in an explicit pinned placement. Unrelated unpinned ranges cannot see old
// copies, even if an old object's numeric version exceeds their current value.
func (s *Server) volumeAllowedForEntry(bucket client.ID, entry client.RangeEntry, id volume.ID, view *systemView) bool {
	if len(entry.Locations) != 1 {
		return false
	}
	loc := entry.Locations[0]
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

// Called with opMu held. A cohort change is flushed on ALL participating data
// volumes before another object mutation is acknowledged. Partial updates are
// not a usable recovery proof: every volume in the advertised cohort must carry
// the same active entry. No client payload is copied or included in this record.
func (s *Server) ensureVolumeAuthorityLocked(allowances ...map[volume.ID]uint64) error {
	if s.controller == nil || s.authority == nil || !s.authority.dirty.Load() {
		return nil
	}
	view := s.currentSystem.Load()
	image := s.placement.Load()
	if view == nil || image == nil {
		return nil
	}
	s.mu.RLock()
	stores := make(map[volume.ID]*volume.Volume)
	for _, managed := range s.volumes {
		if !managed.config.System && managed.store.Identity().Class != volume.RAM {
			stores[managed.store.Identity().VolumeID] = managed.store
		}
	}
	s.mu.RUnlock()
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
			if txn.plan.Source == r.node && txn.fenced && !txn.aborted && !txn.forceRetired {
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
			return err
		}
		b, err := s.Bucket(bid)
		if err != nil {
			return err
		}
		for _, entry := range image.bucketEntries(client.ID(bid)) {
			if len(entry.Locations) != 1 || entry.Locations[0].Node != s.control.ledger.node || entry.State != 0 && entry.State != 5 || entry.Runtime != (client.ID{}) {
				continue
			}
			var cohort []client.ID
			for _, store := range b.volumesFor(description) {
				id := store.Identity().VolumeID
				if stores[id] != nil && bucketClass(description.Policy).Allows(store.Identity().Class) && s.volumeAllowedForEntry(client.ID(bid), entry, id, view) {
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
				return err
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
		if !reflect.DeepEqual(prior, record) {
			budget := uint64(0)
			if len(allowances) > 0 {
				budget = allowances[0][id]
			}
			if err := writeVolumeAuthorityBudget(stores[id], record, budget); err != nil {
				return err
			}
		}
		s.authority.records[id] = record
	}
	s.authority.dirty.Store(false)
	return nil
}
func writeVolumeAuthority(store *volume.Volume, record volumeAuthorityRecord) error {
	return writeVolumeAuthorityBudget(store, record, 0)
}
func writeVolumeAuthorityBudget(store *volume.Volume, record volumeAuthorityRecord, budget uint64) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	h, err := store.Open(true)
	if err != nil {
		return err
	}
	defer h.Close()
	return h.SetLocalMetadataWithOptions(raw, volume.Options{InFlightAllowance: budget})
}

// opMu and range-manager mutex are held by fence. Invalidate every member of
// the source cohort BEFORE publishing source FENCE/READY in its private journal.
func (s *Server) invalidateVolumeAuthorityLocked(p rangePlan) error {
	if s.authority == nil || p.ram() {
		return nil
	}
	s.dirtyVolumeAuthority()
	s.mu.RLock()
	var stores []*volume.Volume
	for _, v := range s.volumes {
		if !v.config.System && v.store.Identity().Class != volume.RAM {
			stores = append(stores, v.store)
		}
	}
	s.mu.RUnlock()
	for _, store := range stores {
		record, err := readVolumeAuthority(store)
		if errors.Is(err, volume.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		changed := false
		for i := range record.Entries {
			entry := &record.Entries[i]
			if entry.Bucket == p.Before.Bucket && sameRangeAuthority(entry.Entry, p.Before.Entries[0]) {
				entry.Active = false
				entry.Fence = p.id()
				changed = true
			}
		}
		if changed {
			if err := writeVolumeAuthority(store, record); err != nil {
				return err
			}
			s.authority.records[store.Identity().VolumeID] = record
		}
	}
	return nil
}
