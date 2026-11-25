// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerPGRebuildCodec uint16 = 0x10a3

// Codec changes retain the replica set and modify each copy in place while
// the primary's range fence excludes writes. Reads remain self describing.
func (r *rangeManager) planPGReplicaCodec(d BucketDescription) (rangePlan, bool) {
	if d.DesiredPolicy == nil || !r.server.replicaPolicyInPlace(d) {
		return rangePlan{}, false
	}
	id, _ := bucketID(d.ID)
	for _, entry := range r.server.placement.Load().bucketEntries(client.ID(id)) {
		if entry.PolicyVersion == d.DesiredPolicy.Version || len(entry.Locations) < 2 || entry.Locations[0].Node != r.node {
			continue
		}
		before, ok := r.server.routingPatch(id, volume.ObjectID(entry.Start))
		if !ok {
			continue
		}
		next := entry
		next.OwnerEpoch++
		next.PolicyVersion = d.DesiredPolicy.Version
		pin := volume.ID(entry.Locations[0].Volume)
		if pin == (volume.ID{}) {
			b, err := r.server.Bucket(id)
			if err != nil {
				continue
			}
			for _, v := range b.volumesFor(d) {
				if r.server.volumeAllowedForEntry(client.ID(id), entry, v.Identity().VolumeID, r.server.currentSystem.Load()) {
					pin = v.Identity().VolumeID
					break
				}
			}
			if pin == (volume.ID{}) {
				continue
			}
		}
		p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: before, Source: r.node, Target: r.node, Volume: pin, Moving: entry, Compression: bucketCodec(*d.DesiredPolicy), CodecOnly: true}
		p.After.Entries = []client.RangeEntry{next}
		if p.validate() == nil {
			return p, true
		}
	}
	return rangePlan{}, false
}
func (p rangePlan) validatePGCodec() error {
	old := p.Before.Entries[0]
	if len(old.Locations) < 2 || old.Locations[0].Node != p.Source || p.Target != p.Source || old.Locations[0].Volume != (client.ID{}) && p.Volume != volume.ID(old.Locations[0].Volume) || len(p.After.Entries) != 1 || old.OwnerEpoch == ^uint64(0) {
		return client.ErrProtocol
	}
	next := p.After.Entries[0]
	next.OwnerEpoch = old.OwnerEpoch
	next.PolicyVersion = old.PolicyVersion
	if p.After.Entries[0].OwnerEpoch != old.OwnerEpoch+1 || p.After.Entries[0].PolicyVersion < old.PolicyVersion || !reflect.DeepEqual(next, old) {
		return client.ErrProtocol
	}
	return nil
}
func (r *rangeManager) movePGCodec(ctx context.Context, p rangePlan) error {
	r.mu.Lock()
	t := r.txns[p.id()]
	committed := t.decision != nil
	r.mu.Unlock()
	if !committed {
		if err := r.fence(p.id()); err != nil {
			return err
		}
		_, members, _, err := r.server.pgForPlan(p)
		if err != nil {
			return err
		}
		for _, node := range members {
			if _, err = r.pgCall(ctx, node, peerPGRebuildCodec, p.id()); err != nil {
				return err
			}
		}
		digest := replicaTopologyDigest(p, 0)
		source, err := r.ready(p.id(), digest, true)
		if err != nil {
			return err
		}
		target, err := r.ready(p.id(), digest, false)
		if err != nil {
			return err
		}
		if _, err = r.commit(rangeCommit{ID: p.id(), Digest: digest, Source: source, Target: target}); err != nil {
			return err
		}
	}
	r.mu.Lock()
	decision := *t.decision
	r.mu.Unlock()
	target, err := r.done(p.id(), nil)
	if err != nil {
		return err
	}
	if _, err = r.done(p.id(), &target); err != nil {
		return err
	}
	r.announce(p, decision)
	return r.finishPG(ctx, p)
}
func (s *Server) applyPGCodec(ctx context.Context, body []byte, actor client.ID) ([]byte, error) {
	var id [32]byte
	if json.Unmarshal(body, &id) != nil {
		return nil, client.ErrProtocol
	}
	r := s.scopes
	r.mu.Lock()
	var p rangePlan
	found := false
	for _, state := range r.pgStates {
		if state.Active != nil && state.Active.Started && state.Active.Round.ID == id {
			p = mustDecodePGPlan(state.Active.Round.Plan)
			found = true
			break
		}
	}
	if !found {
		if completed, ok := r.pgRounds[id]; ok && !completed.Cancelled {
			r.mu.Unlock()
			return nil, nil
		}
	}
	r.mu.Unlock()
	if !found || !p.CodecOnly || p.Source != actor {
		return nil, ErrBucketForbidden
	}
	b, err := s.Bucket(volume.ID(p.Before.Bucket))
	if err != nil {
		return nil, err
	}
	var stores []*volume.Volume
	for _, loc := range p.Before.Entries[0].Locations {
		if loc.Node != r.node {
			continue
		}
		if loc.Volume == (client.ID{}) {
			for _, v := range b.volumesFor(b.Describe()) {
				if s.volumeAllowedForEntry(p.Before.Bucket, p.Before.Entries[0], v.Identity().VolumeID, s.currentSystem.Load()) {
					stores = append(stores, v)
				}
			}
		} else {
			s.mu.RLock()
			v := s.volumes[loc.Volume.String()]
			s.mu.RUnlock()
			if v == nil {
				return nil, volume.ErrNotFound
			}
			stores = append(stores, v.store)
		}
	}
	for _, store := range stores {
		var after *volume.ObjectID
		for {
			if err = s.waitForUserIO(ctx); err != nil {
				return nil, err
			}
			s.opMu.Lock()
			h, e := store.Open(false)
			if e != nil {
				s.opMu.Unlock()
				return nil, e
			}
			records, e := p.scanObjects(h, after, 128)
			h.Close()
			if e != nil {
				s.opMu.Unlock()
				return nil, e
			}
			s.opMu.Unlock()
			if len(records) == 0 {
				break
			}
			for _, record := range records {
				if record.Info.Deleted || record.Info.Compression == p.Compression {
					continue
				}
				if err := s.waitForUserIO(ctx); err != nil {
					return nil, err
				}
				s.opMu.Lock()
				h, e = store.Open(true)
				if e == nil {
					e = h.RecompressWithOptions(record.Key, p.Compression, volume.Options{})
					h.Close()
				}
				s.opMu.Unlock()
				if e != nil {
					return nil, e
				}
			}
			next := records[len(records)-1].Key.Object
			after = &next
		}
	}
	return nil, nil
}
func (r *rangeManager) codecFence(bucket volume.ID, object volume.ObjectID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, state := range r.pgStates {
		if state.Active == nil || !state.Active.Started {
			continue
		}
		p := mustDecodePGPlan(state.Active.Round.Plan)
		if p.CodecOnly && p.Before.Bucket == client.ID(bucket) && p.Source == r.node && p.contains(object) {
			return true
		}
	}
	return false
}
