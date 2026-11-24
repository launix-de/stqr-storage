// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (s *Server) replicaTopologyMessage(m client.Message, actor client.ID) ([]byte, error) {
	if s.scopes == nil {
		return nil, client.ErrProtocol
	}
	d := client.Decoder{B: m.Body}
	schema := d.U16()
	var id [32]byte
	copy(id[:], d.Take(32))
	if schema != 1 || d.Done() != nil {
		return nil, client.ErrProtocol
	}
	r := s.scopes
	s.opMu.Lock()
	defer s.opMu.Unlock()
	r.mu.Lock()
	t := r.txns[id]
	valid := t != nil && t.plan.ReplicaRemove && t.plan.Source == actor && t.plan.Target == r.node && !t.aborted && !t.forceRetired
	r.mu.Unlock()
	if !valid {
		return nil, errControlConflict
	}
	if err := s.checkPendingReplicas(func(k volume.Key) bool {
		return client.ID(k.Bucket) == t.plan.Before.Bucket && t.plan.contains(k.Object)
	}); err != nil {
		return nil, err
	}
	if m.Opcode == peerReplicaTopologyReady {
		proof, err := r.ready(id, sha256.Sum256(mustRangeWire(t.plan)), false, 0, 0)
		if err != nil {
			return nil, err
		}
		return proof.MarshalBinary()
	}
	r.mu.Lock()
	committed := t.decision != nil
	r.mu.Unlock()
	if !committed {
		return nil, errControlConflict
	}
	s.mu.RLock()
	managed := s.volumes[t.plan.Volume.String()]
	s.mu.RUnlock()
	if managed == nil {
		return nil, volume.ErrNotFound
	}
	h, err := managed.store.Open(true)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	owned := func(k volume.Key) bool {
		entry, ok := s.placement.Load().lookup(client.ID(k.Bucket), client.ObjectID(k.Object))
		if !ok {
			return false
		}
		for _, loc := range entry.Locations {
			if loc.Node == r.node && loc.Role <= 1 && (loc.Volume == (client.ID{}) || loc.Volume == client.ID(t.plan.Volume)) {
				return true
			}
		}
		return false
	}
	var cursor *volume.ObjectID
	for {
		records, err := h.ScanBucketObjects(volume.ID(t.plan.Before.Bucket), cursor, 128)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			break
		}
		var obsolete []volume.ObjectRecord
		for _, record := range records {
			if t.plan.contains(record.Key.Object) && !owned(record.Key) {
				obsolete = append(obsolete, record)
			}
		}
		if err = h.PurgeBatch(obsolete); err != nil {
			return nil, err
		}
		last := records[len(records)-1].Key.Object
		cursor = &last
	}
	var after *volume.ID
	for {
		records, err := h.ScanRequestRecords(after, 128)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if client.ID(record.Key.Bucket) == t.plan.Before.Bucket && t.plan.contains(record.Key.Object) && !owned(record.Key) {
				if err = h.ForgetRequest(record.ID); err != nil {
					return nil, err
				}
			}
		}
		last := records[len(records)-1].ID
		after = &last
	}
	return nil, nil
}

func (r *rangeManager) planReplicaRemove(d BucketDescription) (rangePlan, bool) {
	if d.DesiredPolicy == nil || d.Policy.StorageStrategy == "ram" {
		return rangePlan{}, false
	}
	goal := *d.DesiredPolicy
	if goal.StorageStrategy != d.Policy.StorageStrategy || goal.Compression != d.Policy.Compression {
		return rangePlan{}, false
	}
	need := r.server.requiredReplicas(goal, r.server.currentSystem.Load().manifest.Membership)
	if need < 1 {
		return rangePlan{}, false
	}
	bucket, err := bucketID(d.ID)
	if err != nil {
		return rangePlan{}, false
	}
	for _, entry := range r.server.placement.Load().bucketEntries(client.ID(bucket)) {
		if entry.State != 0 || len(entry.Locations) <= need || entry.Locations[0].Node != r.node {
			continue
		}
		before, ok := r.server.routingPatch(bucket, volume.ObjectID(entry.Start))
		if !ok {
			continue
		}
		removed := entry.Locations[len(entry.Locations)-1]
		if removed.Role != 1 || removed.Volume == (client.ID{}) {
			continue
		}
		r.server.peerStatsMu.RLock()
		observation := r.server.peerStats[removed.Node]
		online := observation != nil && observation.online
		r.server.peerStatsMu.RUnlock()
		if !online {
			continue
		}
		next := entry
		next.OwnerEpoch++
		next.Locations = append([]client.Location(nil), entry.Locations[:len(entry.Locations)-1]...)
		if len(next.Locations) == need {
			next.PolicyVersion = goal.Version
		}
		after := before
		after.Entries = []client.RangeEntry{next}
		p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Source: r.node, Target: removed.Node, Volume: volume.ID(removed.Volume), Moving: entry, Compression: bucketCodec(goal), ReplicaRemove: true}
		if p.validate() == nil {
			return p, true
		}
	}
	return rangePlan{}, false
}
