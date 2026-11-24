// SPDX-License-Identifier: GPL-3.0-only
package server

import (
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
	valid := t != nil && (t.plan.ReplicaRemove || t.plan.ReplicaPromote || t.plan.ReplicaRelocate) && t.plan.Source == actor && t.plan.Target == r.node && !t.aborted && !t.forceRetired
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
		if !t.plan.ReplicaRemove && !t.plan.ReplicaPromote {
			return nil, errControlConflict
		}
		used := uint64(0)
		if t.plan.ReplicaPromote {
			store, err := r.destination(t.plan)
			if err != nil {
				return nil, err
			}
			used, err = r.replicaLogicalUsage(t.plan, store)
			if err != nil {
				return nil, err
			}
		}
		proof, err := r.ready(id, replicaTopologyDigest(t.plan, used), false, used, 0)
		if err != nil {
			return nil, err
		}
		wire, err := proof.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if t.plan.ReplicaPromote {
			e := client.Encoder{B: wire}
			e.U64(used)
			wire = e.B
		}
		return wire, nil
	}
	r.mu.Lock()
	committed := t.decision != nil
	r.mu.Unlock()
	if !committed {
		return nil, errControlConflict
	}
	s.mu.RLock()
	retired := t.plan.Volume
	if t.plan.ReplicaRelocate {
		for _, loc := range t.plan.Before.Entries[0].Locations {
			if loc.Node == r.node {
				retired = volume.ID(loc.Volume)
			}
		}
	}
	managed := s.volumes[retired.String()]
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
			if loc.Node == r.node && loc.Role <= 1 && (loc.Volume == (client.ID{}) || loc.Volume == client.ID(retired)) {
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
	var replicaAfter *volume.ID
	for {
		records, err := h.ScanReplicaRecords(replicaAfter, 128)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if record.State == volume.ReplicaAborted && client.ID(record.Request.Key.Bucket) == t.plan.Before.Bucket && t.plan.contains(record.Request.Key.Object) && !owned(record.Request.Key) {
				if err := h.ForgetRequest(record.Request.ID); err != nil {
					return nil, err
				}
			}
		}
		last := records[len(records)-1].Request.ID
		replicaAfter = &last
	}
	return nil, nil
}

func (r *rangeManager) planReplicaRemove(d BucketDescription) (rangePlan, bool) {
	if d.Policy.StorageStrategy == "ram" {
		return rangePlan{}, false
	}
	goal := d.Policy
	if d.DesiredPolicy != nil {
		goal = *d.DesiredPolicy
	}
	if goal.StorageStrategy != d.Policy.StorageStrategy {
		return rangePlan{}, false
	}
	need := r.replicaRemovalCount(goal)
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
		index := len(entry.Locations) - 1
		for i := 1; i < len(entry.Locations); i++ {
			_, draining := r.replicaLocationState(entry.Locations[i])
			if draining || !r.server.replicaLocationAllowed(goal, entry.Locations[i]) {
				index = i
				break
			}
		}
		removed := entry.Locations[index]
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
		next.Locations = append(append([]client.Location(nil), entry.Locations[:index]...), entry.Locations[index+1:]...)
		if goal.Compression == d.Policy.Compression && r.server.replicaSetSatisfies(goal, next.Locations) {
			next.PolicyVersion = goal.Version
		}
		after := before
		after.Entries = []client.RangeEntry{next}
		p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Source: r.node, Target: removed.Node, Volume: volume.ID(removed.Volume), Moving: entry, Compression: bucketCodec(d.Policy), ReplicaRemove: true}
		if p.validate() == nil {
			return p, true
		}
	}
	return rangePlan{}, false
}
