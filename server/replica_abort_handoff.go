// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerReplicaAbortHistory uint16 = 0x1077

func encodeReplicaAbort(e *client.Encoder, record volume.ReplicaRecord) {
	e.B = append(e.B, encodeRangeReceipt(record.Request)...)
	e.U64(record.Binding.Epoch)
	e.U64(record.Binding.Sequence)
	e.B = append(e.B, record.Binding.SetHash[:]...)
	e.U16(record.AbortStatus)
	e.U64(record.PreviousLength)
}

func decodeReplicaAbort(d *client.Decoder) volume.ReplicaRecord {
	r := volume.ReplicaRecord{Request: decodeRangeReceipt(d), State: volume.ReplicaAborted}
	r.Binding.Epoch, r.Binding.Sequence = d.U64(), d.U64()
	copy(r.Binding.SetHash[:], d.Take(32))
	r.AbortStatus, r.PreviousLength = d.U16(), d.U64()
	return r
}

// Failures are authoritative at the old primary. A follower may have rejected
// PREPARE before creating a record, or retained only the default abort status.
// Copy terminal outcomes after fencing, before granting new primary authority.
func (r *rangeManager) sendReplicaAbortHistory(ctx context.Context, p rangePlan) error {
	view := r.server.currentSystem.Load()
	if view == nil || view.manifest.ReplicationProtocol < 2 || (!p.ReplicaPromote && !p.ReplicaRelocate) {
		return nil
	}
	for _, store := range r.sourceVolumes(p) {
		var after *volume.ID
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			h, err := store.Open(false)
			if err != nil {
				return err
			}
			records, err := h.ScanReplicaRecords(after, 64)
			h.Close()
			if err != nil {
				return err
			}
			if len(records) == 0 {
				break
			}
			var failures []volume.ReplicaRecord
			for _, record := range records {
				if record.State == volume.ReplicaAborted && client.ID(record.Request.Key.Bucket) == p.Before.Bucket && p.contains(record.Request.Key.Object) {
					failures = append(failures, record)
				}
			}
			if len(failures) > 0 {
				e := rangeMessage(peerReplicaAbortHistory, p.id())
				e.V(uint64(len(failures)))
				for _, record := range failures {
					encodeReplicaAbort(&e, record)
				}
				if _, err := r.call(ctx, p.Target, peerReplicaAbortHistory, e.B); err != nil {
					return err
				}
			}
			last := records[len(records)-1].Request.ID
			after = &last
		}
	}
	return nil
}

func (s *Server) receiveReplicaAbortHistory(m client.Message, actor client.ID) ([]byte, error) {
	if m.Kind != client.Request {
		return nil, client.ErrProtocol
	}
	d := client.Decoder{B: m.Body}
	if d.U16() != 1 {
		return nil, client.ErrProtocol
	}
	var id [32]byte
	copy(id[:], d.Take(32))
	count := d.V()
	if count == 0 || count > 64 {
		return nil, client.ErrProtocol
	}
	records := make([]volume.ReplicaRecord, int(count))
	for i := range records {
		records[i] = decodeReplicaAbort(&d)
	}
	if d.Done() != nil {
		return nil, client.ErrProtocol
	}
	r := s.scopes
	if r == nil {
		return nil, errControlConflict
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.scopedTransition(nil); err != nil {
		return nil, err
	}
	r.mu.Lock()
	txn := r.txns[id]
	valid := txn != nil && txn.plan.Target == r.node && txn.plan.Source == actor && !txn.aborted && !txn.forceRetired && txn.decision == nil
	r.mu.Unlock()
	if !valid || s.currentSystem.Load().manifest.ReplicationProtocol < 2 {
		return nil, errControlConflict
	}
	store, err := r.destination(txn.plan)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if client.ID(record.Request.Key.Bucket) != txn.plan.Before.Bucket || !txn.plan.contains(record.Request.Key.Object) {
			return nil, client.ErrProtocol
		}
		allowance, err := r.allowance(txn)
		if err != nil {
			return nil, err
		}
		before := store.Stats().InFlightReservedPhysical
		h, err := store.Open(true)
		if err != nil {
			return nil, err
		}
		err = h.RestoreReplicaAbort(record, volume.Options{InFlightAllowance: allowance, ImportShadow: true})
		h.Close()
		r.consumed(txn, store, before)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Replica promotion also uses the maintained physical total as an advisory
// upper budget for retained abort/retry history. Never scan the replica index
// on a planning tick; transfer walks actual history only after selecting a move.
func (r *rangeManager) replicaAbortCost(before client.MapPatch, entry client.RangeEntry, retained ...map[volume.ID]uint64) (uint64, error) {
	cost, _, _, err := r.placementCostTotals(before, entry, client.ID{}, volume.ID{}, retained...)
	if err == nil && cost > 0 {
		cost = addCapacity(cost, 64<<10)
	}
	return cost, err
}
