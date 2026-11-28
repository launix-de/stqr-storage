// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"

	"github.com/launix-de/stqr-storage/client"
)

// Cancellation releases an already accepted scheduling shell. Its immutable
// binding remains authoritative even after the bucket is deleted or reconfigured;
// it does not authorize preparing another round, moving data, or reviving ranges.
// Caller holds r.mu. Only active PG metadata is inspected, never bucket objects.
func (r *rangeManager) retainedPGRoundLocked(id [32]byte) (pgRebuildRound, bool) {
	if a, ok := r.pgRounds[id]; ok {
		return a, true
	}
	for _, state := range r.pgStates {
		if state.Active != nil && state.Active.Round.ID == id {
			return state.Active.Round, true
		}
	}
	return pgRebuildRound{}, false
}
func (r *rangeManager) acceptPGCancellation(a pgRebuildRound) error {
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.pgRounds[a.ID]; ok {
		if !samePGCompletion(old, a) {
			return errControlConflict
		}
		return nil
	}
	state := r.pgStates[a.Group]
	if state == nil || state.Active == nil || !samePGRound(state.Active.Round, a) {
		return errControlConflict
	}
	p := mustDecodePGPlan(a.Plan)
	// Only the source's actual journalled cancellation may release its shell.
	// Other original members receive this binding from their authenticated fixed
	// coordinator, and still reject any locally non-aborted range transaction.
	if p.Source == r.node && state.Active.Cancellation != a.Done && !state.Active.Cancellations[a.Done] {
		return errControlConflict
	}
	if txn := r.txns[a.ID]; txn != nil && (!txn.aborted || txn.forceRetired || txn.decision != nil || txn.fenced || txn.fencing || txn.sourceReady.RecordSequence != 0 || txn.targetReady.RecordSequence != 0) {
		return errControlConflict
	}
	_, err := r.persist(rangePGCompleted, rangeEvent{PG: &a})
	return err
}
func (r *rangeManager) requestPGCancellation(ctx context.Context, p rangePlan, done client.Proof) ([]byte, error) {
	r.mu.Lock()
	round, ok := r.retainedPGRoundLocked(p.id())
	r.mu.Unlock()
	if !ok {
		return nil, errControlConflict
	}
	if round.Members[0] != r.node {
		return nil, ErrBucketForbidden
	}
	gateValue, _ := r.pgRuntime.gates.LoadOrStore(round.Group, make(chan struct{}, 1))
	gate := gateValue.(chan struct{})
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	default:
		return nil, fail(503, "PG coordinator is already processing this group")
	}
	// Preserve the normal completion catch-up before choosing a proof: another
	// original participant may already have durably completed this generation.
	for _, node := range round.Members {
		if node != r.node {
			if err := r.syncPGArchive(ctx, node, round.Group); err != nil {
				return nil, err
			}
		}
	}
	r.mu.Lock()
	completed, finished := r.pgRounds[p.id()]
	r.mu.Unlock()
	if finished {
		if !completed.Cancelled {
			return nil, errControlConflict
		}
		if err := r.completePG(ctx, completed); err != nil {
			return nil, err
		}
		return json.Marshal(completed)
	}
	round.Cancelled = true
	round.Commit = nil
	round.Done = done
	if !validPGCompletion(round) {
		return nil, client.ErrProtocol
	}
	if err := r.completePG(ctx, round); err != nil {
		return nil, err
	}
	return json.Marshal(round)
}
