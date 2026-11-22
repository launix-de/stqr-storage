// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
)

// Both sides durably reject delayed RESERVE before the source forgets a failed
// offer. Fenced/READY/committed handoffs are never cancelled by this path.
// An absent participant is still required; no failure timeout is introduced.
func (r *rangeManager) cancelBeforeFence(ctx context.Context, p rangePlan, cause error) error {
	r.mu.Lock()
	txn := r.txns[p.id()]
	allowed := txn != nil && !txn.fenced && txn.decision == nil && txn.sourceReady.RecordSequence == 0
	r.mu.Unlock()
	if !allowed {
		return cause
	}
	if _, err := r.call(ctx, p.Target, peerRangeAbort, mustRangeWire(p)); err != nil {
		return err
	}
	if err := r.abort(p); err != nil {
		return err
	}
	return cause
}
func (r *rangeManager) abort(p rangePlan) error {
	if _, err := p.marshal(); err != nil {
		return err
	}
	if p.Source != r.node && p.Target != r.node {
		return errControlConflict
	}
	var snapshot *rangeSnapshot
	err := func() error {
		r.server.opMu.Lock()
		defer r.server.opMu.Unlock()
		r.mu.Lock()
		defer r.mu.Unlock()
		txn := r.txns[p.id()]
		if txn != nil {
			if txn.aborted {
				return nil
			}
			if txn.fenced || txn.decision != nil || txn.sourceReady.RecordSequence != 0 || txn.targetReady.RecordSequence != 0 {
				return errControlConflict
			}
		}
		if _, err := r.persist(rangeAborted, rangeEvent{ID: p.id(), Plan: mustRangeWire(p)}); err != nil {
			return err
		}
		txn = r.txns[p.id()]
		snapshot = txn.snapshot
		txn.snapshot = nil
		if p.Target == r.node {
			return r.releaseReserveLocked(txn)
		}
		return nil
	}()
	// Never wait for a snapshot while holding opMu/r.mu: a receiver may already
	// hold that snapshot and be waiting to recheck the abort under opMu.
	if snapshot != nil {
		snapshot.mu.Lock()
		if snapshot.object != nil {
			snapshot.object.close()
			snapshot.object = nil
		}
		snapshot.mu.Unlock()
	}
	return err
}
