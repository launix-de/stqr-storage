// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"

	"github.com/launix-de/stqr-storage/client"
)

const peerForceScopes uint16 = 0x1067

// Reports are bounded pages of durable participant state. They are collected
// only after every survivor has promised the exact exclusion, so normal range
// workers cannot advance the reported state while it is being reconciled.
type forceScopeReport struct {
	Cancellation *client.Proof `json:",omitempty"`
	Plan         []byte
	SourceLive   bool
	Ready        bool
	Decision     *rangeCommit
	Promoted     bool
}
type forceScopePage struct {
	Next    uint64
	Done    bool
	Reports []forceScopeReport
}

func (s *Server) receiveForceScopes(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	claim, err := decodeControlClaim(d.Blob())
	action := d.U8()
	if schema != 1 || err != nil || claim.Exclusion == nil || claim.Round.Initiator != actor || s.scopes == nil {
		return nil, client.ErrProtocol
	}
	if err := s.scopedTransition(&claim); err != nil {
		return nil, err
	}
	r := s.scopes
	switch action {
	case 0:
		offset := d.U64()
		if d.Done() != nil {
			return nil, client.ErrProtocol
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if offset > uint64(len(r.order)) {
			return nil, client.ErrProtocol
		}
		page := forceScopePage{Next: offset}
		for page.Next < uint64(len(r.order)) && len(page.Reports) < 24 {
			t := r.txns[r.order[page.Next]]
			page.Next++
			pendingPromotion := t.forcePromotion != nil && equalControlClaim(*t.forcePromotion, claim)
			cancelled := claim.Exclusion.RecoverOnly && t.forceCancellation != nil && equalControlClaim(*t.forceCancellation, claim)
			if !r.active(t) && !pendingPromotion && !cancelled {
				continue
			}
			report := forceScopeReport{Plan: mustRangeWire(t.plan), SourceLive: t.plan.Source == r.node && (!t.plan.ram() || t.sourceBoot == s.boot), Ready: t.targetReady.RecordSequence != 0 && (!t.plan.ram() || t.targetBoot == s.boot)}
			report.Promoted = pendingPromotion
			if cancelled {
				proof := t.cancellationProof
				report.Cancellation = &proof
			}
			if t.decision != nil {
				copy := *t.decision
				report.Decision = &copy
			}
			page.Reports = append(page.Reports, report)
		}
		page.Done = page.Next == uint64(len(r.order))
		return json.Marshal(page)
	case 1:
		p, err := decodeRangePlan(d.Blob())
		if err != nil || d.Done() != nil {
			return nil, client.ErrProtocol
		}
		return nil, r.cancelForExclusion(claim, p)
	case 2:
		p, err := decodeRangePlan(d.Blob())
		if err != nil || d.Done() != nil {
			return nil, client.ErrProtocol
		}
		return nil, r.retireForExclusion(claim, p)
	case 3:
		p, err := decodeRangePlan(d.Blob())
		if err != nil || d.Done() != nil {
			return nil, client.ErrProtocol
		}
		return nil, r.finishCommitForExclusion(claim, p)
	case 4:
		p, err := decodeRangePlan(d.Blob())
		if err != nil || d.Done() != nil {
			return nil, client.ErrProtocol
		}
		return nil, r.promoteForExclusion(claim, p)
	default:
		return nil, client.ErrProtocol
	}
}

func (r *rangeManager) cancelForExclusion(claim controlClaim, p rangePlan) error {
	if p.Source != r.node && p.Target != r.node {
		return errControlConflict
	}
	s := r.server
	s.opMu.Lock()
	r.mu.Lock()
	var snapshot *rangeSnapshot
	err := func() error {
		if err := s.scopedTransition(&claim); err != nil {
			return err
		}
		t := r.txns[p.id()]
		if t != nil {
			if t.aborted {
				if p.Target == r.node {
					return r.releaseReserveLocked(t)
				}
				return nil
			}
			if t.decision != nil {
				return errControlConflict
			}
			if hasControlNode(claim.Exclusion.Nodes, p.Source) && t.targetReady.RecordSequence != 0 && (!p.ram() || t.targetBoot == s.boot) {
				return fail(503, "sealed target must be recovered instead of discarded")
			}
		}
		if _, err := r.persist(rangeForceCancelled, rangeEvent{ID: p.id(), Plan: mustRangeWire(p), Exclusion: &claim}); err != nil {
			return err
		}
		t = r.txns[p.id()]
		snapshot, t.snapshot = t.snapshot, nil
		if p.Target == r.node {
			if err := r.releaseReserveLocked(t); err != nil {
				return err
			}
		}
		return nil
	}()
	r.mu.Unlock()
	s.opMu.Unlock()
	// A streaming receiver may be waiting for opMu while holding snapshot.mu.
	// Closing it outside both manager locks avoids an inversion.
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

func (c *clusterController) reconcileExcludedScopes(ctx context.Context, claim controlClaim) ([]forceRecoveredRange, error) {
	wire, err := marshalControlClaim(claim)
	if err != nil {
		return nil, err
	}
	type participants struct {
		plan    rangePlan
		reports map[client.ID]forceScopeReport
	}
	plans := make(map[[32]byte]*participants)
	transport := peerControlParticipants{runtime: c.server.control}
	for _, node := range claim.Required {
		var offset uint64
		for {
			e := client.Encoder{}
			e.U16(1)
			e.Blob(wire)
			e.U8(0)
			e.U64(offset)
			body, err := transport.call(ctx, node, peerForceScopes, e.B)
			if err != nil {
				return nil, err
			}
			var page forceScopePage
			if decodeControlRecord(body, &page) != nil || page.Next < offset || !page.Done && page.Next == offset || len(page.Reports) > 24 {
				return nil, client.ErrProtocol
			}
			for _, report := range page.Reports {
				// Cancellation is terminal for this attempt. Keep its proof in
				// recovery reports, but never retry or promote the old transfer.
				if report.Cancellation != nil {
					continue
				}
				p, err := decodeRangePlan(report.Plan)
				if err != nil || p.Source != node && p.Target != node {
					return nil, client.ErrProtocol
				}
				id := p.id()
				pair := plans[id]
				if pair == nil {
					pair = &participants{plan: p, reports: make(map[client.ID]forceScopeReport)}
					plans[id] = pair
				}
				pair.reports[node] = report
			}
			if page.Done {
				break
			}
			offset = page.Next
		}
	}
	var recovered []forceRecoveredRange
	// Resolve every known COMMIT on both surviving participants before quota
	// watermarks are captured. Otherwise a source debit before the checkpoint
	// and a delayed target credit after it could allocate the same bytes twice.
	for _, pair := range plans {
		p := pair.plan
		if (p.ReplicaPromote || p.ReplicaRelocate) && (!hasControlNode(claim.Required, p.Source) || !hasControlNode(claim.Required, p.Target)) {
			return nil, fail(501, "replica topology handoff requires both participants; forced replica reconciliation is not implemented")
		}
		committed := false
		for _, report := range pair.reports {
			committed = committed || report.Decision != nil
		}
		if committed {
			if hasControlNode(claim.Required, p.Source) && hasControlNode(claim.Required, p.Target) {
				e := client.Encoder{}
				e.U16(1)
				e.Blob(wire)
				e.U8(3)
				e.Blob(mustRangeWire(p))
				if _, err := transport.call(ctx, p.Source, peerForceScopes, e.B); err != nil {
					return nil, err
				}
			} else {
				for _, node := range []client.ID{p.Target, p.Source} {
					if !hasControlNode(claim.Required, node) {
						continue
					}
					e := client.Encoder{}
					e.U16(1)
					e.Blob(wire)
					e.U8(2)
					e.Blob(mustRangeWire(p))
					if _, err := transport.call(ctx, node, peerForceScopes, e.B); err != nil {
						return nil, err
					}
				}
			}
			continue
		}
		if hasControlNode(claim.Required, p.Source) {
			source, ok := pair.reports[p.Source]
			if !ok {
				return nil, errControlConflict
			}
			if !source.SourceLive {
				if target, known := pair.reports[p.Target]; known && target.Ready {
					e := client.Encoder{}
					e.U16(1)
					e.Blob(wire)
					e.U8(3)
					e.Blob(mustRangeWire(p))
					if _, err := transport.call(ctx, p.Source, peerForceScopes, e.B); err != nil {
						return nil, err
					}
					continue
				}
			}
		} else if target, ok := pair.reports[p.Target]; ok && (target.Ready || target.Promoted) {
			if !target.Promoted {
				e := client.Encoder{}
				e.U16(1)
				e.Blob(wire)
				e.U8(4)
				e.Blob(mustRangeWire(p))
				if _, err := transport.call(ctx, p.Target, peerForceScopes, e.B); err != nil {
					return nil, err
				}
			}
			recovered = append(recovered, forceRecoveredRange{Plan: p, Lost: !target.Ready})
			continue
		}
		// Target first: even a target that never saw RESERVE gets a durable
		// tombstone before the surviving source is allowed to drop its fence.
		nodes := []client.ID{p.Target, p.Source}
		for i, node := range nodes {
			if i > 0 && node == nodes[0] || !hasControlNode(claim.Required, node) {
				continue
			}
			e := client.Encoder{}
			e.U16(1)
			e.Blob(wire)
			e.U8(1)
			e.Blob(mustRangeWire(p))
			if _, err := transport.call(ctx, node, peerForceScopes, e.B); err != nil {
				return nil, err
			}
		}
	}
	return recovered, nil
}

// A confirmed COMMIT retains its historical proof and placement overlay. Only
// the now-unfinishable participant claim is retired; this never rolls back to
// the obsolete source, even when the excluded target had newer writes.
func (r *rangeManager) retireForExclusion(claim controlClaim, p rangePlan) error {
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(&claim); err != nil {
		return err
	}
	t := r.txns[p.id()]
	if t == nil || t.decision == nil || !hasControlNode(claim.Exclusion.Nodes, p.Source) && !hasControlNode(claim.Exclusion.Nodes, p.Target) {
		return errControlConflict
	}
	if !t.forceRetired {
		if _, err := r.persist(rangeForceRetired, rangeEvent{ID: p.id(), Exclusion: &claim}); err != nil {
			return err
		}
	}
	if p.Target == r.node {
		return r.releaseReserveLocked(t)
	}
	return nil
}

func (r *rangeManager) finishCommitForExclusion(claim controlClaim, p rangePlan) error {
	if p.Source != r.node || !hasControlNode(claim.Required, p.Target) {
		return errControlConflict
	}
	r.mu.Lock()
	t := r.txns[p.id()]
	if t == nil || t.aborted || t.forceRetired {
		r.mu.Unlock()
		return errControlConflict
	}
	committed := t.decision != nil
	if !committed && (!p.ram() || t.sourceBoot == r.server.boot) {
		r.mu.Unlock()
		return errControlConflict
	}
	r.mu.Unlock()
	ctx := context.WithValue(r.server.ctx, forceScopeKey{}, &claim)
	if !committed {
		state, err := r.targetRAMState(ctx, p)
		if err != nil {
			return err
		}
		if !state.ready {
			return fail(503, "sealed RAM recovery target is unavailable")
		}
	}
	if _, err := r.prepareRAMRecovery(ctx, p, t, committed); err != nil {
		return err
	}
	r.mu.Lock()
	if t.decision == nil {
		r.mu.Unlock()
		return errControlConflict
	}
	decision := *t.decision
	r.mu.Unlock()
	wire, err := decision.marshal()
	if err != nil {
		return err
	}
	_, err = r.call(ctx, p.Target, peerRangeCommit, wire)
	return err
}

func (r *rangeManager) promoteForExclusion(claim controlClaim, p rangePlan) error {
	if p.ReplicaPromote || p.ReplicaRelocate {
		return fail(501, "forced replica topology reconciliation is not implemented")
	}
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(&claim); err != nil {
		return err
	}
	t := r.txns[p.id()]
	if t == nil || t.plan.Target != r.node || !hasControlNode(claim.Exclusion.Nodes, p.Source) {
		return errControlConflict
	}
	if t.forcePromotion != nil {
		if !equalControlClaim(*t.forcePromotion, claim) {
			return errControlConflict
		}
		return r.releaseReserveLocked(t)
	}
	if t.aborted || t.forceRetired || t.decision != nil || t.targetReady.RecordSequence == 0 || p.ram() && t.targetBoot != r.server.boot {
		return errControlConflict
	}
	if _, err := r.persist(rangeForcePromoted, rangeEvent{ID: p.id(), Exclusion: &claim, Boot: r.server.boot}); err != nil {
		return err
	}
	return r.releaseReserveLocked(t)
}
