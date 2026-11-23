// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
)

type ramTargetState struct {
	boot, sourceBoot client.ID
	ready, committed bool
	digest           [32]byte
	logicalUsed      uint64
	proof            client.Proof
}

func (r *rangeManager) targetRAMState(ctx context.Context, p rangePlan) (ramTargetState, error) {
	e := rangeMessage(peerRangeState, p.id())
	body, err := r.call(ctx, p.Target, peerRangeState, e.B)
	if err != nil {
		return ramTargetState{}, err
	}
	d := client.Decoder{B: body}
	schema := d.U16()
	state := ramTargetState{boot: d.ID(), sourceBoot: d.ID(), ready: d.Bool(), committed: d.Bool()}
	copy(state.digest[:], d.Take(32))
	if d.Bool() {
		state.proof, err = client.DecodeProof(d.Take(56))
	}
	if schema == 2 {
		state.logicalUsed = d.U64()
	}
	if (schema != 1 && schema != 2) || err != nil || d.Done() != nil || state.boot == (client.ID{}) || state.ready && state.proof.Node != p.Target {
		return ramTargetState{}, client.ErrProtocol
	}
	return state, nil
}

// A live sealed RAM target can finish the exact frozen snapshot after source
// restart. An unsealed shadow is not a replica: if its sole source died, an
// explicit loss record closes that offer before a new empty runtime is planned.
func (r *rangeManager) prepareRAMRecovery(ctx context.Context, p rangePlan, t *rangeTxn, committed bool) (bool, error) {
	if !p.ram() {
		return false, nil
	}
	state, err := r.targetRAMState(ctx, p)
	if err != nil {
		return true, err
	}
	r.mu.Lock()
	sourceBoot := t.sourceBoot
	sourceLive := sourceBoot == r.server.boot
	fenced := t.fenced
	var decision *rangeCommit
	if t.decision != nil {
		copy := *t.decision
		decision = &copy
	}
	r.mu.Unlock()
	if state.sourceBoot != sourceBoot {
		return true, errControlConflict
	}
	if !committed {
		if state.ready {
			if !fenced {
				return true, errControlConflict
			}
			source, err := r.readyWithForce(p.id(), state.digest, true, forceScopeContext(ctx), state.logicalUsed)
			if err != nil {
				return true, err
			}
			_, err = r.commitWithForce(rangeCommit{ID: p.id(), Digest: state.digest, LogicalUsed: state.logicalUsed, Source: source, Target: state.proof}, forceScopeContext(ctx))
			return true, err
		}
		if !sourceLive {
			e := client.Encoder{}
			e.Blob(mustRangeWire(p))
			e.ID(sourceBoot)
			e.ID(r.server.boot)
			if _, err := r.call(ctx, p.Target, peerRangeRAMAbort, e.B); err != nil {
				return true, err
			}
			if err := r.abortLostRAM(p, sourceBoot, r.server.boot); err != nil {
				return true, err
			}
			return true, errControlConflict // claim is closed; the next scope resets its era
		}
		return false, nil
	}
	if state.committed || state.ready {
		return true, nil
	}
	if decision == nil {
		return true, errControlConflict
	}
	if !sourceLive {
		wire, err := decision.marshal()
		if err != nil {
			return true, err
		}
		e := client.Encoder{}
		e.Blob(wire)
		e.ID(sourceBoot)
		e.ID(r.server.boot)
		_, err = r.call(ctx, p.Target, peerRangeRAMLostCommit, e.B)
		return true, err
	}
	// The source's RAM process survived, and remains fenced. Rebuild the lost
	// pre-COMMIT target from the BEFORE data, retaining its original READY proof.
	digest, proof, err := r.sendSnapshot(ctx, p, true)
	if err != nil {
		return true, err
	}
	if digest != decision.Digest || proof != decision.Target {
		return true, errControlConflict
	}
	return true, nil
}

func (r *rangeManager) ramStateReply(id [32]byte, actor client.ID) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.txns[id]
	if t == nil || t.aborted || t.plan.Target != r.node || t.plan.Source != actor {
		return nil, errControlConflict
	}
	e := client.Encoder{}
	e.U16(2)
	e.ID(r.server.boot)
	e.ID(t.sourceBoot)
	live := t.targetReady.RecordSequence != 0 && (!t.plan.ram() || t.targetBoot == r.server.boot)
	e.Bool(live)
	e.Bool(t.decision != nil)
	e.B = append(e.B, t.digest[:]...)
	e.Bool(t.targetReady.RecordSequence != 0)
	if t.targetReady.RecordSequence != 0 {
		proof, err := t.targetReady.MarshalBinary()
		if err != nil {
			return nil, err
		}
		e.B = append(e.B, proof...)
	}
	e.U64(t.logicalUsed)
	return e.B, nil
}

func (r *rangeManager) abortLostRAM(p rangePlan, oldBoot, newBoot client.ID) error {
	if !p.ram() || oldBoot == newBoot || newBoot == (client.ID{}) {
		return errControlConflict
	}
	r.server.opMu.Lock()
	r.mu.Lock()
	var snapshot *rangeSnapshot
	err := func() error {
		t := r.txns[p.id()]
		if t != nil {
			if t.aborted {
				return nil
			}
			if t.sourceBoot != oldBoot || t.decision != nil || t.plan.Target == r.node && t.targetReady.RecordSequence != 0 && t.targetBoot == r.server.boot {
				return errControlConflict
			}
		}
		if _, err := r.persist(rangeRAMOfferLost, rangeEvent{ID: p.id(), Plan: mustRangeWire(p), SourceBoot: oldBoot, PeerBoot: newBoot, Boot: r.server.boot}); err != nil {
			return err
		}
		t = r.txns[p.id()]
		snapshot = t.snapshot
		t.snapshot = nil
		return r.releaseReserveLocked(t)
	}()
	r.mu.Unlock()
	r.server.opMu.Unlock()
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

// This is a historical COMMIT with explicit RAM loss, not a claim that the old
// runtime is readable. A newer process exists on both sides, neither old copy
// survives, and the target immediately retires that era before serving objects.
func (r *rangeManager) commitLostRAM(c rangeCommit, oldBoot, newBoot client.ID) (client.Proof, error) {
	return r.commitLostRAMWithForce(c, oldBoot, newBoot, nil)
}
func (r *rangeManager) commitLostRAMWithForce(c rangeCommit, oldBoot, newBoot client.ID, force *controlClaim) (client.Proof, error) {
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(force); err != nil {
		return client.Proof{}, err
	}
	t := r.txns[c.ID]
	if t == nil || !t.plan.ram() || t.plan.Target != r.node || t.sourceBoot != oldBoot || newBoot == (client.ID{}) || newBoot == oldBoot {
		return client.Proof{}, errControlConflict
	}
	if t.decision != nil {
		if *t.decision != c {
			return client.Proof{}, errControlConflict
		}
		return t.commitProof, nil
	}
	if t.targetBoot == r.server.boot || !r.validCommit(t, c, false) {
		return client.Proof{}, errControlConflict
	}
	proof, err := r.persist(rangeRAMCommitLost, rangeEvent{ID: c.ID, Commit: &c, SourceBoot: oldBoot, PeerBoot: newBoot, Boot: r.server.boot})
	if err != nil {
		return client.Proof{}, err
	}
	if err := r.rebuildLocked(); err != nil {
		return client.Proof{}, err
	}
	if _, err := r.persist(rangeRAMTargetLost, rangeEvent{ID: c.ID, Boot: r.server.boot}); err != nil {
		return client.Proof{}, err
	}
	if err := r.releaseReserveLocked(t); err != nil {
		return client.Proof{}, err
	}
	return proof, nil
}
