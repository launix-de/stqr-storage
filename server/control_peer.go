// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// The runtime is installed before accepting peer requests. install must switch
// the validated runtime metadata view before APPLIED; leaving it nil never
// grants a candidate data authority. Journal replay itself does no callbacks.
type controlRuntime struct {
	server  *Server
	ledger  *controlLedger
	applyMu sync.Mutex
	install func(*systemView) error
}

func controlPeerOpcode(op uint16) bool {
	return op >= client.PeerControlReserve && op <= client.PeerControlAbort
}

func (s *Server) executePeerControl(m client.Message, actor client.ID) ([]byte, error) {
	if m.Kind != client.Request {
		return nil, fail(400, "control transition requires a request")
	}
	if s.control == nil {
		return nil, fail(503, "durable cluster controller is not active")
	}
	result, err := s.control.execute(m.Opcode, m.Body, actor)
	if errors.Is(err, client.ErrProtocol) {
		return nil, fail(400, "invalid control payload")
	}
	if errors.Is(err, errControlConflict) {
		return nil, fail(409, "control promise conflicts with local durable state")
	}
	return result, err
}

func (r *controlRuntime) decisionActor(decision client.Decision, actor client.ID) bool {
	r.ledger.mu.Lock()
	defer r.ledger.mu.Unlock()
	if prior, ok := r.ledger.completed[decision]; ok {
		return prior.Slot.Claim.Round.Initiator == actor
	}
	return r.ledger.slot != nil && r.ledger.slot.Decision == decision && r.ledger.slot.Claim.Round.Initiator == actor
}

func (r *controlRuntime) execute(op uint16, body []byte, actor client.ID) ([]byte, error) {
	var proof client.Proof
	var err error
	switch op {
	case client.PeerControlReserve, client.PeerControlAbort:
		claim, decodeErr := decodeControlClaim(body)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if claim.Round.Initiator != actor {
			return nil, fail(403, "control requester identity mismatch")
		}
		if op == client.PeerControlReserve {
			err = r.ledger.reserve(claim)
			return nil, err
		}
		proof, err = r.ledger.abortClaim(claim)
	case client.PeerControlPrepare:
		proposal, decodeErr := decodeControlProposal(body)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if proposal.Claim.Round.Initiator != actor {
			return nil, fail(403, "control requester identity mismatch")
		}
		proof, err = r.server.prepareSystemImage(r.ledger, proposal)
	case client.PeerControlCommit, client.PeerControlRelease:
		decision, certificate, decodeErr := decodeControlCertificate(body)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if !r.decisionActor(decision, actor) {
			return nil, errControlConflict
		}
		if op == client.PeerControlCommit {
			proof, err = r.ledger.commit(decision, certificate)
		} else {
			err = r.ledger.release(decision, certificate)
			if err == nil {
				proof, err = r.ledger.releasedProof(decision)
			}
		}
	case client.PeerControlApply:
		decision, decodeErr := decodeControlApply(body)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if !r.decisionActor(decision, actor) {
			return nil, errControlConflict
		}
		proof, err = r.apply(decision)
	default:
		return nil, client.ErrProtocol
	}
	if err != nil {
		return nil, err
	}
	return proof.MarshalBinary()
}

func (r *controlRuntime) apply(decision client.Decision) (client.Proof, error) {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	return r.applyLocked(decision)
}

func (r *controlRuntime) applyLocked(decision client.Decision) (client.Proof, error) {
	r.ledger.mu.Lock()
	if prior, ok := r.ledger.completed[decision]; ok {
		r.ledger.mu.Unlock()
		return prior.Slot.Applied, nil
	}
	slot := r.ledger.slot
	if slot == nil || slot.Decision != decision || slot.Committed.RecordSequence == 0 {
		r.ledger.mu.Unlock()
		return client.Proof{}, errControlConflict
	}
	if slot.Applied.RecordSequence != 0 {
		proof := slot.Applied
		r.ledger.mu.Unlock()
		return proof, nil
	}
	root := volume.ObjectID(slot.Proposal.Image)
	r.ledger.mu.Unlock()
	if r.install == nil {
		return client.Proof{}, fail(503, "runtime system image installer is not ready")
	}
	view, err := r.server.publishSystemImage(r.ledger, decision, root)
	if err != nil {
		return client.Proof{}, err
	}
	if err = r.install(view); err != nil {
		return client.Proof{}, err
	}
	return r.ledger.markApplied(decision)
}

// restore runs before exposing a reopened runtime. APPLIED in the journal is a
// durable decision, not evidence that fresh process memory has been installed.
// A publication interrupted before APPLIED is replayed idempotently. Missing or
// conflicting acknowledged objects never fall back to legacy empty metadata.
func (r *controlRuntime) restore() error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.ledger.mu.Lock()
	current := cloneControlBase(r.ledger.current)
	var pending client.Decision
	if slot := r.ledger.slot; slot != nil && slot.Committed.RecordSequence != 0 && slot.Applied.RecordSequence == 0 {
		pending = slot.Decision
	}
	r.ledger.mu.Unlock()
	if pending.Sequence != 0 {
		_, err := r.applyLocked(pending)
		return err
	}
	head, err := r.server.readSystemHead()
	if current.SystemImage == ([32]byte{}) {
		if errors.Is(err, volume.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return volume.ErrCorrupt
	}
	if err != nil {
		return err
	}
	if head.Manifest != volume.ObjectID(current.SystemImage) || head.Decision.Sequence != current.Sequence || head.Decision.Digest != current.Head {
		return volume.ErrCorrupt
	}
	r.ledger.mu.Lock()
	_, proven := r.ledger.completed[head.Decision]
	if slot := r.ledger.slot; slot != nil && slot.Decision == head.Decision && slot.Applied.RecordSequence != 0 {
		proven = true
	}
	r.ledger.mu.Unlock()
	if !proven {
		return volume.ErrCorrupt
	}
	view, err := r.server.loadSystemView(head.Manifest)
	if err != nil {
		return err
	}
	if view.manifest.Sequence != current.Sequence || !equalControlMembership(view.manifest.Membership, current.Membership) || view.manifest.Generation != current.Generation || view.manifest.Revision != current.MapRevision {
		return volume.ErrCorrupt
	}
	if hasControlNode(membershipIDs(current.Membership), r.ledger.node) {
		store, err := r.server.metadataStore()
		if err != nil {
			return err
		}
		if !view.groupImage.allows(systemPlacementGroup.String(), r.ledger.node.String(), store.Identity().VolumeID.String()) {
			return volume.ErrIdentity
		}
	}
	if r.install == nil {
		return fail(503, "runtime system image installer is not ready")
	}
	return r.install(view)
}

type peerControlParticipants struct{ runtime *controlRuntime }

func (p peerControlParticipants) call(ctx context.Context, node client.ID, opcode uint16, body []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if node == p.runtime.ledger.node {
		return p.runtime.execute(opcode, body, node)
	}
	s := p.runtime.server
	s.peerStatsMu.RLock()
	observation := s.peerStats[node]
	var peer *peerConnection
	if observation != nil && observation.online {
		peer = observation.connection
	}
	s.peerStatsMu.RUnlock()
	if peer == nil || peer.ctx.Err() != nil {
		return nil, fail(503, "required control participant is offline")
	}
	return peer.request(ctx, opcode, body)
}

func (p peerControlParticipants) Reserve(ctx context.Context, node client.ID, claim controlClaim) error {
	body, err := marshalControlClaim(claim)
	if err != nil {
		return err
	}
	result, err := p.call(ctx, node, client.PeerControlReserve, body)
	if err == nil && len(result) != 0 {
		return client.ErrProtocol
	}
	return err
}

func (p peerControlParticipants) Prepare(ctx context.Context, node client.ID, proposal controlProposal) (client.Proof, error) {
	body, err := marshalControlProposal(proposal)
	if err != nil {
		return client.Proof{}, err
	}
	return p.proof(ctx, node, client.PeerControlPrepare, body)
}

func (p peerControlParticipants) Commit(ctx context.Context, node client.ID, decision client.Decision, certificate client.Certificate) (client.Proof, error) {
	body, err := marshalControlCertificate(decision, certificate)
	if err != nil {
		return client.Proof{}, err
	}
	return p.proof(ctx, node, client.PeerControlCommit, body)
}

func (p peerControlParticipants) Apply(ctx context.Context, node client.ID, decision client.Decision) (client.Proof, error) {
	body, err := marshalControlApply(decision)
	if err != nil {
		return client.Proof{}, err
	}
	return p.proof(ctx, node, client.PeerControlApply, body)
}

func (p peerControlParticipants) Release(ctx context.Context, node client.ID, decision client.Decision, certificate client.Certificate) (client.Proof, error) {
	body, err := marshalControlCertificate(decision, certificate)
	if err != nil {
		return client.Proof{}, err
	}
	return p.proof(ctx, node, client.PeerControlRelease, body)
}

func (p peerControlParticipants) Abort(ctx context.Context, node client.ID, claim controlClaim) (client.Proof, error) {
	body, err := marshalControlClaim(claim)
	if err != nil {
		return client.Proof{}, err
	}
	return p.proof(ctx, node, client.PeerControlAbort, body)
}

func (p peerControlParticipants) proof(ctx context.Context, node client.ID, opcode uint16, body []byte) (client.Proof, error) {
	result, err := p.call(ctx, node, opcode, body)
	if err != nil {
		return client.Proof{}, err
	}
	proof, err := client.DecodeProof(result)
	if err != nil || proof.Node != node {
		return client.Proof{}, client.ErrProtocol
	}
	return proof, nil
}
