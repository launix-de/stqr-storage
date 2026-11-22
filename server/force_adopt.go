// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerForceAdoptCommit uint16 = 0x1070

// A COMMIT witness plus the exact ALL-READY certificate identifies an already
// durable decision. This exception replays that decision; it cannot propose a
// replacement value or fabricate an acknowledgement from the fenced node.
func validForceCommitWitness(slot *controlSlot) bool {
	if slot == nil || slot.Proposal == nil || slot.Committed.RecordSequence == 0 || slot.Ready.RecordSequence == 0 {
		return false
	}
	decision, err := slot.Proposal.decision()
	if err != nil || decision != slot.Decision || !equalControlClaim(slot.Claim, slot.Proposal.Claim) || !slot.Certificate.Covers(slot.Claim.Required) || !containsOwnProof(slot.Certificate, slot.Ready) || slot.Committed.Node != slot.Ready.Node || slot.Committed.RecordSequence <= slot.Ready.RecordSequence {
		return false
	}
	if _, err := slot.Committed.MarshalBinary(); err != nil {
		return false
	}
	for _, proof := range slot.Certificate {
		if _, err := proof.MarshalBinary(); err != nil {
			return false
		}
	}
	return true
}

func (s *Server) receiveForceAdoptCommit(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema, user, name := d.U16(), d.ID(), d.Text()
	request, requestErr := decodeForceDisconnect(d.Blob())
	proposal, proposalErr := decodeControlProposal(d.Blob())
	certificate, certificateErr := client.DecodeCertificate(d.Blob())
	witness, witnessErr := client.DecodeProof(d.Take(56))
	if schema != 1 || requestErr != nil || proposalErr != nil || certificateErr != nil || witnessErr != nil || d.Done() != nil || s.control == nil {
		return nil, client.ErrProtocol
	}
	decision, err := proposal.decision()
	membershipChange := committedControlMembership(proposal)
	epochMatches := proposal.Claim.Base.Membership.Epoch == request.Membership || membershipChange && proposal.NextMembership.Epoch == request.Membership
	if !epochMatches {
		epochMatches = s.acceptsObservedMembership(request.Membership)
	}
	actorMember := hasControlNode(membershipIDs(proposal.Claim.Base.Membership), actor) || membershipChange && hasControlNode(membershipIDs(*proposal.NextMembership), actor)
	if err != nil || (proposal.Kind != controlAdministration && proposal.Kind != controlShutdown && !membershipChange) || !epochMatches || !actorMember || hasControlNode(request.Nodes, actor) || hasControlNode(request.Nodes, witness.Node) || !hasControlNode(proposal.Claim.Required, witness.Node) || !certificate.Covers(proposal.Claim.Required) {
		return nil, errControlConflict
	}
	var ready client.Proof
	for _, proof := range certificate {
		if proof.Node == witness.Node {
			ready = proof
		}
	}
	if ready.RecordSequence == 0 || witness.RecordSequence <= ready.RecordSequence {
		return nil, client.ErrProtocol
	}
	// Administrative identity is checked against the local published state. If
	// the committed parent revokes this admin, the later exclusion is rejected;
	// replay never rolls that parent back to keep stale credentials privileged.
	s.opMu.Lock()
	l := s.control.ledger
	l.mu.Lock()
	slot := l.slot
	current := cloneControlBase(l.current)
	same := slot != nil && slot.Proposal != nil && equalControlProposal(*slot.Proposal, proposal) && containsOwnProof(certificate, slot.Ready)
	already := current.Sequence == decision.Sequence && current.Head == decision.Digest
	joining := current.Membership.Epoch == 0 && monotoneControlJoin(proposal) && hasControlNode(membershipIDs(*proposal.NextMembership), l.node) && !hasControlNode(membershipIDs(proposal.Claim.Base.Membership), l.node)
	if completed, ok := l.completed[decision]; ok && equalControlProposal(*completed.Slot.Proposal, proposal) {
		same = true
	}
	l.mu.Unlock()
	if !same || !already && !joining && !equalControlBase(current, proposal.Claim.Base) {
		s.opMu.Unlock()
		return nil, errControlConflict
	}
	parentView, err := s.loadSystemView(volume.ObjectID(proposal.Image))
	if err != nil {
		s.opMu.Unlock()
		return nil, err
	}
	account, accountErr := s.findAccount(name)
	if joining {
		var accounts accountsImage
		if decodeControlRecord(parentView.accounts, &accounts) != nil {
			s.opMu.Unlock()
			return nil, volume.ErrCorrupt
		}
		accountErr = ErrBucketForbidden
		for _, candidate := range accounts.Users {
			if candidate.ID == user.String() && candidate.Name == name {
				account, accountErr = candidate, nil
				break
			}
		}
	}
	if accountErr != nil || account.ID != user.String() || account.Disabled || account.Role != "ADMIN" {
		s.opMu.Unlock()
		return nil, ErrBucketForbidden
	}
	if !already {
		if _, err := l.commit(decision, certificate); err != nil {
			s.opMu.Unlock()
			return nil, err
		}
	}
	s.opMu.Unlock()
	proof, err := s.control.apply(decision)
	if err != nil {
		return nil, err
	}
	return proof.MarshalBinary()
}

func (c *clusterController) adoptKnownForceParent(ctx context.Context, request forceDisconnectRequest, actor principal) error {
	transport := peerControlParticipants{runtime: c.server.control}
	l := c.coordinator.ledger
	l.mu.Lock()
	base := cloneControlBase(l.current)
	var priorExcluded []client.ID
	if l.slot != nil && l.slot.Claim.Exclusion != nil {
		priorExcluded = append(priorExcluded, l.slot.Claim.Exclusion.Nodes...)
	}
	l.mu.Unlock()
	var states []forceControlState
	var highest *controlSlot
	nodes := append([]client.ID{}, membershipIDs(base.Membership)...)
	seen := map[client.ID]bool{}
	for _, node := range nodes {
		seen[node] = true
	}
	boundJoin := false
	e := client.Encoder{}
	e.U16(1)
	for at := 0; at < len(nodes); at++ {
		node := nodes[at]
		if hasControlNode(request.Nodes, node) || hasControlNode(priorExcluded, node) {
			continue
		}
		if node != l.node {
			c.server.peerStatsMu.RLock()
			observation := c.server.peerStats[node]
			available := observation != nil && observation.online && observation.connection.Capabilities&client.CapabilityControlRecovery != 0
			c.server.peerStatsMu.RUnlock()
			if !available {
				return fail(503, "a bound control participant is unavailable or needs a recovery-capable upgrade")
			}
		}
		raw, err := transport.call(ctx, node, peerForceControlState, e.B)
		if err != nil {
			return err
		}
		var state forceControlState
		if decodeControlRecord(raw, &state) != nil || state.Schema != 1 || state.Node != node {
			return client.ErrProtocol
		}
		for _, slot := range []*controlSlot{state.Slot} {
			if slot == nil {
				continue
			}
			if slot.Suspended != nil {
				slot = slot.Suspended
			}
			if slot.Proposal == nil || slot.Proposal.Kind != controlMembership {
				continue
			}
			for _, participant := range slot.Claim.Required {
				if !hasControlNode(membershipIDs(base.Membership), participant) && slot.Proposal.NextMembership != nil && hasControlNode(membershipIDs(*slot.Proposal.NextMembership), participant) {
					boundJoin = true
				}
				if !seen[participant] {
					if len(nodes) >= 10000 {
						return fail(503, "too many bound control recovery participants")
					}
					seen[participant] = true
					nodes = append(nodes, participant)
				}
			}
		}
		if state.Committed != nil {
			if !validForceCommitWitness(state.Committed) || state.Committed.Committed.Node != node {
				return client.ErrProtocol
			}
			if highest != nil && highest.Decision.Sequence == state.Committed.Decision.Sequence && highest.Decision != state.Committed.Decision {
				return fail(409, "survivors report conflicting committed control decisions")
			}
			if highest == nil || state.Committed.Decision.Sequence > highest.Decision.Sequence {
				highest = state.Committed
			}
		}
		states = append(states, state)
	}
	if highest == nil {
		if boundJoin {
			return fail(409, "uncommitted bound join participants must be recovered before exclusion")
		}
		return nil
	}
	if boundJoin && !monotoneControlJoin(*highest.Proposal) {
		return fail(409, "uncommitted bound join participants must be recovered before exclusion")
	}
	behind := false
	for _, state := range states {
		if state.Current.Sequence < highest.Decision.Sequence {
			behind = true
		}
		if state.Current.Sequence > highest.Decision.Sequence || state.Current.Sequence == highest.Decision.Sequence && state.Current.Head != highest.Decision.Digest {
			return errControlConflict
		}
	}
	if !behind {
		return nil
	}
	proposal := highest.Proposal
	compatibleMembership := equalControlMembership(proposal.Claim.Base.Membership, base.Membership) || committedControlMembership(*proposal) && equalControlMembership(*proposal.NextMembership, base.Membership)
	if (proposal.Kind != controlAdministration && proposal.Kind != controlShutdown && !committedControlMembership(*proposal)) || !compatibleMembership {
		return fail(409, "committed membership recovery must finish before this exclusion")
	}
	proposalWire, err := marshalControlProposal(*proposal)
	if err != nil {
		return err
	}
	certificate, err := highest.Certificate.MarshalBinary()
	if err != nil {
		return err
	}
	witness, err := highest.Committed.MarshalBinary()
	if err != nil {
		return err
	}
	admin, err := parseID(actor.User.ID)
	if err != nil {
		return ErrBucketForbidden
	}
	original := client.Encoder{}
	original.V(uint64(len(request.Nodes)))
	for _, node := range request.Nodes {
		original.ID(node)
	}
	original.Bool(request.AcceptDataLoss)
	original.Bool(request.ExternalFencingConfirmed)
	original.U64(request.Membership)
	message := client.Encoder{}
	message.U16(1)
	message.ID(client.ID(admin))
	message.Text(actor.User.Name)
	message.Blob(original.B)
	message.Blob(proposalWire)
	message.Blob(certificate)
	message.B = append(message.B, witness...)
	// Keep the initiating HTTP/admin session until the peers have advanced. Its
	// own parent installation may legitimately revoke that very account.
	for _, node := range nodes {
		if hasControlNode(request.Nodes, node) || hasControlNode(priorExcluded, node) || node == l.node {
			continue
		}
		if _, err := transport.call(ctx, node, peerForceAdoptCommit, message.B); err != nil {
			return err
		}
	}
	_, err = transport.call(ctx, l.node, peerForceAdoptCommit, message.B)
	return err
}
