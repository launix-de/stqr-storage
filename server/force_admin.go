// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerForceAdmin uint16 = 0x1065

type forceDisconnectRequest struct {
	RecoverOnly                              bool `json:",omitempty"`
	Nodes                                    []client.ID
	AcceptDataLoss, ExternalFencingConfirmed bool
	Membership                               uint64
}

func decodeForceDisconnect(body []byte) (forceDisconnectRequest, error) {
	return decodeForceDisconnectMode(body, false)
}
func decodeForceDisconnectMode(body []byte, recovery bool) (forceDisconnectRequest, error) {
	d := client.Decoder{B: body}
	count := d.Count(10000)
	r := forceDisconnectRequest{RecoverOnly: recovery}
	if count == 0 || count > len(d.B)/16 {
		return r, client.ErrProtocol
	}
	r.Nodes = make([]client.ID, count)
	for i := range r.Nodes {
		r.Nodes[i] = d.ID()
		if r.Nodes[i] == (client.ID{}) || i > 0 && r.Nodes[i-1].String() >= r.Nodes[i].String() {
			d.Err = client.ErrProtocol
		}
	}
	r.AcceptDataLoss, r.ExternalFencingConfirmed, r.Membership = d.Bool(), d.Bool(), d.U64()
	if d.Done() != nil || r.AcceptDataLoss == recovery || !r.ExternalFencingConfirmed || r.Membership == 0 {
		return r, fail(400, "explicit data-loss acceptance and external fencing confirmation are required")
	}
	return r, nil
}

func (s *Server) forceFrozen() bool {
	if s.joinRequested.Load() {
		return true
	}
	if s.control == nil {
		return false
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.slot != nil && l.slot.Claim.Exclusion != nil
}

func (s *Server) validateExclusionActor(claim controlClaim) error {
	if !validExclusionClaim(claim) {
		return client.ErrProtocol
	}
	x := claim.Exclusion
	a, err := s.findAccount(x.Username)
	if err != nil || a.ID != x.Administrator.String() || a.Disabled || a.Role != "ADMIN" {
		return ErrBucketForbidden
	}
	return nil
}

func (s *Server) completedExclusionProposal(id client.ID) *controlProposal {
	if s.control == nil {
		return nil
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, done := range l.completed {
		p := done.Slot.Proposal
		if p != nil && p.Kind == controlForceDisconnect && p.Claim.Exclusion.Operation == id {
			copy := *p
			return &copy
		}
	}
	for _, ancestor := range l.ancestors {
		p := ancestor.Proposal
		if p != nil && p.Kind == controlForceDisconnect && p.Claim.Exclusion.Operation == id {
			copy := *p
			return &copy
		}
	}
	return nil
}
func (s *Server) completedExclusion(id client.ID) *controlExclusion {
	p := s.completedExclusionProposal(id)
	if p != nil && p.Claim.Round.Initiator == s.control.ledger.node {
		x := *p.Claim.Exclusion
		return &x
	}
	return nil
}

func (s *Server) forceDisconnect(m client.Message, actor principal) ([]byte, int, error) {
	if m.Kind != client.Request || m.ID == (client.ID{}) || s.controller == nil {
		return nil, 0, fail(400, "force disconnect requires a cluster request")
	}
	request, err := decodeForceDisconnectMode(m.Body, m.Opcode == client.NodeRecoveryBegin)
	if err != nil {
		return nil, 0, err
	}
	s.opMu.Lock()
	if err := s.requireAdmin(actor); err != nil {
		s.opMu.Unlock()
		return nil, 0, err
	}
	// Check an existing local receipt before comparing the now-advanced epoch.
	// beginOperation binds the complete original body, including loss flags.
	var prior Operation
	priorErr := s.readBlob("admin-op/"+m.ID.String(), &prior)
	if priorErr == nil {
		op, _, err := s.beginOperation(m.ID.String(), request.operationKind(), request)
		if err == nil && s.completedExclusion(m.ID) != nil {
			op, err = s.finishOperation(op, nil)
		}
		if err != nil {
			s.opMu.Unlock()
			return nil, 0, err
		}
		if op.State == "fulfilled" || op.State == "failed" {
			s.opMu.Unlock()
			if op.Status >= 400 {
				return nil, 0, fail(op.Status, op.Detail)
			}
			return encodeAck(op), 200, nil
		}
	} else if !errors.Is(priorErr, volume.ErrNotFound) {
		s.opMu.Unlock()
		return nil, 0, priorErr
	}
	if done := s.completedExclusionProposal(m.ID); done != nil {
		x := done.Claim.Exclusion
		if request.Membership != exclusionObservedEpoch(done.Claim) || !reflect.DeepEqual(x.Nodes, request.Nodes) || x.RecoverOnly != request.RecoverOnly || request.AcceptDataLoss == request.RecoverOnly || !request.ExternalFencingConfirmed {
			s.opMu.Unlock()
			return nil, 0, fail(400, "operation ID reused with different exclusion input")
		}
		s.opMu.Unlock()
		return encodeAck(Operation{ID: m.ID.String(), Kind: "force-disconnect", State: "fulfilled", Status: 200}), 200, nil
	}
	if pending := s.pendingExclusionClaim(m.ID); pending != nil {
		if !matchesExclusionRequest(*pending, request) {
			s.opMu.Unlock()
			return nil, 0, fail(400, "operation ID reused with different exclusion input")
		}
		s.opMu.Unlock()
		own := s.controller.coordinator.requestSnapshot()
		if own != nil && own.Claim.Round == pending.Round {
			return s.resumeOwnExclusion(m, request, actor)
		}
		return encodeAck(Operation{ID: m.ID.String(), Kind: "force-disconnect", State: "running", Status: 202}), 202, nil
	}
	view := s.currentSystem.Load()
	if view == nil || !s.acceptsObservedMembership(request.Membership) {
		s.opMu.Unlock()
		return nil, 0, fail(409, "membership changed before force disconnect")
	}
	adminID, err := parseID(actor.User.ID)
	if err != nil {
		s.opMu.Unlock()
		return nil, 0, ErrBucketForbidden
	}
	x := &controlExclusion{ExpectedMembership: request.Membership, Operation: m.ID, Nodes: request.Nodes, Administrator: client.ID(adminID), Username: actor.User.Name, AcceptDataLoss: request.AcceptDataLoss, ExternalFencingConfirmed: true, RecoverOnly: request.RecoverOnly}
	x.AlreadyRetired = s.knownRetiredVictims(request.Membership, request.Nodes)
	next, err := exclusionMembership(view.manifest.Membership, x)
	if err != nil {
		s.opMu.Unlock()
		return nil, 0, fail(400, "exclusion must remove current members and retain a survivor")
	}
	s.opMu.Unlock()
	// This is an explicitly confirmed fencing operation, not timeout-based
	// failure detection. Break outstanding RPC waits to those instances before
	// waiting for the local coordinator mutex; their packets cannot complete a
	// safe new round. Membership still changes only through the survivor commit.
	s.closeExcludedTransports(client.Membership{Excluded: request.Nodes})
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if pending := c.coordinator.requestSnapshot(); pending == nil || pending.Claim.Exclusion == nil || pending.Claim.Exclusion.Operation != m.ID {
		if err := c.adoptKnownForceParent(s.ctx, request, actor); err != nil {
			return nil, 0, err
		}
		s.opMu.Lock()
		if err := s.requireAdmin(actor); err != nil {
			s.opMu.Unlock()
			return nil, 0, err
		}
		view = s.currentSystem.Load()
		if view == nil || !s.acceptsObservedMembership(request.Membership) {
			s.opMu.Unlock()
			return nil, 0, fail(409, "membership advanced during committed control recovery")
		}
		x.AlreadyRetired = s.knownRetiredVictims(request.Membership, request.Nodes)
		next, err = exclusionMembership(view.manifest.Membership, x)
		s.opMu.Unlock()
		if err != nil {
			return nil, 0, err
		}
		if next.Members[0].ID != s.control.ledger.node {
			return s.delegateForceDisconnect(m, actor, next.Members[0].ID)
		}
	}
	if pending := c.coordinator.requestSnapshot(); pending != nil && pending.Claim.Exclusion != nil && pending.Claim.Exclusion.Operation == m.ID {
		if pending.Claim.Exclusion == nil || pending.Claim.Exclusion.Operation != m.ID {
			return nil, 0, fail(409, "a previous control decision must be recovered before exclusion")
		}
		if _, err := c.resume(s.ctx); err != nil {
			s.opMu.Lock()
			op, loadErr := s.operation(m.ID.String())
			s.opMu.Unlock()
			if loadErr != nil {
				return nil, 0, loadErr
			}
			if op.State == "failed" {
				return nil, 0, fail(op.Status, op.Detail)
			}
			op.State, op.Status = "running", 202
			return encodeAck(op), 202, nil
		}
		if s.completedExclusion(m.ID) != nil {
			s.opMu.Lock()
			op, err := s.operation(m.ID.String())
			if err == nil {
				op, err = s.finishOperation(op, nil)
			}
			s.opMu.Unlock()
			return encodeAck(op), 200, err
		}
	}
	// Do not strand a schema-3 requester on a peer which cannot even decode
	// the corresponding abort. All remaining members must support this path.
	for _, member := range next.Members {
		if member.ID == s.control.ledger.node {
			continue
		}
		s.peerStatsMu.RLock()
		o := s.peerStats[member.ID]
		available := o != nil && o.online && o.connection.Capabilities&client.CapabilityControlRecovery != 0 && (!request.RecoverOnly || o.connection.Capabilities&client.CapabilityRecoveryFenceClearance != 0)
		s.peerStatsMu.RUnlock()
		if !available {
			return nil, 0, fail(503, "a required survivor is unavailable or needs an exclusion-capable upgrade")
		}
	}
	s.opMu.Lock()
	if err := s.requireAdmin(actor); err != nil {
		s.opMu.Unlock()
		return nil, 0, err
	}
	if current := s.currentSystem.Load(); current != view {
		s.opMu.Unlock()
		return nil, 0, fail(409, "control state changed before exclusion")
	}
	op, _, err := s.beginOperation(m.ID.String(), request.operationKind(), request)
	s.opMu.Unlock()
	if err != nil {
		return nil, 0, err
	}
	_, _, err = c.changeWithExclusion(s.ctx, &next, nil, nil, x)
	if err != nil {
		pending := c.coordinator.requestSnapshot()
		if pending != nil && pending.Claim.Exclusion != nil && pending.Claim.Exclusion.Operation == m.ID {
			op.State, op.Status = "running", 202
			s.opMu.Lock()
			saveErr := s.saveOperation(op)
			s.opMu.Unlock()
			if saveErr != nil {
				return nil, 0, saveErr
			}
			return encodeAck(op), 202, nil
		}
		s.opMu.Lock()
		_, saveErr := s.finishOperation(op, err)
		s.opMu.Unlock()
		if saveErr != nil {
			return nil, 0, saveErr
		}
		return nil, 0, err
	}
	s.opMu.Lock()
	op, err = s.finishOperation(op, nil)
	s.opMu.Unlock()
	return encodeAck(op), 200, err
}

func (s *Server) delegateForceDisconnect(m client.Message, actor principal, node client.ID) ([]byte, int, error) {
	user, err := parseID(actor.User.ID)
	if err != nil {
		return nil, 0, ErrBucketForbidden
	}
	e := client.Encoder{}
	if m.Opcode == client.NodeRecoveryBegin {
		e.U16(2)
		e.U16(m.Opcode)
	} else {
		e.U16(1)
	}
	e.ID(client.ID(user))
	e.Text(actor.User.Name)
	e.ID(m.ID)
	e.Blob(m.Body)
	body, err := (peerControlParticipants{runtime: s.control}).call(s.ctx, node, peerForceAdmin, e.B)
	if err != nil {
		return nil, 0, err
	}
	d := client.Decoder{B: body}
	status := int(d.U16())
	result := d.Blob()
	if d.Done() != nil || status < 200 || status >= 300 {
		return nil, 0, client.ErrProtocol
	}
	return result, status, nil
}

func (s *Server) receiveForceAdmin(ctx context.Context, body []byte, node client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	opcode := client.ForceDisconnect
	if schema == 2 {
		opcode = d.U16()
	}
	user, name, operation := d.ID(), d.Text(), d.ID()
	inner := d.Blob()
	if (schema != 1 && schema != 2) || opcode != client.ForceDisconnect && opcode != client.NodeRecoveryBegin || d.Done() != nil {
		return nil, client.ErrProtocol
	}
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), node) {
		return nil, ErrBucketForbidden
	}
	a, err := s.findAccount(name)
	if err != nil || a.ID != user.String() || a.Disabled || a.Role != "ADMIN" {
		return nil, ErrBucketForbidden
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, status, err := s.forceDisconnect(client.Message{Kind: client.Request, Opcode: opcode, ID: operation, Body: inner}, principal{User: a})
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U16(uint16(status))
	e.Blob(result)
	return e.B, nil
}

func (c *clusterController) finishExclusionReceipts() {
	s := c.server
	l := c.coordinator.ledger
	l.mu.Lock()
	var ids []client.ID
	for _, done := range l.completed {
		p := done.Slot.Proposal
		if p != nil && p.Kind == controlForceDisconnect && p.Claim.Round.Initiator == l.node && !c.finishedExclusions[p.Claim.Exclusion.Operation] {
			ids = append(ids, p.Claim.Exclusion.Operation)
		}
	}
	for _, ancestor := range l.ancestors {
		p := ancestor.Proposal
		if p != nil && p.Kind == controlForceDisconnect && p.Claim.Round.Initiator == l.node && !c.finishedExclusions[p.Claim.Exclusion.Operation] {
			ids = append(ids, p.Claim.Exclusion.Operation)
		}
	}
	l.mu.Unlock()
	for _, id := range ids {
		s.opMu.Lock()
		op, err := s.operation(id.String())
		if err == nil && (op.Kind == "force-disconnect" || op.Kind == "node-recovery" || op.Kind == "volume-recovery") {
			if op.State != "fulfilled" {
				_, err = s.finishOperation(op, nil)
			}
		}
		s.opMu.Unlock()
		if err == nil || errors.Is(err, volume.ErrNotFound) {
			if c.finishedExclusions == nil {
				c.finishedExclusions = make(map[client.ID]bool)
			}
			c.finishedExclusions[id] = true
		}
	}
}

func (s *Server) closeExcludedTransports(membership client.Membership) {
	if len(membership.Excluded) == 0 {
		return
	}
	s.peerStatsMu.RLock()
	var peers []*peerConnection
	for node, o := range s.peerStats {
		if hasControlNode(membership.Excluded, node) && o.connection != nil {
			peers = append(peers, o.connection)
		}
	}
	s.peerStatsMu.RUnlock()
	for _, peer := range peers {
		peer.close()
	}
	s.wsMu.Lock()
	var sockets []*wsSession
	for ws := range s.sockets {
		if ws.authorization != nil && ws.authorization.clusterPeer {
			id, _ := parseID(ws.principal.User.ID)
			if hasControlNode(membership.Excluded, client.ID(id)) {
				sockets = append(sockets, ws)
			}
		}
	}
	s.wsMu.Unlock()
	for _, ws := range sockets {
		ws.close()
	}
}

func (r forceDisconnectRequest) operationKind() string {
	if r.RecoverOnly {
		return "node-recovery"
	}
	return "force-disconnect"
}
