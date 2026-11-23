// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"reflect"

	"github.com/launix-de/stqr-storage/client"
)

func (s *Server) pendingExclusionClaim(id client.ID) *controlClaim {
	if s.control == nil {
		return nil
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	match := func(claim controlClaim) *controlClaim {
		if claim.Exclusion != nil && claim.Exclusion.Operation == id {
			copy := claim
			return &copy
		}
		return nil
	}
	if l.request != nil {
		if claim := match(l.request.Claim); claim != nil {
			return claim
		}
	}
	if l.slot != nil {
		if claim := match(l.slot.Claim); claim != nil {
			return claim
		}
		if l.slot.Suspended != nil {
			return match(l.slot.Suspended.Claim)
		}
	}
	return nil
}

func matchesExclusionRequest(claim controlClaim, request forceDisconnectRequest) bool {
	return claim.Exclusion != nil && exclusionObservedEpoch(claim) == request.Membership && reflect.DeepEqual(claim.Exclusion.Nodes, request.Nodes) && claim.Exclusion.RecoverOnly == request.RecoverOnly && request.AcceptDataLoss != request.RecoverOnly && request.ExternalFencingConfirmed
}

func (s *Server) resumeOwnExclusion(m client.Message, request forceDisconnectRequest, actor principal) ([]byte, int, error) {
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	s.opMu.Lock()
	if err := s.requireAdmin(actor); err != nil {
		s.opMu.Unlock()
		return nil, 0, err
	}
	op, _, err := s.beginOperation(m.ID.String(), request.operationKind(), request)
	s.opMu.Unlock()
	if err != nil {
		return nil, 0, err
	}
	_, resumeErr := c.resume(s.ctx)
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.completedExclusionProposal(m.ID) != nil {
		op, err = s.finishOperation(op, nil)
		return encodeAck(op), 200, err
	}
	if updated, loadErr := s.operation(m.ID.String()); loadErr == nil {
		op = updated
	}
	if op.State == "failed" {
		return nil, 0, fail(op.Status, op.Detail)
	}
	// The original durable intent remains pending, even after its own APPLY.
	// Another member may still be absent; never start a second exclusion here.
	op.State, op.Status = "running", 202
	if resumeErr != nil {
		op.Detail = resumeErr.Error()
	}
	if err := s.saveOperation(op); err != nil {
		return nil, 0, err
	}
	return encodeAck(op), 202, nil
}
