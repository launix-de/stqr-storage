// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
)

const peerForceScopeEnvelope uint16 = 0x1066

type forceScopeKey struct{}

func (s *Server) scopedTransition(claim *controlClaim) error {
	if s.control == nil {
		if claim != nil {
			return errControlConflict
		}
		return nil
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	frozen := l.slot != nil && l.slot.Claim.Exclusion != nil
	if claim == nil {
		if frozen {
			return fail(503, "range transitions are paused for explicit exclusion")
		}
		return nil
	}
	if !frozen || !l.slot.ConstructionStarted || !equalControlClaim(l.slot.Claim, *claim) {
		return errControlConflict
	}
	return nil
}

func (s *Server) receiveForceScopeEnvelope(body []byte, actor, boot client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	claim, err := decodeControlClaim(d.Blob())
	opcode := d.U16()
	inner := d.Blob()
	if schema != 1 || err != nil || d.Done() != nil || claim.Exclusion == nil || !hasControlNode(claim.Required, actor) {
		return nil, client.ErrProtocol
	}
	if opcode < peerRangeSnapshot || opcode > peerRangeCommit && opcode != peerRangeProbe && opcode != peerRangeState && opcode != peerRangeRAMLostCommit {
		return nil, client.ErrProtocol
	}
	if err := s.scopedTransition(&claim); err != nil {
		return nil, err
	}
	return s.rangePeerMessagePermitted(client.Message{Kind: client.Request, Opcode: opcode, Body: inner}, actor, []client.ID{boot}, &claim)
}

func forceScopeContext(ctx context.Context) *controlClaim {
	claim, _ := ctx.Value(forceScopeKey{}).(*controlClaim)
	return claim
}
