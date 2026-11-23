// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"

	"github.com/launix-de/stqr-storage/client"
)

const peerForceControlState uint16 = 0x1068

// This diagnostic does not grant a new promise or revoke an old one. A force
// coordinator must first discover the highest known durable COMMIT among every
// survivor; a mere READY or requester result is never evidence of COMMIT.
// Fields are versioned explicitly and contain immutable object hashes/proofs,
// never the raw account or bucket mutation that produced the candidate image.
type forceControlState struct {
	Schema    uint16
	Node      client.ID
	Current   controlBase
	Slot      *controlSlot
	Committed *controlSlot
}

func (s *Server) receiveForceControlState(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	if d.U16() != 1 || d.Done() != nil || s.control == nil {
		return nil, client.ErrProtocol
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	admitted := hasControlNode(membershipIDs(l.current.Membership), actor)
	if !admitted && l.slot != nil {
		admitted = hasControlNode(l.slot.Claim.Required, actor)
	}
	if !admitted {
		return nil, ErrBucketForbidden
	}
	state := forceControlState{Schema: 1, Node: l.node, Current: l.current, Slot: l.slot}
	consider := func(slot *controlSlot) {
		if slot != nil && slot.Committed.RecordSequence != 0 && (state.Committed == nil || slot.Decision.Sequence > state.Committed.Decision.Sequence) {
			state.Committed = slot
		}
	}
	consider(l.slot)
	if l.slot != nil {
		consider(l.slot.Suspended)
	}
	for _, completed := range l.completed {
		copy := completed.Slot
		consider(&copy)
	}
	for _, ancestor := range l.ancestors {
		copy := ancestor
		consider(&copy)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if len(raw) > client.MaxMessage-64 {
		return nil, fail(503, "control recovery metadata exceeds message budget")
	}
	return raw, nil
}
