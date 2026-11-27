// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"github.com/launix-de/stqr-storage/client"
)

func directedCOWRequiredSemantics() map[uint16]uint16 {
	result := map[uint16]uint16{client.BucketClone: 4, peerControlInfo: 3, peerSystemChunk: 3}
	for _, op := range []uint16{client.PeerControlReserve, client.PeerControlPrepare, client.PeerControlCommit, client.PeerControlApply, client.PeerControlRelease, client.PeerControlAbort} {
		result[op] = 4
	}
	for op := peerCowCapture; op <= peerCowSync; op++ {
		result[op] = 3
	}
	return result
}

// Schema-3 inheritance may be activated only after every committed member
// advertises the matching semantics. Profiles come from authenticated node
// discovery, never an unauthenticated client header. An offline/unknown/older
// member remains a member: this gate never drains it or changes ownership.
// The control exchange repeats the operation gate on the actual connection,
// closing the race with a peer which restarts into an older build afterwards.
func (s *Server) requireDirectedCOWCompatibility(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	local := s.protocolVersion()
	required := directedCOWRequiredSemantics()
	for op, revision := range required {
		sem := local.semantics(op)
		if sem.Emit != revision || sem.Min > revision || sem.Max < revision {
			return fail(426, fmt.Sprintf("directed COW requires semantics %d for opcode 0x%04x", revision, op))
		}
	}
	if s.controller == nil {
		return nil
	}
	view := s.currentSystem.Load()
	if view == nil {
		return fail(503, "directed COW membership is not available")
	}
	for _, member := range view.manifest.Membership.Members {
		if member.ID.String() == s.running.Node.ID {
			continue
		}
		remote := s.issuerProfile(member.ID)
		for op := range required {
			if err := checkPeerOperation(local, remote, op); err != nil {
				return fail(426, "directed COW requires compatible control and inheritance semantics on every cluster member")
			}
		}
	}
	if s.currentSystem.Load() != view {
		return fail(503, "directed COW membership changed during compatibility check")
	}
	return ctx.Err()
}
