// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerFrozenBucketUsage uint16 = 0x1048

func (s *Server) bucketFrozen(bucket volume.ID) bool {
	if s.clusterPaused() {
		return true
	}
	if s.control == nil {
		return false
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.slot != nil && (l.slot.Claim.Exclusion != nil || hasControlNode(l.slot.Claim.FrozenBuckets, client.ID(bucket)) || l.slot.Proposal != nil && l.slot.Proposal.Kind == controlShutdown)
}

// The slot itself durably carries the fence. Reserve drains in-flight local
// mutations under opMu; Release/Abort remove it. Recovery cannot forget a fence
// while an administrator's quota/policy decision is still incomplete.
func (s *Server) frozenBucketUsage(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	claim, err := decodeControlClaim(d.Blob())
	bucket := d.ID()
	if err != nil || d.Done() != nil || claim.Round.Initiator != actor || !hasControlNode(claim.FrozenBuckets, bucket) || s.control == nil {
		return nil, fail(403, "invalid bucket mutation fence")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	l := s.control.ledger
	l.mu.Lock()
	valid := l.slot != nil && equalControlClaim(l.slot.Claim, claim)
	l.mu.Unlock()
	if !valid {
		return nil, errControlConflict
	}
	b, err := s.Bucket(volume.ID(bucket))
	if err != nil {
		return nil, err
	}
	used, err := b.logicalUsage()
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U64(used)
	return e.B, nil
}

func (c *clusterController) frozenUsage(ctx context.Context, bucket client.ID) (uint64, error) {
	request := c.coordinator.requestSnapshot()
	if request == nil || !hasControlNode(request.Claim.FrozenBuckets, bucket) {
		return 0, errControlConflict
	}
	wire, err := marshalControlClaim(request.Claim)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, node := range request.Claim.Required {
		e := client.Encoder{}
		e.Blob(wire)
		e.ID(bucket)
		body, err := (peerControlParticipants{runtime: c.server.control}).call(ctx, node, peerFrozenBucketUsage, e.B)
		if err != nil {
			return 0, err
		}
		d := client.Decoder{B: body}
		used := d.U64()
		if d.Done() != nil || used > ^uint64(0)-total {
			return 0, volume.ErrRange
		}
		total += used
	}
	return total, nil
}
