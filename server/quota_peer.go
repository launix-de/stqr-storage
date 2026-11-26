// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func quotaPeerOpcode(op uint16) bool {
	return op >= peerQuotaCheckpoint && op <= peerQuotaAck || op == peerQuotaPhysical || op == peerQuotaPreview
}
func encodeQuotaGrant(g quotaGrant) ([]byte, error) {
	proof, err := g.Debit.MarshalBinary()
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U16(1)
	e.ID(g.ID)
	e.ID(g.Bucket)
	e.ID(g.Epoch)
	e.ID(g.Source)
	e.ID(g.Target)
	e.U64(g.Bytes)
	e.B = append(e.B, proof...)
	return e.B, nil
}
func decodeQuotaGrant(body []byte) (quotaGrant, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	g := quotaGrant{ID: d.ID(), Bucket: d.ID(), Epoch: d.ID(), Source: d.ID(), Target: d.ID(), Bytes: d.U64()}
	var err error
	g.Debit, err = client.DecodeProof(d.Take(56))
	if schema != 1 || err != nil || d.Done() != nil || !validQuotaGrant(g) || g.Debit.Node != g.Source {
		return quotaGrant{}, client.ErrProtocol
	}
	return g, nil
}

func (s *Server) quotaControlBusy(bucket client.ID) bool {
	b, err := s.Bucket(volume.ID(bucket))
	global := err != nil || b.Describe().Policy.PlacementGroup == ""
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.slot != nil && (global || l.slot.Claim.GlobalBarrier || l.slot.Claim.Exclusion != nil || hasControlNode(l.slot.Claim.FrozenBuckets, bucket))
}

func (s *Server) quotaPeerMessage(m client.Message, actor client.ID) ([]byte, error) {
	if m.Kind != client.Request || s.quotas == nil {
		return nil, client.ErrProtocol
	}
	if m.Opcode == peerQuotaPreview {
		return s.quotaPreview(m.Body, actor)
	}
	if m.Opcode == peerQuotaPhysical {
		return s.quotaPhysicalSnapshot(m.Body, actor)
	}
	if m.Opcode == peerQuotaCheckpoint {
		return s.quotaCheckpoint(m.Body, actor)
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, errControlConflict
	}
	q := s.quotas
	switch m.Opcode {
	case peerQuotaRequest:
		d := client.Decoder{B: m.Body}
		schema := d.U16()
		id, bucket, epoch, needed := d.ID(), d.ID(), d.ID(), d.U64()
		if schema != 1 || d.Done() != nil || id == (client.ID{}) || needed == 0 {
			return nil, client.ErrProtocol
		}
		a := findQuota(view, bucket)
		if a == nil || a.Epoch != epoch {
			return nil, errControlConflict
		}
		q.ledger.mu.Lock()
		prior := q.ledger.outgoing[id]
		q.ledger.mu.Unlock()
		if prior != nil {
			if prior.grant.Target != actor || prior.grant.Bucket != bucket || prior.grant.Epoch != epoch {
				return nil, errControlConflict
			}
			e := client.Encoder{}
			wire, err := encodeQuotaGrant(prior.grant)
			if err != nil {
				return nil, err
			}
			e.Bool(true)
			e.Blob(wire)
			return e.B, nil
		}
		if s.quotaControlBusy(bucket) || s.shortStopping.Load() {
			return nil, fail(503, "quota grants are paused")
		}
		b, err := s.Bucket(volume.ID(bucket))
		if err != nil {
			return nil, err
		}
		if b.Describe().State == BucketDeleting {
			return nil, errControlConflict
		}
		allowance, err := s.quotaAllowance(bucket)
		if err != nil {
			return nil, err
		}
		used, err := b.accountedUsage()
		if err != nil {
			return nil, err
		}
		used = max(used, s.heldPhysicalQuota(bucket))
		free := q.lendable(volume.ID(bucket), actor, allowance, used)
		amount := min(needed, free)
		e := client.Encoder{}
		e.Bool(amount != 0)
		if amount != 0 {
			g, err := q.ledger.debit(quotaGrant{ID: id, Bucket: bucket, Epoch: epoch, Source: q.ledger.node, Target: actor, Bytes: amount}, free)
			if err != nil {
				return nil, err
			}
			wire, err := encodeQuotaGrant(g)
			if err != nil {
				return nil, err
			}
			e.Blob(wire)
			select {
			case q.wake <- struct{}{}:
			default:
			}
		}
		return e.B, nil
	case peerQuotaGrant:
		g, err := decodeQuotaGrant(m.Body)
		if err != nil {
			return nil, err
		}
		a := findQuota(view, g.Bucket)
		if a == nil {
			return nil, errControlConflict
		}
		q.ledger.mu.Lock()
		prior := q.ledger.incoming[g.ID]
		q.ledger.mu.Unlock()
		if prior == nil && s.quotaControlBusy(g.Bucket) {
			return nil, fail(503, "quota checkpoint is pending")
		}
		proof, err := q.ledger.receive(g, actor, a.Epoch)
		if err != nil {
			return nil, err
		}
		return proof.MarshalBinary()
	case peerQuotaAck:
		d := client.Decoder{B: m.Body}
		schema := d.U16()
		id := d.ID()
		proof, err := client.DecodeProof(d.Take(56))
		if schema != 1 || d.Done() != nil || err != nil {
			return nil, client.ErrProtocol
		}
		return nil, q.ledger.settle(id, proof, actor)
	}
	return nil, client.ErrProtocol
}

func (s *Server) startQuotaWorker() {
	q := s.quotas
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
			case <-q.wake:
			}
			// Independent of request handlers: no peer's request queue waits
			// for a callback into a node which is itself awaiting that queue.
			for _, g := range s.pendingQuotaGrants() {
				body, err := encodeQuotaGrant(g)
				if err != nil {
					continue
				}
				ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
				reply, err := (peerControlParticipants{runtime: s.control}).call(ctx, g.Target, peerQuotaGrant, body)
				cancel()
				if err == nil {
					proof, e := client.DecodeProof(reply)
					if e == nil {
						s.opMu.Lock()
						_ = q.ledger.settle(g.ID, proof, g.Target)
						s.opMu.Unlock()
					}
				}
			}
			q.mu.Lock()
			demands := append([]quotaDemand{}, q.demands...)
			q.mu.Unlock()
			for _, h := range demands {
				if !h.exhausted && time.Now().Before(h.until) {
					s.fulfillQuotaDemand(h)
				}
			}
		}
	}()
}

func (s *Server) fulfillQuotaDemand(h quotaDemand) {
	q := s.quotas
	s.opMu.Lock()
	view := s.currentSystem.Load()
	a := findQuota(view, client.ID(h.bucket))
	if a == nil || s.quotaControlBusy(client.ID(h.bucket)) || s.shortStopping.Load() {
		s.opMu.Unlock()
		return
	}
	physicalReplica := false
	if bucket, err := s.Bucket(h.bucket); err == nil {
		physicalReplica = quotaPolicy(bucket.storageDescription()).Limits.Accounting == "physical"
	}
	if err := s.checkObjectOwner(h.bucket, h.object, physicalReplica); err != nil && !h.activation {
		s.opMu.Unlock()
		q.fulfilled(h.bucket, h.object)
		return
	}
	b, err := s.Bucket(h.bucket)
	if err != nil {
		s.opMu.Unlock()
		return
	}
	allowance, err := s.quotaAllowance(client.ID(h.bucket))
	if err != nil {
		s.opMu.Unlock()
		return
	}
	used, err := b.accountedUsage()
	s.opMu.Unlock()
	if err != nil {
		return
	}
	available := subtractCapacity(subtractCapacity(allowance, used), q.reserveHints(h.bucket, &h.object, used))
	if !h.activation && available >= h.growth {
		return
	}
	needed := h.growth - available
	if h.activation {
		if allowance >= h.growth {
			return
		}
		needed = h.growth - allowance
	}
	allAnswered := true
	// A range handoff transfers used credits, so a live cluster member can
	// still hold unused credits after leaving this bucket's placement mask.
	// Ask every current member; the donor checks the quota epoch and debits
	// only its unused allowance. Object ownership remains checked above.
	for _, member := range view.manifest.Membership.Members {
		if member.ID == q.ledger.node {
			continue
		}
		e := client.Encoder{}
		e.U16(1)
		e.ID(client.NewRequestID())
		e.ID(client.ID(h.bucket))
		e.ID(a.Epoch)
		// Amortize the durable grant exchange across several native 1-MiB
		// blocks. A 64-KiB budget throttles a new owner to one small write per
		// worker/ledger round trip after a handoff. The donor still caps this
		// request at its unused allowance, preserving the global hard quota.
		// Small quotas shared by replica owners need smaller reusable shares:
		// do not drain the donor's entire remaining budget after one write.
		reusable := min(uint64(64<<20), max(uint64(64<<10), a.Limit/uint64(4*len(view.manifest.Membership.Members))))
		asked := max(needed, reusable)
		e.U64(asked)
		ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
		body, err := (peerControlParticipants{runtime: s.control}).call(ctx, member.ID, peerQuotaRequest, e.B)
		cancel()
		if err != nil {
			allAnswered = false
			continue
		}
		d := client.Decoder{B: body}
		hasGrant := d.Bool()
		var wire []byte
		if hasGrant {
			wire = d.Blob()
		}
		if d.Done() != nil {
			allAnswered = false
			continue
		}
		if !hasGrant {
			continue
		}
		g, err := decodeQuotaGrant(wire)
		if err != nil || g.Bytes > asked || g.Target != q.ledger.node || g.Source != member.ID || g.Bucket != client.ID(h.bucket) || g.Epoch != a.Epoch {
			allAnswered = false
			continue
		}
		reply, err := s.quotaPeerMessage(client.Message{Kind: client.Request, Opcode: peerQuotaGrant, Body: wire}, member.ID)
		if err != nil {
			allAnswered = false
			continue
		}
		ack := client.Encoder{}
		ack.U16(1)
		ack.ID(g.ID)
		ack.B = append(ack.B, reply...)
		ctx, cancel = context.WithTimeout(s.ctx, 3*time.Second)
		_, _ = (peerControlParticipants{runtime: s.control}).call(ctx, member.ID, peerQuotaAck, ack.B)
		cancel()
		needed -= min(needed, g.Bytes)
		if needed == 0 {
			return
		}
	}
	if allAnswered {
		q.mu.Lock()
		for i, d := range q.demands {
			if d.bucket == h.bucket && d.object == h.object && d.growth == h.growth {
				q.demands[i].exhausted = true
				q.demands[i].until = time.Now().Add(3 * time.Second)
			}
		}
		q.mu.Unlock()
	}
}

// An explicit exclusion establishes fresh allocations measured under the ALL
// survivor fence. Grants from a superseded epoch remain historical debits, not
// refunds, and cannot be received/spent against that new allocation. They must
// not block later administrative decisions waiting on an excluded recipient.
func (s *Server) pendingQuotaGrants() []quotaGrant {
	if s.quotas == nil {
		return nil
	}
	view := s.currentSystem.Load()
	var pending []quotaGrant
	for _, grant := range s.quotas.ledger.pending() {
		allocation := findQuota(view, grant.Bucket)
		if allocation != nil && allocation.Epoch == grant.Epoch {
			pending = append(pending, grant)
		}
	}
	return pending
}
