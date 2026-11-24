// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
)

func (c rangeCommit) marshal() ([]byte, error) {
	source, err := c.Source.MarshalBinary()
	if err != nil {
		return nil, err
	}
	target, err := c.Target.MarshalBinary()
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	schema := uint16(2)
	if c.QuotaUsed != 0 {
		schema = 3
	}
	e.U16(schema)
	e.B = append(e.B, c.ID[:]...)
	e.B = append(e.B, c.Digest[:]...)
	e.B = append(e.B, source...)
	e.B = append(e.B, target...)
	e.U64(c.LogicalUsed)
	if schema == 3 {
		e.U64(c.QuotaUsed)
	}
	return e.B, nil
}
func decodeRangeCommit(body []byte) (rangeCommit, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	var c rangeCommit
	copy(c.ID[:], d.Take(32))
	copy(c.Digest[:], d.Take(32))
	var a, b error
	c.Source, a = client.DecodeProof(d.Take(56))
	c.Target, b = client.DecodeProof(d.Take(56))
	if schema >= 2 {
		c.LogicalUsed = d.U64()
	}
	if schema == 3 {
		c.QuotaUsed = d.U64()
	}
	if (schema != 1 && schema != 2 && schema != 3) || a != nil || b != nil || d.Done() != nil {
		return rangeCommit{}, client.ErrProtocol
	}
	return c, nil
}

func (s *Server) rangePeerMessage(m client.Message, actor client.ID, sourceBoot ...client.ID) ([]byte, error) {
	return s.rangePeerMessagePermitted(m, actor, sourceBoot, nil)
}
func (s *Server) rangePeerMessagePermitted(m client.Message, actor client.ID, sourceBoot []client.ID, force *controlClaim) ([]byte, error) {
	if m.Opcode != peerRangeArchive && m.Opcode != peerRangeState {
		if err := s.scopedTransition(force); err != nil {
			return nil, err
		}
	}

	if m.Kind != client.Request && !(m.Kind == client.FireAndForget && m.Opcode == peerRangeObserve) {
		return nil, fail(400, "range transition requires a request")
	}
	r := s.scopes
	if r == nil {
		return nil, fail(503, "range controller is not active")
	}
	if m.Opcode == peerRangeState {
		d := client.Decoder{B: m.Body}
		schema := d.U16()
		var id [32]byte
		copy(id[:], d.Take(32))
		if schema != 1 || d.Done() != nil {
			return nil, client.ErrProtocol
		}
		return r.ramStateReply(id, actor)
	}
	if m.Opcode == peerRangeRAMAbort || m.Opcode == peerRangeRAMLostCommit {
		d := client.Decoder{B: m.Body}
		wire := d.Blob()
		oldBoot, newBoot := d.ID(), d.ID()
		if d.Done() != nil || len(sourceBoot) == 0 || sourceBoot[0] != newBoot {
			return nil, fail(403, "RAM loss process identity mismatch")
		}
		if m.Opcode == peerRangeRAMAbort {
			plan, err := decodeRangePlan(wire)
			if err != nil {
				return nil, err
			}
			if plan.Source != actor {
				return nil, errControlConflict
			}
			return nil, r.abortLostRAM(plan, oldBoot, newBoot)
		}
		commit, err := decodeRangeCommit(wire)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		t := r.txns[commit.ID]
		valid := t != nil && t.plan.Source == actor
		r.mu.Unlock()
		if !valid {
			return nil, errControlConflict
		}
		proof, err := r.commitLostRAMWithForce(commit, oldBoot, newBoot, force)
		if err != nil {
			return nil, err
		}
		return proof.MarshalBinary()
	}
	if m.Opcode == peerRangeAbort {
		p, err := decodeRangePlan(m.Body)
		if err != nil {
			return nil, err
		}
		if p.Source != actor {
			return nil, fail(403, "range abort source identity mismatch")
		}
		return nil, r.abort(p)
	}
	if m.Opcode == peerRangeReserve {
		p, err := decodeRangePlan(m.Body)
		if err != nil {
			return nil, err
		}
		if p.Source != actor {
			return nil, fail(403, "range source identity mismatch")
		}
		return nil, r.reserve(p, sourceBoot...)
	}
	if m.Opcode >= peerRangeSnapshot && m.Opcode <= peerRangeSnapshotEnd || m.Opcode == peerRangeProbe {
		return r.receiveSnapshot(m, actor, force)
	}
	if m.Opcode == peerRangeArchive {
		return r.archive(m.Body)
	}
	if m.Opcode == peerRangeObserve {
		d := client.Decoder{B: m.Body}
		p, err := decodeRangePlan(d.Blob())
		if err != nil {
			return nil, err
		}
		c, err := decodeRangeCommit(d.Blob())
		if err != nil || d.Done() != nil {
			return nil, client.ErrProtocol
		}
		return nil, r.observe(p, c)
	}
	if m.Opcode == peerRangeCommit {
		c, err := decodeRangeCommit(m.Body)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		t := r.txns[c.ID]
		valid := t != nil && t.plan.Source == actor
		r.mu.Unlock()
		if !valid {
			return nil, errControlConflict
		}
		proof, err := r.commitWithForce(c, force)
		if err != nil {
			return nil, err
		}
		return proof.MarshalBinary()
	}
	if m.Opcode == peerRangeDone {
		d := client.Decoder{B: m.Body}
		if d.U16() != 1 {
			return nil, client.ErrProtocol
		}
		var id [32]byte
		copy(id[:], d.Take(32))
		if d.Done() != nil {
			return nil, client.ErrProtocol
		}
		r.mu.Lock()
		t := r.txns[id]
		valid := t != nil && t.plan.Source == actor
		r.mu.Unlock()
		if !valid {
			return nil, errControlConflict
		}
		proof, err := r.done(id, nil)
		if err != nil {
			return nil, err
		}
		return proof.MarshalBinary()
	}
	return nil, client.ErrProtocol
}

func (r *rangeManager) archive(body []byte) ([]byte, error) {
	d := client.Decoder{B: body}
	offset := d.U64()
	if d.Done() != nil {
		return nil, client.ErrProtocol
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if offset > uint64(len(r.order)) {
		return nil, client.ErrProtocol
	}
	var records [][]byte
	at := offset
	size := 0
	for at < uint64(len(r.order)) && len(records) < 24 {
		t := r.txns[r.order[at]]
		if t.decision != nil {
			e := client.Encoder{}
			e.Blob(mustRangeWire(t.plan))
			commit, err := t.decision.marshal()
			if err != nil {
				return nil, err
			}
			e.Blob(commit)
			if size+len(e.B) > 1<<20 && len(records) > 0 {
				break
			}
			size += len(e.B)
			records = append(records, e.B)
		}
		at++
	}
	e := client.Encoder{}
	e.U64(at)
	e.Bool(at == uint64(len(r.order)))
	e.V(uint64(len(records)))
	for _, record := range records {
		e.Blob(record)
	}
	return e.B, nil
}

func (r *rangeManager) syncArchives(ctx context.Context) error {
	l := r.server.control.ledger
	l.mu.Lock()
	members := membershipIDs(l.current.Membership)
	l.mu.Unlock()
	for _, node := range members {
		if node == r.node {
			continue
		}
		if err := r.syncArchiveFrom(ctx, node); err != nil {
			return err
		}
	}
	return nil
}

func (r *rangeManager) syncArchiveFrom(ctx context.Context, node client.ID) error {
	var offset uint64
	for {
		e := client.Encoder{}
		e.U64(offset)
		body, err := r.call(ctx, node, peerRangeArchive, e.B)
		if err != nil {
			return err
		}
		d := client.Decoder{B: body}
		next, done, n := d.U64(), d.Bool(), d.Count(24)
		if next < offset || !done && next == offset {
			return client.ErrProtocol
		}
		for i := 0; i < n; i++ {
			sub := client.Decoder{B: d.Blob()}
			p, err := decodeRangePlan(sub.Blob())
			if err != nil {
				return err
			}
			c, err := decodeRangeCommit(sub.Blob())
			if err != nil || sub.Done() != nil {
				return client.ErrProtocol
			}
			if err := r.observe(p, c); err != nil {
				return err
			}
		}
		if d.Done() != nil {
			return client.ErrProtocol
		}
		if done {
			break
		}
		offset = next
	}
	return nil
}
