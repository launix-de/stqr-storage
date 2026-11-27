// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const maxObjectReferences = 8192
const maxAccountReferences = 1024

type objectReferenceEntry struct {
	object     volume.ObjectID
	proof      [32]byte
	account    volume.ID
	claim      bool
	dependency bool
}

func decodeObjectReferences(raw []byte, bucket volume.ID) ([]objectReferenceEntry, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	d := client.Decoder{B: raw}
	if !bytes.Equal(d.Take(4), []byte("ORF1")) {
		return nil, volume.ErrCorrupt
	}
	owner := volume.ID(d.ID())
	n := d.U32()
	if n > maxObjectReferences || uint64(n)*81 != uint64(len(raw)-24) {
		return nil, volume.ErrCorrupt
	}
	entries := make([]objectReferenceEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		var e objectReferenceEntry
		copy(e.object[:], d.Take(32))
		copy(e.proof[:], d.Take(32))
		e.account = volume.ID(d.ID())
		flags := d.U8()
		if flags > 2 {
			return nil, volume.ErrCorrupt
		}
		e.claim = flags == 1
		e.dependency = flags == 2
		entries = append(entries, e)
	}
	if d.Done() != nil {
		return nil, volume.ErrCorrupt
	}
	// A bucket clone copies bytes, not the source bucket's active holders.
	if owner != bucket {
		return nil, nil
	}
	return entries, nil
}
func encodeObjectReferences(entries []objectReferenceEntry, bucket volume.ID) []byte {
	e := client.Encoder{B: []byte("ORF1")}
	e.ID(client.ID(bucket))
	e.U32(uint32(len(entries)))
	for _, r := range entries {
		e.B = append(e.B, r.object[:]...)
		e.B = append(e.B, r.proof[:]...)
		e.ID(client.ID(r.account))
		flags := uint8(0)
		if r.claim {
			flags = 1
		}
		if r.dependency {
			flags = 2
		}
		e.U8(flags)
	}
	return e.B
}

// Legacy collectors do not know that durable dependency pins are roots. Every
// lease participant checks its local copies before granting offline maintenance.
func (s *Server) checkReferenceAwareMaintenance(b *Bucket, aware bool) error {
	if aware {
		return nil
	}
	key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID(client.ObjectReferenceRootID())}
	for _, v := range b.allVolumes() {
		h, err := v.Open(false)
		if err != nil {
			return err
		}
		raw, _, err := h.Get(key)
		h.Close()
		if errors.Is(err, volume.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		entries, err := decodeObjectReferences(raw, b.ID())
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.dependency {
				return fail(426, "durable references require a reference-aware collector")
			}
		}
	}
	return nil
}

func (s *Server) executeObjectReference(ctx context.Context, m client.Message, ws *wsSession, p principal) ([]byte, error) {
	if m.Kind != client.Request {
		return nil, fail(400, "object references require requests")
	}
	op, d, err := s.parseObject(m, ws, p)
	if err != nil {
		return nil, err
	}
	if op.object != volume.ObjectID(client.ObjectReferenceRootID()) {
		return nil, ErrBucketForbidden
	}
	var target volume.ObjectID
	copy(target[:], d.Take(32))
	action, token := d.U8(), d.ID()
	// The legacy body retains its durable-ACK contract. Filesystem adapters
	// may explicitly choose a visible-only result with the negotiated trailer;
	// this is generic object coordination, never a server-side FS interpretation.
	accepted := false
	if len(d.B) > 0 && d.Err == nil {
		accepted = d.Bool()
		if ws == nil || ws.authorization == nil || !ws.authorization.objectReferences.Load() || !ws.acceptedReferencesCap.Load() || !ws.connectionFenceCap.Load() || !ws.durabilityCap.Load() {
			return nil, fail(501, "accepted reference capability was not negotiated")
		}
	}
	if d.Done() != nil || action > 8 || token == (client.ID{}) || target == op.object {
		return nil, fail(400, "invalid object reference")
	}
	if _, err = op.bucket.authorize(p, BucketRead); err != nil {
		return nil, err
	}
	if action == 6 || action == 7 || action == 8 {
		if accepted {
			return nil, fail(400, "offline reference maintenance requires durable requests")
		}
		if ws == nil || p.connection == nil || !p.connection.objectReferences.Load() {
			return nil, ErrBucketForbidden
		}
		op.bucket.mu.RLock()
		maintenance := op.bucket.maintenance
		exclusive := maintenance == p.connection && !p.connection.objectLease.Load()
		op.bucket.mu.RUnlock()
		if !exclusive {
			lease := s.leaseRecord(op.bucket)
			exclusive = lease != nil && !lease.Released && !lease.ObjectIO && lease.ReferenceAware && op.bucket.leaseAllows(p.connection)
		}
		if !exclusive {
			return nil, ErrBucketForbidden
		}
		if _, err := op.bucket.authorize(p, BucketDelete); err != nil {
			return nil, err
		}
	}

	// Every coordination update belongs to this root's current primary. Its
	// ordinary replicated contents move with it during range handoff.
	if err = s.checkObjectOwner(op.bucket.ID(), op.object); err != nil {
		return nil, err
	}
	account, err := s.findAccount(p.User.Name)
	if err != nil || account.ID != p.User.ID {
		return nil, ErrBucketForbidden
	}
	uid, err := bucketID(account.ID)
	if err != nil {
		return nil, ErrBucketForbidden
	}
	// READ authorizes only this bounded, server-encoded reference object; it
	// confers no WRITE or DELETE capability on user objects.
	internal := p
	internal.internalCoordination = true
	internal.User = account
	internal.frontendPermission = BucketRead
	if action == 8 {
		s.opMu.Lock()
		safe := s.canReclaimDependencyAnchors(op.bucket.ID())
		s.opMu.Unlock()
		if safe {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	}
	if action == 7 {
		if err := s.reclaimBucketMetadata(ctx, op.bucket); err != nil {
			return nil, err
		}
		return []byte{1}, nil
	}
	proof := sha256.Sum256(token[:])
	for retry := 0; retry < 128; retry++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, _, err := op.bucket.Get(withMembershipRead(ctx), internal, op.object)
		absent := errors.Is(err, volume.ErrNotFound)
		if absent {
			raw = nil
		} else if err != nil {
			return nil, err
		}
		entries, err := decodeObjectReferences(raw, op.bucket.ID())
		if err != nil {
			return nil, err
		}
		own := -1
		count := 0
		busy := false
		for i, r := range entries {
			if r.account == uid {
				count++
			}
			if action != 6 && r.proof == proof {
				ownerOK := r.account == uid || account.Role == "ADMIN" && (action == 1 || action == 3 || action == 5)
				if !ownerOK || r.object != target || r.claim != (action == 2 || action == 3) || r.dependency != (action == 4 || action == 5) {
					return nil, ErrBucketForbidden
				}
				own = i
			}
			if r.object == target && (action == 2 || (action == 0 || action == 4) && r.claim) {
				busy = true
			}
		}
		if action == 6 {
			kept := entries[:0]
			for _, entry := range entries {
				if entry.dependency {
					kept = append(kept, entry)
				}
			}
			if len(kept) == len(entries) {
				return []byte{1}, nil
			}
			entries = kept
		} else if action == 1 || action == 3 || action == 5 {
			if own < 0 {
				return []byte{1}, nil
			}
			entries = append(entries[:own], entries[own+1:]...)
		} else {
			if own >= 0 {
				return []byte{1}, nil
			}
			if busy {
				return nil, fail(409, "object reference is busy")
			}
			if len(entries) >= maxObjectReferences || count >= maxAccountReferences {
				return nil, fail(429, "object reference limit reached")
			}
			entries = append(entries, objectReferenceEntry{target, proof, uid, action == 2, action == 4})
		}
		next := encodeObjectReferences(entries, op.bucket.ID())
		_, err = op.bucket.CAS(ctx, internal, op.object, 0, raw, next, volume.Options{RequestID: volume.ID(client.NewRequestID()), CreateOnly: absent, WholeObject: true, Accepted: accepted})
		if errors.Is(err, volume.ErrPrecondition) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return []byte{1}, nil
	}
	return nil, fail(503, "object references changed concurrently")
}

// Clone receipts already survive detach. Shared IDs on both endpoints therefore
// distinguish a genuinely sole surviving owner from a detached but live clone.
func (s *Server) canReclaimDependencyAnchors(bucket volume.ID) bool {
	descriptions := s.bucketSnapshots()
	seenBuckets := map[string]bool{bucket.String(): true}
	links := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, d := range descriptions {
			member := seenBuckets[d.ID]
			if !member {
				for _, r := range d.Receipts {
					if r.Opcode == client.BucketClone && links[r.ID] {
						member = true
						break
					}
				}
			}
			if !member {
				continue
			}
			if !seenBuckets[d.ID] {
				seenBuckets[d.ID] = true
				changed = true
			}
			for _, r := range d.Receipts {
				if r.Opcode == client.BucketClone && !links[r.ID] {
					links[r.ID] = true
					changed = true
				}
			}
		}
	}
	for _, d := range descriptions {
		if d.ID == bucket.String() && len(d.ClonePeers) != 0 {
			return false
		}
		if d.ID != bucket.String() && seenBuckets[d.ID] && !d.DeletionComplete {
			return false
		}
	}
	return true
}

func validateNewCloneReferenceEvidence(before, after []BucketDescription) error {
	old := map[string]bool{}
	for _, d := range before {
		for _, edge := range d.ClonePeers {
			old[edge.Link] = true
		}
	}
	for _, d := range after {
		for _, edge := range d.ClonePeers {
			if old[edge.Link] {
				continue
			}
			found := false
			for _, r := range d.Receipts {
				if r.ID == edge.Link && r.Opcode == client.BucketClone && r.Status == 200 && !r.Running {
					found = true
					break
				}
			}
			if !found {
				return fail(426, "clone needs reference-sharing evidence on both endpoints")
			}
		}
	}
	return nil
}
