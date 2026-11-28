// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// referenceCoordination contains only running/waiting requests, never object
// metadata or cached ownership. The registry mutex covers lifetimes only;
// storage work holds one bucket gate. Ordinary volume/replica locks still apply. A gate
// disappears when its last holder/waiter leaves; cancellation is not a release
// of a durable object pin. The volume lock and ordinary CAS remain authoritative.
type referenceCoordination struct {
	mu    sync.Mutex
	gates map[volume.ID]*referenceGate
}
type referenceGate struct {
	held    bool
	refs    int
	pending []*referenceWaiter
}

func (r *referenceCoordination) acquire(ctx context.Context, bucket volume.ID) (func(), error) {
	_, release, err := r.join(ctx, bucket, nil)
	return release, err
}

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
	if len(raw) < referenceHeaderBytes || uint64(n)*referenceEntryBytes != uint64(len(raw)-referenceHeaderBytes) {
		return nil, volume.ErrCorrupt
	}
	// PIN commonly appends one entry immediately after decoding. Reserve that
	// slot without relaxing the encoded length. No account/object count quota
	// exists; the existing ORF1 uint32 and address-space bounds still apply.
	capacity := n
	if capacity < math.MaxUint32 && uint64(capacity)+1 <= uint64((math.MaxInt-referenceHeaderBytes)/referenceEntryBytes) {
		capacity++
	}
	entries := make([]objectReferenceEntry, 0, capacity)
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
	// Reserve the validated representation's exact encoded size, avoiding
	// repeated buffer growth. Admission has no artificial reference quota.
	e := client.Encoder{B: make([]byte, 4, 24+81*len(entries))}
	copy(e.B, "ORF1")
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
	return s.executeObjectReferenceHeld(ctx, m, ws, p, false)
}

func (s *Server) executeObjectReferenceHeld(ctx context.Context, m client.Message, ws *wsSession, p principal, held bool) ([]byte, error) {
	if !referencePrincipalSupported(p) {
		return nil, ErrBucketForbidden
	}
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
	if d.Done() != nil || action > 9 || token == (client.ID{}) || target == op.object {
		return nil, fail(400, "invalid object reference")
	}
	// Administrative repair is deliberately rejected before reading the root.
	// It may remove only absent-account plain PINs, never abandoned claims or
	// dependencies; it does not confer offline maintenance on the caller.
	if action == 9 {
		if target != (volume.ObjectID{}) {
			return nil, fail(400, "reference repair requires an empty target")
		}
		if err := s.requireAdmin(p); err != nil {
			return nil, err
		}
	}
	if !held && accepted && (action == 0 || action == 1) {
		return s.combineObjectReference(ctx, m, ws, p, op.bucket.ID(), token)
	}
	if !held {
		release, err := s.referenceCoordination.acquire(ctx, op.bucket.ID())
		if err != nil {
			return nil, err
		}
		defer release()
		// Placement epochs, maintenance and permissions may change while a
		// request waits. Reparse before touching the root, then retain the
		// ordinary read/CAS loop so external object writers cannot be lost.
		op, _, err = s.parseObject(m, ws, p)
		if err != nil {
			return nil, err
		}
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
	account, stamp, err := s.referenceAccountSnapshot(p)
	if err != nil {
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
	internal.referenceAuth = stamp
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
	if action == 9 {
		return s.pruneAbsentAccountPins(ctx, op.bucket, internal, accepted)
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
		table, err := borrowReferenceBytes(raw, op.bucket.ID(), 1)
		if err != nil {
			return nil, err
		}
		err = table.apply(target, proof, uid, account.Role == "ADMIN", action)
		if err != nil {
			return nil, err
		}
		if !table.changed {
			return []byte{1}, nil
		}
		next := table.raw
		_, err = op.bucket.CAS(ctx, internal, op.object, 0, raw, next, volume.Options{RequestID: volume.ID(client.NewRequestID()), CreateOnly: absent, WholeObject: true, Accepted: accepted})
		if errors.Is(err, volume.ErrPrecondition) && !errors.Is(err, volume.ErrRecovery) {
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
