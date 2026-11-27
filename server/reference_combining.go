// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const maxCombinedReferences = 32

type referenceResult struct {
	body []byte
	err  error
}
type combinedReferenceRequest struct {
	ctx       context.Context
	message   client.Message
	ws        *wsSession
	principal principal
	token     client.ID
	result    referenceResult
}
type referenceWaiter struct {
	request  *combinedReferenceRequest
	ready    chan struct{}
	selected bool // Removed from pending and owned by a running combiner.
	granted  bool // Owns the bucket; selected followers never own the gate.
}

// join preserves FIFO barriers without introducing a worker or a batching timer.
// A selected follower waits for publication even if its context is canceled:
// its connectionWork must remain alive until its final fence dependency is known.
func (r *referenceCoordination) join(ctx context.Context, bucket volume.ID, request *combinedReferenceRequest) (*referenceWaiter, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	w := &referenceWaiter{request: request, ready: make(chan struct{})}
	r.mu.Lock()
	if r.gates == nil {
		r.gates = make(map[volume.ID]*referenceGate)
	}
	g := r.gates[bucket]
	if g == nil {
		g = &referenceGate{}
		r.gates[bucket] = g
	}
	g.refs++
	g.pending = append(g.pending, w)
	r.grantNext(g)
	r.mu.Unlock()
	select {
	case <-w.ready:
	case <-ctx.Done():
		r.mu.Lock()
		if !w.selected && !w.granted {
			for i, p := range g.pending {
				if p == w {
					copy(g.pending[i:], g.pending[i+1:])
					g.pending[len(g.pending)-1] = nil
					g.pending = g.pending[:len(g.pending)-1]
					break
				}
			}
			r.drop(bucket, g)
			r.mu.Unlock()
			return nil, nil, ctx.Err()
		}
		r.mu.Unlock()
		<-w.ready
	}
	r.mu.Lock()
	if w.selected {
		r.drop(bucket, g)
		r.mu.Unlock()
		return w, nil, nil
	}
	r.mu.Unlock()
	release := func() { r.mu.Lock(); g.held = false; r.drop(bucket, g); r.grantNext(g); r.mu.Unlock() }
	if err := ctx.Err(); err != nil {
		release()
		return nil, nil, err
	}
	return w, release, nil
}
func (r *referenceCoordination) grantNext(g *referenceGate) {
	if g.held || len(g.pending) == 0 {
		return
	}
	w := g.pending[0]
	g.pending[0] = nil
	g.pending = g.pending[1:]
	g.held = true
	w.granted = true
	close(w.ready)
}
func (r *referenceCoordination) drop(bucket volume.ID, g *referenceGate) {
	g.refs--
	if g.refs == 0 {
		delete(r.gates, bucket)
		if len(r.gates) == 0 {
			r.gates = nil
		}
	}
}
func (r *referenceCoordination) take(bucket volume.ID, leader *combinedReferenceRequest) []*referenceWaiter {
	r.mu.Lock()
	defer r.mu.Unlock()
	g := r.gates[bucket]
	var out []*referenceWaiter
	seen := map[client.ID]bool{leader.token: true}
	for len(g.pending) > 0 && len(out) < maxCombinedReferences-1 {
		w := g.pending[0]
		q := w.request
		// Other sockets/accounts and repeated proofs are barriers, not candidates
		// to skip. This bounds monopolization and preserves their dependency order.
		if q == nil || q.ws != leader.ws || q.principal.User.ID != leader.principal.User.ID || seen[q.token] {
			break
		}
		g.pending[0] = nil
		g.pending = g.pending[1:]
		w.selected = true
		seen[q.token] = true
		out = append(out, w)
	}
	return out
}
func (s *Server) combineObjectReference(ctx context.Context, m client.Message, ws *wsSession, p principal, bucket volume.ID, token client.ID) ([]byte, error) {
	q := &combinedReferenceRequest{ctx: ctx, message: m, ws: ws, principal: p, token: token}
	_, release, err := s.referenceCoordination.join(ctx, bucket, q)
	if err != nil {
		return nil, err
	}
	if release == nil {
		return q.result.body, q.result.err
	}
	defer release()
	followers := s.referenceCoordination.take(bucket, q)
	if len(followers) == 0 {
		return s.executeObjectReferenceHeld(ctx, m, ws, p, true)
	}
	requests := []*combinedReferenceRequest{q}
	for _, f := range followers {
		requests = append(requests, f.request)
	}
	s.executeCombinedReferences(requests)
	for _, f := range followers {
		close(f.ready)
	}
	return q.result.body, q.result.err
}

type combinedReference struct {
	q        *combinedReferenceRequest
	op       *objectMutation
	internal principal
	uid      volume.ID
	target   volume.ObjectID
	proof    [32]byte
	unpin    bool
}

func (s *Server) validateCombinedReference(q *combinedReferenceRequest) (combinedReference, error) {
	c := combinedReference{q: q}
	if err := q.ctx.Err(); err != nil {
		return c, err
	}
	op, d, err := s.parseObject(q.message, q.ws, q.principal)
	if err != nil {
		return c, err
	}
	c.op = op
	copy(c.target[:], d.Take(32))
	action := d.U8()
	token := d.ID()
	accepted := d.Bool()
	if d.Done() != nil || !accepted || action > 1 || token != q.token || op.object != volume.ObjectID(client.ObjectReferenceRootID()) || c.target == op.object {
		return c, fail(400, "invalid object reference")
	}
	if q.ws == nil || q.ws.authorization == nil || !q.ws.authorization.objectReferences.Load() || !q.ws.acceptedReferencesCap.Load() || !q.ws.connectionFenceCap.Load() || !q.ws.durabilityCap.Load() {
		return c, fail(501, "accepted reference capability was not negotiated")
	}
	if _, err = op.bucket.authorize(q.principal, BucketRead); err != nil {
		return c, err
	}
	if err = s.checkObjectOwner(op.bucket.ID(), op.object); err != nil {
		return c, err
	}
	account, stamp, err := s.referenceAccountSnapshot(q.principal)
	if err != nil {
		return c, ErrBucketForbidden
	}
	c.uid, err = bucketID(account.ID)
	if err != nil {
		return c, ErrBucketForbidden
	}
	c.internal = q.principal
	c.internal.internalCoordination = true
	c.internal.User = account
	c.internal.frontendPermission = BucketRead
	c.internal.referenceAuth = stamp
	c.proof = sha256.Sum256(token[:])
	c.unpin = action == 1
	return c, nil
}

// applyCombinedReference is the ordinary PIN/UNPIN state machine. Each result
// is provisional until the one CAS succeeds; a competing external writer forces
// all validation and limit/busy decisions to be recomputed from its new root.
func applyCombinedReference(entries []objectReferenceEntry, c combinedReference) ([]objectReferenceEntry, error) {
	own, count := -1, 0
	busy := false
	for i, e := range entries {
		if e.account == c.uid {
			count++
		}
		if e.proof == c.proof {
			if !(e.account == c.uid || c.internal.User.Role == "ADMIN" && c.unpin) || e.object != c.target || e.claim || e.dependency {
				return entries, ErrBucketForbidden
			}
			own = i
		}
		if e.object == c.target && e.claim {
			busy = true
		}
	}
	if c.unpin {
		if own >= 0 {
			entries = append(entries[:own], entries[own+1:]...)
		}
		return entries, nil
	}
	if own >= 0 {
		return entries, nil
	}
	if busy {
		return entries, fail(409, "object reference is busy")
	}
	if len(entries) >= maxObjectReferences || count >= maxAccountReferences {
		return entries, fail(429, "object reference limit reached")
	}
	return append(entries, objectReferenceEntry{object: c.target, proof: c.proof, account: c.uid}), nil
}

func (s *Server) executeCombinedReferences(requests []*combinedReferenceRequest) {
	// The elected caller's cancellation must not abort other selected requests.
	// The collector is request-local and its exact observations are copied to
	// EVERY successful participant before any selected job is allowed to finish.
	ws := requests[0].ws
	ws.fenceMu.Lock()
	if ws.connectionTouches == nil {
		ws.connectionTouches = make(map[*volume.Volume]uint64)
	}
	ws.fenceMu.Unlock()
	collector := &connectionWork{session: ws, touches: map[*volume.Volume]uint64{}}
	// This also runs for failed reads, retry exhaustion and individual fallback:
	// no selected job may retire before its known observations are retained.
	defer fanoutCombinedReferenceTouches(ws, collector, requests)
	ctx := context.WithValue(s.ctx, connectionWorkKey{}, collector)
referenceBatchRetry:
	for retry := 0; retry < 128; retry++ {
		active := make([]combinedReference, 0, len(requests))
		for _, q := range requests {
			c, err := s.validateCombinedReference(q)
			q.result = referenceResult{err: err}
			if err == nil {
				active = append(active, c)
			}
		}
		if len(active) == 0 {
			return
		}
		first := active[0]
		raw, _, err := first.op.bucket.Get(withMembershipRead(ctx), first.internal, first.op.object)
		absent := errors.Is(err, volume.ErrNotFound)
		if absent {
			raw = nil
		} else if err != nil {
			for _, c := range active {
				c.q.result.err = err
			}
			return
		}
		entries, err := decodeObjectReferences(raw, first.op.bucket.ID())
		if err != nil {
			for _, c := range active {
				c.q.result.err = err
			}
			return
		}
		changed := false
		casCtx := ctx
		for _, c := range active {
			before := len(entries)
			entries, c.q.result.err = applyCombinedReference(entries, c)
			if !c.unpin && len(entries) >= maxObjectReferences && c.q.result.err != nil && objectStatus(c.q.result.err) == 429 && referenceAccountHasRoom(entries, c.uid) {
				filtered, nextCtx, e := s.pruneAbsentPlainReferencePins(casCtx, entries)
				if e != nil {
					if errors.Is(e, volume.ErrPrecondition) && !errors.Is(e, volume.ErrRecovery) {
						continue referenceBatchRetry
					}
					c.q.result.err = e
					for _, other := range active {
						if other.q.result.err == nil {
							other.q.result.err = e
						}
					}
					return
				}
				casCtx = nextCtx
				if len(filtered) < len(entries) {
					entries = filtered
					changed = true // Removed PINs must publish even if add restores length.
					entries, c.q.result.err = applyCombinedReference(entries, c)
				}
			}
			changed = changed || len(entries) != before
		}
		if changed {
			next := encodeObjectReferences(entries, first.op.bucket.ID())
			_, err = first.op.bucket.CAS(casCtx, first.internal, first.op.object, 0, raw, next, volume.Options{RequestID: volume.ID(client.NewRequestID()), CreateOnly: absent, WholeObject: true, Accepted: true})
			if errors.Is(err, volume.ErrPrecondition) && !errors.Is(err, volume.ErrRecovery) {
				continue
			}
			if err != nil {
				// Only an explicitly non-published storage-full result permits replay.
				// Recovery/ambiguous failures must never be turned into fresh mutations.
				var full *prepublicationQuotaFull
				if errors.As(err, &full) && !errors.Is(err, volume.ErrRecovery) {
					for _, q := range requests {
						q.result.body, q.result.err = s.executeObjectReferenceHeld(q.ctx, q.message, q.ws, q.principal, true)
					}
					return
				}
				for _, c := range active {
					if c.q.result.err == nil {
						c.q.result.err = err
					}
				}
				return
			}
		}
		for _, c := range active {
			if c.q.result.err == nil {
				c.q.result.body = []byte{1}
			}
		}

		return
	}
	for _, q := range requests {
		if q.result.err == nil {
			q.result.err = fail(503, "object references changed concurrently")
		}
	}
}

func fanoutCombinedReferenceTouches(ws *wsSession, collector *connectionWork, requests []*combinedReferenceRequest) {
	ws.fenceMu.Lock()
	touches := make(map[*volume.Volume]uint64, len(collector.touches))
	for v, g := range collector.touches {
		touches[v] = g
	}
	ws.fenceMu.Unlock()
	for _, q := range requests {
		for v, g := range touches {
			touchConnectionVolume(q.ctx, v, g)
		}
	}
}

// prepublicationQuotaFull is emitted only by audited logical quota admission
// checks before a Volume mutation or replicated preparation has started. Generic
// ErrFull (including physical allocation failure) is deliberately not enough to
// replay a combined operation: a backend may have partially published it.
type prepublicationQuotaFull struct{}

func (*prepublicationQuotaFull) Error() string { return volume.ErrFull.Error() }
func (*prepublicationQuotaFull) Unwrap() error { return volume.ErrFull }

// A full per-account budget cannot be repaired by discarding another account's
// stale PINs. This scan is used only after a full-root429, never on the hot path.
func referenceAccountHasRoom(entries []objectReferenceEntry, uid volume.ID) bool {
	count := 0
	for _, entry := range entries {
		if entry.account == uid {
			count++
			if count >= maxAccountReferences {
				return false
			}
		}
	}
	return true
}
