// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Retention is deliberately bounded. A slow subscriber is told to resnapshot;
// a full socket queue closes the socket, invalidating every dependent cache.
const maxObjectSubscriptions = 16384
const maxSessionObjectSubscriptions = 2048
const maxSubscriptionJournalBytes = 64 << 20
const maxSubscriptionEntryBytes = 1 << 20
const maxSubscriptionJournalEvents = 16

type objectSubscription struct {
	ws                     *wsSession
	id                     client.ID
	key                    volume.Key
	ranged                 bool
	offset, length, cursor uint64
	epoch                  uint64
	runtime                client.ID
	journal                []subscriptionJournalEntry
	bytes                  int
}
type subscriptionJournalEntry struct {
	cursor uint64
	bytes  int
	update client.ObjectUpdate
}
type objectSubscriptionRegistry struct {
	mu            sync.Mutex
	objects       map[volume.Key]map[*objectSubscription]struct{}
	sessions      map[*wsSession]map[client.ID]*objectSubscription
	count, bytes  int
	snapshotBytes uint64
}

// Snapshot payloads are transient RAM, never temporary files or durable state.
// Reserve before allocating so concurrent large subscriptions cannot exhaust RAM.
// This shares the existing per-transfer staging ceiling across the node; exhaustion
// is temporary resource pressure (507), not an on-disk object size restriction.
func (r *objectSubscriptionRegistry) reserveSnapshot(n uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > maxObjectStagingBytes-r.snapshotBytes {
		return fail(507, "subscription snapshot memory budget exhausted")
	}
	r.snapshotBytes += n
	return nil
}
func (r *objectSubscriptionRegistry) releaseSnapshot(n uint64) {
	r.mu.Lock()
	r.snapshotBytes -= n
	r.mu.Unlock()
}

func (r *objectSubscriptionRegistry) removeLocked(sub *objectSubscription) {
	if r.sessions[sub.ws][sub.id] != sub {
		return
	}
	delete(r.sessions[sub.ws], sub.id)
	if len(r.sessions[sub.ws]) == 0 {
		delete(r.sessions, sub.ws)
	}
	delete(r.objects[sub.key], sub)
	if len(r.objects[sub.key]) == 0 {
		delete(r.objects, sub.key)
	}
	r.count--
	r.bytes -= sub.bytes
}
func (r *objectSubscriptionRegistry) closeSession(ws *wsSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sub := range r.sessions[ws] {
		r.removeLocked(sub)
	}
}
func (r *objectSubscriptionRegistry) add(sub *objectSubscription) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.objects == nil {
		r.objects = map[volume.Key]map[*objectSubscription]struct{}{}
		r.sessions = map[*wsSession]map[client.ID]*objectSubscription{}
	}
	select {
	case <-sub.ws.done:
		return context.Canceled
	default:
	}
	if sub.ws.authorization == nil || sub.ws.authorization.invalid.Load() {
		return ErrBucketForbidden
	}
	if old := r.sessions[sub.ws][sub.id]; old != nil {
		return fail(409, "subscription ID is already active; unsubscribe before reusing it")
	}
	if r.count >= maxObjectSubscriptions || len(r.sessions[sub.ws]) >= maxSessionObjectSubscriptions {
		return fail(507, "object subscription budget exhausted")
	}
	if r.objects[sub.key] == nil {
		r.objects[sub.key] = map[*objectSubscription]struct{}{}
	}
	if r.sessions[sub.ws] == nil {
		r.sessions[sub.ws] = map[client.ID]*objectSubscription{}
	}
	r.objects[sub.key][sub] = struct{}{}
	r.sessions[sub.ws][sub.id] = sub
	r.count++
	return nil
}
func (r *objectSubscriptionRegistry) watched(key volume.Key) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.objects[key]) != 0
}
func (r *objectSubscriptionRegistry) sendLocked(sub *objectSubscription, u client.ObjectUpdate) bool {
	body, err := u.MarshalBinary()
	if err != nil {
		sub.ws.close()
		return false
	}
	if sub.ws.durabilityCap.Load() {
		if mark, ok := sub.ws.server.acceptedMark(sub.key.Bucket); ok {
			notice := client.EncodeMessage(client.Message{Kind: client.Push, Opcode: client.DurabilityNotice, Body: mark.MarshalBinary()})
			select {
			case sub.ws.responses <- notice:
			default:
				sub.ws.close()
				return false
			}
		}
	}
	frame := client.EncodeMessage(client.Message{Kind: client.Push, Opcode: client.ObjectUpdateMessage, ID: client.NewRequestID(), Body: body})
	select {
	case sub.ws.responses <- frame:
		return true
	default:
		sub.ws.close()
		return false
	}
}

// Called under the same native write handle which committed the mutation. The
// reader registers under a native read handle, closing the snapshot/listen gap.
func (r *objectSubscriptionRegistry) publish(key volume.Key, info volume.Info, h *volume.Handle, volumes ...*volume.Volume) {
	var generation uint64
	if h != nil {
		generation, _ = h.Generation()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	type window struct{ off, length uint64 }
	type snapshot struct {
		data []byte
		err  error
	}
	snapshots := map[window]snapshot{}
	for sub := range r.objects[key] {
		if sub.ws.authorization == nil || sub.ws.authorization.invalid.Load() {
			r.removeLocked(sub)
			sub.ws.close()
			continue
		}
		sub.cursor++
		u := client.ObjectUpdate{Subscription: sub.id, Cursor: sub.cursor, Parts: 1, Change: client.UpdateReplace, NewLength: info.Length}
		off, n := uint64(0), info.Length
		if sub.ranged {
			off = sub.offset
			n = min(sub.length, info.Length-min(off, info.Length))
			u.Offset = off
		}
		if info.Deleted {
			u.Change = client.UpdateDelete
			n = 0
		} else if n > maxSubscriptionEntryBytes {
			u.Change = client.UpdateResyncRequired
			n = 0
		}
		// Check pressure before reading any payload. Slow clients must not cause
		// object-sized copies which will immediately be discarded for resync.
		cost := int(n) + 128
		if sub.cursor == 0 || len(sub.journal) >= maxSubscriptionJournalEvents || sub.bytes+cost > maxSubscriptionEntryBytes || r.bytes+cost > maxSubscriptionJournalBytes {
			u.Change = client.UpdateResyncRequired
			n = 0
		}
		if len(sub.ws.responses) == cap(sub.ws.responses) {
			sub.ws.close()
			r.removeLocked(sub)
			continue
		}
		if n > 0 {
			keyWindow := window{off, n}
			snap, ok := snapshots[keyWindow]
			if !ok {
				snap.data, _, snap.err = h.GetRange(key, off, n)
				if len(snapshots) < 4 {
					snapshots[keyWindow] = snap
				}
			}
			if snap.err != nil {
				u.Change = client.UpdateResyncRequired
			} else {
				u.Data = snap.data
			}
		}
		cost = len(u.Data) + 128
		if len(volumes) > 0 {
			sub.ws.observeConnectionVolume(volumes[0], generation)
		}
		if !r.sendLocked(sub, u) || u.Change == client.UpdateResyncRequired {
			r.removeLocked(sub)
			continue
		}
		sub.journal = append(sub.journal, subscriptionJournalEntry{sub.cursor, cost, u})
		sub.bytes += cost
		r.bytes += cost
	}
}
func (s *Server) executeObjectSubscription(m client.Message, ws *wsSession, p principal) ([]byte, int, error) {
	if ws == nil || ws.authorization == nil || ws.authorization.bucket == nil {
		return nil, 0, fail(400, "subscriptions require a bucket WebSocket")
	}
	r := &s.objectSubscriptions
	if m.Opcode != client.ObjectSubscribe {
		d := client.Decoder{B: m.Body}
		id := d.ID()
		cursor := uint64(0)
		if m.Opcode == client.SubscriptionAck {
			cursor = d.U64()
		}
		if d.Done() != nil || id == (client.ID{}) {
			return nil, 0, fail(400, "invalid subscription control")
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		sub := r.sessions[ws][id]
		if sub == nil {
			return nil, 200, nil
		}
		if m.Opcode == client.ObjectUnsubscribe {
			r.removeLocked(sub)
			return nil, 200, nil
		}
		if cursor > sub.cursor {
			return nil, 0, fail(400, "subscription ACK is ahead of publication")
		}
		i := 0
		for i < len(sub.journal) && sub.journal[i].cursor <= cursor {
			sub.bytes -= sub.journal[i].bytes
			r.bytes -= sub.journal[i].bytes
			i++
		}
		sub.journal = append([]subscriptionJournalEntry(nil), sub.journal[i:]...)
		return nil, 200, nil
	}
	if m.Kind != client.Request {
		return nil, 0, fail(400, "SUBSCRIBE requires a request")
	}
	op, d, err := s.parseObject(m, ws, p)
	if err != nil {
		return nil, 0, err
	}
	id, ranged, off, length := d.ID(), d.Bool(), d.U64(), d.U64()
	resume := uint64(0)
	if d.Bool() {
		resume = d.U64()
	}
	if d.Done() != nil || id == (client.ID{}) || length > math.MaxUint64-off || resume == math.MaxUint64 {
		return nil, 0, fail(400, "invalid SUBSCRIBE body")
	}
	if _, err = op.bucket.authorize(p, BucketRead); err != nil {
		return nil, 0, err
	}
	ctx := bindObjectRequest(ws.objectContext(m.ID), m)
	var release func()
	if s.scopes != nil {
		release, err = s.scopes.guard(op.bucket.ID(), op.object)
		if err != nil {
			return nil, 0, err
		}
		defer func() {
			if release != nil {
				release()
			}
		}()
	}

	desc := op.bucket.storageDescription()
	key := volume.Key{Bucket: op.bucket.ID(), Object: op.object}
	v, prior, _, err := op.bucket.selectVolume(desc, key, volume.ID{}, false)
	var h *volume.Handle
	var inherited *cowSnapshot
	var info volume.Info
	if errors.Is(err, volume.ErrNotFound) && len(desc.ClonePeers) > 0 {
		inherited, err = s.cowLogicalSnapshot(ctx, key.Bucket, key.Object, false)
		if err != nil {
			return nil, 0, err
		}
		defer inherited.close()
		if inherited == nil || inherited.Info.Deleted {
			return nil, 0, volume.ErrNotFound
		}
		info = inherited.Info
	} else {
		if err != nil {
			return nil, 0, op.bucket.absenceError(op.object, prior, err)
		}
		h, err = v.Open(false)
		if err != nil {
			return nil, 0, err
		}
		defer h.Close()
		if err = s.checkObjectBinding(ctx, key.Bucket, key.Object); err != nil {
			return nil, 0, err
		}
		info, err = h.Stat(key)
		if err != nil {
			return nil, 0, err
		}
		if generation, e := h.Generation(); e == nil {
			touchConnectionVolume(ctx, v, generation)
		}
	}
	offset, n := uint64(0), info.Length
	if ranged {
		offset, n = off, length
	}
	if offset > info.Length {
		return nil, 0, volume.ErrRange
	}
	n = min(n, info.Length-offset)
	if err = r.reserveSnapshot(n); err != nil {
		return nil, 0, err
	}
	defer r.releaseSnapshot(n)
	// Preallocate exactly once; the native volume handle still protects snapshot
	// capture and registration, and is released before any socket output.
	snapshot := bytes.NewBuffer(make([]byte, 0, int(n)))
	hash := sha256.New()
	var copied uint64
	writer := &objectBudgetWriter{dst: bucketContextWriter{ctx: ctx, w: io.MultiWriter(snapshot, hash)}, remaining: maxObjectStagingBytes}
	if h != nil {
		copied, err = h.CopyTo(key, offset, n, writer)
	} else {
		if offset > info.Length {
			return nil, 0, volume.ErrRange
		}
		n = min(n, info.Length-offset)
		count, e := io.CopyN(writer, io.NewSectionReader(inherited.Data, int64(offset), int64(n)), int64(n))
		copied, err = uint64(count), e
	}
	if err != nil {
		return nil, 0, err
	}
	sub := &objectSubscription{ws: ws, id: id, key: key, ranged: ranged, offset: off, length: length, cursor: resume + 1}
	if err = ctx.Err(); err != nil {
		return nil, 0, err
	}
	if patch, known := s.routingPatch(key.Bucket, key.Object); known {
		sub.epoch = patch.Entries[0].OwnerEpoch
		sub.runtime = patch.Entries[0].Runtime
	}
	initialCursor := sub.cursor
	if inherited != nil {
		// All logical publications and first materializations take opMu. If no
		// local record exists at this barrier, our captured fallback is still the
		// namespace's contents. A later materialization preserves these same bytes;
		// a later logical write is queued after this initial subscription cursor.
		s.opMu.Lock()
		if err = s.checkObjectBinding(ctx, key.Bucket, key.Object); err == nil {
			_, _, found, e := op.bucket.selectVolume(op.bucket.storageDescription(), key, volume.ID{}, false)
			if e != nil && !errors.Is(e, volume.ErrNotFound) {
				err = e
			} else if found {
				err = fail(503, "clone object materialized during subscription; retry snapshot")
			}
		}
		if err == nil {
			err = r.add(sub)
		}
		s.opMu.Unlock()
		if err != nil {
			return nil, 0, err
		}
	} else {
		if err = r.add(sub); err != nil {
			return nil, 0, err
		}
		h.Close()
	}
	// Only local snapshot I/O is inside the storage barrier, never network.
	if release != nil {
		release()
		release = nil
	}
	e := client.Encoder{}
	e.ID(id)
	e.U64(initialCursor)
	runtime := client.ID{}
	if desc.Policy.StorageStrategy == "ram" {
		runtime = sub.runtime
		if s.controller == nil {
			runtime = s.boot
		}
	}

	encodeObjectInfo(&e, info, runtime)
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	if copied <= maxSubscriptionEntryBytes {
		encodeObjectTransfer(&e, 0, copied, digest, snapshot.Bytes())
	} else {
		err = ws.sendObjectStream(m.ID, snapshot, copied, digest)
		if err == nil {
			encodeObjectTransfer(&e, 1, copied, digest, nil)
		}
	}

	if err != nil {
		r.mu.Lock()
		r.removeLocked(sub)
		r.mu.Unlock()
		return nil, 0, err
	}
	return e.B, 200, nil
}

// Ownership changes use the explicit resnapshot contract. Descriptors never
// silently remain attached to an old owner. Clients re-route their next snapshot.
func (s *Server) resyncMovedSubscriptions(image *placementImage) {
	r := &s.objectSubscriptions
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, subs := range r.objects {
		entry, ok := image.lookup(client.ID(key.Bucket), client.ObjectID(key.Object))
		local := false
		if ok {
			for _, loc := range entry.Locations {
				if loc.Role == 0 && loc.Node == s.control.ledger.node {
					local = true
				}
			}
		}
		for sub := range subs {
			if local && entry.OwnerEpoch == sub.epoch && entry.Runtime == sub.runtime {
				continue
			}
			sub.cursor++
			r.sendLocked(sub, client.ObjectUpdate{Subscription: sub.id, Cursor: sub.cursor, Parts: 1, Change: client.UpdateResyncRequired})
			r.removeLocked(sub)
		}
	}
}

func (r *objectSubscriptionRegistry) resyncWhere(match func(volume.Key) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, subs := range r.objects {
		if !match(key) {
			continue
		}
		for sub := range subs {
			sub.cursor++
			r.sendLocked(sub, client.ObjectUpdate{Subscription: sub.id, Cursor: sub.cursor, Parts: 1, Change: client.UpdateResyncRequired})
			r.removeLocked(sub)
		}
	}
}

// Keep existing admin-session reply behavior, but terminate subscribed sockets
// immediately. add() checks the same revoked capability before registration,
// so a concurrent snapshot cannot install a listener after this barrier.
func (s *Server) endRevokedSubscriptions(ws *wsSession) {
	r := &s.objectSubscriptions
	r.mu.Lock()
	active := len(r.sessions[ws]) != 0
	r.mu.Unlock()
	if active {
		ws.close()
	}
}
