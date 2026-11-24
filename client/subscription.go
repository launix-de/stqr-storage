// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"runtime"
	"sync"
	"weak"
)

var ErrSubscriptionResync = errors.New("client: subscription requires a fresh snapshot")

// Subscription owns a versioned snapshot and its ordered update stream. The
// transport retains only a weak reference. Close releases it deterministically;
// AddCleanup releases abandoned subscriptions without keeping them alive.
// Snapshots become invalid as soon as transport loss, a cursor gap, backpressure
// or owner movement is observed. Cached snapshots are not linearizable reads
// across independent connections: an update may still be in flight.
type Subscription struct {
	mu                   sync.RWMutex
	client               *Client
	id                   ID
	source               *objectTransport
	options              SubscribeOptions
	data                 []byte
	info                 ObjectInfo
	cursor               uint64
	ready, valid, closed bool
	err                  error
	pending              []ObjectUpdate
	pendingBytes         int
	changed              chan struct{}
	observer             func()
	cleanup              runtime.Cleanup
}
type subscriptionCleanup struct {
	client *Client
	id     ID
}

func (c *Client) Subscribe(ctx context.Context, id ObjectID, opt SubscribeOptions) (*Subscription, error) {
	if err := c.ready(ctx); err != nil {
		return nil, err
	}
	if opt.Length > ^uint64(0)-opt.Offset || opt.Resume != nil && *opt.Resume == ^uint64(0) {
		return nil, ErrRange
	}
	if opt.ID == (ID{}) {
		opt.ID = NewRequestID()
	}
	sub := &Subscription{client: c, id: opt.ID, options: opt, changed: make(chan struct{}, 1)}
	c.subMu.Lock()
	if c.subscriptions == nil {
		c.subscriptions = map[ID]weak.Pointer[Subscription]{}
	}
	if _, exists := c.subscriptions[opt.ID]; exists {
		c.subMu.Unlock()
		return nil, ErrPrecondition
	}
	c.subscriptions[opt.ID] = weak.Make(sub)
	c.subMu.Unlock()
	sub.cleanup = runtime.AddCleanup(sub, func(arg subscriptionCleanup) { arg.client.abandonSubscription(arg.id) }, subscriptionCleanup{c, opt.ID})
	e := Encoder{}
	c.objectRef(&e, id)
	e.ID(opt.ID)
	e.Bool(opt.Ranged)
	e.U64(opt.Offset)
	e.U64(opt.Length)
	e.Bool(opt.Resume != nil)
	if opt.Resume != nil {
		e.U64(*opt.Resume)
	}
	r, err := c.do(ctx, ObjectSubscribe, e.B, ID{}, false, nil)
	if err != nil {
		sub.cleanup.Stop()
		c.abandonSubscription(opt.ID)
		return nil, err
	}
	defer r.close()
	d := Decoder{B: r.result}
	returned, cursor := d.ID(), d.U64()
	info := readObjectInfo(&d)
	mode, total, digest, data := decodeTransfer(&d)
	expected := info.Length
	if opt.Ranged {
		expected = min(opt.Length, info.Length-min(opt.Offset, info.Length))
	}
	if d.Done() != nil || returned != opt.ID || cursor == 0 || total != expected || total > 16<<20 {
		err = ErrProtocol
		if total > 16<<20 {
			err = ErrRange
		}
	}
	if err == nil {
		if mode == 0 {
			if r.transfer != nil || uint64(len(data)) != total || sha256.Sum256(data) != digest {
				err = ErrProtocol
			} else {
				data = append([]byte(nil), data...)
			}
		} else if mode == 1 && r.transfer != nil && r.transfer.length == total && r.transfer.digest == digest {
			data, err = io.ReadAll(io.LimitReader(r.transfer.file, 16<<20+1))
		} else {
			err = ErrProtocol
		}
	}
	if err == nil {
		err = c.acceptObjectInfo(info)
	}
	if err != nil {
		sub.cleanup.Stop()
		c.abandonSubscription(opt.ID)
		return nil, err
	}
	sub.mu.Lock()
	sub.source = r.source
	sub.data = data
	sub.info = info
	sub.cursor = cursor
	sub.ready = true
	sub.valid = sub.err == nil
	pending := sub.pending
	sub.pending = nil
	sub.pendingBytes = 0
	for _, u := range pending {
		sub.applyLocked(u)
	}
	ackPending := sub.cursor > cursor
	sub.mu.Unlock()
	c.subMu.Lock()
	if c.subscriptionSources == nil {
		c.subscriptionSources = map[ID]*objectTransport{}
	}
	c.subscriptionSources[opt.ID] = r.source
	c.subMu.Unlock()
	// The initial snapshot has no retained journal entry. A redundant ACK
	// for each cold load can flood the bounded request queue while other
	// readers already admit their next SUBSCRIBE. Only buffered updates need
	// acknowledgment here; later pushes acknowledge themselves in receive.
	if ackPending {
		sub.ack()
	}
	runtime.KeepAlive(sub)
	return sub, nil
}
func (s *Subscription) ID() ID { return s.id }

// Snapshot returns an independent copy; callers cannot corrupt later readers.
func (s *Subscription) Snapshot() ([]byte, ObjectInfo, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.source != nil {
		select {
		case <-s.source.done:
			return nil, s.info, s.cursor, ErrSubscriptionResync
		default:
		}
	}
	if s.closed {
		return nil, s.info, s.cursor, ErrClosed
	}
	if !s.valid {
		return nil, s.info, s.cursor, ErrSubscriptionResync
	}
	if s.err != nil {
		return nil, s.info, s.cursor, s.err
	}
	return append([]byte(nil), s.data...), s.info, s.cursor, nil
}
func (s *Subscription) Changes() <-chan struct{} { return s.changed }

// SetObserver installs a nonblocking invalidation callback. It runs on the
// transport reader and must never issue RPCs or block on a filesystem request.
func (s *Subscription) SetObserver(fn func()) { s.mu.Lock(); s.observer = fn; s.mu.Unlock() }
func (s *Subscription) notify() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
	s.mu.RLock()
	fn := s.observer
	s.mu.RUnlock()
	if fn != nil {
		fn()
	}
}
func (s *Subscription) applyLocked(u ObjectUpdate) {
	if s.closed || u.Cursor <= s.cursor {
		return
	}
	if u.Change == UpdateResyncRequired || u.Part != 0 || u.Parts != 1 || u.Cursor != s.cursor+1 || u.PreviousVersion != s.info.Version || u.NewVersion <= u.PreviousVersion {
		s.valid = false
		s.err = ErrSubscriptionResync
		s.data = nil
		return
	}
	if !s.valid {
		return
	}
	switch u.Change {
	case UpdateReplace:
		length := u.NewLength
		off := uint64(0)
		if s.options.Ranged {
			off = s.options.Offset
			length = min(s.options.Length, u.NewLength-min(off, u.NewLength))
		}
		if u.Offset != off || uint64(len(u.Data)) != length {
			s.valid = false
			s.err = ErrSubscriptionResync
			return
		}
		s.data = append([]byte(nil), u.Data...)
		s.err = nil
	case UpdateDelete:
		s.data = nil
		s.err = ErrNotFound
	default:
		s.valid = false
		s.err = ErrSubscriptionResync
		return
	}
	s.info.Version = u.NewVersion
	s.info.Length = u.NewLength
	s.cursor = u.Cursor
}
func (s *Subscription) receive(t *objectTransport, u ObjectUpdate) {
	s.mu.Lock()
	if s.source != nil && s.source != t {
		s.mu.Unlock()
		return
	}
	s.source = t
	if !s.ready {
		if len(s.pending) >= 32 || s.pendingBytes+len(u.Data) > 2<<20 {
			s.valid = false
			s.err = ErrSubscriptionResync
		} else {
			s.pending = append(s.pending, u)
			s.pendingBytes += len(u.Data)
		}
		s.mu.Unlock()
		return
	}
	s.applyLocked(u)
	s.mu.Unlock()
	s.notify()
	s.ack()
}
func (s *Subscription) ack() {
	s.mu.RLock()
	t, cursor, valid := s.source, s.cursor, s.valid
	s.mu.RUnlock()
	if t == nil || !valid {
		return
	}
	e := Encoder{}
	e.ID(s.id)
	e.U64(cursor)
	if err := t.enqueue(context.Background(), outboundObject{message: Message{Kind: FireAndForget, Opcode: SubscriptionAck, ID: NewRequestID(), Body: e.B}}, false); err != nil {
		t.fail(err)
	}
}
func (s *Subscription) invalidate() {
	s.mu.Lock()
	s.valid = false
	s.data = nil
	s.err = ErrSubscriptionResync
	s.mu.Unlock()
	s.notify()
}
func (s *Subscription) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.valid = false
	s.data = nil
	t := s.source
	s.mu.Unlock()
	s.cleanup.Stop()
	s.client.subMu.Lock()
	delete(s.client.subscriptions, s.id)
	delete(s.client.subscriptionSources, s.id)
	s.client.subMu.Unlock()
	s.notify()
	if t == nil {
		return nil
	}
	e := Encoder{}
	e.ID(s.id)
	r, err := t.roundTrip(ctx, Message{Kind: Request, Opcode: ObjectUnsubscribe, ID: NewRequestID(), Body: e.B}, false, nil)
	r.close()
	if err != nil {
		if enqueueErr := t.enqueue(context.Background(), outboundObject{message: Message{Kind: FireAndForget, Opcode: ObjectUnsubscribe, ID: NewRequestID(), Body: e.B}}, false); enqueueErr != nil {
			t.fail(enqueueErr)
		}
	}
	return err
}
func (c *Client) abandonSubscription(id ID) {
	c.subMu.Lock()
	ptr, ok := c.subscriptions[id]
	delete(c.subscriptions, id)
	c.subMu.Unlock()
	if !ok {
		return
	}
	// If the weak target has already been collected, the transport binding is
	// kept separately so cleanup still sends its unsubscribe.
	c.subMu.Lock()
	t := c.subscriptionSources[id]
	delete(c.subscriptionSources, id)
	c.subMu.Unlock()
	if sub := ptr.Value(); sub != nil {
		sub.mu.RLock()
		if t == nil {
			t = sub.source
		}
		sub.mu.RUnlock()
	}
	if t != nil {
		e := Encoder{}
		e.ID(id)
		if err := t.enqueue(context.Background(), outboundObject{message: Message{Kind: FireAndForget, Opcode: ObjectUnsubscribe, ID: NewRequestID(), Body: e.B}}, false); err != nil {
			t.fail(err)
		}
	}
}
func (c *Client) receiveSubscription(t *objectTransport, m Message) error {
	if m.Opcode == SubscriptionMoved {
		d := Decoder{B: m.Body}
		id, cursor := d.ID(), d.U64()
		patch, err := DecodeMapPatch(d.B)
		if d.Err != nil || id == (ID{}) || cursor == 0 || err != nil || patch.Bucket != c.bucket {
			return ErrProtocol
		}
		if _, err = c.routes.Apply(patch); err != nil {
			return err
		}
		c.rememberRouters(patch)
		c.subMu.Lock()
		sub := c.subscriptions[id].Value()
		c.subMu.Unlock()
		if sub != nil {
			sub.invalidate()
		}
		return nil
	}
	u, err := DecodeObjectUpdate(m.Body)
	if err != nil {
		return err
	}
	c.subMu.Lock()
	sub := c.subscriptions[u.Subscription].Value()
	if c.subscriptionSources == nil {
		c.subscriptionSources = map[ID]*objectTransport{}
	}
	if sub != nil {
		c.subscriptionSources[u.Subscription] = t
	}
	c.subMu.Unlock()
	if sub != nil {
		sub.receive(t, u)
	}
	return nil
}
func (c *Client) failSubscriptions(t *objectTransport) {
	c.subMu.Lock()
	var subs []*Subscription
	for id, p := range c.subscriptions {
		if c.subscriptionSources[id] == t {
			if sub := p.Value(); sub != nil {
				subs = append(subs, sub)
			}
			delete(c.subscriptionSources, id)
		}
	}
	c.subMu.Unlock()
	for _, sub := range subs {
		sub.invalidate()
	}
}

func (c *Client) bindSubscriptionTransport(id ID, t *objectTransport) {
	c.subMu.Lock()
	sub := c.subscriptions[id].Value()
	if sub != nil {
		if c.subscriptionSources == nil {
			c.subscriptionSources = map[ID]*objectTransport{}
		}
		c.subscriptionSources[id] = t
	}
	c.subMu.Unlock()
	if sub != nil {
		sub.mu.Lock()
		if !sub.ready {
			sub.source = t
		}
		sub.mu.Unlock()
	}
}
