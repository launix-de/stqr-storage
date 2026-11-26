// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ConnectionSync fences earlier messages on one directly connected owner.
// Its counters describe volatile receive progress, never object versions.
const ConnectionSync uint16 = 0x0122
const CapabilityConnectionSync uint64 = 1 << 19

type connectionSubmission struct {
	sequence        uint64
	noAck, accepted bool
	message         Message // routing reference only; payload ownership stays with send queue
	err             error
}
type connectionCut struct{ sequence, readSequence uint64 }
type connectionCutKey struct{}

func (c *Client) rememberConnection(t *objectTransport) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.connectionTransports == nil {
		c.connectionTransports = map[*objectTransport]struct{}{}
	}
	c.connectionTransports[t] = struct{}{}
	c.mu.Unlock()
}
func (t *objectTransport) noteConnectionDependency(boot ID) {
	t.mu.Lock()
	t.connectionSequence++
	t.connectionReadSequence = t.connectionSequence
	if t.connectionBoot == (ID{}) {
		t.connectionBoot = boot
	}
	if t.subscriptionClient != nil {
		t.subscriptionClient.rememberConnection(t)
	}
	t.mu.Unlock()
}

// enqueueOperation establishes a local send cut before starting any wait. A
// connection fence and a concurrent submission cannot pass one another between
// recording outstanding work and enqueueing its frame.
func (t *objectTransport) enqueueOperation(ctx context.Context, m Message, mutation bool) error {
	if t.capabilities&CapabilityConnectionSync == 0 {
		return t.enqueue(ctx, outboundObject{message: m}, false)
	}
	t.submitMu.Lock()
	defer t.submitMu.Unlock()
	var tracked *connectionSubmission
	t.mu.Lock()
	if mutation {
		if t.connectionWrites == nil {
			t.connectionWrites = map[ID]*connectionSubmission{}
		}
		if existing := t.connectionWrites[m.ID]; existing != nil && existing.noAck {
			t.mu.Unlock()
			return ErrWouldBlock
		}
		t.connectionSequence++
		ref := m
		ref.Body = append([]byte(nil), m.Body[:min(80, len(m.Body))]...)
		// ObjectAccepted's inner opcode is needed only to learn an error's map patch.
		if m.Opcode == ObjectAccepted && len(m.Body) >= 82 {
			ref.Opcode = uint16(m.Body[80]) | uint16(m.Body[81])<<8
		}
		tracked = &connectionSubmission{sequence: t.connectionSequence, noAck: m.Kind == FireAndForget, accepted: m.Opcode == ObjectAccepted || m.Opcode == ObjectReference, message: ref}
		t.connectionWrites[m.ID] = tracked
	}
	if cut, _ := ctx.Value(connectionCutKey{}).(*connectionCut); cut != nil && m.Opcode == ConnectionSync {
		cut.sequence = t.connectionSequence
		cut.readSequence = t.connectionReadSequence
	}
	t.mu.Unlock()
	if mutation && t.subscriptionClient != nil {
		t.subscriptionClient.rememberConnection(t)
	}
	if err := t.enqueue(ctx, outboundObject{message: m}, true); err != nil {
		if tracked != nil {
			t.mu.Lock()
			if t.connectionWrites[m.ID] == tracked {
				delete(t.connectionWrites, m.ID)
			}
			t.mu.Unlock()
		}
		return err
	}
	return nil
}
func (t *objectTransport) finishConnectionRequest(id ID, r wireReply, err error) {
	if t.capabilities&CapabilityConnectionSync == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if work := t.connectionWrites[id]; work != nil && !work.noAck {
		if err != nil || !work.accepted && len(r.headers[DurabilityHeader]) == 0 {
			delete(t.connectionWrites, id)
		}
	}
}
func (t *objectTransport) receiveConnectionError(m Message) error {
	t.mu.Lock()
	work := t.connectionWrites[m.ID]
	t.mu.Unlock()
	if work == nil || !work.noAck {
		return nil
	}
	if m.Opcode != ObjectAccepted {
		return ErrProtocol
	}
	reply, err := decodeWireReply(m)
	if err != nil {
		return err
	}
	if reply.status < 300 {
		return ErrProtocol
	} // no success ACK was requested
	failure := &StatusError{Status: reply.status, Message: responseDetail(reply.headers), RequestID: m.ID, Headers: reply.headers}
	if (reply.status == 301 || reply.status == 302) && t.subscriptionClient != nil {
		if err := t.subscriptionClient.learnRedirect(work.message, failure); err != nil {
			return err
		}
	}
	t.mu.Lock()
	if current := t.connectionWrites[m.ID]; current == work {
		work.err = failure
	}
	t.mu.Unlock()
	return nil
}
func (t *objectTransport) hasConnectionWork() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.connectionWrites) > 0 || t.connectionReadSequence > t.connectionReadSynced
}
func (t *objectTransport) syncConnection(ctx context.Context) error {
	t.connectionSyncMu.Lock()
	defer t.connectionSyncMu.Unlock()
	if !t.hasConnectionWork() {
		return nil
	}
	// A replacement socket cannot confirm the prior connection's admission cut.
	select {
	case <-t.done:
		t.mu.Lock()
		defer t.mu.Unlock()
		for id, work := range t.connectionWrites {
			if work.noAck || work.accepted {
				if work.err != nil {
					return work.err
				}
				return &UnknownOutcomeError{RequestID: id, Cause: t.cause}
			}
		}
		return &UnknownOutcomeError{Cause: t.cause}
	default:
	}
	cut := &connectionCut{}
	r, err := t.roundTrip(context.WithValue(ctx, connectionCutKey{}, cut), Message{Kind: Request, Opcode: ConnectionSync, ID: NewRequestID()}, false, nil)
	defer r.close()
	t.mu.Lock()
	defer t.mu.Unlock()
	var first *connectionSubmission
	for _, work := range t.connectionWrites {
		if work.sequence <= cut.sequence && work.noAck && work.err != nil && (first == nil || work.sequence < first.sequence) {
			first = work
		}
	}
	if err != nil {
		var status *StatusError
		var prior *StatusError
		if errors.As(err, &status) && first != nil && errors.As(first.err, &prior) && status.Status == prior.Status && (status.Message == prior.Message || status.Message == fmt.Sprintf("request %s opcode %04x: %s", first.message.ID.String(), ObjectAccepted, prior.Message)) {
			// All prior errors precede the fence response. Surface each failed write
			// through this fence once, retaining successful tickets for a later ACK.
			for id, work := range t.connectionWrites {
				if work.sequence <= cut.sequence && work.noAck && work.err != nil {
					delete(t.connectionWrites, id)
				}
			}
			return first.err
		}
		return err
	}
	d := Decoder{B: r.result}
	boot := d.ID()
	d.U64()
	if d.Done() != nil || boot == (ID{}) || t.connectionBoot != (ID{}) && boot != t.connectionBoot {
		return ErrProtocol
	}
	t.connectionBoot = boot
	for id, work := range t.connectionWrites {
		if work.sequence <= cut.sequence {
			delete(t.connectionWrites, id)
		}
	}
	if t.connectionReadSynced < cut.readSequence {
		t.connectionReadSynced = cut.readSequence
	}
	if first != nil {
		return first.err
	}
	return nil
}
func (c *Client) syncConnections(ctx context.Context, except *objectTransport) error {
	c.mu.Lock()
	transports := make([]*objectTransport, 0, len(c.connectionTransports))
	for t := range c.connectionTransports {
		if t != except {
			transports = append(transports, t)
		}
	}
	c.mu.Unlock()
	sort.Slice(transports, func(i, j int) bool { return transports[i].endpoint < transports[j].endpoint })
	errs := make([]error, len(transports))
	var calls sync.WaitGroup
	for i, t := range transports {
		calls.Add(1)
		go func(i int, t *objectTransport) {
			defer calls.Done()
			errs[i] = t.syncConnection(ctx)
			// Retain failed/uncertain sockets, not every clean historical
			// transport. Admission and dependency observation use submitMu too.
			t.submitMu.Lock()
			t.mu.Lock()
			if len(t.connectionWrites) == 0 && t.connectionReadSequence <= t.connectionReadSynced {
				c.mu.Lock()
				delete(c.connectionTransports, t)
				c.mu.Unlock()
			}
			t.mu.Unlock()
			t.submitMu.Unlock()
		}(i, t)
	}
	calls.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ConcurrentObjectRequests enables independent object bursts in format libraries.
func (c *Client) ConcurrentObjectRequests() bool {
	t, err := c.activeTransport()
	return err == nil && t != nil && t.capabilities&CapabilityConnectionSync != 0
}

// PutAsync submits an object write without an individual success ACK. Sync is
// the durability/error boundary. A later redirect/quota failure is returned by
// Sync with the original request identity. The caller may reuse data on return.
// Operations may complete out of order. Fence before submitting an overlapping
// dependent write or reading its result. Older peers retain acknowledged fallback.
func (c *Client) PutAsync(ctx context.Context, id ObjectID, data []byte, opt PutOptions) error {
	return c.putAsync(ctx, objectPut, id, 0, data, opt)
}
func (c *Client) PutRangeAsync(ctx context.Context, id ObjectID, off uint64, data []byte, opt PutOptions) error {
	return c.putAsync(ctx, objectPutRange, id, off, data, opt)
}
func (c *Client) putAsync(ctx context.Context, op uint16, id ObjectID, off uint64, data []byte, opt PutOptions) error {
	if err := c.ready(ctx); err != nil {
		return err
	}
	if uint64(len(data)) > ^uint64(0)-off {
		return ErrRange
	}
	if len(data) > transferChunk {
		opt.Accepted = true
		if op == objectPut {
			_, err := c.Put(ctx, id, data, opt)
			return err
		}
		_, err := c.PutRange(ctx, id, off, data, opt)
		return err
	}
	c.mu.Lock()
	maintenance := c.maintenance
	c.mu.Unlock()
	if maintenance != nil {
		opt.Accepted = true
		if op == objectPut {
			_, err := c.Put(ctx, id, data, opt)
			return err
		}
		_, err := c.PutRange(ctx, id, off, data, opt)
		return err
	}
	e := Encoder{}
	c.objectRef(&e, id)
	if op == objectPutRange {
		e.U64(off)
	}
	if err := encodePrecondition(&e, opt); err != nil {
		return err
	}
	e.U8(0)
	e.U64(uint64(len(data)))
	hash := sha256.Sum256(data)
	e.B = append(e.B, hash[:]...)
	e.Blob(data)
	request := opt.RequestID
	if request == (ID{}) {
		request = NewRequestID()
	}
	m := Message{Kind: Request, Opcode: op, ID: request, Body: e.B}
	if err := c.ensureRoute(ctx, m); err != nil {
		return err
	}
	endpoint, node, err := c.routeMessage(&m)
	if err != nil {
		return err
	}
	t, err := c.connectionFor(ctx, endpoint, node)
	if err != nil {
		return err
	}
	if t.capabilities&CapabilityConnectionSync == 0 {
		opt.Accepted = true
		opt.RequestID = request
		if op == objectPut {
			_, err = c.Put(ctx, id, data, opt)
		} else {
			_, err = c.PutRange(ctx, id, off, data, opt)
		}
		return err
	}
	m = acceptedMessage(m)
	m.Kind = FireAndForget
	return t.enqueueOperation(ctx, m, true)
}
