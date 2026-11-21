// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const objectSendQueue = 32
const maxPendingObjectRequests = 12

type wireReply struct {
	status   int
	headers  map[uint64][]byte
	result   []byte
	transfer *incomingTransfer
	err      error
}

func (r *wireReply) close() {
	if r.transfer != nil {
		r.transfer.close()
		r.transfer = nil
	}
}

type pendingObject struct {
	opcode         uint16
	reply          chan wireReply
	sent           bool
	uploadFinished atomic.Bool
	completed      atomic.Bool
	canceled       bool
	upload         bool
	streamMu       sync.Mutex
	transfer       *incomingTransfer
}
type outboundObject struct {
	message Message
	parent  ID
}
type uploadFunc func(context.Context, *objectTransport, *pendingObject, ID) error
type objectTransport struct {
	conn    *websocket.Conn
	mu      sync.Mutex
	pending map[ID]*pendingObject
	send    chan outboundObject
	done    chan struct{}
	once    sync.Once
	workers sync.WaitGroup
	cause   error
}

func newObjectTransport(conn *websocket.Conn) *objectTransport {
	t := &objectTransport{conn: conn, pending: map[ID]*pendingObject{}, send: make(chan outboundObject, objectSendQueue), done: make(chan struct{})}
	conn.SetReadLimit(MaxMessage)
	t.workers.Add(2)
	go func() { defer t.workers.Done(); t.readLoop() }()
	go func() { defer t.workers.Done(); t.writeLoop() }()
	return t
}
func (t *objectTransport) fail(e error) {
	t.once.Do(func() {
		t.mu.Lock()
		t.cause = e
		close(t.done)
		for id, p := range t.pending {
			delete(t.pending, id)
			p.reply <- wireReply{err: e}
		}
		t.mu.Unlock()
		t.conn.Close()
	})
}
func (t *objectTransport) enqueue(ctx context.Context, out outboundObject, wait bool) error {
	if wait {
		select {
		case t.send <- out:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-t.done:
			return t.failure()
		}
	}
	select {
	case <-t.done:
		return t.failure()
	default:
	}
	select {
	case t.send <- out:
		return nil
	default:
		return ErrWouldBlock
	}
}
func (t *objectTransport) failure() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cause == nil {
		return ErrClosed
	}
	return t.cause
}
func (t *objectTransport) roundTrip(ctx context.Context, m Message, mutation bool, upload uploadFunc) (wireReply, error) {
	if e := ctx.Err(); e != nil {
		return wireReply{}, e
	}
	p := &pendingObject{opcode: m.Opcode, reply: make(chan wireReply, 1), upload: upload != nil}
	t.mu.Lock()
	if len(t.pending) >= maxPendingObjectRequests {
		t.mu.Unlock()
		return wireReply{}, ErrWouldBlock
	}
	select {
	case <-t.done:
		t.mu.Unlock()
		return wireReply{}, t.failure()
	default:
	}
	if _, exists := t.pending[m.ID]; exists {
		t.mu.Unlock()
		return wireReply{}, ErrWouldBlock
	}
	t.pending[m.ID] = p
	t.mu.Unlock()
	cleanup := func() {
		t.mu.Lock()
		if t.pending[m.ID] == p {
			delete(t.pending, m.ID)
		}
		t.mu.Unlock()
		p.streamMu.Lock()
		p.canceled = true
		select {
		case left := <-p.reply:
			left.close()
		default:
		}
		if p.transfer != nil {
			p.transfer.close()
			p.transfer = nil
		}
		p.streamMu.Unlock()
	}
	defer cleanup()
	if e := t.enqueue(ctx, outboundObject{message: m}, false); e != nil {
		return wireReply{}, e
	}
	if upload != nil {
		if e := upload(ctx, t, p, m.ID); e != nil && e != errEarlyReply {
			select {
			case r := <-p.reply:
				if r.err != nil {
					return wireReply{}, r.err
				}
				return r, nil
			default:
				return wireReply{}, &uploadError{Cause: e}
			}
		}
	}
	select {
	case r := <-p.reply:
		if r.err != nil {
			return wireReply{}, r.err
		}
		return r, nil
	case <-ctx.Done():
		t.mu.Lock()
		sent := p.sent
		delete(t.pending, m.ID)
		t.mu.Unlock()
		cancel := Encoder{}
		cancel.ID(m.ID)
		_ = t.enqueue(context.Background(), outboundObject{message: Message{Kind: FireAndForget, Opcode: 7, ID: NewRequestID(), Body: cancel.B}}, false)
		if mutation && sent {
			return wireReply{}, &UnknownOutcomeError{RequestID: m.ID, Cause: ctx.Err()}
		}
		return wireReply{}, ctx.Err()
	}
}
func (t *objectTransport) writeLoop() {
	for {
		select {
		case <-t.done:
			return
		case out := <-t.send:
			t.mu.Lock()
			valid := true
			if out.message.Kind == Request {
				p := t.pending[out.message.ID]
				if p == nil {
					valid = false
				} else {
					p.sent = true
				}
			} else if out.parent != (ID{}) {
				parent := t.pending[out.parent]
				valid = parent != nil
				if valid && out.message.Opcode == 6 {
					parent.uploadFinished.Store(true)
				}
			}
			t.mu.Unlock()
			if !valid {
				continue
			}
			t.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if e := t.conn.WriteMessage(websocket.BinaryMessage, EncodeMessage(out.message)); e != nil {
				t.fail(e)
				return
			}
		}
	}
}
func (t *objectTransport) readLoop() {
	defer t.fail(ErrClosed)
	for {
		kind, b, e := t.conn.ReadMessage()
		if e != nil {
			t.fail(e)
			return
		}
		if kind != websocket.BinaryMessage {
			t.fail(ErrProtocol)
			return
		}
		m, e := DecodeMessage(b)
		if e != nil {
			t.fail(e)
			return
		}
		if m.Kind == Push {
			if m.Opcode >= 5 && m.Opcode <= 6 || m.Opcode == 13 {
				if e = t.streamMessage(m); e != nil {
					t.fail(e)
					return
				}
			}
			continue
		}
		if m.Kind != Response {
			t.fail(ErrProtocol)
			return
		}
		t.mu.Lock()
		p := t.pending[m.ID]
		if p != nil {
			delete(t.pending, m.ID)
		}
		t.mu.Unlock()
		if p == nil {
			continue
		}
		if p.opcode != m.Opcode {
			p.reply <- wireReply{err: ErrProtocol}
			t.fail(ErrProtocol)
			return
		}
		r, e := decodeWireReply(m)
		if e == nil && r.status >= 300 {
			e = &StatusError{Status: r.status, Message: responseDetail(r.headers), RequestID: m.ID, Headers: r.headers}
		}
		p.streamMu.Lock()
		if p.canceled {
			p.streamMu.Unlock()
			continue
		}
		if e == nil && p.upload && !p.uploadFinished.Load() {
			e = ErrProtocol
		}
		if e == nil && p.transfer != nil {
			if !p.transfer.ended {
				e = ErrProtocol
			} else {
				r.transfer = p.transfer
				p.transfer = nil
			}
		}
		r.err = e
		p.completed.Store(true)
		p.reply <- r
		p.streamMu.Unlock()
	}
}
func decodeWireReply(m Message) (wireReply, error) {
	d := Decoder{B: m.Body}
	status := d.V()
	if status < 100 || status > 599 {
		return wireReply{}, ErrProtocol
	}
	h := Decoder{B: d.Blob()}
	headers := map[uint64][]byte{}
	for n := h.Count(64); n > 0; n-- {
		tag := h.V()
		value := h.Blob()
		if _, exists := headers[tag]; exists {
			return wireReply{}, ErrProtocol
		}
		headers[tag] = value
	}
	if h.Done() != nil || d.Err != nil {
		return wireReply{}, ErrProtocol
	}
	return wireReply{status: int(status), headers: headers, result: d.B}, nil
}
func responseDetail(headers map[uint64][]byte) string {
	if b, ok := headers[3]; ok {
		d := Decoder{B: b}
		s := d.Text()
		if d.Done() == nil {
			return s
		}
	}
	return "request rejected"
}

type uploadError struct{ Cause error }

func (e *uploadError) Error() string { return e.Cause.Error() }
func (e *uploadError) Unwrap() error { return e.Cause }
