// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type connectionTestWrite struct {
	id     ID
	object ObjectID
	data   []byte
}
type connectionTestJob struct {
	done   chan struct{}
	status int
}
type connectionTestPeer struct {
	t           *testing.T
	node        ID
	boot        ID
	gate        func(ObjectID) <-chan struct{}
	status      func(ObjectID) int
	started     chan struct{}
	received    chan connectionTestWrite
	fences      chan struct{}
	connections atomic.Int64
	puts        atomic.Int64
	closeSocket chan struct{}
}

func newConnectionTestPeer(t *testing.T, node byte) (*connectionTestPeer, *httptest.Server) {
	p := &connectionTestPeer{t: t, node: ID{node}, boot: ID{node, 99}, started: make(chan struct{}, 512), received: make(chan connectionTestWrite, 512), fences: make(chan struct{}, 32), closeSocket: make(chan struct{})}
	host := httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(host.Close)
	return p, host
}
func (p *connectionTestPeer) serve(w http.ResponseWriter, r *http.Request) {
	ws, e := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer ws.Close()
	p.connections.Add(1)
	var sender sync.Mutex
	send := func(m Message, status int, body []byte) {
		sender.Lock()
		defer sender.Unlock()
		_ = ws.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: ResponseBody(status, body, "")}))
	}
	go func() {
		select {
		case <-p.closeSocket:
			ws.Close()
		case <-r.Context().Done():
		}
	}()
	var jobs []*connectionTestJob
	for {
		_, raw, e := ws.ReadMessage()
		if e != nil {
			return
		}
		m, e := DecodeMessage(raw)
		if e != nil {
			p.t.Error(e)
			return
		}
		switch m.Opcode {
		case Hello:
			result := Encoder{}
			result.U16(2)
			result.U64(CapabilityConnectionSync | CapabilityDurability)
			result.ID(p.node)
			result.ID(ID{42})
			result.U64(1)
			result.U64(1)
			send(m, 200, result.B)
		case ObjectAccepted:
			if m.Kind != FireAndForget {
				p.t.Error("write requested an individual ACK")
				return
			}
			d := Decoder{B: m.Body}
			d.ID()
			var object ObjectID
			copy(object[:], d.Take(32))
			d.Take(32)
			op := d.U16()
			if op == objectPutRange {
				d.U64()
			}
			if d.Bool() {
				d.U64()
			}
			d.Bool()
			if d.Bool() {
				d.U64()
				d.Blob()
			}
			mode, size, _, data := decodeTransfer(&d)
			if d.Done() != nil || mode != 0 || size != uint64(len(data)) {
				p.t.Error("bad no-ACK payload")
				return
			}
			job := &connectionTestJob{done: make(chan struct{})}
			jobs = append(jobs, job)
			p.started <- struct{}{}
			payload := bytes.Clone(data)
			go func(m Message, object ObjectID, job *connectionTestJob) {
				if p.gate != nil {
					select {
					case <-p.gate(object):
					case <-r.Context().Done():
						return
					}
				}
				if p.status != nil {
					job.status = p.status(object)
				}
				if job.status >= 300 {
					send(m, job.status, nil)
				} else {
					p.puts.Add(1)
				}
				p.received <- connectionTestWrite{m.ID, object, payload}
				close(job.done)
			}(m, object, job)
		case ConnectionSync:
			if len(m.Body) != 0 {
				p.t.Error("fence body is not empty")
				return
			}
			capture := append([]*connectionTestJob(nil), jobs...)
			jobs = nil
			p.fences <- struct{}{}
			go func(m Message) {
				status := 200
				for _, job := range capture {
					select {
					case <-job.done:
					case <-r.Context().Done():
						return
					}
					if status == 200 && job.status >= 300 {
						status = job.status
					}
				}
				result := Encoder{}
				result.ID(p.boot)
				result.U64(uint64(len(capture)))
				send(m, status, result.B)
			}(m)
		case objectStat:
			result := Encoder{}
			fixtureInfo(&result, 0, 0)
			send(m, 200, result.B)
		default:
			p.t.Errorf("unexpected opcode %x", m.Opcode)
			return
		}
	}
}
func dialConnectionTest(t *testing.T, host *httptest.Server) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, e := Dial(ctx, host.URL, "user", "credential", ID{7}, "fs")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func waitConnectionSignal(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not arrive")
	}
}

func TestConnectionAsyncOneSocketAndFenceCut(t *testing.T) {
	first, late := make(chan struct{}), make(chan struct{})
	p, host := newConnectionTestPeer(t, 1)
	p.gate = func(id ObjectID) <-chan struct{} {
		if id[0] == 1 {
			return first
		}
		return late
	}
	c := dialConnectionTest(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	payload := []byte("original")
	if e := c.PutAsync(ctx, ObjectID{1}, payload, PutOptions{}); e != nil {
		t.Fatal(e)
	}
	copy(payload, []byte("modified"))
	synced := make(chan error, 1)
	go func() { synced <- c.Sync(ctx) }()
	waitConnectionSignal(t, p.fences)
	if _, e := c.Stat(ctx, ObjectID{9}); e != nil {
		t.Fatal("blocked write/fence pinned independent request", e)
	}
	if e := c.PutAsync(ctx, ObjectID{2}, []byte("later"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	close(first)
	select {
	case e := <-synced:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("later write postponed earlier fence")
	}
	got := <-p.received
	if string(got.data) != "original" {
		t.Fatal("caller buffer retained after submission", string(got.data))
	}
	if !c.transport.hasConnectionWork() {
		t.Fatal("earlier fence consumed a later write")
	}
	close(late)
	if e := c.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	if p.connections.Load() != 1 {
		t.Fatal("parallel work opened extra connections", p.connections.Load())
	}
}
func TestConnectionAsyncErrorsAndLostSocket(t *testing.T) {
	t.Run("quota", func(t *testing.T) {
		p, host := newConnectionTestPeer(t, 1)
		p.status = func(ObjectID) int { return 507 }
		c := dialConnectionTest(t, host)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		request := NewRequestID()
		if e := c.PutAsync(ctx, ObjectID{1}, []byte("quota"), PutOptions{RequestID: request}); e != nil {
			t.Fatal(e)
		}
		e := c.Sync(ctx)
		var status *StatusError
		if !errors.As(e, &status) || status.Status != 507 || status.RequestID != request {
			t.Fatal("deferred failure lost its identity", e)
		}
		if e = c.Sync(ctx); e != nil {
			t.Fatal("consumed error poisoned later fences", e)
		}
	})
	t.Run("disconnect", func(t *testing.T) {
		gate := make(chan struct{})
		p, host := newConnectionTestPeer(t, 1)
		p.gate = func(ObjectID) <-chan struct{} { return gate }
		c := dialConnectionTest(t, host)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		request := NewRequestID()
		if e := c.PutAsync(ctx, ObjectID{1}, []byte("unknown"), PutOptions{RequestID: request}); e != nil {
			t.Fatal(e)
		}
		close(p.closeSocket)
		select {
		case <-c.transport.done:
		case <-ctx.Done():
			t.Fatal("socket did not close")
		}
		var unknown *UnknownOutcomeError
		if e := c.Sync(ctx); !errors.As(e, &unknown) || unknown.RequestID != request {
			t.Fatal("replaced transport silently confirmed write", e)
		}
		close(gate)
	})
}
func TestConnectionAsyncOwnerFencesRunConcurrently(t *testing.T) {
	a, ha := newConnectionTestPeer(t, 1)
	b, hb := newConnectionTestPeer(t, 2)
	ga, gb := make(chan struct{}), make(chan struct{})
	a.gate = func(ObjectID) <-chan struct{} { return ga }
	b.gate = func(ObjectID) <-chan struct{} { return gb }
	c := dialConnectionTest(t, ha)
	patch := testMapPatch()
	patch.Bucket = c.bucket
	patch.Policies[0].Bucket = c.bucket
	left, right, e := SplitRange(patch.Entries[0])
	if e != nil {
		t.Fatal(e)
	}
	left.Locations = []Location{{Node: a.node}}
	right.Locations = []Location{{Node: b.node}}
	patch.Entries = []RangeEntry{left, right}
	patch.Addresses = []NodeAddress{{Node: a.node, Addresses: []string{ha.URL}}, {Node: b.node, Addresses: []string{hb.URL}}}
	if _, e = c.routes.Apply(patch); e != nil {
		t.Fatal(e)
	}
	c.rememberRouters(patch)
	c.mu.Lock()
	c.clusterRouting = true
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if e = c.PutAsync(ctx, left.Start, []byte("left"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	if e = c.PutAsync(ctx, right.Start, []byte("right"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- c.Sync(ctx) }()
	waitConnectionSignal(t, a.fences)
	waitConnectionSignal(t, b.fences)
	close(ga)
	close(gb)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if a.connections.Load() != 1 || b.connections.Load() != 1 {
		t.Fatal("fence broadcast or opened redundant owner connection")
	}
}
func TestConnectionAsyncLegacyFallback(t *testing.T) {
	p, host := newWirePeer(t)
	c := dialConnectionTest(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e := c.PutAsync(ctx, ObjectID{1}, []byte("fallback"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	if p.commits.Load() != 1 {
		t.Fatal("old peer write skipped")
	}
	if e := c.Sync(ctx); e != nil {
		t.Fatal(e)
	}
}

type burstIntentStore struct {
	*MemoryFilesystemStore
	entered    chan ObjectID
	release    chan struct{}
	descriptor ObjectID
	published  atomic.Bool
}

func (s *burstIntentStore) ConcurrentObjectRequests() bool { return true }
func (s *burstIntentStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	if id == s.descriptor {
		s.published.Store(true)
	} else {
		s.entered <- id
		select {
		case <-s.release:
		case <-ctx.Done():
			return ObjectInfo{}, ctx.Err()
		}
	}
	return s.MemoryFilesystemStore.CAS(ctx, id, off, expected, replacement, opt)
}
func TestFilesystemImmutableBurstPreservesPublicationOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := ObjectID{88}
	s := &burstIntentStore{MemoryFilesystemStore: NewMemoryFilesystemStore(), entered: make(chan ObjectID, 4), release: make(chan struct{}), descriptor: id}
	f := &Filesystem{store: s}
	raw := append(bytes.Repeat([]byte{1}, FSObjectSize), bytes.Repeat([]byte{2}, 99)...)
	done := make(chan error, 1)
	go func() { _, err := f.storeIntentAt(ctx, id, raw); done <- err }()
	for i := 0; i < 2; i++ {
		select {
		case <-s.entered:
		case <-ctx.Done():
			t.Fatal("immutable writes serialized", ctx.Err())
		}
	}
	if s.published.Load() {
		t.Fatal("descriptor published before durable pieces")
	}
	close(s.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	descriptor, _, err := s.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	d := Decoder{B: descriptor}
	d.Take(4)
	d.U16()
	d.U8()
	d.U32()
	count := d.U32()
	var assembled []byte
	for i := uint32(0); i < count; i++ {
		var part ObjectID
		copy(part[:], d.Take(32))
		b, _, err := s.Get(ctx, part)
		if err != nil {
			t.Fatal(err)
		}
		assembled = append(assembled, b...)
	}
	if d.Done() != nil || !bytes.Equal(assembled, raw) {
		t.Fatal("completion order changed intent bytes")
	}
}

func TestConnectionAsyncCloseDoesNotFence(t *testing.T) {
	p, host := newConnectionTestPeer(t, 1)
	gate := make(chan struct{})
	p.gate = func(ObjectID) <-chan struct{} { return gate }
	c := dialConnectionTest(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.PutAsync(ctx, ObjectID{1}, []byte("unfenced"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.started:
	case <-ctx.Done():
		t.Fatal("server did not admit queued write", ctx.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close waited for storage", ctx.Err())
	}
	select {
	case <-p.fences:
		t.Fatal("Close implicitly requested durability")
	default:
	}
	close(gate)
}
