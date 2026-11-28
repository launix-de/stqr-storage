// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type mappedIdentityWriter struct {
	header, payload []byte
	calls           int
	closed          bool
	fail            int
}

func (w *mappedIdentityWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.fail == w.calls {
		return 0, io.ErrClosedPipe
	}
	want := w.header
	if w.calls == 2 {
		want = w.payload
	}
	if len(p) != len(want) || len(p) > 0 && &p[0] != &want[0] {
		return 0, errors.New("copied header or borrowed payload")
	}
	return len(p), nil
}
func (w *mappedIdentityWriter) Close() error { w.closed = true; return nil }
func TestMappedDownloadWritesBorrowedPayload(t *testing.T) {
	header := []byte("header")
	payload := bytes.Repeat([]byte("p"), 1<<20)
	view := volume.ReadView{Length: uint64(len(payload)), Segments: []volume.ReadSegment{{Data: payload, Length: uint64(len(payload)), Borrowed: true}}}
	w := &mappedIdentityWriter{header: header, payload: payload}
	if count, err := writeMappedFrame(w, header, view); err != nil || count != uint64(len(payload)) || w.calls != 2 || !w.closed {
		t.Fatal(count, err, w)
	}
	if n := testing.AllocsPerRun(20, func() {
		w.calls = 0
		w.closed = false
		if _, err := writeMappedFrame(w, header, view); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatal("frame output allocated", n)
	}
	for _, fail := range []int{1, 2} {
		w.calls = 0
		w.closed = false
		w.fail = fail
		if _, err := writeMappedFrame(w, header, view); !errors.Is(err, io.ErrClosedPipe) || w.closed {
			t.Fatal("partial frame flushed", err, w)
		}
	}
}

type mappedBlockedConn struct {
	net.Conn
	armed             atomic.Bool
	entered           chan struct{}
	closed            chan struct{}
	once, enteredOnce sync.Once
}

func (c *mappedBlockedConn) Write(p []byte) (int, error) {
	if c.armed.Load() {
		c.enteredOnce.Do(func() { close(c.entered) })
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}
func (c *mappedBlockedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type mappedListener struct {
	net.Listener
	accepted chan *mappedBlockedConn
}

func (l mappedListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	wrapped := &mappedBlockedConn{Conn: c, entered: make(chan struct{}), closed: make(chan struct{})}
	l.accepted <- wrapped
	return wrapped, nil
}

func mappedSocket(t *testing.T, s *Server) (*wsSession, *websocket.Conn, *mappedBlockedConn) {
	t.Helper()
	ready := make(chan *wsSession, 1)
	accepted := make(chan *mappedBlockedConn, 1)
	host := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ready <- &wsSession{server: s, conn: conn, ctx: context.Background(), done: make(chan struct{})}
	}))
	host.Listener = mappedListener{host.Listener, accepted}
	host.Start()
	t.Cleanup(host.Close)
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(host.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	ws := <-ready
	t.Cleanup(ws.close)
	return ws, c, <-accepted
}

func TestMappedDownloadWireAndCancellation(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{121}
	payload := bytes.Repeat([]byte("mapped"), 4096)
	if _, err := writer.Put(ctx, id, payload, client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(volume.ID(reader.BucketID()))
	if err != nil {
		t.Fatal(err)
	}
	p := userPrincipal(t, s)
	for _, ranged := range []bool{false, true} {
		ws, c, _ := mappedSocket(t, s)
		op := &objectMutation{bucket: b, object: volume.ObjectID(id)}
		m := client.Message{Kind: client.Request, Opcode: 0x100, ID: client.NewRequestID()}
		length := uint64(math.MaxUint64)
		e := client.Encoder{}
		e.ID(reader.BucketID())
		e.B = append(e.B, id[:]...)
		e.U64(0)
		e.U64(0)
		e.ID(client.ID{})
		if ranged {
			m.Opcode = 0x101
			op.offset = 7
			length = 37
			e.U64(op.offset)
			e.U64(length)
		}
		e.Bool(false)
		m.Body = e.B
		body, status, err := s.executeObjects(m, nil, p)
		if err != nil {
			t.Fatal(err)
		}
		want := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: s.durabilityResponse(status, body, b.ID())})
		done := make(chan error, 1)
		go func() {
			sent, code, err := ws.writeMappedObject(ctx, m, p, op, length, nil, client.ID{})
			if !sent || code != status {
				err = errors.New("response not sent")
			}
			done <- err
		}()
		_, got, err := c.ReadMessage()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("wire encoding differs", len(got), len(want), err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	// A condition error is returned before acquiring a frame writer.
	ws, _, blocked := mappedSocket(t, s)
	op := &objectMutation{bucket: b, object: volume.ObjectID(id)}
	m := client.Message{Kind: client.Request, Opcode: 0x100, ID: client.NewRequestID()}
	if sent, _, err := ws.writeMappedObject(ctx, m, p, op, 100, &volume.Options{Compare: []byte("wrong")}, client.ID{}); sent || !errors.Is(err, volume.ErrPrecondition) {
		t.Fatal("condition emitted frame", sent, err)
	}
	// Deterministic network backpressure after NextWriter: cancellation must close
	// the partial frame and release the volume handle, not emit an error frame.
	blocked.armed.Store(true)
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		sent, _, err := ws.writeMappedObject(cancelCtx, m, p, op, math.MaxUint64, nil, client.ID{})
		done <- sent && err != nil
	}()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("network write did not start")
	}
	cancel()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("partial frame failure not marked sent")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not release mapped writer")
	}
	if _, err := writer.Put(ctx, id, []byte("unlocked"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal("volume stayed locked", err)
	}
}

func TestMappedDownloadQueuedCancellation(t *testing.T) {
	ws := &wsSession{done: make(chan struct{}), borrowedWrites: make(chan borrowedWriteJob)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// No server/bucket is needed: a queued job must not even begin storage I/O.
	if sent, _, err := ws.directObjectDownload(ctx, client.Message{}, principal{}, nil, 0, nil, client.ID{}); sent || !errors.Is(err, context.Canceled) {
		t.Fatal(sent, err)
	}
	close(ws.done)
	if sent, _, err := ws.directObjectDownload(context.Background(), client.Message{}, principal{}, nil, 0, nil, client.ID{}); sent || !errors.Is(err, context.Canceled) {
		t.Fatal(sent, err)
	}
}

func TestMappedDownloadRetiresConnectionWork(t *testing.T) {
	_, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{122}
	if _, err := writer.Put(ctx, id, []byte("mapped reply"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if data, _, err := reader.Get(ctx, id); err != nil || string(data) != "mapped reply" {
			t.Fatal(string(data), err)
		}
	}
	fenceCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := reader.Sync(fenceCtx); err != nil {
		t.Fatal("direct replies left unfinished connection jobs", err)
	}
}

func TestMappedDownloadRejectsLeaseBeforeFrame(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{123}
	if _, err := writer.Put(ctx, id, []byte("protected"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(volume.ID(reader.BucketID()))
	if err != nil {
		t.Fatal(err)
	}
	p := userPrincipal(t, s)
	auth := &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketRead}
	auth.lease.Store(&bucketLease{Token: client.ID{1}})
	p.connection = auth
	b.mu.Lock()
	old := b.lease
	b.lease = &bucketLease{Token: client.ID{2}}
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.lease = old; b.mu.Unlock() }()
	// A nil transport would panic if the revoked lease reached frame output.
	ws := &wsSession{server: s}
	op := &objectMutation{bucket: b, object: volume.ObjectID(id)}
	if sent, _, err := ws.writeMappedObject(ctx, client.Message{}, p, op, 10, nil, client.ID{}); sent || !errors.Is(err, ErrBucketForbidden) {
		t.Fatal("revoked lease emitted response", sent, err)
	}
}

func TestMappedDownloadRejectsBindingBeforeFrame(t *testing.T) {
	s := startDaemonTestNode(t, t.TempDir()+"/node.yaml", nil, false).node
	addTestVolume(t, s, "mapped-owner", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "mapped-owner")
	id := volume.ObjectID{124}
	p := quotaPrincipal(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.Put(ctx, p, id, []byte("bound"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	ctx = context.WithValue(ctx, objectBindingKey{}, objectBinding{bucket: volume.ID{0xde}, object: id})
	ws := &wsSession{server: s}
	op := &objectMutation{bucket: b, object: id}
	if sent, _, err := ws.writeMappedObject(ctx, client.Message{}, p, op, 5, nil, client.ID{}); sent || statusOf(err) != 400 {
		t.Fatal("wrong binding emitted response", sent, err)
	}
}

func TestMappedDownloadFragmentFallback(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{125}
	if _, err := writer.Put(ctx, id, bytes.Repeat([]byte("f"), 1<<20), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(volume.ID(reader.BucketID()))
	if err != nil {
		t.Fatal(err)
	}
	// Accepted pages are separate RAM allocations, so this read must retain the
	// owned fallback rather than exceed the native client's64-fragment guard.
	ws := &wsSession{server: s}
	op := &objectMutation{bucket: b, object: volume.ObjectID(id)}
	if sent, _, err := ws.writeMappedObject(ctx, client.Message{}, userPrincipal(t, s), op, 1<<20, nil, client.ID{}); sent || err != nil {
		t.Fatal("fragmented read did not fall back before frame", sent, err)
	}
	if got, _, err := reader.Get(ctx, id); err != nil || len(got) != 1<<20 {
		t.Fatal("fallback failed", len(got), err)
	}
}
