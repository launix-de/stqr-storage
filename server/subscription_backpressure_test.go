// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type subscriptionBlockedConn struct {
	net.Conn
	armed                    atomic.Bool
	entered, closed, release chan struct{}
	once, enteredOnce        sync.Once
}

func (c *subscriptionBlockedConn) Write(p []byte) (int, error) {
	if c.armed.Load() {
		c.enteredOnce.Do(func() { close(c.entered) })
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.release:
		}
	}
	return c.Conn.Write(p)
}
func (c *subscriptionBlockedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type subscriptionBlockedListener struct {
	net.Listener
	accepted chan *subscriptionBlockedConn
}

func (l subscriptionBlockedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	wrapped := &subscriptionBlockedConn{Conn: c, entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	l.accepted <- wrapped
	return wrapped, nil
}
func subscriptionBlockedSocket(t *testing.T, s *Server) (*wsSession, *websocket.Conn, *subscriptionBlockedConn) {
	t.Helper()
	ready := make(chan *wsSession, 1)
	accepted := make(chan *subscriptionBlockedConn, 1)
	host := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ready <- &wsSession{server: s, conn: conn, ctx: context.Background(), done: make(chan struct{})}
	}))
	host.Listener = subscriptionBlockedListener{host.Listener, accepted}
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

// A socket writer may temporarily own a GET frame while independent
// storage operations publish subscription updates. That is ordinary burst
// pressure, not a lost transport. In particular, invalidating a slow cache must
// not disconnect unrelated accepted mutations or invent their durability.
func TestSubscriptionBurstPreservesRepliesAndPendingMutation(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{173}
	payload := bytes.Repeat([]byte("queued-get"), 4096)
	if _, err := writer.Put(ctx, id, payload, client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(volume.ID(reader.BucketID()))
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := s.acceptedMark(b.ID()); !pending {
		t.Fatal("fixture must exercise durability notices as well as updates")
	}
	ws, socket, blocked := subscriptionBlockedSocket(t, s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketRead | BucketWrite}
	ws.responses = make(chan []byte, sendQueueCapacity)
	ws.terminal = make(chan terminalFrame, 1)
	ws.telemetry = newStatsSendQueue()
	ws.connectionFenceCap.Store(true)
	ws.durabilityCap.Store(true)
	var release sync.Once
	releaseWriter := func() { release.Do(func() { close(blocked.release) }) }
	defer releaseWriter()
	blocked.armed.Store(true)
	writeDone := make(chan struct{})
	go func() { defer close(writeDone); ws.writeLoop() }()
	defer func() {
		ws.close()
		select {
		case <-writeDone:
		case <-time.After(time.Second):
			t.Error("socket writer did not retire")
		}
	}()
	get := connectionTestMessage(0x100, client.Request)
	p := userPrincipal(t, s)
	e := client.Encoder{}
	e.ID(reader.BucketID())
	e.B = append(e.B, id[:]...)
	e.U64(0)
	e.U64(0)
	e.ID(client.ID{})
	e.Bool(false)
	get.Body = e.B
	body, status, err := s.executeObjects(get, nil, p)
	if err != nil || status != 200 {
		t.Fatal("GET fixture failed", status, err)
	}
	ws.responses <- client.EncodeMessage(client.Message{Kind: client.Response, Opcode: get.Opcode, ID: get.ID, Body: s.durabilityResponse(status, body, b.ID())})

	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal("GET never reached the socket writer")
	}
	mutation := connectionTestMessage(client.ObjectAccepted, client.Request)
	if !ws.admitConnectionMessage(mutation) {
		t.Fatal("unrelated storage mutation was not admitted")
	}
	// Each publication may carry both a commit notice and an update. Separate
	// objects avoid the per-object journal-overflow path; the pressure here is
	// specifically the shared response queue behind the socket writer.
	const subscriptions = sendQueueCapacity/2 + 1
	want := make(map[client.ID]byte, subscriptions)
	for i := 0; i < subscriptions; i++ {
		sub := &objectSubscription{ws: ws, id: client.NewRequestID(), key: volume.Key{Bucket: b.ID(), Object: volume.ObjectID{byte(i + 1)}}, cursor: 1}
		if err := s.objectSubscriptions.add(sub); err != nil {
			t.Fatal(err)
		}
		want[sub.id] = byte(i + 1)
	}
	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 0; i < subscriptions; i++ {
			value := byte(i + 1)
			key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{value}}
			s.objectSubscriptions.publish(key, volume.Info{Deleted: true}, nil)
		}
	}()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("subscription publication blocked behind socket output")
	}
	select {
	case <-ws.done:
		t.Fatal("subscription burst closed a healthy socket with an unrelated pending mutation")
	default:
	}
	ws.fenceMu.Lock()
	pendingMutation := ws.connectionJobs[mutation.ID]
	ws.fenceMu.Unlock()
	if pendingMutation == nil {
		t.Fatal("subscription pressure retired unrelated storage admission")
	}

	fence := connectionTestMessage(client.ConnectionSync, client.Request)
	observed := make(chan error, 1)
	go func() {
		seen := make(map[client.ID]bool, subscriptions)
		gotGet, gotFence := false, false
		for {
			_, wire, err := socket.ReadMessage()
			if err != nil {
				observed <- err
				return
			}
			m, err := client.DecodeMessage(wire)
			if err != nil {
				observed <- err
				return
			}
			if m.ID == get.ID {
				gotGet = m.Opcode == get.Opcode && bytes.HasSuffix(m.Body, payload)
			}
			if m.Opcode == client.ObjectUpdateMessage {
				u, err := client.DecodeObjectUpdate(m.Body)
				if err != nil {
					observed <- err
					return
				}
				if _, ok := want[u.Subscription]; ok && (u.Change == client.UpdateResyncRequired || u.Cursor == 2 && u.Change == client.UpdateDelete) {
					seen[u.Subscription] = true
				}
			}
			if m.ID == fence.ID {
				gotFence = true
			}
			if gotGet && gotFence && len(seen) == subscriptions {
				observed <- nil
				return
			}
		}
	}()
	releaseWriter()
	ws.completeConnectionMessage(mutation, 200, client.ResponseBody(200, nil, ""))
	if !ws.admitConnectionMessage(fence) {
		t.Fatal("explicit fence was not admitted after the burst")
	}
	body, status = ws.executeConnectionSync(ctx, fence)
	if status != 200 {
		t.Fatal("explicit fence failed after unrelated storage completion", status)
	}
	select {
	case ws.responses <- client.EncodeMessage(client.Message{Kind: client.Response, Opcode: fence.Opcode, ID: fence.ID, Body: body}):
	case <-ctx.Done():
		t.Fatal("explicit fence reply was stranded")
	}
	select {
	case err := <-observed:
		if err != nil {
			t.Fatal("GET, subscription data/invalidation, and fence did not all survive", err)
		}
	case <-ctx.Done():
		t.Fatal("burst output did not recover after releasing the socket writer")
	}
}
