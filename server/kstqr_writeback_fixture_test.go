// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

// kernelWritebackFault rejects an actual native write before forwarding it to
// the authenticated backend. It does not revoke capabilities, replace sockets,
// or discard accepted-prefix knowledge: this models a definite write failure,
// separately from the outage test's unknown-outcome durability contract.
type kernelWritebackFault struct {
	deny        atomic.Bool
	rejected    atomic.Uint64
	connections atomic.Uint64
	before      atomic.Uint64
	ranges      kernelRangeGate
}

// kernelRangeGate holds the first selected leaf reply until another distinct
// leaf request arrives. Serial readers fail this oracle even if their eventual
// bytes are correct. A bounded delay on independent replies models storage RTT.
type kernelRangeGate struct {
	mu                 sync.Mutex
	leaves             map[client.ObjectID]bool
	requests           map[client.ID]bool
	first              client.ID
	firstLeaf          client.ObjectID
	release            chan struct{}
	released, timedOut bool
	seen               uint64
	indexes            map[client.ObjectID]bool
	visited            map[client.ObjectID]bool
	failureLeaves      map[client.ObjectID]bool
	failure            bool
	failureFirst       client.ID
	failurePending     map[client.ID]bool
	failureRelease     chan struct{}
	failureDelivered   int
	failureOverlap     int
}

func (g *kernelRangeGate) configureFailure(ids []client.ObjectID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failureLeaves = make(map[client.ObjectID]bool)
	for _, id := range ids {
		g.failureLeaves[id] = true
		g.leaves[id] = true
	}
}

func (g *kernelRangeGate) beginFailure() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failure = true
	g.failurePending = make(map[client.ID]bool)
	g.failureRelease = make(chan struct{})
}

func (g *kernelRangeGate) failureReply(raw []byte) (bool, bool, <-chan struct{}) {
	m, err := client.DecodeMessage(raw)
	if err != nil {
		return false, false, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failure && g.failurePending[m.ID], m.ID == g.failureFirst, g.failureRelease
}

func (g *kernelRangeGate) deliveredFailure(raw []byte, first bool) {
	m, _ := client.DecodeMessage(raw)
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.failurePending, m.ID)
	g.failureDelivered++
	if first {
		g.failureOverlap = len(g.failurePending)
		close(g.failureRelease)
	}
}

func (g *kernelRangeGate) verifyFailure() (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.failure || g.failureDelivered < 2 || g.failureOverlap == 0 || len(g.failurePending) != 0 {
		return g.failureDelivered, fmt.Errorf("failed read returned without draining its overlapping ranges: delivered=%d overlapping=%d pending=%d", g.failureDelivered, g.failureOverlap, len(g.failurePending))
	}
	g.failure = false
	return g.failureDelivered, nil
}

func (g *kernelRangeGate) configure(ids []client.ObjectID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.leaves = make(map[client.ObjectID]bool)
	for _, id := range ids {
		g.leaves[id] = true
	}
	g.requests = make(map[client.ID]bool)
	g.release = make(chan struct{})
}
func (g *kernelRangeGate) configureIndexes(ids []client.ObjectID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.indexes = make(map[client.ObjectID]bool)
	g.visited = make(map[client.ObjectID]bool)
	for _, id := range ids {
		g.indexes[id] = true
	}
}
func (g *kernelRangeGate) verifyIndexes() (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := len(g.visited)
	if n == 0 || n > 4 {
		return n, errors.New("small read traversed unrelated index subtrees")
	}
	return n, nil
}
func (g *kernelRangeGate) request(raw []byte) {
	m, err := client.DecodeMessage(raw)
	if err != nil || (m.Opcode != 0x0101 && m.Opcode != 0x0100) || len(m.Body) < 48 {
		return
	}
	var leaf client.ObjectID
	copy(leaf[:], m.Body[16:48])
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.Opcode == 0x0100 {
		if g.indexes[leaf] {
			g.visited[leaf] = true
		}
		return
	}
	if !g.leaves[leaf] {
		return
	}
	if g.failure && g.failureLeaves[leaf] {
		if g.failureFirst == (client.ID{}) {
			g.failureFirst = m.ID
		}
		g.failurePending[m.ID] = true
	}
	g.seen++
	if g.seen == 1 {
		g.first = m.ID
		g.firstLeaf = leaf
	}
	g.requests[m.ID] = m.ID == g.first
	if leaf != g.firstLeaf && !g.released {
		close(g.release)
		g.released = true
	}
}
func (g *kernelRangeGate) reply(raw []byte) (bool, bool, <-chan struct{}) {
	m, err := client.DecodeMessage(raw)
	if err != nil || m.Kind != client.Response {
		return false, false, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	first, ok := g.requests[m.ID]
	if ok {
		delete(g.requests, m.ID)
	}
	return ok, first, g.release
}
func (g *kernelRangeGate) verify() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.timedOut || !g.released || g.seen < 2 {
		return errors.New("independent leaf was not submitted while the first reply was blocked")
	}
	return nil
}

func (f *kernelWritebackFault) begin() {
	f.rejected.Store(0)
	f.before.Store(f.connections.Load())
	f.deny.Store(true)
}

func (f *kernelWritebackFault) verify() error {
	if f.rejected.Load() == 0 {
		return errors.New("writeback failure was not injected")
	}
	if f.connections.Load() != f.before.Load() {
		return errors.New("definite writeback failure replaced a native connection")
	}
	return nil
}

func (f *kernelWritebackFault) end() error {
	f.deny.Store(false)
	return f.verify()
}

func writebackMutation(m client.Message) bool {
	if m.Kind != client.Request && m.Kind != client.FireAndForget {
		return false
	}
	op := m.Opcode
	if op == client.ObjectAccepted && len(m.Body) >= 82 {
		op = binary.LittleEndian.Uint16(m.Body[80:82])
	}
	// Kernel writeback publishes inline CAS messages; unlike streamed PUT,
	// the entire rejected mutation is in this one envelope.
	return op == 0x0108
}

func (f *kernelWritebackFault) proxy(w http.ResponseWriter, r *http.Request, upstream string) {
	header := r.Header.Clone()
	for _, key := range []string{"Connection", "Upgrade", "Sec-WebSocket-Version", "Sec-WebSocket-Key", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
		header.Del(key)
	}
	peer, response, err := (&websocket.Dialer{Subprotocols: []string{"stqr.v2"}}).DialContext(r.Context(), "ws"+strings.TrimPrefix(upstream, "http")+r.URL.RequestURI(), header)
	if err != nil {
		if response != nil {
			defer response.Body.Close()
			w.WriteHeader(response.StatusCode)
			io.Copy(w, io.LimitReader(response.Body, 4096))
		} else {
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
		return
	}
	defer peer.Close()
	canonical := http.Header{}
	if id := response.Header.Get(client.BucketHeader); id != "" {
		canonical.Set(client.BucketHeader, id)
	}
	downstream, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, canonical)
	if err != nil {
		return
	}
	defer downstream.Close()
	f.connections.Add(1)
	var writer sync.Mutex
	write := func(kind int, raw []byte) error {
		writer.Lock()
		defer writer.Unlock()
		return downstream.WriteMessage(kind, raw)
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	var replies sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer downstream.Close()
		for {
			kind, raw, err := peer.ReadMessage()
			if err != nil {
				return
			}
			selected, first, release := f.ranges.reply(raw)
			if selected {
				replies.Add(1)
				go func() {
					defer replies.Done()
					if failed, failFirst, failureRelease := f.ranges.failureReply(raw); failed {
						if failFirst {
							m, _ := client.DecodeMessage(raw)
							m.Body = client.ResponseBody(403, nil, "injected selected-range rejection")
							raw = client.EncodeMessage(m)
						} else {
							select {
							case <-failureRelease:
							case <-ctx.Done():
								return
							}
							select {
							case <-time.After(100 * time.Millisecond):
							case <-ctx.Done():
								return
							}
						}
						if write(kind, raw) != nil {
							downstream.Close()
							return
						}
						f.ranges.deliveredFailure(raw, failFirst)
						return
					}
					if first {
						select {
						case <-release:
						case <-time.After(2 * time.Second):
							f.ranges.mu.Lock()
							f.ranges.timedOut = true
							f.ranges.mu.Unlock()
						case <-ctx.Done():
							return
						}
					}
					select {
					case <-time.After(20 * time.Millisecond):
					case <-ctx.Done():
						return
					}
					if write(kind, raw) != nil {
						downstream.Close()
					}
				}()
			} else if write(kind, raw) != nil {
				return
			}
		}
	}()
	defer func() {
		cancel()
		peer.Close()
		downstream.Close()
		<-done
		replies.Wait()
	}()
	for {
		kind, raw, err := downstream.ReadMessage()
		if err != nil {
			return
		}
		if kind == websocket.BinaryMessage && f.deny.Load() {
			m, err := client.DecodeMessage(raw)
			if err == nil && writebackMutation(m) {
				f.rejected.Add(1)
				reply := client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(500, nil, "injected definite writeback failure")}
				if write(kind, client.EncodeMessage(reply)) != nil {
					return
				}
				continue
			}
		}
		f.ranges.request(raw)
		if peer.WriteMessage(kind, raw) != nil {
			return
		}
	}
}

func TestKernelWritebackFaultKeepsAuthenticatedSession(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "writeback-fault", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "writeback-fault")
	upstream := httptest.NewServer(s.Handler())
	defer upstream.Close()
	fault := &kernelWritebackFault{}
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fault.proxy(w, r, upstream.URL) }))
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	c, err := client.DialBucket(ctx, host.URL, a.Username, a.Password, "writeback-fault", "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.BucketID() != client.ID(b.ID()) {
		t.Fatal("proxy did not preserve authenticated canonical identity")
	}
	id := client.ObjectID{31}
	if _, err = c.Put(ctx, id, []byte("initial"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	fault.begin()
	_, err = c.CAS(ctx, id, 0, []byte("initial"), []byte("must not reach storage"), client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{Accepted: true}})
	var status *client.StatusError
	if !errors.As(err, &status) || status.Status != 500 {
		t.Fatal("missing definite failure", err)
	}
	data, _, err := c.Get(ctx, id)
	if err != nil || string(data) != "initial" {
		t.Fatal("rejected write changed backend", string(data), err)
	}
	if err := fault.end(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CAS(ctx, id, 0, []byte("initial"), []byte("recovered"), client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{Accepted: true}}); err != nil {
		t.Fatal(err)
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	data, _, err = c.Get(ctx, id)
	if err != nil || string(data) != "recovered" {
		t.Fatal("retry did not reach backend", string(data), err)
	}
	if err := fault.verify(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d write(s) rejected without replacing the authenticated session", fault.rejected.Load())
}
