// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"context"
	"encoding/binary"
	"errors"
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
	unpin       kernelUnpinGate
}

// kernelUnpinGate holds one retirement request before storage observes it.
// The HTTP control channel remains independent, so close must return before
// the test releases or drops this WebSocket incarnation.
type kernelUnpinGate struct {
	mu                            sync.Mutex
	target                        client.ObjectID
	release                       chan struct{}
	enabled, seen, timedOut, drop bool
}

func (g *kernelUnpinGate) begin() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.release = make(chan struct{})
	g.enabled = true
	g.seen = false
	g.timedOut = false
	g.drop = false
}
func (g *kernelUnpinGate) match(raw []byte) <-chan struct{} {
	m, e := client.DecodeMessage(raw)
	if e != nil || m.Opcode != client.ObjectReference || len(m.Body) < 113 || m.Body[112] != 1 {
		return nil
	}
	var id client.ObjectID
	copy(id[:], m.Body[80:112])
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.enabled || g.seen || id != g.target {
		return nil
	}
	g.seen = true
	return g.release
}
func (g *kernelUnpinGate) end(drop bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.enabled || !g.seen || g.timedOut {
		return errors.New("UNPIN gate was not observed before close returned")
	}
	g.drop = drop
	g.enabled = false
	close(g.release)
	return nil
}
func (g *kernelUnpinGate) observed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.seen && !g.timedOut
}
func (g *kernelUnpinGate) shouldDrop() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.drop }

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
	var peerWriter sync.Mutex
	forward := func(kind int, raw []byte) error {
		peerWriter.Lock()
		defer peerWriter.Unlock()
		return peer.WriteMessage(kind, raw)
	}
	var held sync.WaitGroup
	var writer sync.Mutex
	write := func(kind int, raw []byte) error {
		writer.Lock()
		defer writer.Unlock()
		return downstream.WriteMessage(kind, raw)
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer downstream.Close()
		for {
			kind, raw, err := peer.ReadMessage()
			if err != nil {
				return
			}
			if write(kind, raw) != nil {
				return
			}
		}
	}()
	defer func() {
		cancel()
		peer.Close()
		downstream.Close()
		<-done
		held.Wait()
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
		if release := f.unpin.match(raw); release != nil {
			held.Add(1)
			go func() {
				defer held.Done()
				select {
				case <-release:
					if f.unpin.shouldDrop() {
						peer.Close()
						downstream.Close()
						return
					}
				case <-time.After(2 * time.Second):
					f.unpin.mu.Lock()
					f.unpin.timedOut = true
					f.unpin.mu.Unlock()
				case <-ctx.Done():
					return
				}
				if forward(kind, raw) != nil {
					peer.Close()
					downstream.Close()
				}
			}()
			continue
		}
		if forward(kind, raw) != nil {
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
