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
	unpin       kernelUnpinGate
	ranges      kernelRangeGate
	editRetry   kernelEditRetryFault
	statfs      kernelStatfsGate
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

// kernelUnpinGate holds one retirement request before storage observes it.
// The HTTP control channel remains independent, so close must return before
// the test releases or drops this WebSocket incarnation.
type kernelUnpinGate struct {
	mu                            sync.Mutex
	target                        client.ObjectID
	pins                          map[client.ID]int
	latestPin                     client.ID
	metadata, metadataSeen        bool
	release                       chan struct{}
	enabled, seen, timedOut, drop bool
}

// Record real PIN envelopes, rather than inferring renewal from an entry that
// could simply have survived the dropped socket. The shared test uses two token
// generations and must renew the newer token before another file joins it.
func (g *kernelUnpinGate) recordPin(raw []byte) {
	m, err := client.DecodeMessage(raw)
	if err != nil || m.Opcode != client.ObjectReference || len(m.Body) < 129 || m.Body[112] != 0 {
		return
	}
	var object client.ObjectID
	var token client.ID
	copy(object[:], m.Body[80:112])
	copy(token[:], m.Body[113:129])
	g.mu.Lock()
	defer g.mu.Unlock()
	if object != g.target {
		return
	}
	if g.pins == nil {
		g.pins = make(map[client.ID]int)
	}
	g.pins[token]++
	g.latestPin = token
}

func (g *kernelUnpinGate) verifyRenewal() (int, int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	total := 0
	for _, n := range g.pins {
		total += n
	}
	if len(g.pins) != 2 || g.pins[g.latestPin] < 2 {
		return total, len(g.pins), fmt.Errorf("new generation was not renewed with its original PIN token: requests=%d tokens=%d latest=%d", total, len(g.pins), g.pins[g.latestPin])
	}
	return total, len(g.pins), nil
}

func (g *kernelUnpinGate) verifyRestartRenewal() (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.pins) != 1 || g.pins[g.latestPin] < 2 {
		return 0, fmt.Errorf("postrestart open did not renew its original PIN token: tokens=%d requests=%d", len(g.pins), g.pins[g.latestPin])
	}
	return g.pins[g.latestPin], nil
}

// Hold a warm inode owner GET before storage, after the open joined its group.
// Closing the real backend cancels this held attempt and exercises GET recovery.
func (g *kernelUnpinGate) beginMetadata() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.metadata = true
	g.metadataSeen = false
	g.release = make(chan struct{})
}
func (g *kernelUnpinGate) sawMetadata() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.metadataSeen
}
func (g *kernelUnpinGate) matchMetadata(raw []byte) <-chan struct{} {
	m, err := client.DecodeMessage(raw)
	if err != nil || m.Opcode != 0x100 || len(m.Body) < 48 {
		return nil
	}
	var object client.ObjectID
	copy(object[:], m.Body[16:48])
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.metadata || g.metadataSeen || object != g.target {
		return nil
	}
	g.metadataSeen = true
	return g.release
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
	connection := f.connections.Add(1)
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
			if release := f.statfs.response(raw, connection); release != nil {
				replies.Add(1)
				go func() {
					defer replies.Done()
					select {
					case <-release:
					case <-time.After(5 * time.Second):
						f.statfs.timeout()
					case <-ctx.Done():
						return
					}
					if write(kind, raw) != nil {
						downstream.Close()
					}
				}()
				continue
			}
			if f.editRetry.response(raw) {
				peer.Close()
				downstream.Close()
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
		held.Wait()
	}()
	for {
		kind, raw, err := downstream.ReadMessage()
		if err != nil {
			return
		}
		f.statfs.request(raw, connection)
		if reject, disconnect := f.editRetry.request(raw); reject || disconnect {
			if disconnect {
				return
			}
			m, _ := client.DecodeMessage(raw)
			reply := client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(503, nil, "node temporarily restarting")}
			if write(kind, client.EncodeMessage(reply)) != nil {
				return
			}
			continue
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
		if release := f.unpin.matchMetadata(raw); release != nil {
			held.Add(1)
			go func() {
				defer held.Done()
				select {
				case <-release:
				case <-ctx.Done():
					return
				case <-time.After(4 * time.Second):
					return
				}
				if forward(kind, raw) != nil {
					peer.Close()
					downstream.Close()
				}
			}()
			continue
		}
		f.ranges.request(raw)
		f.unpin.recordPin(raw)
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
