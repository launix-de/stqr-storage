// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

// Holds one real STATFS response while forwarding other responses on that same
// socket. Only the disposable kernel VM proxy uses these unauthenticated hooks.
type kernelStatfsGate struct {
	enabled                  atomic.Bool
	mu                       sync.Mutex
	id                       client.ID
	connection               uint64
	release                  chan struct{}
	held, finished, timedOut bool
	reads, cas               uint64
	pending                  map[client.ID]uint16
}

func (g *kernelStatfsGate) control(w http.ResponseWriter, r *http.Request) {
	action := strings.TrimPrefix(r.URL.Path, "/__kstqr_test/statfs/")
	g.mu.Lock()
	defer g.mu.Unlock()
	var err error
	switch action {
	case "begin":
		if g.enabled.Load() {
			err = errors.New("STATFS gate already armed")
			break
		}
		g.id = client.ID{}
		g.connection = 0
		g.release = make(chan struct{})
		g.held = false
		g.finished = false
		g.timedOut = false
		g.reads = 0
		g.cas = 0
		g.pending = make(map[client.ID]uint16)
		g.enabled.Store(true)
	case "seen":
		if !g.held {
			err = errors.New("STATFS response not held")
		}
	case "verify":
		if !g.held || g.timedOut || g.reads == 0 || g.cas == 0 {
			err = errors.New("blocked STATFS prevented same-channel GET/CAS progress")
		}
	case "release":
		if g.release == nil || g.finished {
			err = errors.New("STATFS gate not active")
		} else {
			close(g.release)
			g.finished = true
			g.held = false
			g.enabled.Store(false)
		}
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 409)
	}
}
func (g *kernelStatfsGate) request(raw []byte, connection uint64) {
	if !g.enabled.Load() {
		return
	}
	m, err := client.DecodeMessage(raw)
	if err != nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.Opcode == client.BucketStatFS && g.id == (client.ID{}) {
		g.id = m.ID
		g.connection = connection
		return
	}
	if !g.held || g.finished || g.connection != connection {
		return
	}
	op := m.Opcode
	if op == client.ObjectAccepted && len(m.Body) >= 82 {
		op = binary.LittleEndian.Uint16(m.Body[80:82])
	}
	if op == 0x0100 || op == 0x0101 || op == 0x0108 {
		g.pending[m.ID] = op
	}
}
func (g *kernelStatfsGate) response(raw []byte, connection uint64) <-chan struct{} {
	if !g.enabled.Load() {
		return nil
	}
	m, err := client.DecodeMessage(raw)
	if err != nil || m.Kind != client.Response {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held && !g.finished && g.connection == connection {
		if op, ok := g.pending[m.ID]; ok {
			delete(g.pending, m.ID)
			d := client.Decoder{B: m.Body}
			if d.V() < 300 {
				if op == 0x0108 {
					g.cas++
				} else {
					g.reads++
				}
			}
		}
	}
	if m.Opcode != client.BucketStatFS {
		return nil
	}
	if g.connection != connection || g.id != m.ID || g.finished {
		return nil
	}
	g.held = true
	return g.release
}
func (g *kernelStatfsGate) timeout() {
	g.mu.Lock()
	g.timedOut = true
	g.held = false
	g.enabled.Store(false)
	g.mu.Unlock()
}

func TestKernelStatfsGateRequiresSameSocketCompletions(t *testing.T) {
	var gate kernelStatfsGate
	control := func(action string) int {
		w := httptest.NewRecorder()
		gate.control(w, httptest.NewRequest("GET", "/__kstqr_test/statfs/"+action, nil))
		return w.Code
	}
	wire := func(kind uint8, op uint16, id byte, body []byte) []byte {
		return client.EncodeMessage(client.Message{Kind: kind, Opcode: op, ID: client.ID{id}, Body: body})
	}
	if control("begin") != 200 {
		t.Fatal("arm")
	}
	gate.request(wire(client.Request, client.BucketStatFS, 1, nil), 11)
	held := gate.response(wire(client.Response, client.BucketStatFS, 1, client.ResponseBody(200, nil, "")), 11)
	if held == nil || control("seen") != 200 {
		t.Fatal("STATFS not held")
	}
	gate.request(wire(client.Request, 0x0100, 2, nil), 11)
	if control("verify") != 409 {
		t.Fatal("requests alone satisfied completion oracle")
	}
	gate.response(wire(client.Response, 0x0100, 2, client.ResponseBody(200, nil, "")), 12)
	if gate.reads != 0 {
		t.Fatal("another socket counted as progress")
	}
	gate.response(wire(client.Response, 0x0100, 2, client.ResponseBody(503, nil, "")), 11)
	if gate.reads != 0 {
		t.Fatal("rejected read counted as progress")
	}
	gate.request(wire(client.Request, 0x0100, 3, nil), 11)
	gate.response(wire(client.Response, 0x0100, 3, client.ResponseBody(200, nil, "")), 11)
	accepted := make([]byte, 82)
	binary.LittleEndian.PutUint16(accepted[80:], 0x0108)
	gate.request(wire(client.Request, client.ObjectAccepted, 4, accepted), 11)
	if control("verify") != 409 {
		t.Fatal("CAS request alone satisfied oracle")
	}
	gate.response(wire(client.Response, client.ObjectAccepted, 4, client.ResponseBody(200, nil, "")), 11)
	if control("verify") != 200 {
		t.Fatal("same-socket successful GET/CAS not recognized")
	}
	gate.timeout()
	if control("verify") != 409 {
		t.Fatal("timed-out original implementation could satisfy oracle")
	}
	if control("release") != 200 {
		t.Fatal("release")
	}
	select {
	case <-held:
	default:
		t.Fatal("held reply not released")
	}
}
