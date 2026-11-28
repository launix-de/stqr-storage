// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"encoding/binary"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

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
