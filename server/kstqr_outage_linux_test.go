// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type kernelOutageFixture struct {
	mu                sync.Mutex
	handler           http.Handler
	listener          net.Listener
	server            *http.Server
	address           string
	connections       map[*kernelOutageConnection]bool
	ctx               context.Context
	cancel            context.CancelFunc
	workers           sync.WaitGroup
	armed             atomic.Bool
	drops             atomic.Int32
	pauses            atomic.Int32
	accepted          atomic.Int32
	busyOpcode        atomic.Uint32
	busyExpected      atomic.Uint32
	busyCompleted     atomic.Bool
	busyReplies       atomic.Int32
	busyBaseline      atomic.Int32
	busyChecked       atomic.Bool
	busyStatChecked   atomic.Bool
	busyDeleteChecked atomic.Bool
	recoveryBaseline  atomic.Int32
	recoveryChecked   atomic.Bool
}
type kernelOutageListener struct {
	net.Listener
	fixture *kernelOutageFixture
}

func (l kernelOutageListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	l.fixture.accepted.Add(1)
	wrapped := &kernelOutageConnection{Conn: c, fixture: l.fixture}
	l.fixture.mu.Lock()
	l.fixture.connections[wrapped] = true
	l.fixture.mu.Unlock()
	return wrapped, nil
}

type kernelOutageConnection struct {
	net.Conn
	fixture *kernelOutageFixture
}

func (c *kernelOutageConnection) Close() error {
	c.fixture.mu.Lock()
	delete(c.fixture.connections, c)
	c.fixture.mu.Unlock()
	return c.Conn.Close()
}
func (c *kernelOutageConnection) Write(p []byte) (int, error) {
	// Gorilla writes these small binary responses as one unmasked FIN frame.
	// Drop a successful CAS response only after the real server committed it.
	if len(p) >= 2 && p[0] == 0x82 && p[1]&0x80 == 0 {
		offset, length := 2, int(p[1]&127)
		if length == 126 && len(p) >= 4 {
			length = int(binary.BigEndian.Uint16(p[2:4]))
			offset = 4
		}
		if length < 126 && len(p) == offset+length || offset == 4 && len(p) == offset+length {
			m, e := client.DecodeMessage(p[offset:])
			// Preserve one live connection across a completed transient
			// rejection. DELETE may have completed before its reply is hidden;
			// its idempotent retry must still use the same socket and identity.
			opcode := uint32(m.Opcode)
			if opcode == 0x101 {
				opcode = 0x100
			}
			if e == nil && opcode == 0x118 {
				d := client.Decoder{B: m.Body}
				status := d.V()
				d.Blob()
				// Accepted DELETE has an empty result; accepted CAS/PUT have
				// object information. Do not turn a committed CAS into a503.
				if d.Err == nil && (status < 300 || status == 404) && len(d.B) == 0 {
					opcode = 0x106
				}
			}
			if e == nil && m.Kind == client.Response && opcode != 0 && c.fixture.busyOpcode.CompareAndSwap(opcode, 0) {
				m.Body = client.ResponseBody(503, nil, "test temporarily busy")
				payload := client.EncodeMessage(m)
				frame := []byte{0x82, byte(len(payload))}
				frame = append(frame, payload...)
				if _, e := c.Conn.Write(frame); e != nil {
					return 0, e
				}
				c.fixture.busyReplies.Add(1)
				return len(p), nil
			}
			if e == nil && m.Kind == client.Response && opcode == c.fixture.busyExpected.Load() && c.fixture.busyReplies.Load() == 1 {
				c.fixture.busyCompleted.Store(true)
			}
			if e == nil && m.Kind == client.Response && (m.Opcode == 0x108 || m.Opcode == 0x118) {
				d := client.Decoder{B: m.Body}
				status := d.V()
				if d.Err == nil && status < 300 && c.fixture.armed.CompareAndSwap(true, false) {
					c.fixture.drops.Add(1)
					if e = c.fixture.pause(); e != nil {
						return 0, e
					}
					return len(p), nil
				}
			}
		}
	}
	return c.Conn.Write(p)
}
func (f *kernelOutageFixture) startLocked() error {
	listener, e := net.Listen("tcp", f.address)
	if e != nil {
		return e
	}
	f.listener = listener
	f.address = listener.Addr().String()
	f.server = &http.Server{Handler: f.handler}
	server := f.server
	f.workers.Add(1)
	go func() { defer f.workers.Done(); _ = server.Serve(kernelOutageListener{Listener: listener, fixture: f}) }()
	return nil
}
func (f *kernelOutageFixture) pause() error {
	f.mu.Lock()
	if f.listener == nil {
		f.mu.Unlock()
		return errors.New("fixture already offline")
	}
	listener := f.listener
	f.listener = nil
	connections := make([]*kernelOutageConnection, 0, len(f.connections))
	for c := range f.connections {
		connections = append(connections, c)
	}
	f.workers.Add(1)
	_ = listener.Close()
	f.mu.Unlock()
	for _, c := range connections {
		_ = c.Close()
	}
	f.pauses.Add(1)
	go func() {
		defer f.workers.Done()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-f.ctx.Done():
			return
		case <-timer.C:
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.ctx.Err() == nil {
			_ = f.startLocked()
		}
	}()
	return nil
}
func startKernelOutageFixture(t *testing.T, s *Server) (string, string, *kernelOutageFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f := &kernelOutageFixture{handler: s.Handler(), address: "127.0.0.1:0", connections: map[*kernelOutageConnection]bool{}, ctx: ctx, cancel: cancel}
	f.mu.Lock()
	e := f.startLocked()
	f.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/busy-read", "/busy-stat", "/busy-delete":
			opcode := uint32(0x100)
			if r.URL.Path == "/busy-stat" {
				opcode = 0x107
			} else if r.URL.Path == "/busy-delete" {
				opcode = 0x106
			}
			f.busyReplies.Store(0)
			f.busyCompleted.Store(false)
			f.busyBaseline.Store(f.accepted.Load())
			f.busyExpected.Store(opcode)
			f.busyOpcode.Store(opcode)
		case "/check-busy", "/check-busy-stat", "/check-busy-delete":
			opcode := uint32(0x100)
			checked := &f.busyChecked
			if r.URL.Path == "/check-busy-stat" {
				opcode, checked = 0x107, &f.busyStatChecked
			} else if r.URL.Path == "/check-busy-delete" {
				opcode, checked = 0x106, &f.busyDeleteChecked
			}
			// Orphan DELETE is performed by kernel work; await its actual
			// retry response rather than guessing that a sleep was enough.
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for !f.busyCompleted.Load() {
				select {
				case <-r.Context().Done():
					return
				case <-deadline.C:
					http.Error(w, "busy operation did not complete its retry", 500)
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
			if f.busyExpected.Load() != opcode || f.busyReplies.Load() != 1 || f.accepted.Load() != f.busyBaseline.Load() {
				http.Error(w, "503 caused shared transport reconnect", 500)
				return
			}
			checked.Store(true)
		case "/check-recovery":
			if got := f.accepted.Load() - f.recoveryBaseline.Load(); got != 1 {
				http.Error(w, "parallel readers did not share one reconnect", 500)
				return
			}
			f.recoveryChecked.Store(true)
		case "/offline":
			f.recoveryBaseline.Store(f.accepted.Load())
			if e := f.pause(); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
		case "/lose-cas-reply":
			f.armed.Store(true)
		default:
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(func() {
		cancel()
		control.Close()
		f.mu.Lock()
		listener := f.listener
		f.listener = nil
		connections := make([]*kernelOutageConnection, 0, len(f.connections))
		for c := range f.connections {
			connections = append(connections, c)
		}
		f.mu.Unlock()
		if listener != nil {
			_ = listener.Close()
		}
		for _, c := range connections {
			_ = c.Close()
		}
		f.workers.Wait()
	})
	_, port, _ := net.SplitHostPort(f.address)
	_, controlPort, _ := net.SplitHostPort(control.Listener.Addr().String())
	return "http://10.0.2.2:" + port, "http://10.0.2.2:" + controlPort, f
}
