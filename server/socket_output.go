// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type socketOutputBudget struct{ capacity, queued int64 }

type socketOutputPort interface {
	budget() (socketOutputBudget, error)
	tryWrite([]byte) (int, error)
}

// The sole WebSocket writer waits for kernel capacity before acquiring storage.
// Capacity is advisory: even after admission a nonblocking send can be partial.
// Only that exceptional unsent suffix becomes owned, bounded by one reply; it is
// flushed after the read handle is released. No borrowed pointer enters a queue.
type socketOutputConn struct {
	net.Conn
	mu       sync.Mutex
	port     socketOutputPort
	borrowed bool
	pending  []byte
}

const socketOutputPendingLimit = 2*inlineObjectDownload + (64 << 10)

func socketOutputCharge(payload int64) int64 { return 2*payload + (64 << 10) }

func (c *socketOutputConn) wait(ctx context.Context, payload int64, exact bool) (bool, error) {
	if c.port == nil {
		return false, nil
	}
	charge := socketOutputCharge(payload)
	delay := 2 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		budget, err := c.port.budget()
		if err != nil || budget.capacity <= 0 {
			// Unsupported instrumentation selects owned capture, never a blind
			// borrowed write. Storage correctness does not depend on a socket API.
			return false, nil
		}
		need := charge
		if exact && need > budget.capacity {
			return false, nil
		}
		if !exact {
			need = min(need, budget.capacity/2)
		}
		if budget.capacity-budget.queued >= need {
			return true, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
		delay = min(2*delay, 20*time.Millisecond)
	}
}

func (c *socketOutputConn) beginBorrow(ctx context.Context, payload int64) (bool, error) {
	ok, err := c.wait(ctx, payload, true)
	if !ok || err != nil {
		return false, err
	}
	// A concurrent WebSocket control frame may be finishing. This wait is
	// deliberately before storage acquisition, like the capacity wait above.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.borrowed || len(c.pending) != 0 {
		return false, errors.New("overlapping borrowed socket output")
	}
	c.borrowed = true
	return true, nil
}

func (c *socketOutputConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.borrowed {
		return c.Conn.Write(p)
	}
	n := 0
	if len(c.pending) == 0 {
		var err error
		n, err = c.port.tryWrite(p)
		if n < 0 || n > len(p) {
			return 0, io.ErrShortWrite
		}
		if err != nil {
			return n, err
		}
	}
	if n < len(p) {
		if len(p)-n > socketOutputPendingLimit-len(c.pending) {
			return n, errors.New("borrowed socket suffix exceeded reply budget")
		}
		c.pending = append(c.pending, p[n:]...)
	}
	return len(p), nil
}

// Called only after every volume/read-view guard has unwound. Hold the output
// mutex while flushing so concurrent Ping/Close frames cannot overtake a suffix.
func (c *socketOutputConn) finishBorrow(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.borrowed = false
	pending := c.pending
	c.pending = nil
	if len(pending) == 0 {
		return nil
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Conn.Close() })
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	for len(pending) != 0 {
		n, err := c.Conn.Write(pending)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(pending) {
			return io.ErrShortWrite
		}
		pending = pending[n:]
	}
	return nil
}

type socketOutputResponseWriter struct{ http.ResponseWriter }

func (w socketOutputResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("HTTP connection cannot be hijacked")
	}
	conn, rw, err := h.Hijack()
	if err != nil {
		return nil, nil, err
	}
	return &socketOutputConn{Conn: conn, port: newSocketOutputPort(conn)}, rw, nil
}

func (ws *wsSession) socketOutput() *socketOutputConn {
	if ws.conn == nil {
		return nil
	}
	output, _ := ws.conn.UnderlyingConn().(*socketOutputConn)
	return output
}
