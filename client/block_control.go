// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"time"
)

// RegisterBlockControl enables bounded reverse management requests on the
// original lease socket. The handler must not close its Client. Legacy nodes
// remain usable but cannot expose their exporter through the dashboard.
func (c *Client) RegisterBlockControl(ctx context.Context, handler func(context.Context, []byte) ([]byte, error)) error {
	c.mu.Lock()
	t := c.maintenance
	if t == nil || handler == nil {
		c.mu.Unlock()
		return ErrProtocol
	}
	if t.capabilities&CapabilityBlockControl == 0 {
		c.mu.Unlock()
		return nil
	}
	c.blockControl = handler
	c.mu.Unlock()
	e := Encoder{}
	e.U16(1)
	r, err := t.roundTrip(ctx, Message{Kind: Request, Opcode: BucketBlockControlRegister, ID: NewRequestID(), Body: e.B}, false, nil)
	r.close()
	return err
}

func (c *Client) receiveBlockControl(t *objectTransport, m Message) {
	c.mu.Lock()
	handler := c.blockControl
	valid := c.maintenance == t && handler != nil && !c.closed
	busy := c.blockControlBusy
	if valid && !busy {
		c.blockControlBusy = true
	}
	c.mu.Unlock()
	reply := func(ctx context.Context, status int, result []byte, detail string) {
		_ = t.enqueue(ctx, outboundObject{message: Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: ResponseBody(status, result, detail)}}, false)
	}
	if !valid || busy || len(m.Body) > 2048 {
		reply(c.life, 409, nil, "block control unavailable or busy")
		if valid && !busy {
			c.mu.Lock()
			c.blockControlBusy = false
			c.mu.Unlock()
		}
		return
	}
	// Keep framing free while the handler performs native object RPCs on this
	// same connection. Only one handler can run at a time.
	t.workers.Add(1)
	go func() {
		defer t.workers.Done()
		defer func() { c.mu.Lock(); c.blockControlBusy = false; c.mu.Unlock() }()
		ctx, cancel := context.WithTimeout(c.life, 30*time.Second)
		defer cancel()
		result, err := handler(ctx, m.Body)
		status, detail := 200, ""
		if err != nil {
			status, detail = 500, err.Error()
			var native *StatusError
			switch {
			case errors.As(err, &native):
				status = native.Status
			case errors.Is(err, ErrRange), errors.Is(err, ErrProtocol):
				status = 400
			case errors.Is(err, ErrPrecondition):
				status = 409
			case errors.Is(err, ErrNotFound):
				status = 404
			}
		}
		if len(result) > 1<<20 {
			status, result, detail = 500, nil, "block control result exceeds budget"
		}
		reply(ctx, status, result, detail)
	}()
}
