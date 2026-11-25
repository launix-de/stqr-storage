// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"time"
)

// BeginObjectLease excludes other clients from this bucket. Its payload format
// remains opaque to the server; normal native operation grants still apply.
func (c *Client) BeginObjectLease(ctx context.Context, readonly bool) error {
	return c.beginMaintenanceCommand(ctx, BucketObjectLease, readonly)
}

// BeginBlockMaintenance is a compatibility alias for a generic object lease.
// Deprecated: use BeginObjectLease.
func (c *Client) BeginBlockMaintenance(ctx context.Context, readonly bool) error {
	return c.BeginObjectLease(ctx, readonly)
}

func (c *Client) beginMaintenanceCommand(ctx context.Context, opcode uint16, readonly bool) error {
	t, err := c.connection(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.maintenance != nil {
		c.mu.Unlock()
		return errors.New("client: maintenance already started; use a dedicated connection")
	}
	c.maintenance = t
	c.mu.Unlock()
	e := Encoder{}
	c.objectRef(&e, ObjectID{})
	if opcode == BucketObjectLease {
		e.Bool(readonly)
	}
	token := NewRequestID()
	c.mu.Lock()
	c.maintenanceToken = token
	c.mu.Unlock()
	var r wireReply
	for {
		r, err = c.do(ctx, opcode, e.B, token, false, nil)
		var status *StatusError
		if !errors.As(err, &status) || status.Status != 503 {
			break
		}
		r.close()
		// A scoped handoff/control barrier must finish before acquiring a
		// lease. Keep the same original socket and logical request, rather
		// than making an otherwise healthy export fail during balancing.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-c.done:
			timer.Stop()
			return ErrClosed
		case <-timer.C:
		}
	}
	defer r.close()
	if err == nil {
		if len(r.result) == 17 && r.result[0] == 1 {
			c.mu.Lock()
			copy(c.maintenanceToken[:], r.result[1:])
			c.distributedMaintenance = true
			c.mu.Unlock()
		} else if len(r.result) != 0 {
			err = ErrProtocol
		}
	}
	return err
}
