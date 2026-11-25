// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/sha256"
	"errors"
)

// ObjectReference coordinates reference lifetime, without interpreting object data.
const ObjectReference uint16 = 0x0120
const CapabilityObjectReferences uint64 = 1 << 17

// ObjectReferencesSupported preserves compatibility with older servers: callers
// must retain their conservative lifetime policy when this capability is absent.
func (c *Client) ObjectReferencesSupported() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.transport != nil && c.transport.capabilities&CapabilityObjectReferences != 0
}

// ObjectReferenceRootID is a bucket-local, routed coordination object.
func ObjectReferenceRootID() ObjectID { return sha256.Sum256([]byte("STQR object references v1")) }

// Pins persist across transport and node restarts. The holder must keep its token
// until release; abandoned pins require exclusive offline recovery, not a timer.
func (c *Client) PinObject(ctx context.Context, id ObjectID, token RequestID) error {
	return c.objectReference(ctx, id, token, 0)
}
func (c *Client) UnpinObject(ctx context.Context, id ObjectID, token RequestID) error {
	return c.objectReference(ctx, id, token, 1)
}
func (c *Client) ClaimObjectRetirement(ctx context.Context, id ObjectID, token RequestID) error {
	return c.objectReference(ctx, id, token, 2)
}
func (c *Client) ReleaseObjectRetirement(ctx context.Context, id ObjectID, token RequestID) error {
	return c.objectReference(ctx, id, token, 3)
}
func (c *Client) PinObjectDependency(ctx context.Context, id ObjectID, token ID) error {
	return c.objectReference(ctx, id, token, 4)
}
func (c *Client) UnpinObjectDependency(ctx context.Context, id ObjectID, token ID) error {
	return c.objectReference(ctx, id, token, 5)
}

// ResetAbandonedObjectReferences requires exclusive offline maintenance and
// DELETE permission. Durable application dependencies are deliberately preserved.
func (c *Client) ResetAbandonedObjectReferences(ctx context.Context) error {
	if !c.ObjectReferencesSupported() {
		return nil
	}
	return c.objectReference(ctx, ObjectID{}, NewRequestID(), 6)
}

// ReclaimBucketMetadata ends the retention of old request results under an
// exclusive offline maintenance lease. Call only after recovery and graph sweep;
// operations from before that boundary must never be retried afterward.
func (c *Client) ReclaimBucketMetadata(ctx context.Context) error {
	if !c.ObjectReferencesSupported() {
		return nil
	}
	return c.objectReference(ctx, ObjectID{}, NewRequestID(), 7)
}

// CanReclaimDependencies is conservative across live and detached bucket clones.
// It requires the caller's exclusive offline maintenance lease.
func (c *Client) CanReclaimDependencies(ctx context.Context) (bool, error) {
	if !c.ObjectReferencesSupported() {
		return false, nil
	}
	err := c.objectReference(ctx, ObjectID{}, NewRequestID(), 8)
	if errors.Is(err, ErrWouldBlock) {
		return false, nil
	}
	return err == nil, err
}

// ReadObjectDependencies names durable application roots, not abandoned open
// handles. An offline collector must keep these targets until their owning
// application graph explicitly retires them.
func (c *Client) ReadObjectDependencies(ctx context.Context) ([]ObjectID, error) {
	raw, _, err := c.Get(ctx, ObjectReferenceRootID())
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d := Decoder{B: raw}
	if string(d.Take(4)) != "ORF1" {
		return nil, ErrProtocol
	}
	bucket := d.ID()
	n := d.U32()
	if n > 8192 || len(raw) != 24+int(n)*81 {
		return nil, ErrProtocol
	}
	var out []ObjectID
	seen := map[ObjectID]bool{}
	for i := uint32(0); i < n; i++ {
		var id ObjectID
		copy(id[:], d.Take(32))
		d.Take(32)
		d.ID()
		flags := d.U8()
		if flags > 2 {
			return nil, ErrProtocol
		}
		if flags == 2 && !seen[id] {
			out = append(out, id)
			seen[id] = true
		}
	}
	if d.Done() != nil {
		return nil, ErrProtocol
	}
	if bucket != c.bucket {
		return nil, nil
	}
	return out, nil
}
func (c *Client) objectReference(ctx context.Context, id ObjectID, token ID, action uint8) error {
	if token == (ID{}) || id == ObjectReferenceRootID() {
		return ErrRange
	}
	e := Encoder{}
	c.objectRef(&e, ObjectReferenceRootID())
	e.B = append(e.B, id[:]...)
	e.U8(action)
	e.ID(token)
	// This operation is intrinsically idempotent with this caller-owned token.
	// Do not wrap it in the optional accepted-write envelope.
	r, err := c.do(ctx, ObjectReference, e.B, NewRequestID(), false, nil)
	if err != nil {
		var status *StatusError
		if errors.As(err, &status) && status.Status == 409 {
			return ErrWouldBlock
		}
		return err
	}
	defer r.close()
	d := Decoder{B: r.result}
	granted := d.Bool()
	if d.Done() != nil {
		return ErrProtocol
	}
	if !granted {
		return ErrWouldBlock
	}
	return nil
}
