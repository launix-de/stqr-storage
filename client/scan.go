// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
)

type ObjectEntry struct {
	ID   ObjectID
	Info ObjectInfo
}

// ScanObjects inventories live native addresses in OID order, with READ access.
// Pagination observes current state; it is not an exclusive maintenance session.
func (c *Client) ScanObjects(ctx context.Context, after *ObjectID, limit uint32) ([]ObjectEntry, *ObjectID, error) {
	if limit == 0 || limit > 1000 {
		return nil, nil, ErrRange
	}
	if err := c.ready(ctx); err != nil {
		return nil, nil, err
	}
	return c.scanObjectEntries(ctx, after, limit, BucketObjectList)
}

func (c *Client) scanObjectEntries(ctx context.Context, after *ObjectID, limit uint32, opcode uint16) ([]ObjectEntry, *ObjectID, error) {
	e := Encoder{}
	c.objectRef(&e, ObjectID{})
	e.Bool(after != nil)
	if after != nil {
		e.B = append(e.B, after[:]...)
	}
	e.U32(limit)
	r, err := c.do(ctx, opcode, e.B, NewRequestID(), false, nil)
	defer r.close()
	if err != nil {
		return nil, nil, err
	}
	d := Decoder{B: r.result}
	count := d.U32()
	if count > limit {
		return nil, nil, ErrProtocol
	}
	objects := make([]ObjectEntry, 0, count)
	previous := after
	for i := uint32(0); i < count; i++ {
		var id ObjectID
		copy(id[:], d.Take(32))
		info := readObjectInfo(&d)
		if d.Err != nil || previous != nil && bytes.Compare(id[:], previous[:]) <= 0 {
			return nil, nil, ErrProtocol
		}
		if err = c.acceptObjectInfo(info); err != nil {
			return nil, nil, err
		}
		objects = append(objects, ObjectEntry{id, info})
		copyID := id
		previous = &copyID
	}
	var next *ObjectID
	if d.Bool() {
		var id ObjectID
		copy(id[:], d.Take(32))
		next = &id
		if after != nil && bytes.Compare(id[:], after[:]) <= 0 || previous != nil && bytes.Compare(id[:], previous[:]) < 0 {
			return nil, nil, ErrProtocol
		}
	}
	if err = d.Done(); err != nil {
		return nil, nil, err
	}
	return objects, next, nil
}
