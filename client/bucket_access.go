// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"strings"
)

type BucketPermissions uint32

const (
	BucketPermissionRead BucketPermissions = 1 << iota
	BucketPermissionWrite
	BucketPermissionAppend
	BucketPermissionDelete
)

// BucketAccess returns the authenticated connection's effective permissions.
// Locked buckets already have their write permissions removed by the server.
// No object is created or changed to probe write access.
func (c *Client) BucketAccess(ctx context.Context) (BucketPermissions, error) {
	if err := c.ready(ctx); err != nil {
		return 0, err
	}
	selectors := []string{strings.TrimPrefix(c.bucketSelector, "name:")}
	if selectors[0] != c.bucket.String() {
		selectors = append(selectors, c.bucket.String())
	}
	for index, selector := range selectors {
		e := Encoder{}
		e.Text(selector)
		r, err := c.do(ctx, BucketInfoGet, e.B, RequestID{}, false, nil)
		if err != nil {
			var status *StatusError
			if index+1 < len(selectors) && errors.As(err, &status) && status.Status == 403 {
				continue
			}
			return 0, err
		}
		permissions, err := decodeBucketAccess(r.result, c.bucket)
		r.close()
		return permissions, err
	}
	return 0, ErrProtocol
}

func decodeBucketAccess(raw []byte, bucket BucketID) (BucketPermissions, error) {
	d := Decoder{B: raw}
	id := d.ID()
	d.U64() // Policy version.
	d.Text()
	state := d.U8()
	d.U8() // Storage strategy.
	d.U8() // Compression.
	d.U8() // Placement mode.
	d.U8() // Redundancy mode.
	d.U16()
	d.U16()
	d.U16()
	d.Bool()
	d.U16()
	d.U64()
	d.U8() // Quota accounting.
	d.U64()
	d.Bool()
	d.U64()
	d.U32()
	permissions := BucketPermissions(d.U32())
	if d.Done() != nil || id != bucket || state > 2 || permissions&^BucketPermissions(15) != 0 || state != 0 && permissions&^BucketPermissionRead != 0 {
		return 0, ErrProtocol
	}
	return permissions, nil
}
