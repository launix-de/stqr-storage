// SPDX-License-Identifier: GPL-3.0-only
package client

import "context"

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
