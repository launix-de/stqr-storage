// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
)

// Bucket is one authenticated connection containing any number of named disks.
// No stripe payload cache or per-disk segment index exists in this client.
type Bucket struct {
	*Catalog
	Client   *client.Client
	ReadOnly bool
}

func Open(ctx context.Context, url, username, password string, id client.BucketID, readonly bool) (*Bucket, error) {
	c, e := client.Dial(ctx, url, username, password, id, "bd")
	if e != nil {
		return nil, e
	}
	if e = c.BeginObjectLease(ctx, readonly); e != nil {
		c.Close()
		return nil, e
	}
	catalog := NewCatalog(c)
	if e = catalog.CheckFormat(ctx); e != nil {
		c.Close()
		return nil, e
	}
	return &Bucket{Catalog: catalog, Client: c, ReadOnly: readonly}, nil
}
func (b *Bucket) Close() error {
	err := b.Client.Sync(context.Background())
	b.Client.Close()
	return err
}
