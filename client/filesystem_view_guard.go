// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
)

// A read may use stable pages while another mount changes them. Recheck the
// inode/intent after materialization rather than retaining old payload objects.
func (f *Filesystem) checkNodeView(ctx context.Context, n fsNode) error {
	if n.identity == (ObjectID{}) {
		return nil
	}
	if n.guardTx != (ObjectID{}) {
		raw, _, err := f.store.Get(ctx, n.guardTx)
		if errors.Is(err, ErrNotFound) {
			return ErrPrecondition
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, n.guardRaw) {
			return ErrPrecondition
		}
		return nil
	}
	raw, _, err := f.store.Get(ctx, n.identity)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, n.head) {
		if cache, ok := f.store.(*SubscriptionStore); ok {
			cache.remove(n.identity, nil)
		}
		return ErrPrecondition
	}
	return nil
}
