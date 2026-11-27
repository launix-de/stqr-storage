// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/volume"
)

// A same-node target may already contain canonical objects. They are never
// purged/re-imported: atomic in-place encoding transitions preserve the source
// on failure. Other source volumes still use the normal snapshot path.
func (r *rangeManager) reencodeLocalDestination(ctx context.Context, p rangePlan) error {
	if p.Source != p.Target {
		return nil
	}
	store, err := r.destination(p)
	if err != nil {
		return err
	}
	b, err := r.server.Bucket(volume.ID(p.Before.Bucket))
	if err != nil {
		return err
	}
	r.mu.Lock()
	txn := r.txns[p.id()]
	r.mu.Unlock()
	if txn == nil {
		return errControlConflict
	}
	var after *volume.ObjectID
	for {
		if err := r.server.waitForUserIO(ctx); err != nil {
			return err
		}
		// Bounded read-only enumeration does not need node-wide admission.
		h, err := store.Open(false)
		if err != nil {
			return err
		}
		records, err := p.scanObjects(h, after, 128)
		h.Close()
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			if !p.contains(record.Key.Object) || record.Info.Deleted || record.Info.Compression == p.Compression {
				continue
			}
			end, err := r.server.beginRebuildIO(ctx)
			if err != nil {
				return err
			}
			err = func() error {
				defer end()
				r.server.opMu.Lock()
				defer r.server.opMu.Unlock()
				selected, _, _, err := b.selectVolume(b.storageDescription(), record.Key, volume.ID{}, false)
				if err != nil {
					return err
				}
				if selected != store {
					return nil
				}
				allowance, err := r.allowance(txn)
				if err != nil {
					return err
				}
				before := store.Stats().InFlightReservedPhysical
				h, err := store.Open(true)
				if err != nil {
					return err
				}
				err = h.RecompressWithOptions(record.Key, p.Compression, volume.Options{Accepted: true, InFlightAllowance: allowance})
				h.Close()
				r.consumed(txn, store, before)
				return err
			}()
			if err != nil {
				return err
			}
		}
		next := records[len(records)-1].Key.Object
		after = &next
	}
}
