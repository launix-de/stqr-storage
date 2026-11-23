// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/volume"
)

func (r *rangeManager) allowance(t *rangeTxn, force ...*controlClaim) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var claim *controlClaim
	if len(force) > 0 {
		claim = force[0]
	}
	if err := r.server.scopedTransition(claim); err != nil {
		return 0, err
	}
	if t.aborted || t.forceRetired {
		return 0, errControlConflict
	}
	return t.remaining, nil
}

// Called under node opMu after closing the data handle. The volume commits
// physical reserve consumption with the import; this mirrors the exact delta
// into the participant's working state without a per-object control log entry.
func (r *rangeManager) consumed(t *rangeTxn, store *volume.Volume, before uint64) {
	after := store.Stats().InFlightReservedPhysical
	if after >= before {
		return
	}
	r.mu.Lock()
	t.remaining -= min(t.remaining, before-after)
	r.publishQuotaImportsLocked()
	r.mu.Unlock()
}

// restoreReservations runs before listeners/workers. The durable plan is the
// claim; canonical target indexes reconstruct import progress, including a
// crash between restoring bytes and recording its advisory transfer memo.
func (r *rangeManager) restoreReservations() error {
	totals := map[volume.ID]uint64{}
	for _, t := range r.txns {
		if !r.active(t) || t.plan.Target != r.node {
			t.remaining = 0
			continue
		}
		store, err := r.destination(t.plan)
		if err != nil {
			return err
		}
		h, err := store.Open(false)
		if err != nil {
			return err
		}
		used, found := uint64(0), false
		var after *volume.ObjectID
		for {
			records, err := h.ScanBucketObjects(volume.ID(t.plan.Before.Bucket), after, 128)
			if err != nil {
				h.Close()
				return err
			}
			if len(records) == 0 {
				break
			}
			for _, record := range records {
				if !t.plan.contains(record.Key.Object) {
					continue
				}
				size, err := h.AllocatedPhysical(record.Key)
				if err != nil {
					h.Close()
					return err
				}
				used = addCapacity(used, addCapacity(size, 256))
				found = true
			}
			next := records[len(records)-1].Key.Object
			after = &next
		}
		err = h.ScanRequests(func(record volume.RequestRecord) error {
			if record.Key.Bucket == volume.ID(t.plan.Before.Bucket) && t.plan.contains(record.Key.Object) {
				used = addCapacity(used, 512)
				found = true
			}
			return nil
		})
		h.Close()
		if err != nil {
			return err
		}
		if found {
			used = addCapacity(used, 64<<10)
		} // shared index/root budget
		t.remaining = t.plan.Physical - min(t.plan.Physical, used)
		totals[t.plan.Volume] = addCapacity(totals[t.plan.Volume], t.remaining)
	}
	r.publishQuotaImportsLocked()
	if err := r.server.applyQuotaProtectionLocked(); err != nil {
		return err
	}
	for id, total := range totals {
		store := r.server.volumes[id.String()]
		if store == nil {
			return volume.ErrNotFound
		}
		if err := store.store.SetInFlightReserve(total); err != nil {
			return err
		}
	}
	return nil
}

// releaseReserveLocked runs under opMu and r.mu, after durable target DONE.
func (r *rangeManager) releaseReserveLocked(t *rangeTxn) error {
	if t.plan.Target != r.node {
		return nil
	}
	t.remaining = 0
	var total uint64
	for _, other := range r.txns {
		if r.active(other) && other.plan.Target == r.node && other.plan.Volume == t.plan.Volume {
			total = addCapacity(total, other.remaining)
		}
	}
	store, err := r.destination(t.plan)
	if err != nil {
		return err
	}
	r.publishQuotaImportsLocked()
	if err := store.SetInFlightReserve(total); err != nil {
		return err
	}
	return r.server.refreshVolumeQuota(store)
}
