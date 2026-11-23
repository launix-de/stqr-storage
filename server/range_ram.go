// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (r *rangeManager) ramAvailable(bucket volume.ID, object volume.ObjectID, entry client.RangeEntry) bool {
	if entry.Runtime == r.server.boot {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ramAvailableLocked(bucket, object, entry)
}
func (r *rangeManager) ramAvailableLocked(bucket volume.ID, object volume.ObjectID, entry client.RangeEntry) bool {
	if entry.Runtime == r.server.boot {
		return true
	}
	for _, txn := range r.txns {
		if txn.aborted || txn.decision == nil && txn.forcePromotion == nil || txn.plan.Target != r.node || txn.targetBoot != r.server.boot || txn.plan.Before.Bucket != client.ID(bucket) || !txn.plan.contains(object) {
			continue
		}
		for _, location := range entry.Locations {
			if location.Role == 0 && location.Node == r.node && location.Volume == client.ID(txn.plan.Volume) {
				for _, moved := range txn.plan.After.Entries {
					if moved.Start == txn.plan.Moving.Start && moved.PrefixBits == txn.plan.Moving.PrefixBits && moved.Runtime == entry.Runtime {
						return true
					}
				}
			}
		}
	}
	return false
}

// A single authoritative RAM copy lost with its owning process cannot be
// restored from an obsolete pre-cutover source. Retire only the old transfer
// claim; the next explicit runtime-reset scope establishes the fresh empty era.
// The durable historical COMMIT remains idempotent for the returning source.
func (r *rangeManager) retireLostRAMTargets() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range r.order {
		txn := r.txns[id]
		if !txn.plan.ram() || txn.plan.Target != r.node || txn.decision == nil || txn.targetBoot == r.server.boot || txn.targetDone.RecordSequence != 0 {
			continue
		}
		proof, err := r.persist(rangeRAMTargetLost, rangeEvent{ID: id, Boot: r.server.boot})
		if err != nil {
			return err
		}
		if err := r.releaseReserveLocked(txn); err != nil {
			return err
		}
		if txn.plan.Source == r.node && txn.sourceDone.RecordSequence == 0 {
			if _, err := r.persist(rangeSourceDone, rangeEvent{ID: id, TargetDone: proof}); err != nil {
				return err
			}
		}
	}
	return nil
}

// A different primary/volume or a fresh runtime must not inherit abandoned RAM
// objects whose version counters belong to an older era. The BEFORE claim and
// opMu/r.mu exclusion prove this target slice is unowned; preserve local data
// only when it is part of the same live source pool.
func (r *rangeManager) clearRAMStagingLocked(store *volume.Volume, p rangePlan) error {
	h, err := store.Open(true)
	if err != nil {
		return err
	}
	defer h.Close()
	var after *volume.ObjectID
	for {
		records, err := h.ScanBucketObjects(volume.ID(p.Before.Bucket), after, 128)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			break
		}
		var obsolete []volume.ObjectRecord
		for _, record := range records {
			if p.contains(record.Key.Object) {
				obsolete = append(obsolete, record)
			}
		}
		if err := h.PurgeBatch(obsolete); err != nil {
			return err
		}
		next := records[len(records)-1].Key.Object
		after = &next
	}
	var cursor *volume.ID
	for {
		records, err := h.ScanRequestRecords(cursor, 128)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if record.Key.Bucket == volume.ID(p.Before.Bucket) && p.contains(record.Key.Object) {
				if err := h.ForgetRequest(record.ID); err != nil {
					return err
				}
			}
		}
		next := records[len(records)-1].ID
		cursor = &next
	}
	return nil
}
