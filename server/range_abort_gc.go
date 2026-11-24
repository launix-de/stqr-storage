// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A cancelled offer may have staged data from an interrupted bulk copy. Reclaim
// at most one bounded page per tick. New active handoffs and authoritative local
// versions/receipts always take precedence over collecting abandoned copies.
func (r *rangeManager) collectAbortedImports() error {
	s := r.server
	s.opMu.Lock()
	defer s.opMu.Unlock()
	r.mu.Lock()
	var txn *rangeTxn
	for _, id := range r.order {
		t := r.txns[id]
		if !t.aborted || t.plan.Target != r.node || t.abortObjectsDone && t.abortRequestsDone && t.abortReplicasDone {
			continue
		}
		busy := false
		for _, other := range r.txns {
			if r.active(other) && rangePlansOverlap(t.plan, other.plan) {
				busy = true
				break
			}
		}
		if !busy {
			txn = t
			break
		}
	}
	r.mu.Unlock()
	if txn == nil {
		return nil
	}
	s.mu.RLock()
	managed := s.volumes[txn.plan.Volume.String()]
	s.mu.RUnlock()
	if managed == nil {
		txn.abortObjectsDone = true
		txn.abortRequestsDone = true
		txn.abortReplicasDone = true
		return nil
	}
	store := managed.store
	locallyOwned := func(key volume.Key) bool {
		entry, known := s.placement.Load().lookup(client.ID(key.Bucket), client.ObjectID(key.Object))
		if !known {
			return false
		}
		for _, location := range entry.Locations {
			if location.Node == r.node && entry.State != 4 && s.volumeAllowedForEntry(client.ID(key.Bucket), entry, store.Identity().VolumeID, s.currentSystem.Load()) {
				return true
			}
		}
		return false
	}
	// No handle remains open while selecting another volume: the selection
	// itself may examine this destination and must not recursively acquire it.
	h, err := store.Open(false)
	if err != nil {
		return err
	}
	if !txn.abortObjectsDone {
		records, err := h.ScanBucketObjects(volume.ID(txn.plan.Before.Bucket), txn.abortObjectCursor, 64)
		h.Close()
		if err != nil {
			return err
		}
		var obsolete []volume.ObjectRecord
		for _, record := range records {
			if !txn.plan.contains(record.Key.Object) {
				continue
			}
			safe := !locallyOwned(record.Key)
			if !safe {
				bucket, err := s.Bucket(record.Key.Bucket)
				if err == nil {
					authoritative, _, found, err := bucket.selectVolume(bucket.Describe(), record.Key, volume.ID{}, false)
					safe = err == nil && found && authoritative != store
				}
			}
			if safe {
				obsolete = append(obsolete, record)
			}
		}
		h, err := store.Open(true)
		if err != nil {
			return err
		}
		err = h.PurgeBatch(obsolete)
		h.Close()
		if err != nil {
			return err
		}
		if len(records) == 0 {
			txn.abortObjectsDone = true
		} else {
			next := records[len(records)-1].Key.Object
			txn.abortObjectCursor = &next
		}
		return nil
	}
	if txn.abortRequestsDone {
		records, err := h.ScanReplicaRecords(txn.abortReplicaCursor, 64)
		h.Close()
		if err != nil {
			return err
		}
		var obsolete []volume.ID
		for _, record := range records {
			if record.State == volume.ReplicaAborted && record.Request.Key.Bucket == volume.ID(txn.plan.Before.Bucket) && txn.plan.contains(record.Request.Key.Object) && !locallyOwned(record.Request.Key) {
				obsolete = append(obsolete, record.Request.ID)
			}
		}
		h, err := store.Open(true)
		if err != nil {
			return err
		}
		for _, id := range obsolete {
			if err = h.ForgetRequest(id); err != nil {
				break
			}
		}
		h.Close()
		if err != nil {
			return err
		}
		if len(records) == 0 {
			txn.abortReplicasDone = true
		} else {
			last := records[len(records)-1].Request.ID
			txn.abortReplicaCursor = &last
		}
		return nil
	}
	records, err := h.ScanRequestRecords(txn.abortRequestCursor, 64)
	h.Close()
	if err != nil {
		return err
	}
	var obsolete []volume.ID
	for _, record := range records {
		if record.Key.Bucket != volume.ID(txn.plan.Before.Bucket) || !txn.plan.contains(record.Key.Object) {
			continue
		}
		safe := !locallyOwned(record.Key)
		if !safe {
			bucket, err := s.Bucket(record.Key.Bucket)
			if err == nil {
				authoritative, _, found, err := bucket.selectVolume(bucket.Describe(), record.Key, volume.ID{}, false)
				if err == nil && found && authoritative != store {
					other, err := authoritative.Open(false)
					if err != nil {
						return err
					}
					previous := record.ID
					for i := len(previous) - 1; i >= 0; i-- {
						if previous[i] != 0 {
							previous[i]--
							break
						}
						previous[i] = 255
					}
					page, err := other.ScanRequestRecords(&previous, 1)
					other.Close()
					if err != nil {
						return err
					}
					if len(page) == 1 {
						original := page[0]
						safe = original.ID == record.ID && original.Key == record.Key && original.Fingerprint == record.Fingerprint && original.Result == record.Result
					}
				}
			}
		}
		if safe {
			obsolete = append(obsolete, record.ID)
		}
	}
	h, err = store.Open(true)
	if err != nil {
		return err
	}
	defer h.Close()
	for _, id := range obsolete {
		if err := h.ForgetRequest(id); err != nil {
			return err
		}
	}
	if len(records) == 0 {
		txn.abortRequestsDone = true
	} else {
		next := records[len(records)-1].ID
		txn.abortRequestCursor = &next
	}
	return nil
}
