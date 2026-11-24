// SPDX-License-Identifier: GPL-3.0-only
package volume

import "errors"

// Purge removes an obsolete physical copy without inventing a higher logical
// tombstone version. Only a trusted, durably committed ownership handoff may
// authorize this cleanup; it is never the implementation of client DELETE.
// ExpectedVersion fences cleanup against a newer copy; missing is idempotent.
// Retry receipts are retained until separately confirmed/forgotten by the caller.
func (h *Handle) Purge(k Key, expectedVersion uint64) error {
	return h.PurgeBatch([]ObjectRecord{{Key: k, Info: Info{Version: expectedVersion}}})
}

// PurgeBatch performs version-fenced cleanup in one durable transaction. A
// missing key is idempotent; a mismatched version or duplicate key aborts the
// entire batch. Logical data and retry receipts have the same contract as Purge.
// At most 4096 records are accepted to bound the transaction's working set.
func (h *Handle) PurgeBatch(records []ObjectRecord) error {
	return h.do(true, func(v *Volume) error {
		if len(records) > 4096 {
			return ErrRange
		}
		seen := make(map[Key]bool, len(records))
		// Validate everything before releasing any storage or editing the tree.
		for _, record := range records {
			if busy, err := v.pending(record.Key); err != nil {
				return err
			} else if busy {
				return ErrBusy
			}
			if record.Info.Version == 0 || seen[record.Key] {
				return ErrRange
			}
			seen[record.Key] = true
			o, err := v.loadObject(record.Key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if o.Info.Version != record.Info.Version {
				return ErrPrecondition
			}
		}
		t := v.transaction()
		changed := false
		for _, record := range records {
			o, err := v.loadObject(record.Key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			releaseData(t, o)
			t.release(o.Base, o.Size, true)
			if !o.Info.Deleted {
				t.sb.Objects--
				t.sb.LogicalBytes -= o.Info.Length
			}
			t.sb.ArenaUnused = arenaAfter(t.sb.ArenaUnused, o, nil)
			if err := t.remove(&t.sb.ObjectRoot, objectKey(record.Key)); err != nil {
				return err
			}
			changed = true
		}
		if !changed {
			return nil
		}
		if err := t.commit(true); err != nil {
			return err
		}
		for _, record := range records {
			v.cache.remove(record.Key)
		}
		return nil
	})
}
