// SPDX-License-Identifier: GPL-3.0-only
package volume

import "errors"

// Purge removes a physical copy under the volume write lock. The caller must
// hold a durable ownership/maintenance fence for the key. Missing is idempotent.
func (h *Handle) Purge(k Key) error {
	return h.PurgeBatch([]ObjectRecord{{Key: k}})
}

// PurgeBatch requires the same externally verified ownership fence for every
// key. Duplicate keys abort the batch; retry receipts remain independently owned.
func (h *Handle) PurgeBatch(records []ObjectRecord) error {
	return h.do(true, func(v *Volume) error { return v.purgeBatchLocked(records) })
}

func (v *Volume) purgeBatchLocked(records []ObjectRecord) error {
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
		if seen[record.Key] {
			return ErrRange
		}
		seen[record.Key] = true
		_, err := v.loadObject(record.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
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
}
