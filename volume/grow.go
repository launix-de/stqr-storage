// SPDX-License-Identifier: GPL-3.0-only
package volume

import "fmt"

// Grow explicitly extends a regular file or RAM backend to at least sizeBytes and adopts the
// added capacity crash-safely (volatile for RAM). It never shrinks or resizes raw
// devices. Call outside a handle session. A successful resize preserves volume ID,
// object contents, sparse holes, tombstones and retained request results.
func (v *Volume) Grow(sizeBytes int64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.beginMaintenance()
	defer v.endMaintenance()
	if e := v.check(true); e != nil {
		return e
	}
	if err := v.flushDeferred(); err != nil {
		return err
	}
	n, e := alignedSize(sizeBytes)
	if e != nil {
		return e
	}
	if uint64(n) < v.sb.Identity.Size {
		return ErrRange
	}
	if memory, ok := v.dev.(*meteredIO).VolumeIO.(*MemoryIO); ok {
		if e := memory.grow(n); e != nil {
			return e
		}
		_, e = v.adoptBackendSizeLocked()
		return e
	}
	file, ok := v.dev.(*meteredIO).VolumeIO.(*FileIO)
	if !ok {
		return ErrUnsupported
	}
	st, e := file.file.Stat()
	if e != nil {
		return e
	}
	if !st.Mode().IsRegular() {
		return ErrUnsupported
	}
	if st.Size() < int64(v.sb.Identity.Size) {
		return ErrIdentity
	}
	if st.Size() < n {
		if e = file.file.Truncate(n); e != nil {
			return fmt.Errorf("%w: %v", ErrFull, e)
		}
		if e = v.sync(); e != nil {
			return e
		}
	}
	_, e = v.rescanLocked()
	return e
}

// Rescan queries the backing device's current physical capacity, e.g. after an
// external LVM enlargement, and adopts added space. It does not resize devices
// or issue a partition-table reread. A larger backend can be reopened safely at
// its previous committed layout until Rescan succeeds. Shrinking is rejected.
// The volume must own the device exclusively; call outside a handle session.
func (v *Volume) Rescan() (uint64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.beginMaintenance()
	defer v.endMaintenance()
	if e := v.check(true); e != nil {
		return 0, e
	}
	if err := v.flushDeferred(); err != nil {
		return 0, err
	}
	return v.rescanLocked()
}
func (v *Volume) rescanLocked() (uint64, error) {
	if v.sb.Identity.Class == RAM || v.sb.Identity.Class == RAMCache {
		return 0, ErrUnsupported
	}
	return v.adoptBackendSizeLocked()
}
func (v *Volume) adoptBackendSizeLocked() (uint64, error) {
	n, e := v.dev.Size()
	if e != nil {
		return 0, e
	}
	if n < 0 || uint64(n) < v.sb.Identity.Size {
		v.failed = ErrIdentity
		return 0, ErrIdentity
	}
	size := uint64(n)
	if size == v.sb.Identity.Size {
		return size, nil
	}
	v.limit = size
	t := v.transaction()
	old := t.sb.Identity.Size
	t.sb.Identity.Size = size
	t.sb.FreeBytes += size - old
	if e = t.freeSpan(old, size-old); e != nil {
		return 0, e
	}
	if e = t.commit(true); e != nil {
		return 0, e
	}
	return size, nil
}

// BackingSize reads the backing handle's current size without adopting it. It
// permits device-growth previews while retaining the exclusive open claim.
func (v *Volume) BackingSize() (uint64, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return 0, e
	}
	n, e := v.dev.Size()
	if e != nil {
		return 0, e
	}
	if n < 0 {
		return 0, ErrRange
	}
	return uint64(n), nil
}

// Shrink defragments first, then shortens a regular file/RAM volume only after
// the entire removed tail is free. Metadata and the size commit before truncation;
// interruption therefore leaves a valid smaller volume on a larger backend.
// It never truncates raw devices. An occupied tail that cannot be evacuated
// with the available workspace returns ErrFull without discarding data.
func (v *Volume) Shrink(sizeBytes int64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.beginMaintenance()
	defer v.endMaintenance()
	if e := v.check(true); e != nil {
		return e
	}
	if err := v.flushDeferred(); err != nil {
		return err
	}
	n, e := alignedSize(sizeBytes)
	if e != nil {
		return e
	}
	size := uint64(n)
	if size > v.sb.Identity.Size {
		return ErrRange
	}
	backend := v.dev.(*meteredIO).VolumeIO
	switch d := backend.(type) {
	case *MemoryIO:
	case *FileIO:
		st, e := d.file.Stat()
		if e != nil {
			return e
		}
		if !st.Mode().IsRegular() {
			return ErrUnsupported
		}
	default:
		return ErrUnsupported
	}
	if size == v.sb.Identity.Size {
		return nil
	}
	if e = v.compactLocked(); e != nil {
		return e
	}
	tail, found, e := treeNeighbor(v.node, v.sb.FreeRoot, spaceKey(size), false)
	if e != nil {
		return e
	}
	start := keyOffset(tail.key)
	if !found || start > size || start+tail.value != v.sb.Identity.Size {
		return ErrFull
	}
	t := v.transaction()
	if e = t.remove(&t.sb.FreeRoot, tail.key); e != nil {
		return e
	}
	if start < size {
		if e = t.set(&t.sb.FreeRoot, spaceKey(start), size-start); e != nil {
			return e
		}
	}
	t.sb.FreeBytes -= t.sb.Identity.Size - size
	t.sb.Identity.Size = size
	if e = t.commit(true); e != nil {
		return e
	}
	switch d := backend.(type) {
	case *MemoryIO:
		d.mu.Lock()
		d.size = n
		for page := range d.pages {
			if page >= n/PageSize {
				delete(d.pages, page)
			}
		}
		d.mu.Unlock()
	case *FileIO:
		if e = d.file.Truncate(n); e == nil {
			e = d.file.Sync()
		}
		if e != nil {
			v.failed = e
			return e
		}
	}
	v.limit = size
	return nil
}
