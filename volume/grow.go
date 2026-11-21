// SPDX-License-Identifier: GPL-3.0-only
package volume

import "fmt"

// Grow explicitly extends a regular file or RAM backend to at least sizeBytes and adopts the
// added capacity crash-safely (volatile for RAM). It never shrinks or resizes raw
// devices. Call outside a handle session. A successful resize preserves volume ID,
// object versions, sparse holes, tombstones and retained request results.
func (v *Volume) Grow(sizeBytes int64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
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
	if e := v.check(true); e != nil {
		return 0, e
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
	if uint64(n) == v.sb.Identity.Size {
		return uint64(n), nil
	}
	// Old bank 0 always ends before new bank 1 starts, for every positive
	// capacity increase. If bank 1 is active, first compact into OLD bank 0 and
	// sync its root. Only then write NEW bank 1. An older root may be overwritten,
	// but the latest committed bank/root remain untouched until final publish.
	if v.sb.Bank != 0 {
		if e = v.compactLocked(); e != nil {
			return 0, e
		}
	}
	if e = v.rewriteLocked(uint64(n), 1); e != nil {
		return 0, e
	}
	return v.sb.Identity.Size, nil
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
