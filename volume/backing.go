// SPDX-License-Identifier: GPL-3.0-only
package volume

// EnsurePhysicalBacking provisions a regular file's advertised address space
// before it backs a persistent capacity promise. A sparse ftruncate alone does
// not reserve filesystem blocks: unrelated volumes could otherwise consume
// their backing despite correct in-volume quota admission. Linux fallocate
// keeps file length and every existing payload byte unchanged. Raw devices are
// already provisioned; RAM/custom VolumeIO providers own their pool contract.
// The conservative file policy provisions the whole volume, rather than
// claiming that a preallocated prefix covers arbitrary allocator offsets.
func (v *Volume) EnsurePhysicalBacking() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return err
	}
	if f, ok := v.dev.(*FileIO); ok {
		if err := v.flushDeferred(); err != nil {
			return err
		}
		var backingErr error
		if err := v.checkpointIO(func(ioView *Volume) error {
			backingErr = ensureFileBacking(f.file, int64(ioView.sb.Identity.Size))
			return nil
		}); err != nil {
			return err
		}
		return backingErr
	}
	return nil
}
