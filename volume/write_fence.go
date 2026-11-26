// SPDX-License-Identifier: GPL-3.0-only
package volume

import "sync"

// A fence owns immutable pages until their physical checkpoint completes. The
// live volume already publishes these bytes; later buffered writes overlay them.
// Only durability waiters wait for I/O. No disk format or object version is added.
type writeFence struct {
	group    *deferredGroup
	image    *transaction
	reserved []allocation
}

func (f *writeFence) page(off uint64) ([]byte, bool, error) {
	if f.group != nil {
		b, ok := f.group.pages[off]
		return b, ok, nil
	}
	if f.image == nil {
		return nil, false, nil
	}
	if b, ok := f.image.pages[off]; ok {
		return b, true, nil
	}
	if _, ok := f.image.spilled[off]; ok {
		b, err := f.image.page(off)
		return b, true, err
	}
	return nil, false, nil
}

// checkBuffered permits bounded mutations while a physical fence is running.
// Other maintenance/publication paths still use check(true) and wait before
// building their changes, rather than constructing a stale allocator image.
func (v *Volume) checkBuffered() error {
	if v.maintenance {
		return ErrBusy
	}
	if v.checkpointing && v.fence != nil {
		if err := v.check(false); err != nil {
			return err
		}
		if v.sb.Moving {
			return ErrBusy
		}
		if v.state == Draining {
			return ErrDraining
		}
		return nil
	}
	return v.check(true)
}

// The caller owns v.mu exclusively. The callback only touches its private I/O
// view and immutable pages. Concurrent accepted writes never modify that view,
// its WAL, or its page images. Failure fences the complete live instance.
func (v *Volume) runFence(f *writeFence, ioView *Volume, run func() error) (err error) {
	if v.prepareCond == nil {
		v.prepareCond = sync.NewCond(&v.mu)
	}
	v.fence, v.checkpointing = f, true
	completed := false
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		v.fence, v.checkpointing = nil, false
		if !completed {
			v.failed = ErrRecovery
		} else if err != nil {
			v.failed = ioView.failed
			if v.failed == nil {
				v.failed = err
			}
		} else {
			v.durableGeneration = max(v.durableGeneration, ioView.durableGeneration)
		}
		v.prepareCond.Broadcast()
	}()
	err = run()
	completed = true
	return err
}

// Opening a handle does not itself publish anything. Operation-specific checks
// decide whether to buffer a write or wait for the current physical fence.
func (v *Volume) checkHandle(write bool) error {
	if write && v.checkpointing && v.fence != nil {
		return v.checkBuffered()
	}
	return v.check(write)
}

// TryOpenBuffered is foreground admission for bounded buffered mutations. Idle
// maintenance must retain TryOpen, which never enters a running checkpoint.
func (v *Volume) TryOpenBuffered() (h *Handle, acquired bool, err error) {
	if !v.mu.TryLock() {
		return nil, false, nil
	}
	if err = v.checkBuffered(); err != nil {
		v.mu.Unlock()
		return nil, false, err
	}
	h, err = v.openHandleLocked(true)
	return h, true, err
}

func (v *Volume) checkpointLockedThrough(generation uint64) error {
	for generation > v.durableGeneration {
		for v.checkpointing && generation > v.durableGeneration {
			v.prepareCond.Wait()
		}
		if generation <= v.durableGeneration {
			return nil
		}
		if err := v.check(false); err != nil {
			return err
		}
		if err := v.flushDeferred(); err != nil {
			return err
		}
	}
	return nil
}
