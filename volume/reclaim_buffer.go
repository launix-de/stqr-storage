// SPDX-License-Identifier: GPL-3.0-only
package volume

import "errors"

// The caller owns a durable deletion/ownership fence. It must checkpoint the
// returned generation before confirming completion. The overlay and its WAL
// are the existing accepted-write mechanism; no on-disk representation changes.
func (v *Volume) bufferCleanup(run func() error) (uint64, error) {
	// A cleanup batch is bounded, but an existing foreground group might already
	// fill the budget. The caller releases its node admission lock before here.
	if err := v.flushDeferred(); err != nil {
		return 0, err
	}
	if err := v.beginDeferred(); err != nil {
		if !errors.Is(err, ErrFull) {
			return 0, err
		}
		// Tiny/full volumes use the existing scratch-WAL durable path. They must
		// still make progress when an extra 8-MiB accepted journal cannot fit.
		err = run()
		return v.sb.Generation, err
	}
	base := v.sb.Generation
	v.staging = map[uint64][]byte{}
	err := func() error { defer func() { v.staging = nil }(); return run() }()
	if v.sb.Generation == base && len(v.deferred.pages) == 0 {
		v.deferred = nil
		if errors.Is(err, errDeferredBudget) {
			err = run()
		}
	}
	return v.sb.Generation, err
}

// PurgeBatchAccepted publishes a bounded purge; checkpoint its generation before
// acknowledging durable reclamation. A tiny-volume fallback may be durable already.
func (h *Handle) PurgeBatchAccepted(records []ObjectRecord) (uint64, error) {
	return withHandle(h, true, func(v *Volume) (uint64, error) {
		return v.bufferCleanup(func() error { return v.purgeBatchLocked(records) })
	})
}

// ForgetRequestsAccepted is the matching buffered cleanup for settled receipts.
// The caller must checkpoint the returned generation before recording completion.
func (h *Handle) ForgetRequestsAccepted(ids []ID) (uint64, error) {
	return withHandle(h, true, func(v *Volume) (uint64, error) {
		return v.bufferCleanup(func() error { return v.forgetRequestsLocked(ids) })
	})
}
