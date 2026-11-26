// SPDX-License-Identifier: GPL-3.0-only
package volume

import "errors"

// Buffered cleanup publishes exactly the same metadata as durable cleanup. The
// caller already owns a persistent deletion/ownership fence and must checkpoint
// the returned generation before recording completion of that cleanup intent.
// Exhausted buffer space returns ErrBusy without a partially published batch;
// the caller leaves its outer locks before flushing/retrying.
func (v *Volume) bufferCleanup(run func() error) (uint64, error) {
	return v.bufferCleanupMode(run, false)
}

func (v *Volume) bufferCleanupMode(run func() error, continuation bool) (uint64, error) {
	if !continuation && v.deferred != nil && len(v.deferred.pages) > v.deferred.pageLimit/2 {
		return 0, ErrBusy
	}
	if err := v.beginDeferred(); err != nil {
		if !errors.Is(err, ErrFull) {
			return 0, err
		}
		if v.checkpointing {
			return 0, ErrBusy
		}
		// A full/tiny volume still has the existing fixed WAL scratch area.
		// Its bounded durable fallback also releases v.mu during every fence.
		err = run()
		return v.sb.Generation, err
	}
	base := v.sb.Generation
	v.staging = map[uint64][]byte{}
	err := run()
	v.staging = nil
	if errors.Is(err, errDeferredBudget) {
		if len(v.deferred.pages) == 0 && v.sb.Generation == base && !v.checkpointing {
			v.deferred = nil
			err = run()
		} else {
			err = ErrBusy
		}
	}
	return v.sb.Generation, err
}

// PurgeAccepted finishes an already accepted DELETE without introducing a
// durable housekeeping operation. The caller must have fenced ownership and
// established that no clone/migration needs the tombstone. The real staging
// budget still bounds the transaction; the soft half-full threshold must not
// force an intervening checkpoint between DELETE and its small index cleanup.
func (h *Handle) PurgeAccepted(k Key) (uint64, error) {
	return withHandleMode(h, true, true, func(v *Volume) (uint64, error) {
		return v.bufferCleanupMode(func() error { return v.purgeBatchLocked([]ObjectRecord{{Key: k}}) }, true)
	})
}

func (h *Handle) PurgeBatchAccepted(records []ObjectRecord) (uint64, error) {
	return withHandleMode(h, true, true, func(v *Volume) (uint64, error) {
		return v.bufferCleanup(func() error { return v.purgeBatchLocked(records) })
	})
}

func (h *Handle) ForgetRequestsAccepted(ids []ID) (uint64, error) {
	return withHandleMode(h, true, true, func(v *Volume) (uint64, error) {
		return v.bufferCleanup(func() error { return v.forgetRequestsLocked(ids) })
	})
}
