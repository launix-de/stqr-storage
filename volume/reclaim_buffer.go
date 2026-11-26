// SPDX-License-Identifier: GPL-3.0-only
package volume

import "errors"

// Buffered cleanup publishes exactly the same metadata as durable cleanup. The
// caller already owns a persistent deletion/ownership fence and must checkpoint
// the returned generation before recording completion of that cleanup intent.
// Exhausted buffer space returns ErrBusy without a partially published batch;
// the caller leaves its outer locks before flushing/retrying.
func (v *Volume) bufferCleanup(run func() error) (uint64, error) {
	if v.deferred != nil && len(v.deferred.pages) > v.deferred.pageLimit/2 {
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
