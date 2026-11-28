// SPDX-License-Identifier: GPL-3.0-only
package volume

// A visible canonical header after an interrupted final barrier is ancestry,
// not proof that its physical fence completed. Accepted work and restart/Close
// never enter here. An explicit fence/durable operation seals the canonical
// baseline BEFORE reusing its WAL; later RAM-only dirty pages remain unconfirmed.
func (v *Volume) confirmRestartCanonicalLocked() error {
	if !v.restartCanonicalPending {
		return nil
	}
	for v.checkpointing {
		v.prepareCond.Wait()
	}
	if err := v.check(false); err != nil {
		return err
	}
	v.beginMaintenance()
	defer v.endMaintenance()
	ioView := &Volume{dev: v.dev, sb: v.sb, limit: v.limit, durableGeneration: v.restartCanonicalGeneration}
	// Keep v.deferred attached: this fence flushes existing canonical OS bytes,
	// not the newer accepted overlay. A nil group must not close that overlay.
	if err := v.runFence(&writeFence{}, ioView, func() error { return ioView.rawSync() }); err != nil {
		return err
	}
	v.restartCanonicalPending = false
	return nil
}
