// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

// Cooperative priorities at safe I/O boundaries. A running volume transaction
// finishes atomically; lower-priority work must not wait while holding its locks.
type ioPriorities struct{ users, rebuilds atomic.Int64 }

func (s *Server) waitForUserIO(ctx context.Context) error {
	// Yield at each bounded action, without starving a generation under a
	// continuous stream of requests. A blocked client lending its priority to
	// this exact handoff bypasses the extra delay.
	runtime.Gosched()
	e, _ := ctx.Value(rangeExecutionKey{}).(*rangeExecution)
	if s.ioPriority.users.Load() > 0 && (e == nil || e.urgent.Load() == 0) {
		timer := time.NewTimer(time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
func (s *Server) cleanupIdle() bool {
	return s.ioPriority.users.Load() == 0 && s.ioPriority.rebuilds.Load() == 0
}

// Count an actual local storage phase. A caller must release this before
// waiting for a peer, so an unavailable PG cannot suppress unrelated cleanup.
func (s *Server) beginRebuildIO(ctx context.Context) (func(), error) {
	if err := s.waitForUserIO(ctx); err != nil {
		return nil, err
	}
	s.ioPriority.rebuilds.Add(1)
	return func() { s.ioPriority.rebuilds.Add(-1) }, nil
}

// Count local running/queued storage work, not time waiting on a remote peer.
// A dependency on a rebuild must not itself prevent that rebuild from running.
func (s *Server) lockUserObjectIO()   { s.ioPriority.users.Add(1); s.opMu.Lock() }
func (s *Server) unlockUserObjectIO() { s.opMu.Unlock(); s.ioPriority.users.Add(-1) }
