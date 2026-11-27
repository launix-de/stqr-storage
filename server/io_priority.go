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
	// Wait for a quiet foreground interval at each bounded action, but cap
	// the grace period so a continuous workload cannot starve handoff forever.
	// Urgent range dependencies bypass grace; no lock is held while waiting.
	runtime.Gosched()
	e, _ := ctx.Value(rangeExecutionKey{}).(*rangeExecution)
	if s.ioPriority.users.Load() == 0 || e != nil && e.urgent.Load() != 0 {
		return ctx.Err()
	}
	deadline := time.NewTimer(25 * time.Millisecond)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for s.ioPriority.users.Load() > 0 && (e == nil || e.urgent.Load() == 0) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return ctx.Err()
		case <-tick.C:
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

// Parallel admission pins administrative lifetime/placement without serializing
// independent volume operations. Callers must first prove stable single-copy
// authority and install exact quota reservations; ordinary lockUserObjectIO
// remains the exclusive fallback for all other paths. Never upgrade while a
// shared admission is held: unwind and revalidate under exclusive admission.
func (s *Server) lockParallelUserObjectIO() {
	s.ioPriority.users.Add(1)
	s.opMu.RLock()
}
func (s *Server) unlockParallelUserObjectIO() {
	s.opMu.RUnlock()
	s.ioPriority.users.Add(-1)
}
