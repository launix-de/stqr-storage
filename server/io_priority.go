// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"sync/atomic"
	"time"
)

// Cooperative priorities at safe I/O boundaries. A running volume transaction
// finishes atomically; lower-priority work must not wait while holding its locks.
type ioPriorities struct{ users, rebuilds atomic.Int64 }

func (s *Server) waitForUserIO(ctx context.Context) error {
	for s.ioPriority.users.Load() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return ctx.Err()
}
func (s *Server) cleanupIdle() bool {
	return s.ioPriority.users.Load() == 0 && s.ioPriority.rebuilds.Load() == 0
}

// Count local running/queued storage work, not time waiting on a remote peer.
// A dependency on a rebuild must not itself prevent that rebuild from running.
func (s *Server) lockUserObjectIO()   { s.ioPriority.users.Add(1); s.opMu.Lock() }
func (s *Server) unlockUserObjectIO() { s.opMu.Unlock(); s.ioPriority.users.Add(-1) }
