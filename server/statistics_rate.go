// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/internal/decay"
	"time"
)

const defaultPlacementStatsHalfLife = decay.DefaultLongHalfLife

// Retain the existing configuration/API while all production counters use the
// common two-window implementation. Explicit new long-window settings win.
func (s *Server) placementStatsHalfLife() time.Duration { return s.placementHalfLives().Long }

// counterAmount converts aggregate deltas without an intermediate float. At
// int64's representational limit an advisory forecast saturates rather than wraps.
func counterAmount(amount uint64) int64 { return int64(min(amount, uint64(1<<63-1))) }

func observeTrafficFrame(readWindow, writeWindow *decay.Windows, start, end time.Time, read, write uint64, h decay.HalfLives) {
	readWindow.AddInterval(start, end, counterAmount(read), h)
	writeWindow.AddInterval(start, end, counterAmount(write), h)
}
