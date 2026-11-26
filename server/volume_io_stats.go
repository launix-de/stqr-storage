// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// volumeAccessIO translates backend requests to the existing wire layout. The
// legacy Physical* names count STORAGE ACCESSES, not OS faults/device traffic.
// mmap/RAM views and positioned reads are disjoint producer counters, so add
// their bytes exactly once. Never put backend page-operation counts in the
// object operation counters (one GET may visit several pages). Active volumes
// and retired history must both use this conversion.
func volumeAccessIO(s volume.IOStats) client.IOStats {
	return client.IOStats{PhysicalReadBytes: s.ReadBytes + s.ViewReadBytes, PhysicalWriteBytes: s.WriteBytes, Errors: s.Errors, Flushes: s.SyncOps, FlushNS: s.SyncNanoseconds}
}
