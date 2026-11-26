// SPDX-License-Identifier: GPL-3.0-only
package server

import "github.com/launix-de/stqr-storage/volume"

// No-scan proofs must observe both persistent read paths. mmap accesses are not
// physical disk I/O, but scanning mapped object metadata is still storage work
// which must not be hidden from an algorithm's no-index-walk oracle.
func testStorageReadOps(stats volume.IOStats) uint64   { return stats.ReadOps + stats.ViewReadOps }
func testStorageReadBytes(stats volume.IOStats) uint64 { return stats.ReadBytes + stats.ViewReadBytes }
