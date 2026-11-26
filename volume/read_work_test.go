// SPDX-License-Identifier: GPL-3.0-only
package volume

// Algorithmic no-scan and work-bound checks include mapped/RAM views as well
// as positioned reads. Memory access is not disk I/O, but mmap must not hide a
// regression which walks unrelated object metadata or checksum blocks.
func testReadWorkOps(stats IOStats) uint64   { return stats.ReadOps + stats.ViewReadOps }
func testReadWorkBytes(stats IOStats) uint64 { return stats.ReadBytes + stats.ViewReadBytes }
