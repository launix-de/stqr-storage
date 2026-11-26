// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"sync/atomic"
	"time"
)

// IOStats distinguishes positioned backend operations from memory views. Neither
// family reports actual OS page-cache misses or device traffic. ViewRead includes
// mmap/sparse-RAM access and must not be displayed as physical disk throughput.
// Values are local to this open instance, not a persisted billing ledger.
type IOStats struct {
	ReadOps         uint64 `json:"read_ops"`
	WriteOps        uint64 `json:"write_ops"`
	ReadBytes       uint64 `json:"read_bytes"`
	ViewReadOps     uint64 `json:"view_read_ops"`
	ViewReadBytes   uint64 `json:"view_read_bytes"`
	WriteBytes      uint64 `json:"write_bytes"`
	SyncOps         uint64 `json:"sync_ops"`
	SyncNanoseconds uint64 `json:"sync_nanoseconds"`
	Errors          uint64 `json:"errors"`
}
type ioCounters struct{ readOps, writeOps, readBytes, writeBytes, viewReadOps, viewReadBytes, syncOps, syncNS, errors atomic.Uint64 }
type meteredIO struct {
	VolumeIO
	counts ioCounters
}

func (d *meteredIO) ReadAt(b []byte, off int64) (int, error) {
	n, e := d.VolumeIO.ReadAt(b, off)
	if file, ok := d.VolumeIO.(*FileIO); ok && file.hasMapping() {
		d.counts.viewReadOps.Add(1)
		if n > 0 {
			d.counts.viewReadBytes.Add(uint64(n))
		}
	} else {
		d.counts.readOps.Add(1)
		if n > 0 {
			d.counts.readBytes.Add(uint64(n))
		}
	}
	if e != nil {
		d.counts.errors.Add(1)
	}
	return n, e
}
func (d *meteredIO) WriteAt(b []byte, off int64) (int, error) {
	n, e := d.VolumeIO.WriteAt(b, off)
	d.counts.writeOps.Add(1)
	if n > 0 {
		d.counts.writeBytes.Add(uint64(n))
	}
	if e != nil {
		d.counts.errors.Add(1)
	}
	return n, e
}
func (d *meteredIO) Sync() error {
	start := time.Now()
	e := d.VolumeIO.Sync()
	d.counts.syncOps.Add(1)
	d.counts.syncNS.Add(uint64(time.Since(start)))
	if e != nil {
		d.counts.errors.Add(1)
	}
	return e
}
func (d *meteredIO) Alignment() uint32 {
	if a, ok := d.VolumeIO.(interface{ Alignment() uint32 }); ok {
		return a.Alignment()
	}
	return PageSize
}
func (v *Volume) IOStats() IOStats { v.mu.RLock(); defer v.mu.RUnlock(); return v.ioStatsLocked() }
func (v *Volume) ioStatsLocked() IOStats {
	d := v.dev.(*meteredIO)
	c := &d.counts
	return IOStats{ViewReadOps: c.viewReadOps.Load(), ViewReadBytes: c.viewReadBytes.Load(), ReadOps: c.readOps.Load(), WriteOps: c.writeOps.Load(), ReadBytes: c.readBytes.Load(), WriteBytes: c.writeBytes.Load(), SyncOps: c.syncOps.Load(), SyncNanoseconds: c.syncNS.Load(), Errors: c.errors.Load()}
}

// Borrowed backend spans are separately visible memory-access counters, never
// positioned-read or physical-I/O counters. Kernel demand paging may cause an
// actual device read, but this interface cannot measure those page faults.
func (d *meteredIO) borrowAt(off uint64, n int) ([]byte, int, error) {
	reader, ok := d.VolumeIO.(interface {
		borrowAt(uint64, int) ([]byte, int, error)
	})
	if !ok {
		return nil, 0, ErrUnsupported
	}
	data, read, err := reader.borrowAt(off, n)
	if err == ErrUnsupported {
		return nil, 0, err
	}
	d.counts.viewReadOps.Add(1)
	if read > 0 {
		d.counts.viewReadBytes.Add(uint64(read))
	}
	if err != nil {
		d.counts.errors.Add(1)
	}
	return data, read, err
}
