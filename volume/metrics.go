// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"sync/atomic"
	"time"
)

// IOStats contains physical backend counters, not logical bucket bytes. Values
// are local to this open instance; they are not a persisted billing ledger.
type IOStats struct {
	ReadOps         uint64 `json:"read_ops"`
	WriteOps        uint64 `json:"write_ops"`
	ReadBytes       uint64 `json:"read_bytes"`
	WriteBytes      uint64 `json:"write_bytes"`
	SyncOps         uint64 `json:"sync_ops"`
	SyncNanoseconds uint64 `json:"sync_nanoseconds"`
	Errors          uint64 `json:"errors"`
}
type ioCounters struct{ readOps, writeOps, readBytes, writeBytes, syncOps, syncNS, errors atomic.Uint64 }
type meteredIO struct {
	VolumeIO
	counts ioCounters
}

func (d *meteredIO) ReadAt(b []byte, off int64) (int, error) {
	n, e := d.VolumeIO.ReadAt(b, off)
	d.counts.readOps.Add(1)
	if n > 0 {
		d.counts.readBytes.Add(uint64(n))
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
	return IOStats{ReadOps: c.readOps.Load(), WriteOps: c.writeOps.Load(), ReadBytes: c.readBytes.Load(), WriteBytes: c.writeBytes.Load(), SyncOps: c.syncOps.Load(), SyncNanoseconds: c.syncNS.Load(), Errors: c.errors.Load()}
}
