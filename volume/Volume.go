// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
)

// Volume is a local STQR store, backed by one file, raw device, or RAM device.
//
// Software contract:
//   - A volume belongs exclusively to one node. Callers must not share its
//     backing bytes with other processes/nodes. File constructors additionally
//     hold an exclusive OS lock; mounted Linux block devices are rejected.
//   - Volume is I/O-bound, not a lock-free data structure. All public operations
//     use its RWMutex: readers may run concurrently; mutation, GC, and close are
//     exclusive. This is also the contract for parallel server/WebSocket calls.
//     Open returns a handle retaining that lock across multiple operations until
//     Handle.Close. Use defer Close; CopyTo does not release the handle. Never
//     reenter Volume methods while holding a handle. Neither type may be copied.
//   - Payloads are accessed in bounded blocks, never through a mandatory mmap.
//     Metadata uses a bounded RAM cache and immutable sorted on-volume index
//     runs. Neither opening nor looking up many objects requires a full RAM
//     directory. In-flight operations can temporarily hold one object's metadata.
//   - A successful disk mutation has synced payload and journal before syncing
//     its new commit root. RAM has the same atomicity but no restart durability.
//   - After an ambiguous I/O/flush failure the instance is fenced until reopened.
//     A retry with the same nonzero RequestID recovers its original result.
//
// Cluster placement, authentication, quotas across volumes, and UI are outside
// this package. They may reserve physical capacity and manage the lifecycle.
type Volume struct {
	mu         sync.RWMutex
	dev        VolumeIO
	sb         Superblock
	delta      map[indexKey]uint64
	runs       []indexRun
	cache      *objectCache
	failed     error
	closed     bool
	name       string
	state      State
	guaranteed uint64
	onClose    func()
}

type State uint8

const (
	Pending State = iota
	Active
	Draining
	Detached
	Failed
)

// New opens filename, creating and formatting it if absent or unformatted.
// sizeBytes is only needed for a new/empty file and is always bytes, not UI text.
// class applies on format; a valid existing header supplies its own size/class.
// A damaged journal under a valid header is never reformatted. A recognizable
// damaged header is rejected, rather than destroying previously owned storage.
// For RAM/RAMCache filename is a label and no filesystem file is created.
// The node owns the returned Volume exclusively until Close.
func New(filename string, sizeBytes int64, class Class) (*Volume, error) {
	if !class.valid() {
		return nil, ErrRange
	}
	if class == RAM || class == RAMCache {
		n, e := alignedSize(sizeBytes)
		if e != nil {
			return nil, e
		}
		m, e := NewMemory(n)
		if e != nil {
			return nil, e
		}
		v, e := Format(m, FormatOptions{Class: class})
		if e != nil {
			m.Close()
			return nil, e
		}
		v.name = filename
		return v, nil
	}
	d, e := OpenFile(filename)
	if errors.Is(e, os.ErrNotExist) {
		n, err := alignedSize(sizeBytes)
		if err != nil {
			return nil, err
		}
		sizeBytes = n
		d, e = CreateFile(filename, sizeBytes)
	}
	if e != nil {
		return nil, e
	}
	if d.size < MinVolumeSize && sizeBytes > 0 {
		prefix := make([]byte, 4)
		_, _ = d.file.ReadAt(prefix, 0)
		if string(prefix) == "STQR" {
			d.Close()
			return nil, ErrCorrupt
		}
		st, err := d.file.Stat()
		if err != nil || !st.Mode().IsRegular() {
			d.Close()
			if err != nil {
				return nil, err
			}
			return nil, ErrRange
		}
		n, err := alignedSize(sizeBytes)
		if err == nil {
			err = d.file.Truncate(n)
		}
		if err == nil {
			err = d.file.Sync()
		}
		if err != nil {
			d.Close()
			return nil, err
		}
		d.size = n
	}
	v, e := Open(d, OpenOptions{})
	if errors.Is(e, ErrUnformatted) {
		v, e = Format(d, FormatOptions{Class: class})
	}
	if e != nil {
		d.Close()
		return nil, e
	}
	v.name = filename
	return v, nil
}

// NewRAM creates a bounded, volatile volume. Every call starts empty.
func NewRAM(sizeBytes int64) (*Volume, error) { return New("", sizeBytes, RAM) }

// Format explicitly replaces the contents of dev with a new empty STQR volume.
// The caller must own dev exclusively. Successful return transfers dev ownership
// to Volume; on error ownership remains with the caller.
func Format(dev VolumeIO, o FormatOptions) (*Volume, error) {
	if a, ok := dev.(interface{ Alignment() uint32 }); ok {
		n := a.Alignment()
		if n == 0 || n > PageSize || PageSize%n != 0 {
			return nil, ErrUnsupported
		}
	}
	size, e := dev.Size()
	if e != nil {
		return nil, e
	}
	if size < MinVolumeSize || !o.Class.valid() {
		return nil, ErrRange
	}
	if o.VolumeID == (ID{}) {
		o.VolumeID, e = NewID()
		if e != nil {
			return nil, e
		}
	}
	s := Superblock{Identity: Identity{o.ClusterID, o.NodeID, o.VolumeID, o.Class, uint64(size)}, Generation: 1, BankEpoch: 1}
	s.DataTop = s.bankSize()
	v := newVolume(dev, s, OpenOptions{})
	b, e := s.MarshalBinary()
	if e != nil {
		return nil, e
	}
	// Both old roots are overwritten before publishing a newly formatted volume.
	if e = writeFullAt(dev, b, 0); e != nil {
		return nil, e
	}
	if e = writeFullAt(dev, b, PageSize); e != nil {
		return nil, e
	}
	if e = dev.Sync(); e != nil {
		return nil, e
	}
	return v, nil
}
func newVolume(dev VolumeIO, s Superblock, o OpenOptions) *Volume {
	if _, ok := dev.(*meteredIO); !ok {
		dev = &meteredIO{VolumeIO: dev}
	}
	return &Volume{dev: dev, sb: s, delta: make(map[indexKey]uint64), cache: newCache(o.CacheEntries, o.CacheBytes), state: Active}
}

// Open validates the newest root and replays only its committed journal. It
// never scans uncommitted tail bytes and never discards a corrupted committed
// record by silently rolling back to an older object version.
func Open(dev VolumeIO, o OpenOptions) (*Volume, error) {
	if a, ok := dev.(interface{ Alignment() uint32 }); ok {
		n := a.Alignment()
		if n == 0 || n > PageSize || PageSize%n != 0 {
			return nil, ErrUnsupported
		}
	}
	s, e := loadSuperblocks(dev)
	if e != nil {
		return nil, e
	}
	size, e := dev.Size()
	if e != nil {
		return nil, e
	}
	if uint64(size) != s.Identity.Size {
		return nil, ErrIdentity
	}
	if (o.ClusterID != (ID{}) && o.ClusterID != s.Identity.ClusterID) || (o.NodeID != (ID{}) && o.NodeID != s.Identity.NodeID) || (o.VolumeID != (ID{}) && o.VolumeID != s.Identity.VolumeID) {
		return nil, ErrIdentity
	}
	v := newVolume(dev, s, o)
	if s.IndexCount > 0 {
		v.runs = []indexRun{{s.IndexLocation, s.IndexCount}}
	}
	off := s.ReplayFrom
	for off < s.JournalBytes {
		r, e := v.readRecord(s, off)
		if e != nil {
			return nil, e
		}
		b, e := encodeRecord(r)
		if e != nil {
			return nil, e
		}
		if e = v.replay(r, off+1); e != nil {
			return nil, e
		}
		off += uint64(len(b))
	}
	return v, nil
}
func (v *Volume) replay(r record, ptr uint64) error {
	switch r.Op {
	case opIndex:
		run := indexRun{r.Offset, r.Length}
		if run.Count == 0 || run.Location < v.sb.DataTop || run.Location > v.sb.bankSize() || run.pages() > (v.sb.bankSize()-run.Location)/PageSize {
			return ErrCorrupt
		}
		if r.Replace {
			v.runs = nil
		}
		v.runs = append(v.runs, run)
		clear(v.delta)
		if len(v.runs) > maxIndexRuns+1 {
			return ErrCorrupt
		}
	case opForget:
		v.delta[requestKey(r.Request)] = 0
	case opDedupe:
		if r.Request == (ID{}) {
			return ErrCorrupt
		}
		v.delta[requestKey(r.Request)] = ptr
	default:
		v.delta[objectKey(r.Key)] = ptr
		if r.Request != (ID{}) {
			v.delta[requestKey(r.Request)] = ptr
		}
	}
	if len(v.delta) > deltaLimit+4 {
		return ErrCorrupt
	}
	return nil
}
func (v *Volume) check(write bool) error {
	if v.closed {
		return ErrClosed
	}
	if v.failed != nil {
		return fmt.Errorf("%w: %v", ErrRecovery, v.failed)
	}
	if write && v.state == Draining {
		return ErrDraining
	}
	if v.state == Detached || v.state == Failed {
		return ErrClosed
	}
	return nil
}
func (v *Volume) write(b []byte, off uint64) error {
	if e := writeFullAt(v.dev, b, off); e != nil {
		v.failed = e
		if errors.Is(e, syscall.ENOSPC) || errors.Is(e, syscall.EDQUOT) || errors.Is(e, ErrFull) {
			return fmt.Errorf("%w: %w: %v", ErrRecovery, ErrFull, e)
		}
		return fmt.Errorf("%w: %v", ErrRecovery, e)
	}
	return nil
}
func (v *Volume) sync() error {
	if e := v.dev.Sync(); e != nil {
		v.failed = e
		if errors.Is(e, syscall.ENOSPC) || errors.Is(e, syscall.EDQUOT) || errors.Is(e, ErrFull) {
			return fmt.Errorf("%w: %w: %v", ErrRecovery, ErrFull, e)
		}
		return fmt.Errorf("%w: %v", ErrRecovery, e)
	}
	return nil
}
func (v *Volume) publish(s Superblock) error {
	if e := v.sync(); e != nil {
		return e
	}
	b, e := s.MarshalBinary()
	if e != nil {
		return e
	}
	slot := uint8(1 - v.sb.rootSlot)
	if e = v.write(b, uint64(slot)*PageSize); e != nil {
		return e
	}
	if e = v.sync(); e != nil {
		return e
	}
	s.rootSlot = slot
	v.sb = s
	return nil
}
func reserve(s *Superblock, n uint64) (uint64, error) {
	n = align(n)
	if n > s.DataTop || s.DataTop-n < s.metaPages()*2*PageSize {
		return 0, ErrFull
	}
	s.DataTop -= n
	return s.DataTop, nil
}

func (v *Volume) commitWithReserve(r record, s Superblock, useGuaranteed bool) error {
	s.Sequence++
	r.Sequence = s.Sequence
	ptr := s.JournalBytes + 1
	b, e := encodeRecord(r)
	if e != nil {
		return e
	}
	if e = v.appendJournal(&s, b); e != nil {
		if !errors.Is(e, ErrFull) {
			v.failed = e
		}
		return e
	}
	delta := make(map[indexKey]uint64, len(v.delta)+2)
	for k, p := range v.delta {
		delta[k] = p
	}
	runs := append([]indexRun(nil), v.runs...)
	switch r.Op {
	case opForget:
		delta[requestKey(r.Request)] = 0
	case opDedupe:
		delta[requestKey(r.Request)] = ptr
	default:
		delta[objectKey(r.Key)] = ptr
		if r.Request != (ID{}) {
			delta[requestKey(r.Request)] = ptr
		}
	}
	if len(delta) >= deltaLimit || s.Sequence%64 == 0 {
		run, e := v.writeIndex(&s, sortedDelta(delta))
		if e != nil {
			return e
		}
		rec := record{Op: opIndex, Offset: run.Location, Length: run.Count, Sequence: s.Sequence}
		bb, _ := encodeRecord(rec)
		if e = v.appendJournal(&s, bb); e != nil {
			if !errors.Is(e, ErrFull) {
				v.failed = e
			}
			return e
		}
		runs = append(runs, run)
		clear(delta)
	}
	old, e := v.loadObject(r.Key)
	if r.Op == opForget || r.Op == opDedupe {
		old = nil
		e = ErrNotFound
	}
	if e != nil && !errors.Is(e, ErrNotFound) {
		return e
	}
	oldLive := old != nil && !old.Info.Deleted
	if r.Op != opForget && r.Op != opDedupe {
		if oldLive {
			s.Objects--
			s.LogicalBytes -= old.Info.Length
		}
		if !r.Deleted {
			if ^uint64(0)-s.LogicalBytes < r.Length {
				return ErrRange
			}
			s.Objects++
			s.LogicalBytes += r.Length
		}
		next := &object{}
		if old != nil {
			*next = *old
			s.LiveData -= liveAllocation(old)
			s.ArenaUnused -= arenaUnused(old)
		}
		if e = applyObject(next, r); e != nil {
			return e
		}
		s.LiveData += liveAllocation(next)
		s.ArenaUnused += arenaUnused(next)
	}
	reserveAfter := v.guaranteed
	free := s.DataTop - s.metaPages()*2*PageSize
	if free < reserveAfter {
		if !useGuaranteed {
			return ErrFull
		}
		reserveAfter = free
	}
	if e = v.publish(s); e != nil {
		return e
	}
	v.guaranteed = reserveAfter
	v.delta = delta
	v.runs = runs
	v.cache.clear()
	return nil
}

func (v *Volume) Name() string       { v.mu.RLock(); defer v.mu.RUnlock(); return v.name }
func (v *Volume) Identity() Identity { v.mu.RLock(); defer v.mu.RUnlock(); return v.sb.Identity }
func (v *Volume) Header() Superblock { v.mu.RLock(); defer v.mu.RUnlock(); return v.sb }
func (v *Volume) State() State {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.failed != nil && !v.closed {
		return Failed
	}
	return v.state
}

func (v *Volume) SetState(s State) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	if s != Pending && s != Active && s != Draining {
		return ErrRange
	}
	v.state = s
	return nil
}

// Bind sets/checks the owning node and cluster IDs, without changing payloads.
func (v *Volume) Bind(cluster, node ID) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
	}
	if cluster == (ID{}) || node == (ID{}) {
		return ErrRange
	}
	s := v.sb
	if (s.Identity.NodeID != (ID{}) && s.Identity.NodeID != node) || (s.Identity.ClusterID != (ID{}) && s.Identity.ClusterID != cluster) {
		return ErrIdentity
	}
	s.Identity.NodeID = node
	s.Identity.ClusterID = cluster
	s.Generation++
	return v.publish(s)
}
func (v *Volume) Sync() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	return v.sync()
}
func (v *Volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	v.state = Detached
	e := v.dev.Close()
	if v.onClose != nil {
		v.onClose()
	}
	return e
}

// SetGuaranteedReserve protects unallocated physical bytes from ordinary writes.
// Cluster-level bucket guarantees must first be allocated to individual volumes.
func (v *Volume) SetGuaranteedReserve(n uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	free := v.sb.DataTop - v.sb.metaPages()*2*PageSize
	if n > free {
		return ErrFull
	}
	v.guaranteed = n
	return nil
}
func (v *Volume) Stats() Stats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	s := v.sb
	meta := uint64(superblockBytes) + s.metaPages()*2*PageSize + s.IndexBytes
	data := s.bankSize() - s.DataTop - s.IndexBytes
	free := s.DataTop - s.metaPages()*2*PageSize
	admit := uint64(0)
	if free > v.guaranteed {
		admit = free - v.guaranteed
	}
	activeIndex := uint64(0)
	for _, r := range v.runs {
		activeIndex += r.pages() * PageSize
	}
	reclaim := s.bankSize() - s.DataTop - s.LiveData - activeIndex
	return Stats{IO: v.ioStatsLocked(), Identity: s.Identity, ArenaUnusedPhysical: s.ArenaUnused, ReclaimablePhysical: reclaim, Generation: s.Generation, Objects: s.Objects, LogicalBytes: s.LogicalBytes, CapacityPhysical: s.Identity.Size, AllocatedPhysical: meta + data, MetadataPhysical: meta, DataPhysical: data, OperatingReservePhysical: s.Identity.Size - superblockBytes - s.bankSize(), GuaranteedRemainingPhysical: v.guaranteed, AdmissionFreePhysical: admit}
}
func (v *Volume) SetCacheLimits(entries int, bytes uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cache = newCache(entries, bytes)
}

func (v *Volume) copyToLocked(k Key, offset, length uint64, dst io.Writer) (uint64, error) {
	if e := v.check(false); e != nil {
		return 0, e
	}
	o, e := v.loadObject(k)
	if e != nil {
		return 0, e
	}
	if o.Info.Deleted {
		return 0, ErrNotFound
	}
	if offset > o.Info.Length {
		return 0, ErrRange
	}
	length = min(length, o.Info.Length-offset)
	buf := make([]byte, BlockSize)
	var n uint64
	for n < length {
		part := min(uint64(len(buf)), length-n)
		if e = v.readObject(o, buf[:part], offset+n); e != nil {
			return n, e
		}
		nw, err := dst.Write(buf[:part])
		n += uint64(nw)
		if err != nil {
			return n, err
		}
		if nw != int(part) {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

func alignedSize(n int64) (int64, error) {
	if n < MinVolumeSize || n > int64(^uint64(0)>>1)-PageSize {
		return 0, ErrRange
	}
	return int64(align(uint64(n))), nil
}
