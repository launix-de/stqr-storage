// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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
//     Handle.Close, except that pinned checkpoints admit readers during I/O.
//     Use defer Close; CopyTo does not release the handle. Never
//     reenter Volume methods while holding a handle. Neither type may be copied.
//   - Linux files/devices use read-only, demand-paged mmap; private RAM devices
//     borrow their existing sparse pages. No eager preload or complete object
//     directory is required. Metadata caches and decompression remain bounded.
//     Unsupported backends retain bounded positioned reads.
//   - Borrowed read slices are read-only and valid only inside WithReadView's
//     callback. The Handle and its private lock pin mapping/overlay lifetimes;
//     Grow, Rescan, Shrink and Close cannot unmap an active read handle. An
//     independently resized/truncated backing device violates exclusive ownership.
//   - Checkpoint I/O runs against a pinned, immutable accepted-write buffer
//     outside the metadata lock. Readers may continue; subsequent in-place
//     writers use a separate bounded overlay until the WAL space is reusable.
//   - A successful disk mutation has synced payload and redo WAL before updating affected pages. RAM has the same atomicity but no restart durability.
//   - After an ambiguous I/O/flush failure the instance is fenced until reopened.
//     A retry with the same nonzero RequestID recovers its original result.
//
// Cluster placement, authentication, quotas across volumes, and UI are outside
// this package. They may reserve physical capacity and manage the lifecycle.
type Volume struct {
	identity          atomic.Pointer[Identity]
	deferred          *deferredGroup
	fence             *writeFence
	staging           map[uint64][]byte
	durableGeneration uint64

	mu                sync.RWMutex
	preparing         bool
	checkpointing     bool
	prepareCond       *sync.Cond
	accountWarm       sync.Mutex
	accountManaged    atomic.Bool
	accountReady      atomic.Bool
	accountEpoch      uint64
	accountCachePath  string
	accountOwnerKey   [32]byte
	accountOwnerKnown bool
	accountMu         sync.Mutex
	accounting        map[ID]bucketAllocation
	owned             map[ID]bucketAllocation
	ownerStamp        any
	ownerFilter       func(Key, bool) bool
	admission         Admission
	observer          func(StorageSnapshot)
	overlay           *transaction
	dev               VolumeIO
	sb                Superblock
	cache             *objectCache
	nodes             *nodeCache
	failed            error
	closed            bool
	closing           bool
	maintenance       uint64
	name              string
	state             State
	guaranteed        uint64
	inFlightReserved  uint64
	onClose           func()
	limit             uint64
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
		if err = d.remap(n); err != nil {
			d.Close()
			return nil, err
		}
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
	s := Superblock{Identity: Identity{o.ClusterID, o.NodeID, o.VolumeID, o.Class, uint64(size)}, Generation: 1, ObjectRoot: treeStart, FreeRoot: treeStart + PageSize, FreeBytes: uint64(size) - dataStart, MetadataBytes: 6 * PageSize}
	b, e := s.MarshalBinary()
	if e != nil {
		return nil, e
	}
	if e = writeFullAt(dev, make([]byte, dataStart), 0); e != nil {
		return nil, e
	}
	for i := 0; i < 2; i++ {
		if e = writeFullAt(dev, b, uint64(i*PageSize)); e != nil {
			return nil, e
		}
	}
	if e = writeFullAt(dev, encodeNode(treeNode{kind: treeObjects}, o.VolumeID, 1), s.ObjectRoot); e != nil {
		return nil, e
	}
	if e = writeFullAt(dev, encodeNode(treeNode{kind: treeFree, entries: []treeEntry{{key: spaceKey(dataStart), value: s.FreeBytes}}}, o.VolumeID, 1), s.FreeRoot); e != nil {
		return nil, e
	}
	if e = dev.Sync(); e != nil {
		return nil, e
	}
	return newVolume(dev, s, OpenOptions{}), nil
}
func newVolume(dev VolumeIO, s Superblock, o OpenOptions) *Volume {
	if _, ok := dev.(*meteredIO); !ok {
		dev = &meteredIO{VolumeIO: dev}
	}
	v := &Volume{dev: dev, sb: s, durableGeneration: s.Generation, limit: s.Identity.Size, cache: newCache(o.CacheEntries, o.CacheBytes), nodes: newNodeCache(o.CacheEntries, o.CacheBytes), state: Active}
	v.publishIdentity()
	return v
}
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
	if size < 0 || uint64(size) < s.Identity.Size {
		return nil, ErrIdentity
	}
	s, e = recoverWAL(dev, s)
	if e != nil {
		return nil, e
	}
	// A torn newer root must have a committed redo anchor. Do not silently
	// roll back an acknowledged generation if both durable copies are damaged.
	for slot := 0; slot < 2; slot++ {
		raw := make([]byte, PageSize)
		if err := readFullAt(dev, raw, uint64(slot*PageSize)); err != nil {
			return nil, err
		}
		if string(raw[:8]) == "STQRVOL2" && checksum(raw[:PageSize-4]) != diskOrder.Uint32(raw[PageSize-4:]) && diskOrder.Uint64(raw[72:]) > s.Generation {
			return nil, ErrCorrupt
		}
	}

	if o.ClusterID != (ID{}) && o.ClusterID != s.Identity.ClusterID || o.NodeID != (ID{}) && o.NodeID != s.Identity.NodeID || o.VolumeID != (ID{}) && o.VolumeID != s.Identity.VolumeID {
		return nil, ErrIdentity
	}
	v := newVolume(dev, s, o)
	if _, e = v.node(s.ObjectRoot); e != nil {
		return nil, e
	}
	if _, e = v.node(s.FreeRoot); e != nil {
		return nil, e
	}
	if s.Moving {
		v.mu.Lock()
		e = v.resumeMoveLocked()
		v.mu.Unlock()
		if e != nil {
			return nil, e
		}
	}
	return v, nil
}
func (v *Volume) publish(s Superblock) error {
	if err := v.flushDeferred(); err != nil {
		return err
	}
	t := v.transaction()
	t.sb = s
	return t.commit(false)
}
func (v *Volume) check(write bool) error {
	if write && !v.AccountingReady() {
		return ErrAccountingNotReady
	}
	// A checkpoint pins the visible write buffer, not the metadata mutex.
	// Writers wait without excluding readers until the in-place WAL is safe
	// to reuse. No second object revision or alternate object ID is created.
	for write && v.checkpointing {
		v.prepareCond.Wait()
	}
	if write && v.preparing {
		return ErrBusy
	}
	if v.closed || v.closing {
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
	if v.staging != nil {
		return v.stageWrite(b, off)
	}
	// Allocation must never implicitly drop the metadata lock to flush a
	// newer visible overlay after its caller has built a mutable plan.
	if v.deferred != nil || v.checkpointing {
		return ErrBusy
	}
	return v.rawWrite(b, off)
}
func (v *Volume) rawWrite(b []byte, off uint64) error {
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
	if v.deferred != nil {
		// The checkpoint already confirms the final header. A fifth Sync
		// adds no durability and would hold the metadata lock again.
		return v.flushDeferred()
	}
	return v.syncVisible()
}
func (v *Volume) rawSync() error {
	if e := v.dev.Sync(); e != nil {
		v.failed = e
		if errors.Is(e, syscall.ENOSPC) || errors.Is(e, syscall.EDQUOT) || errors.Is(e, ErrFull) {
			return fmt.Errorf("%w: %w: %v", ErrRecovery, ErrFull, e)
		}
		return fmt.Errorf("%w: %v", ErrRecovery, e)
	}
	return nil
}
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
	return v.syncVisible()
}
func (v *Volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closing = true
	for v.preparing || v.checkpointing {
		v.prepareCond.Wait()
	}
	if v.closed {
		return nil
	}
	var flushErr error
	if v.failed == nil && v.deferred != nil {
		// New publications are rejected while the final accepted buffer is
		// checkpointed, so clean close cannot discard a later overlay.
		flushErr = v.checkpointIO((*Volume).flushDeferredIO)
	}
	if flushErr == nil && v.failed == nil {
		// Derived cache failure never changes an acknowledged storage result.
		_ = v.saveAccountingCacheLocked()
	}
	v.closed = true
	v.state = Detached
	e := errors.Join(flushErr, v.dev.Close())
	v.deferred, v.staging = nil, nil
	if v.onClose != nil {
		v.onClose()
	}
	return e
}

// SetGuaranteedReserve protects aggregate physical capacity for bucket guarantees.
// The counter is volatile and must be restored from committed allocation metadata
// before accepting writes after reopen. Mutations consume only their explicitly
// granted GuaranteedAllowance; the owner accounts that exact physical decrease.
func (v *Volume) SetGuaranteedReserve(n uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	if n > v.sb.FreeBytes || v.inFlightReserved > v.sb.FreeBytes-n {
		return ErrFull
	}
	v.guaranteed = n
	return nil
}

// SetInFlightReserve protects physical bytes for trusted in-progress imports.
// This counter is volatile: its owner must reconstruct it from its durable
// plans before exposing the volume after reopening. It is separate from bucket
// guarantees; an import may consume only its explicitly granted allowance.
func (v *Volume) SetInFlightReserve(n uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return err
	}
	if n > v.sb.FreeBytes || v.guaranteed > v.sb.FreeBytes-n {
		return ErrFull
	}
	v.inFlightReserved = n
	return nil
}
func (v *Volume) Stats() Stats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	s := v.sb
	admit := uint64(0)
	if s.FreeBytes > v.guaranteed && s.FreeBytes-v.guaranteed > v.inFlightReserved {
		admit = s.FreeBytes - v.guaranteed - v.inFlightReserved
	}
	return Stats{IO: v.ioStatsLocked(), Identity: s.Identity, Generation: s.Generation, Objects: s.Objects, LogicalBytes: s.LogicalBytes, CapacityPhysical: s.Identity.Size, AllocatedPhysical: s.MetadataBytes + s.PayloadBytes, MetadataPhysical: s.MetadataBytes, DataPhysical: s.PayloadBytes, ArenaUnusedPhysical: s.ArenaUnused, OperatingReservePhysical: scratchPages * PageSize, GuaranteedRemainingPhysical: v.guaranteed, InFlightReservedPhysical: v.inFlightReserved, AdmissionFreePhysical: admit}
}
func (v *Volume) SetCacheLimits(entries int, bytes uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cache = newCache(entries, bytes)
	v.nodes = newNodeCache(entries, bytes)
}

func alignedSize(n int64) (int64, error) {
	if n < MinVolumeSize || n > int64(^uint64(0)>>1)-PageSize {
		return 0, ErrRange
	}
	return int64(align(uint64(n))), nil
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
func (v *Volume) Name() string { v.mu.RLock(); defer v.mu.RUnlock(); return v.name }

// Identity is a committed immutable snapshot. Looking up a candidate volume
// must not wait behind unrelated payload I/O on that volume. Bind/grow/takeover
// publish a replacement only after their normal durable transaction succeeds.
func (v *Volume) Identity() Identity {
	if identity := v.identity.Load(); identity != nil {
		return *identity
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.sb.Identity // private transaction views need no published snapshot
}
func (v *Volume) publishIdentity() {
	if old := v.identity.Load(); old != nil && *old == v.sb.Identity {
		return
	}
	identity := v.sb.Identity
	v.identity.Store(&identity)
}
func (v *Volume) Header() Superblock { v.mu.RLock(); defer v.mu.RUnlock(); return v.sb }
func (v *Volume) State() State {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.failed != nil && !v.closed {
		return Failed
	}
	return v.state
}
