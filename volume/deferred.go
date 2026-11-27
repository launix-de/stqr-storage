// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"crypto/sha256"
	"errors"
	"io"
	"sync"
)

// Accepted mutations live in a bounded page overlay. Neither payload nor metadata
// of the durable checkpoint is touched until the complete group has a synced redo
// image. The temporary WAL region is free in both the old and new checkpoints and
// is excluded from every allocation while the group is open.
const deferredPageLimit = (4 << 20) / PageSize
const deferredJournalBytes = 8 * PageSize // start small; grow for protected redo only

type deferredGroup struct {
	journal        allocation
	pages          map[uint64][]byte
	pageLimit      int
	store          *dirtyStore
	protectedPages uint64
	baseFreeRoot   uint64
	baseNodes      *nodeCache
}

func (v *Volume) beginDeferred() error {
	if v.deferred != nil {
		return nil
	}
	t := v.transaction()
	defer t.close()
	size := uint64(deferredJournalBytes)
	var off uint64
	for {
		// Fixed scratch is excluded from client capacity. Prefer it so a small
		// journal cannot split the only free extent that fits a new payload.
		// An older checkpoint retains exclusive ownership until its I/O ends.
		scratch := allocation{4 * PageSize, scratchPages * PageSize, false}
		blocked := false
		if v.fence != nil {
			for _, r := range v.fence.reserved {
				blocked = blocked || overlap(scratch, r)
			}
		}
		if !blocked {
			off, size = scratch.off, scratch.n
			break
		}
		start, length, chosen, err := t.findSpace(size, PageSize, false)
		if err == nil {
			// Keep the WAL at the free extent's aligned end to preserve a
			// contiguous payload prefix. Recheck exclusions for an in-flight
			// checkpoint; small volumes can reduce this reservation below.
			if tail, ok := t.candidate(start, length, size, PageSize, (start+length-size)/PageSize*PageSize); ok {
				chosen = tail
			}
		}
		if err == nil {
			off = chosen
			break
		}
		if !errors.Is(err, ErrFull) || size <= 8*PageSize {
			return err
		}
		size = size / 2 / PageSize * PageSize
	}
	limit := deferredPageLimit
	store := newDirtyStore(deferredPageLimit, v.sb.Identity.Class == RAM || v.sb.Identity.Class == RAMCache)
	v.deferred = &deferredGroup{journal: allocation{off, size, false}, pageLimit: limit, pages: store.pages, store: store, baseFreeRoot: v.sb.FreeRoot, baseNodes: newNodeCache(128, 1<<20)}
	return nil
}
func (v *Volume) beginStaging() {
	v.stageStore = newDirtyStore(deferredPageLimit, v.sb.Identity.Class == RAM || v.sb.Identity.Class == RAMCache)
	v.staging = v.stageStore.pages
}
func (v *Volume) endStaging() {
	v.staging = nil
	if v.stageStore != nil {
		v.stageStore.close()
		v.stageStore = nil
	}
}
func (v *Volume) stageWrite(b []byte, off uint64) error {
	for len(b) > 0 {
		page, at := off/PageSize*PageSize, off%PageSize
		p, ok, err := v.stageStore.get(page)
		if err != nil {
			return err
		}
		if !ok {
			p, err = v.readPage(page)
			if err != nil {
				return err
			}
		}
		n := min(len(b), PageSize-int(at))
		copy(p[at:], b[:n])
		if err = v.stageStore.put(page, p, 1); err != nil {
			if err == ErrBusy {
				return ErrWritebackPressure
			}
			return err
		}
		off += uint64(n)
		b = b[n:]
	}
	return nil
}
func (v *Volume) readLayer(dst []byte, off uint64) error { return v.readVisibleBytes(dst, off) }

// acceptedBuild gives trusted metadata/import builders the same no-sync staging
// contract as PUT/CAS. No fallback may silently strengthen acceptance to Sync.
func (v *Volume) acceptedBuild(run func() error) error {
	if err := v.beginDeferred(); err != nil {
		if errors.Is(err, ErrFull) {
			return ErrWritebackPressure
		}
		return err
	}
	v.beginStaging()
	defer v.endStaging()
	return run()
}

func (v *Volume) acceptedMutation(op uint8, k Key, off, length uint64, src io.Reader, expected []byte, opt Options) (Result, error) {
	if length > 0 && op != opTrunc && op != opDelete && src == nil {
		return Result{}, ErrRange
	}
	if err := v.beginDeferred(); err != nil {
		if errors.Is(err, ErrFull) {
			return Result{}, ErrWritebackPressure
		}
		return Result{}, err
	}
	v.beginStaging()
	defer v.endStaging()
	return v.mutateCore(op, k, off, length, src, expected, opt)
}

// CheckpointThrough may also confirm a generation after clean close. A later
// failed operation cannot revoke a previously completed durable checkpoint.
// Generation zero waits only for an ongoing checkpoint/admission lock. It does
// not start another flush; callers must recheck mutation authority afterward.
func (v *Volume) CheckpointThrough(generation uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for generation == 0 && v.checkpointing {
		v.prepareCond.Wait()
	}
	if generation <= v.durableGeneration {
		return nil
	}
	if err := v.check(false); err != nil {
		return err
	}
	if generation > v.sb.Generation {
		return ErrPrecondition
	}
	return v.checkpointLockedThrough(generation)
}
func (v *Volume) FlushAccepted() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return err
	}
	return v.checkpointLockedThrough(v.sb.Generation)
}

// PendingCheckpoint lets a caller leave its own admission lock before a slow
// explicit fence. Accepted pressure never requests a checkpoint. It is a hint, not mutation authority;
// the actual operation must recheck placement/permissions after waiting.
func (v *Volume) PendingCheckpoint(accepted bool, length uint64) (uint64, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if accepted {
		return 0, v.checkpointing && v.fence == nil
	}
	if v.checkpointing {
		return 0, true
	}
	return v.sb.Generation, v.deferred != nil
}

// IsDurableThrough only inspects local flush progress. The counter belongs to
// the existing volume checkpoint; it is not a user-visible object version.
func (v *Volume) IsDurableThrough(generation uint64) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return generation <= v.durableGeneration
}

// TryIsDurableThrough reports only immediately available, confirmed local
// progress. Optional response hints may omit confirmation when metadata is busy;
// persistence barriers must retain IsDurableThrough/CheckpointThrough instead.
func (v *Volume) TryIsDurableThrough(generation uint64) bool {
	if !v.mu.TryRLock() {
		return false
	}
	defer v.mu.RUnlock()
	return generation <= v.durableGeneration
}

func (v *Volume) flushDeferred() (err error) {
	// All callers own the exclusive metadata lock. A second barrier waits for
	// the same physical checkpoint while releasing that lock to readers.
	for v.checkpointing {
		v.prepareCond.Wait()
	}
	if err = v.check(false); err != nil {
		return err
	}
	if v.deferred == nil {
		return nil
	}
	if v.preparing || v.staging != nil || v.overlay != nil {
		return ErrBusy
	}
	if v.prepareCond == nil {
		v.prepareCond = sync.NewCond(&v.mu)
	}
	return v.checkpointIO((*Volume).flushDeferredIO)
}

// syncVisible confirms already published storage without holding the metadata
// lock during the physical fence. Private payload staging is not visible yet;
// its publisher still owns its own WAL/durability ordering.
func (v *Volume) syncVisible() error {
	for v.checkpointing {
		v.prepareCond.Wait()
	}
	if err := v.check(false); err != nil {
		return err
	}
	if v.deferred != nil {
		return v.flushDeferred()
	}
	if v.preparing {
		if v.sb.Generation <= v.durableGeneration {
			return nil
		}
		return ErrBusy
	}
	if v.prepareCond == nil {
		v.prepareCond = sync.NewCond(&v.mu)
	}
	return v.checkpointIO((*Volume).rawSync)
}

func (v *Volume) checkpointIO(run func(*Volume) error) error {
	g := v.deferred
	ioView := &Volume{dev: v.dev, sb: v.sb, limit: v.limit, deferred: g,
		durableGeneration: v.durableGeneration}
	f := &writeFence{group: g}
	if g != nil {
		f.reserved = []allocation{g.journal}
	}
	// The checkpoint keeps g immutable. New writes use a separate bounded
	// overlay whose base is this checkpoint's published image.
	v.deferred = nil
	return v.runFence(f, ioView, func() error { return run(ioView) })
}

// Only the checkpoint owner uses this private view. WAL encoding, ordering and
// crash recovery are unchanged; metadata publication happens back under v.mu.
func (v *Volume) flushDeferredIO() error {
	g := v.deferred
	if g == nil {
		return nil
	}
	if v.staging != nil {
		return ErrBusy
	}
	if v.sb.Generation == v.durableGeneration {
		v.deferred = nil
		return nil
	}
	header, err := v.sb.MarshalBinary()
	if err != nil {
		return err
	}
	// The private redo spool is bounded in RAM and never synced itself. Only
	// explicit Sync reaches this function; all recovery authority stays on-volume.
	payload := &encodedSpool{noSpill: v.sb.Identity.Class == RAM || v.sb.Identity.Class == RAMCache}
	defer payload.Close()
	digest := sha256.New()
	writer := io.MultiWriter(payload, digest)
	if _, err = writer.Write(header); err != nil {
		return err
	}
	var raw [8]byte
	diskOrder.PutUint64(raw[:], g.protectedPages)
	if _, err = writer.Write(raw[:]); err != nil {
		return err
	}
	var protected uint64
	err = g.store.eachSorted(func(off uint64, flag byte, page []byte) error {
		if flag == 1 {
			return v.rawWrite(page[:min(uint64(PageSize), v.limit-off)], off)
		}
		if flag != 2 || off < treeStart {
			return ErrCorrupt
		}
		protected++
		diskOrder.PutUint64(raw[:], off)
		if _, err := writer.Write(raw[:]); err != nil {
			return err
		}
		_, err := writer.Write(page)
		return err
	})
	if err != nil {
		return err
	}
	if protected != g.protectedPages {
		return ErrCorrupt
	}
	count := (payload.size + PageSize - 17) / (PageSize - 16)
	if uint64(count)*PageSize > g.journal.n {
		return ErrCorrupt
	}
	for i := int64(0); i < count; i++ {
		page := make([]byte, PageSize)
		if i+1 < count {
			diskOrder.PutUint64(page, g.journal.off+uint64(i+1)*PageSize)
		}
		from := i * (PageSize - 16)
		size := min(int64(PageSize-16), payload.size-from)
		if _, err = payload.ReadAt(page[16:16+size], from); err != nil {
			return err
		}
		diskOrder.PutUint32(page[8:], uint32(size))
		diskOrder.PutUint32(page[12:], checksum(page[16:]))
		if err = v.rawWrite(page, g.journal.off+uint64(i)*PageSize); err != nil {
			return err
		}
	}
	if err = v.rawSync(); err != nil {
		return err
	}
	anchor := make([]byte, PageSize)
	copy(anchor, "STQRCMT2")
	diskOrder.PutUint64(anchor[8:], v.sb.Generation)
	diskOrder.PutUint64(anchor[16:], g.journal.off)
	diskOrder.PutUint64(anchor[24:], uint64(payload.size))
	copy(anchor[32:64], digest.Sum(nil))
	copy(anchor[64:80], v.sb.Identity.VolumeID[:])
	diskOrder.PutUint32(anchor[PageSize-4:], checksum(anchor[:PageSize-4]))
	if err = v.rawWrite(anchor, uint64(2+int(v.sb.Generation%2))*PageSize); err != nil {
		return err
	}
	if err = v.rawSync(); err != nil {
		return err
	}
	if err = g.store.eachSorted(func(off uint64, flag byte, page []byte) error {
		if flag != 2 {
			return nil
		}
		return v.rawWrite(page[:min(uint64(PageSize), v.limit-off)], off)
	}); err != nil {
		return err
	}
	if err = v.rawSync(); err != nil {
		return err
	}
	if err = v.rawWrite(header, v.sb.Generation%2*PageSize); err != nil {
		return err
	}
	if err = v.rawSync(); err != nil {
		return err
	}
	v.durableGeneration = v.sb.Generation
	v.deferred = nil
	return nil
}
