// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"sort"
	"sync"
)

// Accepted mutations live in a bounded page overlay. Neither payload nor metadata
// of the durable checkpoint is touched until the complete group has a synced redo
// image. The temporary WAL region is free in both the old and new checkpoints and
// is excluded from every allocation while the group is open.
const deferredPageLimit = (4 << 20) / PageSize
const deferredJournalBytes = 8 << 20

var errDeferredBudget = errors.New("accepted page budget exhausted")

type deferredGroup struct {
	journal      allocation
	pages        map[uint64][]byte
	pageLimit    int
	baseFreeRoot uint64
	baseNodes    *nodeCache
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
	limit := min(deferredPageLimit, int((size/PageSize*(PageSize-16)-PageSize-8)/(PageSize+8)))
	v.deferred = &deferredGroup{journal: allocation{off, size, false}, pageLimit: limit, pages: map[uint64][]byte{}, baseFreeRoot: v.sb.FreeRoot, baseNodes: newNodeCache(128, 1<<20)}
	return nil
}
func (v *Volume) stageWrite(b []byte, off uint64) error {
	for len(b) > 0 {
		page, at := off/PageSize*PageSize, off%PageSize
		p := v.staging[page]
		if p == nil {
			if len(v.staging) >= v.deferred.pageLimit {
				return errDeferredBudget
			}
			var err error
			p, err = v.readPage(page)
			if err != nil {
				return err
			}
			v.staging[page] = p
		}
		n := min(len(b), PageSize-int(at))
		copy(p[at:], b[:n])
		b = b[n:]
		off += uint64(n)
	}
	return nil
}
func (v *Volume) readLayer(dst []byte, off uint64) error {
	if off > v.limit || uint64(len(dst)) > v.limit-off {
		return ErrCorrupt
	}
	for len(dst) > 0 {
		page, at := off/PageSize*PageSize, off%PageSize
		n := min(len(dst), PageSize-int(at))
		p := v.staging[page]
		if p == nil && v.deferred != nil {
			p = v.deferred.pages[page]
		}
		if p == nil && v.fence != nil {
			var err error
			p, _, err = v.fence.page(page)
			if err != nil {
				return err
			}
		}
		if p != nil {
			copy(dst[:n], p[at:at+uint64(n)])
		} else {
			// Raw devices may reject sub-page reads. The overlay changes read
			// visibility, never the backend alignment contract.
			b := make([]byte, PageSize)
			if err := readFullAt(v.dev, b, page); err != nil {
				return err
			}
			copy(dst[:n], b[at:at+uint64(n)])
		}
		off += uint64(n)
		dst = dst[n:]
	}
	return nil
}
func (v *Volume) acceptedMutation(op uint8, k Key, off, length uint64, src io.Reader, expected []byte, opt Options) (Result, error) {
	// Oversized calls use the stronger durable path. Buffering the bounded source
	// permits an identical-ID durable retry if metadata growth exceeds the budget.
	if length > 1<<20 && op != opTrunc && op != opDelete {
		return v.durableMutation(op, k, off, length, src, expected, opt)
	}
	var payload []byte
	if op != opTrunc && op != opDelete {
		if length > 0 && src == nil {
			return Result{}, ErrRange
		}
		payload = make([]byte, int(length))
		if _, err := io.ReadFull(src, payload); err != nil {
			return Result{}, err
		}
		src = bytes.NewReader(payload)
	}
	if v.deferred != nil && len(v.deferred.pages) > v.deferred.pageLimit/2 {
		if err := v.flushDeferred(); err != nil {
			return Result{}, err
		}
	}
	if err := v.beginDeferred(); err != nil {
		if errors.Is(err, ErrFull) {
			// Exhausted buffering is backpressure, never permission to overwrite
			// another checkpoint's WAL or to replace its fence.
			return v.durableMutation(op, k, off, length, src, expected, opt)
		}
		return Result{}, err
	}
	v.staging = map[uint64][]byte{}
	result, err := v.mutateCore(op, k, off, length, src, expected, opt)
	v.staging = nil
	if errors.Is(err, errDeferredBudget) || errors.Is(err, ErrFull) {
		if src != nil {
			src = bytes.NewReader(payload)
		}
		return v.durableMutation(op, k, off, length, src, expected, opt)
	}
	return result, err
}

// A buffer-exhausted/oversized operation uses the existing durable path. Its
// allocator image must stay stable while preceding overlays are drained. Reads
// remain available at fences; new buffered writers receive bounded backpressure.
func (v *Volume) durableMutation(op uint8, k Key, off, length uint64, src io.Reader, expected []byte, opt Options) (Result, error) {
	v.beginMaintenance()
	defer v.endMaintenance()
	if err := v.flushDeferred(); err != nil {
		return Result{}, err
	}
	opt.Accepted = false
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
// flush or a full accepted-write buffer. It is a hint, not mutation authority;
// the actual operation must recheck placement/permissions after waiting.
func (v *Volume) PendingCheckpoint(accepted bool, length uint64) (uint64, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.checkpointing {
		if accepted && length <= 1<<20 && v.fence != nil &&
			(v.deferred == nil || len(v.deferred.pages) <= v.deferred.pageLimit/2) {
			return 0, false
		}
		return 0, true
	}
	return v.sb.Generation, v.deferred != nil &&
		(!accepted || length > 1<<20 || len(v.deferred.pages) > v.deferred.pageLimit/2)
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
	offsets := make([]uint64, 0, len(g.pages))
	for off := range g.pages {
		offsets = append(offsets, off)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	// Full pages free in the durable checkpoint can be staged directly. Only
	// pages sharing confirmed data need redo images. Consult the raw old tree,
	// never the overlay's newer allocator view, especially after delete/reuse.
	baseNode := func(off uint64) (treeNode, error) {
		if node, ok := g.baseNodes.get(off); ok {
			return node, nil
		}
		page := make([]byte, PageSize)
		if err := readFullAt(v.dev, page, off); err != nil {
			return treeNode{}, err
		}
		node, err := decodeNode(page, v.sb.Identity.VolumeID)
		if err == nil {
			g.baseNodes.put(off, node)
		}
		return node, err
	}
	redo := make([]uint64, 0, len(offsets))
	for _, off := range offsets {
		span, found, err := treeNeighbor(baseNode, g.baseFreeRoot, spaceKey(off), false)
		if err != nil {
			return err
		}
		start := keyOffset(span.key)
		if found && start <= off && off-start <= span.value && PageSize <= span.value-(off-start) {
			if err = v.rawWrite(g.pages[off][:min(uint64(PageSize), v.limit-off)], off); err != nil {
				return err
			}
		} else {
			redo = append(redo, off)
		}
	}
	payload := bytes.NewBuffer(make([]byte, 0, PageSize+8+len(redo)*(PageSize+8)))
	payload.Write(header)
	var n [8]byte
	diskOrder.PutUint64(n[:], uint64(len(redo)))
	payload.Write(n[:])
	for _, off := range redo {
		if off < treeStart {
			return ErrCorrupt
		}
		diskOrder.PutUint64(n[:], off)
		payload.Write(n[:])
		payload.Write(g.pages[off])
	}
	raw := payload.Bytes()
	count := (len(raw) + PageSize - 17) / (PageSize - 16)
	if uint64(count)*PageSize > g.journal.n {
		return ErrCorrupt
	}
	for i := 0; i < count; i++ {
		page := make([]byte, PageSize)
		if i+1 < count {
			diskOrder.PutUint64(page, g.journal.off+uint64(i+1)*PageSize)
		}
		from := i * (PageSize - 16)
		size := min(PageSize-16, len(raw)-from)
		diskOrder.PutUint32(page[8:], uint32(size))
		copy(page[16:], raw[from:from+size])
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
	diskOrder.PutUint64(anchor[24:], uint64(len(raw)))
	hash := sha256.Sum256(raw)
	copy(anchor[32:64], hash[:])
	copy(anchor[64:80], v.sb.Identity.VolumeID[:])
	diskOrder.PutUint32(anchor[PageSize-4:], checksum(anchor[:PageSize-4]))
	if err = v.rawWrite(anchor, uint64(2+int(v.sb.Generation%2))*PageSize); err != nil {
		return err
	}
	if err = v.rawSync(); err != nil {
		return err
	}
	for _, off := range redo {
		if err = v.rawWrite(g.pages[off][:min(uint64(PageSize), v.limit-off)], off); err != nil {
			return err
		}
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
