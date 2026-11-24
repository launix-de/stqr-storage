// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"sort"
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
	baseFreeRoot uint64
	baseNodes    *nodeCache
}

func (v *Volume) beginDeferred() error {
	if v.deferred != nil {
		return nil
	}
	t := v.transaction()
	defer t.close()
	_, _, off, err := t.findSpace(deferredJournalBytes, PageSize, false)
	if err != nil {
		return err
	}
	v.deferred = &deferredGroup{journal: allocation{off, deferredJournalBytes, false}, pages: map[uint64][]byte{}, baseFreeRoot: v.sb.FreeRoot, baseNodes: newNodeCache(128, 1<<20)}
	return nil
}
func (v *Volume) stageWrite(b []byte, off uint64) error {
	for len(b) > 0 {
		page, at := off/PageSize*PageSize, off%PageSize
		p := v.staging[page]
		if p == nil {
			if len(v.staging) >= deferredPageLimit {
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
		if p != nil {
			copy(dst[:n], p[at:at+uint64(n)])
		} else if err := readFullAt(v.dev, dst[:n], off); err != nil {
			return err
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
		if err := v.flushDeferred(); err != nil {
			return Result{}, err
		}
		return v.mutateCore(op, k, off, length, src, expected, opt)
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
	if v.deferred != nil && len(v.deferred.pages) > deferredPageLimit/2 {
		if err := v.flushDeferred(); err != nil {
			return Result{}, err
		}
	}
	if err := v.beginDeferred(); err != nil {
		if errors.Is(err, ErrFull) {
			return v.mutateCore(op, k, off, length, src, expected, opt)
		}
		return Result{}, err
	}
	v.staging = map[uint64][]byte{}
	result, err := v.mutateCore(op, k, off, length, src, expected, opt)
	v.staging = nil
	if errors.Is(err, errDeferredBudget) || errors.Is(err, ErrFull) {
		if err = v.flushDeferred(); err != nil {
			return Result{}, err
		}
		if src != nil {
			src = bytes.NewReader(payload)
		}
		return v.mutateCore(op, k, off, length, src, expected, opt)
	}
	return result, err
}

// CheckpointThrough may also confirm a generation after clean close. A later
// failed operation cannot revoke a previously completed durable checkpoint.
func (v *Volume) CheckpointThrough(generation uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if generation <= v.durableGeneration {
		return nil
	}
	if err := v.check(false); err != nil {
		return err
	}
	if generation > v.sb.Generation {
		return ErrPrecondition
	}
	return v.flushDeferred()
}
func (v *Volume) FlushAccepted() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return err
	}
	return v.flushDeferred()
}
func (v *Volume) flushDeferred() error {
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
