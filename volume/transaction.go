// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"slices"
	"sort"
)

type allocation struct {
	off, n uint64
	meta   bool
}
type transaction struct {
	v                                *Volume
	changed                          map[indexKey]bool
	replaceAccounting                bool
	replacementAll, replacementOwned map[ID]bucketAllocation
	sb                               Superblock
	pages                            map[uint64][]byte
	spilled                          map[uint64]int64
	spool                            *encodedSpool
	acceptedPages                    *dirtyStore
	busy, pending, retired           []allocation
	reusable                         allocation
	// Split free-tree siblings remain searchable until their parent is published.
	freeSiblings []uint64
	// A claim removes its old extent before reinserting its free remainders.
	// Those bytes must remain available for free-tree metadata allocation.
	freeClaims []allocation
	inClaim    bool
}

func (v *Volume) transaction() *transaction {
	return &transaction{v: v, sb: v.sb, pages: map[uint64][]byte{}}
}

// Large in-place writes retain dirty page images in a bounded temporary spool,
// rather than keeping a second object-sized payload in the Go heap.
func (t *transaction) close() {
	if t.acceptedPages != nil {
		t.acceptedPages.close()
		t.acceptedPages = nil
	}
	if t.spool != nil {
		t.spool.Close()
		t.spool = nil
	}
}
func (t *transaction) page(off uint64) ([]byte, error) {
	if t.acceptedPages != nil {
		if p, ok, err := t.acceptedPages.get(off); err != nil || ok {
			if hot := t.pages[off]; hot != nil {
				return hot, nil
			}
			return p, err
		}
	}
	if b := t.pages[off]; b != nil {
		return b, nil
	}
	if at, ok := t.spilled[off]; ok {
		b := make([]byte, PageSize)
		_, e := t.spool.ReadAt(b, at)
		return b, e
	}
	return t.v.readPage(off)
}
func (t *transaction) spillPages() error {
	if len(t.pages) <= 1024 {
		return nil
	}
	if t.v.staging != nil {
		if t.acceptedPages == nil {
			t.acceptedPages = newDirtyStore(deferredPageLimit, t.sb.Identity.Class == RAM || t.sb.Identity.Class == RAMCache)
		}
		for off, page := range t.pages {
			if err := t.acceptedPages.put(off, page, 1); err != nil {
				if errors.Is(err, ErrBusy) {
					return ErrWritebackPressure
				}
				return err
			}
			delete(t.pages, off)
		}
		return nil
	}
	if t.spool == nil {
		t.spool = &encodedSpool{noSpill: t.sb.Identity.Class == RAM || t.sb.Identity.Class == RAMCache}
		t.spilled = map[uint64]int64{}
	}
	for off, b := range t.pages {
		at, ok := t.spilled[off]
		if ok {
			if _, e := t.spool.WriteAt(b, at); e != nil {
				return e
			}
		} else {
			at = t.spool.size
			if _, e := t.spool.Write(b); e != nil {
				return e
			}
			t.spilled[off] = at
		}
		delete(t.pages, off)
	}
	return nil
}
func overlap(a, b allocation) bool { return a.off < b.off+b.n && b.off < a.off+a.n }
func (t *transaction) candidate(start, length, n, alignment, minimum uint64) (uint64, bool) {
	end := start + length
	off := (max(start, minimum) + alignment - 1) / alignment * alignment
	for off <= end && n <= end-off {
		blocked := false
		if t.v.deferred != nil && overlap(allocation{off, n, false}, t.v.deferred.journal) {
			off = (t.v.deferred.journal.off + t.v.deferred.journal.n + alignment - 1) / alignment * alignment
			continue
		}
		var fenced []allocation
		if t.v.fence != nil {
			fenced = t.v.fence.reserved
		}
		for _, list := range [][]allocation{fenced, t.busy, t.retired} {
			for _, x := range list {
				if t.reusable.n > 0 && x.off >= t.reusable.off && x.off+x.n <= t.reusable.off+t.reusable.n {
					continue
				}
				if overlap(allocation{off, n, false}, x) {
					off = (x.off + x.n + alignment - 1) / alignment * alignment
					blocked = true
					break
				}
			}
			if blocked {
				break
			}
		}
		if !blocked {
			return off, true
		}
	}
	return 0, false
}

var stopTree = errors.New("tree scan complete")

func (t *transaction) findSpace(n, a uint64, tail bool, lower ...uint64) (uint64, uint64, uint64, error) {
	minimum := uint64(0)
	if len(lower) != 0 {
		minimum = lower[0]
	}
	var start, length, chosen uint64
	var visit func(transactionTreeView, int) error
	visit = func(node transactionTreeView, depth int) error {
		if depth > 32 || node.kind() != treeFree {
			return ErrCorrupt
		}
		for i := 0; i < node.count(); i++ {
			x := node.entry(i)
			if node.level() > 0 {
				maxlen := x.maximum
				if a == PageSize {
					maxlen = x.aligned
				}
				if maxlen < n {
					continue
				}
				child, err := t.readTreeView(x.value)
				if err != nil {
					return err
				}
				if !treeViewChildValid(node, child) {
					return ErrCorrupt
				}
				if err = visit(child, depth+1); err != nil {
					return err
				}
				continue
			}
			s := keyOffset(x.key)
			if tail && s%PageSize == 0 && x.value >= PageSize {
				continue
			}
			if c, ok := t.candidate(s, x.value, n, a, minimum); ok {
				start, length, chosen = s, x.value, c
				return stopTree
			}
		}
		return nil
	}
	root, e := t.readTreeView(t.sb.FreeRoot)
	if e == nil {
		e = visit(root, 0)
	}
	if e == nil {
		for _, off := range t.freeSiblings {
			sibling, err := t.readTreeView(off)
			if err != nil {
				e = err
				break
			}
			if e = visit(sibling, 0); e != nil {
				break
			}
		}
	}
	if e == stopTree {
		return start, length, chosen, nil
	}
	if e != nil {
		return 0, 0, 0, e
	}
	for _, x := range t.freeClaims {
		if tail && x.off%PageSize == 0 && x.n >= PageSize {
			continue
		}
		if off, ok := t.candidate(x.off, x.n, n, a, minimum); ok {
			return x.off, x.n, off, nil
		}
	}
	return 0, 0, 0, ErrFull
}
func (t *transaction) metaPage() (uint64, error) {
	_, _, off, e := t.findSpace(PageSize, PageSize, false)
	if e != nil {
		return 0, e
	}
	x := allocation{off, PageSize, true}
	t.busy = append(t.busy, x)
	t.pending = append(t.pending, x)
	t.sb.FreeBytes -= PageSize
	t.sb.MetadataBytes += PageSize
	return off, nil
}
func (t *transaction) claim(off, n uint64) error {
	t.inClaim = true
	defer func() { t.inClaim = false }()
	x, found, e := t.neighbor(t.sb.FreeRoot, spaceKey(off), false)
	if e != nil {
		return e
	}
	s, l := keyOffset(x.key), x.value
	if !found || s > off || off-s > l || n > l-(off-s) {
		return ErrCorrupt
	}
	if e = t.remove(&t.sb.FreeRoot, x.key); e != nil {
		return e
	}
	// Publish the larger remainder first: inserting the smaller fragment may
	// split the free tree and need a metadata page from the larger remainder.
	left, right := allocation{s, off - s, false}, allocation{off + n, s + l - off - n, false}
	if right.n > left.n {
		left, right = right, left
	}
	count := len(t.freeClaims)
	t.freeClaims = append(t.freeClaims, left, right)
	defer func() { t.freeClaims = t.freeClaims[:count] }()
	for _, remainder := range []allocation{left, right} {
		if remainder.n > 0 {
			if e = t.set(&t.sb.FreeRoot, spaceKey(remainder.off), remainder.n); e != nil {
				return e
			}
		}
	}
	return nil
}
func (t *transaction) alloc(n uint64, meta bool) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	a := uint64(1)
	if n >= PageSize {
		a = PageSize
	}
	// Retired initial index pages remain available for metadata, but object
	// descriptors require payloads at or above dataStart.
	minimum := uint64(0)
	if !meta {
		minimum = dataStart
	}
	s, l, off, e := t.findSpace(n, a, n < PageSize, minimum)
	_ = s
	_ = l
	if e == ErrFull && n < PageSize {
		_, _, off, e = t.findSpace(n, a, false, minimum)
	}
	if e != nil {
		return 0, e
	}
	t.busy = append(t.busy, allocation{off, n, meta})
	if e = t.claim(off, n); e != nil {
		return 0, e
	}
	t.sb.FreeBytes -= n
	if meta {
		t.sb.MetadataBytes += n
	} else {
		t.sb.PayloadBytes += n
	}
	return off, nil
}
func (t *transaction) release(off, n uint64, meta bool) {
	if n == 0 {
		return
	}
	t.retired = append(t.retired, allocation{off, n, meta})
	t.sb.FreeBytes += n
	if meta {
		t.sb.MetadataBytes -= n
	} else {
		t.sb.PayloadBytes -= n
	}
}
func (t *transaction) freeSpan(off, n uint64) error {
	previous, hasPrevious, e := t.neighbor(t.sb.FreeRoot, spaceKey(off), false)
	if e != nil {
		return e
	}
	next, hasNext, e := t.neighbor(t.sb.FreeRoot, spaceKey(off), true)
	if e != nil {
		return e
	}
	p, q := keyOffset(previous.key), keyOffset(next.key)
	if hasPrevious && p+previous.value > off || hasNext && off+n > q {
		return ErrCorrupt
	}
	mergePrevious := hasPrevious && p+previous.value == off
	mergeNext := hasNext && off+n == q
	if mergePrevious {
		length := previous.value + n
		if mergeNext {
			length += next.value
		}
		if e = t.set(&t.sb.FreeRoot, previous.key, length); e != nil {
			return e
		}
		if mergeNext {
			return t.remove(&t.sb.FreeRoot, next.key)
		}
		return nil
	}
	if mergeNext {
		_, e = t.replaceKeyAt(t.sb.FreeRoot, next.key, spaceKey(off), n+next.value)
		return e
	}
	return t.set(&t.sb.FreeRoot, spaceKey(off), n)
}
func (t *transaction) finalize() error {
	p, r := 0, 0
	for p < len(t.pending) || r < len(t.retired) {
		for p < len(t.pending) {
			x := t.pending[p]
			p++
			if e := t.claim(x.off, x.n); e != nil {
				return e
			}
		}
		// Coalescing can allocate and retire metadata pages. Claim any newly
		// allocated pages before processing the next retired extent.
		if r < len(t.retired) {
			x := t.retired[r]
			r++
			if e := t.freeSpan(x.off, x.n); e != nil {
				return e
			}
		}
	}
	return nil
}
func (v *Volume) readPage(off uint64) ([]byte, error) {
	if v.overlay != nil {
		return v.overlay.page(off)
	}
	if off%PageSize != 0 || off >= v.limit {
		return nil, ErrCorrupt
	}
	b := make([]byte, PageSize)
	var e error
	if v.deferred != nil || v.staging != nil || v.fence != nil {
		e = v.readLayer(b[:min(uint64(PageSize), v.limit-off)], off)
	} else {
		e = readFullAt(v.dev, b[:min(uint64(PageSize), v.limit-off)], off)
	}
	if e != nil {
		return nil, e
	}
	return b, nil
}
func (v *Volume) readBytes(dst []byte, off uint64) error {
	for len(dst) > 0 {
		n, at := len(dst), off
		if v.sb.Moving {
			from, end := v.sb.MoveFrom, v.sb.MoveFrom+v.sb.MoveDone
			if off >= from && off < end {
				n = min(n, int(end-off))
				at = v.sb.MoveTo + off - from
			} else if off < from {
				n = min(n, int(from-off))
			}
		}
		if err := v.readVisibleBytes(dst[:n], at); err != nil {
			return err
		}
		off += uint64(n)
		dst = dst[n:]
	}
	return nil
}

func (v *Volume) readVisibleBytes(dst []byte, off uint64) error {
	for len(dst) > 0 {
		data, n, _, err := v.visibleRegion(off, len(dst))
		if err != nil {
			return err
		}
		if n <= 0 {
			return ErrCorrupt
		}
		if data == nil {
			clear(dst[:n])
		} else {
			copy(dst[:n], data)
		}
		off += uint64(n)
		dst = dst[n:]
	}
	return nil
}

func (t *transaction) writeBytes(off uint64, b []byte) error {
	for len(b) > 0 {
		at := off % PageSize
		if at == 0 && len(b) >= PageSize {
			n := len(b) / PageSize * PageSize
			if e := t.v.write(b[:n], off); e != nil {
				return e
			}
			off += uint64(n)
			b = b[n:]
			continue
		}
		page := off - at
		p := t.pages[page]
		if p == nil {
			var e error
			p, e = t.page(page)
			if e != nil {
				return e
			}
			t.pages[page] = p
		}
		n := min(len(b), PageSize-int(at))
		copy(p[at:], b[:n])
		off += uint64(n)
		b = b[n:]
	}
	return t.spillPages()
}
func (t *transaction) patch(off uint64, b []byte) error {
	for len(b) > 0 {
		page := off / PageSize * PageSize
		p := t.pages[page]
		if p == nil {
			var e error
			p, e = t.page(page)
			if e != nil {
				return e
			}
			t.pages[page] = p
		}
		at := int(off - page)
		n := min(len(b), PageSize-at)
		copy(p[at:], b[:n])
		off += uint64(n)
		b = b[n:]
	}
	return t.spillPages()
}
func (t *transaction) commit(useGuaranteed bool) error {
	return t.commitReserved(useGuaranteed, 0, 0)
}
func (t *transaction) commitReserved(useGuaranteed bool, allowance, guaranteedAllowance uint64) error {
	defer t.close()
	if t.v.staging == nil && (t.v.deferred != nil || t.v.checkpointing) {
		return ErrBusy
	}
	if e := t.finalize(); e != nil {
		return e
	}
	accounts, err := t.projectAccounting()
	if err != nil {
		return err
	}
	owned, err := t.projectAccounts(t.v.owned, t.v.ownerFilter)
	if err != nil {
		return err
	}
	if t.replaceAccounting {
		accounts = t.replacementAll
		owned = t.replacementOwned
	}
	dynamicReserve := uint64(0)
	var afterUsage StorageSnapshot
	if t.v.admission != nil || accounts != nil && t.v.observer != nil {
		afterUsage = t.v.storageSnapshot(accounts, owned, t.sb)
	}
	if t.v.admission != nil {
		beforeUsage := t.v.storageSnapshot(t.v.accounting, t.v.owned, t.v.sb)
		admissionAfter := afterUsage
		if accounts != nil && t.v.observer != nil {
			// A callback may alter its temporary slices. Keep the later observer's
			// publication isolated without repeating the accounting calculation.
			admissionAfter.All = slices.Clone(afterUsage.All)
			admissionAfter.Owned = slices.Clone(afterUsage.Owned)
		}
		dynamicReserve, err = t.v.admission(StorageChange{Before: beforeUsage.All, After: admissionAfter.All, BeforeOwned: beforeUsage.Owned, AfterOwned: admissionAfter.Owned, Temporary: allowance != 0})
		if err != nil {
			return err
		}
	}
	allowance = min(allowance, t.v.inFlightReserved)
	guaranteedAllowance = min(guaranteedAllowance, t.v.guaranteed)
	if useGuaranteed {
		guaranteedAllowance = t.v.guaranteed
	}
	protected := t.v.inFlightReserved - allowance
	if t.v.guaranteed-guaranteedAllowance > ^uint64(0)-protected {
		return ErrFull
	}
	protected += t.v.guaranteed - guaranteedAllowance
	if t.v.admission != nil {
		protected = t.v.inFlightReserved - allowance
		if dynamicReserve > ^uint64(0)-protected {
			return ErrFull
		}
		protected += dynamicReserve
	}
	if t.sb.FreeBytes < protected {
		return ErrFull
	}
	consumed, guaranteedConsumed := uint64(0), uint64(0)
	if t.sb.FreeBytes < t.v.sb.FreeBytes {
		growth := t.v.sb.FreeBytes - t.sb.FreeBytes
		consumed = min(allowance, growth)
		guaranteedConsumed = min(guaranteedAllowance, growth-consumed)
	}
	t.sb.Generation++
	t.sb.Sequence++
	header, e := t.sb.MarshalBinary()
	if e != nil {
		return e
	}
	if t.v.staging != nil {
		if t.acceptedPages != nil {
			if err := t.acceptedPages.each(func(off uint64, _ byte, p []byte) error { return t.v.stageWrite(p, off) }); err != nil {
				return err
			}
		}
		for off := range t.pages {
			if err := t.v.stageWrite(t.pages[off], off); err != nil {
				return err
			}
		}
		for off := range t.spilled {
			page, err := t.page(off)
			if err != nil {
				return err
			}
			if err = t.v.stageWrite(page, off); err != nil {
				return err
			}
		}

		if err := t.v.deferred.publishStaging(t); err != nil {
			return err
		}
		t.v.sb = t.sb
		t.v.publishIdentity()
		if accounts != nil {
			t.v.accounting = accounts
			t.v.owned = owned
			if t.v.observer != nil {
				t.v.observer(afterUsage)
			}
		}
		t.v.inFlightReserved -= consumed
		t.v.guaranteed -= guaranteedConsumed
		if t.v.admission != nil {
			t.v.guaranteed = dynamicReserve
		}
		// A bounded cache is cheaper and safer to invalidate than walking an
		// unbounded spill to update every decoded metadata entry.
		t.v.nodes.clear()

		return nil
	}
	all := make(map[uint64]bool, len(t.pages)+len(t.spilled))
	for off := range t.pages {
		all[off] = true
	}
	for off := range t.spilled {
		all[off] = true
	}
	offsets := make([]uint64, 0, len(all))
	for off := range all {
		page, err := t.page(off)
		if err != nil {
			return err
		}
		// An entirely free old page contains no confirmed neighbor. Stage it
		// directly; only updates to occupied/shared pages need redo images.
		span, ok, err := treeNeighbor(t.v.node, t.v.sb.FreeRoot, spaceKey(off), false)
		if err != nil {
			return err
		}
		start := keyOffset(span.key)
		if ok && start <= off && off-start <= span.value && PageSize <= span.value-(off-start) {
			if err = t.v.write(page, off); err != nil {
				return err
			}
			continue
		}
		offsets = append(offsets, off)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	payload := &encodedSpool{noSpill: t.sb.Identity.Class == RAM || t.sb.Identity.Class == RAMCache}
	defer payload.Close()
	digest := sha256.New()
	writer := io.MultiWriter(payload, digest)
	if _, e = writer.Write(header); e != nil {
		return e
	}
	var n [8]byte
	diskOrder.PutUint64(n[:], uint64(len(offsets)))
	if _, e = writer.Write(n[:]); e != nil {
		return e
	}
	for _, off := range offsets {
		diskOrder.PutUint64(n[:], off)
		if _, e = writer.Write(n[:]); e != nil {
			return e
		}
		page, err := t.page(off)
		if err != nil {
			return err
		}
		if _, e = writer.Write(page); e != nil {
			return e
		}
	}
	count := (payload.size + PageSize - 17) / (PageSize - 16)
	extra := uint64(max(int64(0), count-scratchPages)) * PageSize
	if extra > t.sb.FreeBytes-protected {
		return ErrFull
	}
	logPages := make([]uint64, count)
	for i := 0; i < len(logPages); {
		if i < scratchPages {
			logPages[i] = uint64(4*PageSize + i*PageSize)
			i++
			continue
		}
		// Claim temporary WAL space in runs, avoiding a growing busy-list lookup for
		// every page of a large range write. Fragmented volumes fall back to smaller runs.
		run := len(logPages) - i
		var off uint64
		for {
			_, _, chosen, err := t.findSpace(uint64(run)*PageSize, PageSize, false)
			if err == nil {
				off = chosen
				break
			}
			if err != ErrFull || run == 1 {
				return err
			}
			run = (run + 1) / 2
		}
		t.busy = append(t.busy, allocation{off, uint64(run) * PageSize, false})
		for j := 0; j < run; j++ {
			logPages[i+j] = off + uint64(j)*PageSize
		}
		i += run
	}
	live := t.v
	ioView := &Volume{dev: live.dev, sb: live.sb, limit: live.limit, durableGeneration: live.durableGeneration}
	fence := &writeFence{image: t}
	for _, off := range logPages {
		fence.reserved = append(fence.reserved, allocation{off, PageSize, false})
	}
	fence.reserved = mergeAllocations(fence.reserved)
	t.v.sb = t.sb
	t.v.publishIdentity()
	if accounts != nil {
		t.v.accounting = accounts
		t.v.owned = owned
		if t.v.observer != nil {
			t.v.observer(afterUsage)
		}
	}
	t.v.inFlightReserved -= consumed
	t.v.guaranteed -= guaranteedConsumed
	if t.v.admission != nil {
		t.v.guaranteed = dynamicReserve
	}
	for off := range all {
		b, err := t.page(off)
		if err != nil {
			live.invalidateKeyMembership()
			live.failed = err
			return err
		}
		if string(b[:4]) == "BPT2" {
			n, err := decodeNode(b, t.sb.Identity.VolumeID)
			if err == nil {
				t.v.nodes.put(off, n)
			}
		}
	}
	// Invalidate object descriptors before unlocking: the new roots/pages are
	// visible now, even though this request's durability ACK is still pending.
	for key := range t.changed {
		if key[0] == 0 {
			live.cache.remove(key.object())
		}
	}
	t.v = ioView
	defer func() { t.v = live }()
	return live.runFence(fence, ioView, func() error {
		for i, off := range logPages {
			p := make([]byte, PageSize)
			if i+1 < len(logPages) {
				diskOrder.PutUint64(p, logPages[i+1])
			}
			from := int64(i) * (PageSize - 16)
			size := min(int64(PageSize-16), payload.size-from)
			diskOrder.PutUint32(p[8:], uint32(size))
			if _, e = payload.ReadAt(p[16:16+size], from); e != nil {
				return e
			}
			diskOrder.PutUint32(p[12:], checksum(p[16:]))
			if e = t.v.write(p, off); e != nil {
				return e
			}
		}
		if e = t.v.rawSync(); e != nil {
			return e
		}
		anchor := make([]byte, PageSize)
		copy(anchor, "STQRCMT2")
		diskOrder.PutUint64(anchor[8:], t.sb.Generation)
		diskOrder.PutUint64(anchor[16:], logPages[0])
		diskOrder.PutUint64(anchor[24:], uint64(payload.size))
		copy(anchor[32:64], digest.Sum(nil))
		copy(anchor[64:80], t.sb.Identity.VolumeID[:])
		diskOrder.PutUint32(anchor[PageSize-4:], checksum(anchor[:PageSize-4]))
		if e = t.v.write(anchor, uint64(2+int(t.sb.Generation%2))*PageSize); e != nil {
			return e
		}
		if e = t.v.rawSync(); e != nil {
			return e
		}
		for _, off := range offsets {
			page, err := t.page(off)
			if err != nil {
				return err
			}
			if e = t.v.write(page[:min(uint64(PageSize), t.sb.Identity.Size-off)], off); e != nil {
				return e
			}
		}
		if e = t.v.rawSync(); e != nil {
			return e
		}
		if e = t.v.write(header, t.sb.Generation%2*PageSize); e != nil {
			return e
		}
		if e = t.v.rawSync(); e != nil {
			return e
		}
		ioView.durableGeneration = t.sb.Generation
		return nil
	})
}
func recoverWAL(dev VolumeIO, s Superblock) (Superblock, error) {
	var anchor []byte
	var gen uint64
	for slot := 2; slot < 4; slot++ {
		b := make([]byte, PageSize)
		if e := readFullAt(dev, b, uint64(slot*PageSize)); e != nil {
			return s, e
		}
		g := diskOrder.Uint64(b[8:])
		if string(b[:8]) == "STQRCMT2" && g > s.Generation && g > gen && bytes.Equal(b[64:80], s.Identity.VolumeID[:]) && checksum(b[:PageSize-4]) == diskOrder.Uint32(b[PageSize-4:]) {
			anchor = b
			gen = g
		}
	}
	if anchor == nil {
		return s, nil
	}
	size, e := dev.Size()
	if e != nil {
		return s, e
	}
	length := diskOrder.Uint64(anchor[24:])
	if length > uint64(size)*2 || length < PageSize+8 {
		return s, ErrCorrupt
	}
	payload := &encodedSpool{noSpill: s.Identity.Class == RAM}
	defer payload.Close()
	digest := sha256.New()
	off := diskOrder.Uint64(anchor[16:])
	seen := map[uint64]bool{}
	for uint64(payload.size) < length {
		if off < PageSize*4 || off%PageSize != 0 || off+PageSize > uint64(size) || seen[off] {
			return s, ErrCorrupt
		}
		seen[off] = true
		p := make([]byte, PageSize)
		if e = readFullAt(dev, p, off); e != nil {
			return s, e
		}
		used := diskOrder.Uint32(p[8:])
		if used > PageSize-16 || used == 0 || uint64(used) > length-uint64(payload.size) || checksum(p[16:]) != diskOrder.Uint32(p[12:]) {
			return s, ErrCorrupt
		}
		if _, e = io.MultiWriter(payload, digest).Write(p[16 : 16+used]); e != nil {
			return s, e
		}
		off = diskOrder.Uint64(p)
	}
	if !bytes.Equal(digest.Sum(nil), anchor[32:64]) {
		return s, ErrCorrupt
	}
	header := make([]byte, PageSize)
	if _, e = payload.ReadAt(header, 0); e != nil {
		return s, e
	}
	next, e := DecodeSuperblock(header)
	if e != nil || next.Generation != gen || next.Identity.VolumeID != s.Identity.VolumeID || next.Identity.Size > uint64(size) {
		return s, ErrCorrupt
	}
	var n [8]byte
	if _, e = payload.ReadAt(n[:], PageSize); e != nil {
		return s, e
	}
	count := diskOrder.Uint64(n[:])
	if count > (uint64(payload.size)-PageSize-8)/(PageSize+8) || uint64(payload.size) != PageSize+8+count*(PageSize+8) {
		return s, ErrCorrupt
	}
	at := int64(PageSize + 8)
	page := make([]byte, PageSize)
	for i := uint64(0); i < count; i++ {
		if _, e = payload.ReadAt(n[:], at); e != nil {
			return s, e
		}
		pos := diskOrder.Uint64(n[:])
		at += 8
		if pos < treeStart || pos%PageSize != 0 || pos >= next.Identity.Size || seen[pos] {
			return s, ErrCorrupt
		}
		if _, e = payload.ReadAt(page, at); e != nil {
			return s, e
		}
		if e = writeFullAt(dev, page[:min(uint64(PageSize), next.Identity.Size-pos)], pos); e != nil {
			return s, e
		}
		at += PageSize
	}
	if e = dev.Sync(); e != nil {
		return s, e
	}
	if e = writeFullAt(dev, header, gen%2*PageSize); e != nil {
		return s, e
	}
	if e = dev.Sync(); e != nil {
		return s, e
	}
	return next, nil
}
