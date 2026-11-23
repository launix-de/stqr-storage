// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"sort"
)

type allocation struct {
	off, n uint64
	meta   bool
}
type transaction struct {
	v                      *Volume
	sb                     Superblock
	pages                  map[uint64][]byte
	spilled                map[uint64]int64
	spool                  *encodedSpool
	busy, pending, retired []allocation
	reusable               allocation
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
	if t.spool != nil {
		t.spool.Close()
		t.spool = nil
	}
}
func (t *transaction) page(off uint64) ([]byte, error) {
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
	if t.spool == nil {
		t.spool = &encodedSpool{}
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
func (t *transaction) candidate(start, length, n, alignment uint64) (uint64, bool) {
	end := start + length
	off := (start + alignment - 1) / alignment * alignment
	for off <= end && n <= end-off {
		blocked := false
		for _, list := range [][]allocation{t.busy, t.retired} {
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

func (t *transaction) findSpace(n, a uint64, tail bool) (uint64, uint64, uint64, error) {
	var start, length, chosen uint64
	var visit func(uint64) error
	visit = func(off uint64) error {
		node, e := t.node(off)
		if e != nil {
			return e
		}
		for _, x := range node.entries {
			if node.level > 0 {
				maxlen := x.maximum
				if a == PageSize {
					maxlen = x.aligned
				}
				if maxlen < n {
					continue
				}
				if e = visit(x.value); e != nil {
					return e
				}
				continue
			}
			s := keyOffset(x.key)
			if tail && s%PageSize == 0 && x.value >= PageSize {
				continue
			}
			if c, ok := t.candidate(s, x.value, n, a); ok {
				start, length, chosen = s, x.value, c
				return stopTree
			}
		}
		return nil
	}
	e := visit(t.sb.FreeRoot)
	if e == nil {
		for _, off := range t.freeSiblings {
			if e = visit(off); e != nil {
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
		if off, ok := t.candidate(x.off, x.n, n, a); ok {
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
	x, found, e := treeNeighbor(t.node, t.sb.FreeRoot, spaceKey(off), false)
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
	s, l, off, e := t.findSpace(n, a, n < PageSize)
	_ = s
	_ = l
	if e == ErrFull && n < PageSize {
		_, _, off, e = t.findSpace(n, a, false)
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
	previous, hasPrevious, e := treeNeighbor(t.node, t.sb.FreeRoot, spaceKey(off), false)
	if e != nil {
		return e
	}
	next, hasNext, e := treeNeighbor(t.node, t.sb.FreeRoot, spaceKey(off), true)
	if e != nil {
		return e
	}
	if hasPrevious {
		p := keyOffset(previous.key)
		if p+previous.value > off {
			return ErrCorrupt
		}
		if p+previous.value == off {
			if e = t.remove(&t.sb.FreeRoot, previous.key); e != nil {
				return e
			}
			off = p
			n += previous.value
		}
	}
	if hasNext {
		q := keyOffset(next.key)
		if off+n > q {
			return ErrCorrupt
		}
		if off+n == q {
			if e = t.remove(&t.sb.FreeRoot, next.key); e != nil {
				return e
			}
			n += next.value
		}
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
	if off%PageSize != 0 || off >= v.limit {
		return nil, ErrCorrupt
	}
	b := make([]byte, PageSize)
	if e := readFullAt(v.dev, b[:min(uint64(PageSize), v.limit-off)], off); e != nil {
		return nil, e
	}
	return b, nil
}
func (v *Volume) readBytes(dst []byte, off uint64) error {
	for len(dst) > 0 {
		at := off % PageSize
		if at == 0 && len(dst) >= PageSize {
			n := len(dst) / PageSize * PageSize
			if off+uint64(n) > v.limit {
				return ErrCorrupt
			}
			if e := readFullAt(v.dev, dst[:n], off); e != nil {
				return e
			}
			off += uint64(n)
			dst = dst[n:]
			continue
		}
		b, e := v.readPage(off - at)
		if e != nil {
			return e
		}
		n := min(len(dst), PageSize-int(at))
		copy(dst, b[at:at+uint64(n)])
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
	return nil
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
	if e := t.finalize(); e != nil {
		return e
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
	payload := &encodedSpool{}
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
	if e = t.v.sync(); e != nil {
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
	if e = t.v.sync(); e != nil {
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
	if e = t.v.sync(); e != nil {
		return e
	}
	if e = t.v.write(header, t.sb.Generation%2*PageSize); e != nil {
		return e
	}
	if e = t.v.sync(); e != nil {
		return e
	}
	t.v.sb = t.sb
	t.v.inFlightReserved -= consumed
	t.v.guaranteed -= guaranteedConsumed
	for off := range all {
		b, err := t.page(off)
		if err != nil {
			return err
		}
		if string(b[:4]) == "BPT2" {
			n, err := decodeNode(b, t.sb.Identity.VolumeID)
			if err == nil {
				t.v.nodes.put(off, n)
			}
		}
	}
	return nil
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
	payload := &encodedSpool{}
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
