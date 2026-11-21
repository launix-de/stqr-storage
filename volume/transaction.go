// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"errors"
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
	busy, pending, retired []allocation
	reusable               allocation
}

func (v *Volume) transaction() *transaction {
	return &transaction{v: v, sb: v.sb, pages: map[uint64][]byte{}}
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
	if e == stopTree {
		return start, length, chosen, nil
	}
	if e != nil {
		return 0, 0, 0, e
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
	if off > s {
		if e = t.set(&t.sb.FreeRoot, spaceKey(s), off-s); e != nil {
			return e
		}
	}
	if off+n < s+l {
		return t.set(&t.sb.FreeRoot, spaceKey(off+n), s+l-off-n)
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
		for r < len(t.retired) {
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
			p, e = t.v.readPage(page)
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
			p, e = t.v.readPage(page)
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
	return nil
}
func (t *transaction) commit(useGuaranteed bool) error {
	if e := t.finalize(); e != nil {
		return e
	}
	if !useGuaranteed && t.sb.FreeBytes < t.v.guaranteed {
		return ErrFull
	}
	t.sb.Generation++
	t.sb.Sequence++
	header, e := t.sb.MarshalBinary()
	if e != nil {
		return e
	}
	offsets := make([]uint64, 0, len(t.pages))
	for off, page := range t.pages {
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
	var payload bytes.Buffer
	payload.Write(header)
	var n [8]byte
	diskOrder.PutUint64(n[:], uint64(len(offsets)))
	payload.Write(n[:])
	for _, off := range offsets {
		diskOrder.PutUint64(n[:], off)
		payload.Write(n[:])
		payload.Write(t.pages[off])
	}
	data := payload.Bytes()
	count := (len(data) + PageSize - 17) / (PageSize - 16)
	logPages := make([]uint64, count)
	for i := range logPages {
		if i < scratchPages {
			logPages[i] = uint64(4*PageSize + i*PageSize)
		} else {
			_, _, off, err := t.findSpace(PageSize, PageSize, false)
			if err != nil {
				return err
			}
			logPages[i] = off
			t.busy = append(t.busy, allocation{off, PageSize, false})
		}
	}
	for i, off := range logPages {
		p := make([]byte, PageSize)
		if i+1 < len(logPages) {
			diskOrder.PutUint64(p, logPages[i+1])
		}
		from := i * (PageSize - 16)
		end := min(from+PageSize-16, len(data))
		diskOrder.PutUint32(p[8:], uint32(end-from))
		copy(p[16:], data[from:end])
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
	diskOrder.PutUint64(anchor[24:], uint64(len(data)))
	digest := sha256.Sum256(data)
	copy(anchor[32:64], digest[:])
	copy(anchor[64:80], t.sb.Identity.VolumeID[:])
	diskOrder.PutUint32(anchor[PageSize-4:], checksum(anchor[:PageSize-4]))
	if e = t.v.write(anchor, uint64(2+int(t.sb.Generation%2))*PageSize); e != nil {
		return e
	}
	if e = t.v.sync(); e != nil {
		return e
	}
	for _, off := range offsets {
		if e = t.v.write(t.pages[off][:min(uint64(PageSize), t.sb.Identity.Size-off)], off); e != nil {
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
	for off, b := range t.pages {
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
	var payload bytes.Buffer
	off := diskOrder.Uint64(anchor[16:])
	seen := map[uint64]bool{}
	for uint64(payload.Len()) < length {
		if off < PageSize*4 || off%PageSize != 0 || off+PageSize > uint64(size) || seen[off] {
			return s, ErrCorrupt
		}
		seen[off] = true
		p := make([]byte, PageSize)
		if e = readFullAt(dev, p, off); e != nil {
			return s, e
		}
		used := diskOrder.Uint32(p[8:])
		if used > PageSize-16 || used == 0 || uint64(used) > length-uint64(payload.Len()) || checksum(p[16:]) != diskOrder.Uint32(p[12:]) {
			return s, ErrCorrupt
		}
		payload.Write(p[16 : 16+used])
		off = diskOrder.Uint64(p)
	}
	data := payload.Bytes()
	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], anchor[32:64]) {
		return s, ErrCorrupt
	}
	next, e := DecodeSuperblock(data[:PageSize])
	if e != nil || next.Generation != gen || next.Identity.VolumeID != s.Identity.VolumeID || next.Identity.Size > uint64(size) {
		return s, ErrCorrupt
	}
	count := diskOrder.Uint64(data[PageSize:])
	if count > (uint64(len(data))-PageSize-8)/(PageSize+8) || uint64(len(data)) != PageSize+8+count*(PageSize+8) {
		return s, ErrCorrupt
	}
	at := PageSize + 8
	for i := uint64(0); i < count; i++ {
		pos := diskOrder.Uint64(data[at:])
		at += 8
		if pos < treeStart || pos%PageSize != 0 || pos >= next.Identity.Size || seen[pos] {
			return s, ErrCorrupt
		}
		if e = writeFullAt(dev, data[at:at+int(min(uint64(PageSize), next.Identity.Size-pos))], pos); e != nil {
			return s, e
		}
		at += PageSize
	}
	if e = dev.Sync(); e != nil {
		return s, e
	}
	if e = writeFullAt(dev, data[:PageSize], gen%2*PageSize); e != nil {
		return s, e
	}
	if e = dev.Sync(); e != nil {
		return s, e
	}
	return next, nil
}
