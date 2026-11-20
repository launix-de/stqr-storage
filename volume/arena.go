// SPDX-License-Identifier: GPL-3.0-only
package volume

import "fmt"

func (v *Volume) arenaPage(s Superblock, pair uint64) (cowPage, uint8, error) {
	var best cowPage
	var slot uint8
	found := false
	if pair < s.DataTop || pair > s.bankSize() || 2*PageSize > s.bankSize()-pair {
		return best, 0, ErrCorrupt
	}
	for i := uint8(0); i < 2; i++ {
		b := make([]byte, PageSize)
		if e := readFullAt(v.dev, b, s.bankStart()+pair+uint64(i)*PageSize); e != nil {
			return best, 0, e
		}
		p, e := decodePage(b, "APG1", s.BankEpoch, pair, s.Identity.VolumeID)
		if e != nil || p.Generation > s.Generation {
			continue
		}
		if !found || p.Generation > best.Generation || (p.Generation == best.Generation && p.Used > best.Used) {
			best = p
			slot = i
			found = true
		}
	}
	if !found {
		return best, 0, ErrCorrupt
	}
	return best, slot, nil
}

// writeArena appends to pre-reserved pairs of pages. The current committed copy
// is never overwritten: the active tail block is copied to the alternate page.
// Thus a torn 10-byte append cannot destroy the previously confirmed prefix.
func (v *Volume) writeArena(s *Superblock, a *arena, data []byte) (uint64, uint64, error) {
	if a.Pages == 0 || uint64(len(data)) > a.capacity()-a.Used {
		pages := uint32(1)
		if a.Pages > 0 {
			pages = min(a.Pages*2, 256)
		}
		for uint64(pages)*pagePayload < uint64(len(data)) {
			if pages >= 256 {
				return 0, 0, ErrRange
			}
			pages *= 2
		}
		base, e := reserve(s, uint64(pages)*2*PageSize)
		if e != nil {
			return 0, 0, e
		}
		*a = arena{Base: base, Pages: pages}
	}
	base, off := a.Base, a.Used
	for len(data) > 0 {
		page := a.Used / pagePayload
		at := int(a.Used % pagePayload)
		pair := a.Base + page*2*PageSize
		buf := make([]byte, pagePayload)
		slot := uint8(0)
		if at > 0 {
			readRoot := *s
			readRoot.Generation++ // earlier writes in this same transaction
			p, old, e := v.arenaPage(readRoot, pair)
			if e != nil {
				return 0, 0, e
			}
			copy(buf, p.Data)
			if uint64(p.Generation) > s.Generation {
				slot = old
			} else {
				slot = 1 - old
			}
		}
		n := min(len(data), pagePayload-at)
		copy(buf[at:], data[:n])
		p := cowPage{VolumeID: s.Identity.VolumeID, Epoch: s.BankEpoch, Index: pair, Generation: s.Generation + 1, Used: uint32(at + n), Data: buf}
		if e := v.write(encodePage("APG1", p), s.bankStart()+pair+uint64(slot)*PageSize); e != nil {
			return 0, 0, e
		}
		a.Used += uint64(n)
		data = data[n:]
	}
	return base, off, nil
}
func (v *Volume) storeBlock(s *Superblock, a *arena, logical uint64, raw []byte, codec Compression, appendable bool) (dataExtent, error) {
	var e dataExtent
	b, err := compressBlock(codec, raw)
	if err != nil {
		return e, err
	}
	e = dataExtent{Offset: logical, Length: uint64(len(raw)), Codec: codec, Stored: uint32(len(b)), Raw: uint32(len(raw)), StoredCRC: checksum(b), RawCRC: checksum(raw)}
	if appendable {
		e.Arena = true
		e.Location, e.ArenaOffset, err = v.writeArena(s, a, b)
		return e, err
	}
	e.Location, err = reserve(s, uint64(len(b)))
	if err != nil {
		return e, err
	}
	p := make([]byte, int(align(uint64(len(b)))))
	copy(p, b)
	if err = v.write(p, s.bankStart()+e.Location); err != nil {
		return e, err
	}
	return e, nil
}
func (v *Volume) readBlock(s Superblock, e dataExtent) ([]byte, error) {
	if !e.Codec.valid() || e.Raw > BlockSize || e.Stored == 0 || e.Stored > 2*BlockSize {
		return nil, ErrCorrupt
	}
	buf := make([]byte, int(e.Stored))
	if e.Arena {
		at := e.ArenaOffset
		dst := buf
		for len(dst) > 0 {
			page := at / pagePayload
			pair, e2 := add(e.Location, page*2*PageSize)
			if e2 != nil {
				return nil, ErrCorrupt
			}
			p, _, err := v.arenaPage(s, pair)
			if err != nil {
				return nil, err
			}
			off := int(at % pagePayload)
			n := min(len(dst), pagePayload-off)
			if off+n > int(p.Used) {
				return nil, ErrCorrupt
			}
			copy(dst[:n], p.Data[off:off+n])
			dst = dst[n:]
			at += uint64(n)
		}
	} else {
		if e.Location < s.DataTop || e.Location%PageSize != 0 || e.Location > s.bankSize() || uint64(e.Stored) > s.bankSize()-e.Location {
			return nil, ErrCorrupt
		}
		whole := make([]byte, int(align(uint64(e.Stored))))
		if err := readFullAt(v.dev, whole, s.bankStart()+e.Location); err != nil {
			return nil, err
		}
		copy(buf, whole)
	}
	if checksum(buf) != e.StoredCRC {
		return nil, fmt.Errorf("%w: payload CRC", ErrCorrupt)
	}
	raw, err := decompressBlock(e.Codec, buf, e.Raw)
	if err != nil {
		return nil, err
	}
	if checksum(raw) != e.RawCRC {
		return nil, fmt.Errorf("%w: decoded CRC", ErrCorrupt)
	}
	return raw, nil
}

// liveAllocation counts unique physical payload pages within one object. Blocks
// are never shared between objects; sliced extents may reference the same block.
// The active tail's spare capacity remains live, all obsolete tails become GC.
func liveAllocation(o *object) uint64 {
	if o == nil || o.Info.Deleted {
		return 0
	}
	pages := map[uint64]uint64{}
	for _, e := range o.Extents {
		if !e.Arena {
			pages[e.Location] = align(uint64(e.Stored))
			continue
		}
		first := e.ArenaOffset / pagePayload
		last := (e.ArenaOffset + uint64(e.Stored) - 1) / pagePayload
		for i := first; i <= last; i++ {
			pages[e.Location+i*2*PageSize] = 2 * PageSize
		}
	}
	if o.Arena.Pages > 0 {
		for i := uint64(0); i < uint64(o.Arena.Pages); i++ {
			pages[o.Arena.Base+i*2*PageSize] = 2 * PageSize
		}
	}
	var n uint64
	for _, s := range pages {
		n += s
	}
	return n
}
func arenaUnused(o *object) uint64 {
	if o == nil || o.Info.Deleted || o.Arena.Pages == 0 {
		return 0
	}
	return o.Arena.capacity() - o.Arena.Used
}
