// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"io"
	"sort"
)

// writeRange changes only addressed bytes/chunks. The volume write handle keeps
// readers out; redo, rather than retained object versions, makes publication atomic.
func (t *transaction) writeRange(o *object, offset, length uint64, src io.Reader, codec Compression) error {
	end := offset + length
	cursor := offset
	next := make([]dataSpan, 0, len(o.Spans)+1)
	addGap := func(until uint64) error {
		if until <= cursor {
			return nil
		}
		spans, err := t.storePacked([]payloadPiece{{cursor, until - cursor, src}}, codec)
		if err != nil {
			return err
		}
		next = append(next, spans...)
		cursor = until
		return nil
	}
	for _, original := range o.Spans {
		spans := []dataSpan{original}
		if length > 0 && original.Offset < end && offset < original.Offset+original.Length && original.Codec != Raw && !original.Direct {
			var err error
			spans, err = t.unpackChunks(original)
			if err != nil {
				return err
			}
		}
		for _, s := range spans {
			start, stop := max(offset, s.Offset), min(end, s.Offset+s.Length)
			if length > 0 && start < stop {
				if err := addGap(start); err != nil {
					return err
				}
				var err error
				if s.Direct {
					s, err = t.patchChunk(s, start-s.Offset, stop-start, src, codec)
				} else {
					err = t.patchRaw(&s, start-s.Offset, stop-start, src)
				}
				if err != nil {
					return err
				}
				cursor = stop
			}
			next = append(next, s)
		}
	}
	if err := addGap(end); err != nil {
		return err
	}
	sort.Slice(next, func(i, j int) bool { return next[i].Offset < next[j].Offset })
	o.Spans = next
	o.Segmented = o.Segmented || len(next) > 1
	// A compressed span may expand into independently owned chunks, or a sparse
	// write may add spans. saveObject updates/relinks the packed segment directory.
	o.dirtyFrom = -1
	return nil
}

func (t *transaction) patchRaw(s *dataSpan, offset, length uint64, src io.Reader) error {
	if s.Skip > s.Stored || s.Length > s.Stored-s.Skip {
		return ErrCorrupt
	}
	position := s.Skip + offset
	for length > 0 {
		index, within := position/BlockSize, position%BlockSize
		size := min(uint64(BlockSize), s.Stored-index*BlockSize)
		raw := make([]byte, size)
		if err := t.readBytes(raw, s.Location+index*BlockSize); err != nil {
			return err
		}
		crc := s.CRC
		if s.Checksums != 0 {
			var b [4]byte
			if err := t.readBytes(b[:], s.Checksums+index*4); err != nil {
				return err
			}
			crc = diskOrder.Uint32(b[:])
		}
		if checksum(raw) != crc {
			return ErrCorrupt
		}
		n := min(length, size-within)
		if _, err := io.ReadFull(src, raw[within:within+n]); err != nil {
			return err
		}
		if err := t.patch(s.Location+position, raw[within:within+n]); err != nil {
			return err
		}
		crc = checksum(raw)
		if s.Checksums == 0 {
			s.CRC = crc
		} else {
			var b [4]byte
			diskOrder.PutUint32(b[:], crc)
			if err := t.patch(s.Checksums+index*4, b[:]); err != nil {
				return err
			}
		}
		position += n
		length -= n
	}
	return nil
}

// unpackChunks retains every encoded payload at its original address. Only the
// old chunk table/padding is freed; the new descriptors own disjoint byte ranges.
func (t *transaction) unpackChunks(s dataSpan) ([]dataSpan, error) {
	if s.Stored < 8 {
		return nil, ErrCorrupt
	}
	var head [8]byte
	if err := t.readBytes(head[:], s.Location); err != nil {
		return nil, err
	}
	count := diskOrder.Uint64(head[:])
	if count == 0 || count > (s.Stored-8)/32 || count != (s.Skip+s.Length+BlockSize-1)/BlockSize {
		return nil, ErrCorrupt
	}
	previous := uint64(8) + count*32
	t.release(s.Location, previous, false)
	out := make([]dataSpan, 0, count)
	for i := uint64(0); i < count; i++ {
		var b [32]byte
		if err := t.readBytes(b[:], s.Location+8+i*32); err != nil {
			return nil, err
		}
		off, n, decoded := diskOrder.Uint64(b[:]), uint64(diskOrder.Uint32(b[8:])), uint64(diskOrder.Uint32(b[12:]))
		actual := Compression(b[24])
		if off < previous || off > s.Stored || n == 0 || n > s.Stored-off || n > BlockSize || decoded == 0 || decoded > BlockSize || !actual.valid() {
			return nil, ErrCorrupt
		}
		// Packed compressed objects have no sliced/shared allocation owners.
		if s.Skip != 0 || decoded != min(uint64(BlockSize), s.Length-i*BlockSize) {
			return nil, ErrCorrupt
		}
		if off > previous {
			t.release(s.Location+previous, off-previous, false)
		}
		out = append(out, dataSpan{Offset: s.Offset + i*BlockSize, Length: decoded, Location: s.Location + off, Stored: n, Capacity: n, Codec: actual, CRC: diskOrder.Uint32(b[16:]), Checksums: decoded<<32 | uint64(diskOrder.Uint32(b[20:])), Direct: true})
		previous = off + n
	}
	if previous < s.Capacity {
		t.release(s.Location+previous, s.Capacity-previous, false)
	}
	return out, nil
}
func (t *transaction) patchChunk(s dataSpan, offset, length uint64, src io.Reader, codec Compression) (dataSpan, error) {
	raw, err := t.v.directChunk(s)
	if err != nil {
		return s, err
	}
	if _, err = io.ReadFull(src, raw[s.Skip+offset:s.Skip+offset+length]); err != nil {
		return s, err
	}
	encoded, err := compressBlock(codec, raw)
	if err != nil {
		return s, err
	}
	actual := codec
	if len(encoded) >= len(raw) {
		encoded = raw
		actual = Raw
	}
	n := uint64(len(encoded))
	if n <= s.Capacity {
		if err = t.patch(s.Location, encoded); err != nil {
			return s, err
		}
		if n < s.Capacity {
			t.release(s.Location+n, s.Capacity-n, false)
		}
	} else {
		at, e := t.alloc(n, false)
		if e != nil {
			return s, e
		}
		if e = t.writeBytes(at, encoded); e != nil {
			return s, e
		}
		t.release(s.Location, s.Capacity, false)
		s.Location = at
	}
	s.Stored = n
	s.Capacity = n
	s.Codec = actual
	s.CRC = checksum(raw)
	s.Checksums = uint64(len(raw))<<32 | uint64(checksum(encoded))
	return s, nil
}
func (v *Volume) directChunk(s dataSpan) ([]byte, error) {
	decoded := uint32(s.Checksums >> 32)
	if decoded == 0 || decoded > BlockSize || s.Stored > BlockSize || s.Skip > uint64(decoded) || s.Length > uint64(decoded)-s.Skip {
		return nil, ErrCorrupt
	}
	encoded := make([]byte, s.Stored)
	if err := v.readBytes(encoded, s.Location); err != nil {
		return nil, err
	}
	if checksum(encoded) != uint32(s.Checksums) {
		return nil, ErrCorrupt
	}
	raw, err := decompressBlock(s.Codec, encoded, decoded)
	if err != nil {
		return nil, err
	}
	if checksum(raw) != s.CRC {
		return nil, ErrCorrupt
	}
	return raw, nil
}
