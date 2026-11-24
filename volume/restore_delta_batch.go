// SPDX-License-Identifier: GPL-3.0-only
package volume

import "sort"

type restorePatchPiece struct {
	position uint64
	data     []byte
}

// A checksum block is read and verified once even when the migration changes
// several disjoint pages in it. Only the changed byte intervals enter redo.
func (t *transaction) patchRawBatch(span *dataSpan, patches []RestorePatch, preimage []byte) error {
	if span.Skip > span.Stored || span.Length > span.Stored-span.Skip {
		return ErrCorrupt
	}
	blocks := map[uint64][]restorePatchPiece{}
	for _, p := range patches {
		start, stop := max(p.Offset, span.Offset), min(p.Offset+uint64(len(p.Data)), span.Offset+span.Length)
		if start >= stop {
			continue
		}
		position := span.Skip + start - span.Offset
		data := p.Data[start-p.Offset : stop-p.Offset]
		for len(data) > 0 {
			index, within := position/BlockSize, position%BlockSize
			n := min(uint64(len(data)), BlockSize-within)
			blocks[index] = append(blocks[index], restorePatchPiece{position, data[:n]})
			data = data[n:]
			position += n
		}
	}
	indices := make([]uint64, 0, len(blocks))
	for index := range blocks {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	for _, index := range indices {
		size := min(uint64(BlockSize), span.Stored-index*BlockSize)
		raw := make([]byte, int(size))
		if span.Skip == 0 && span.Stored == span.Length && span.Offset+index*BlockSize+size <= uint64(len(preimage)) {
			copy(raw, preimage[span.Offset+index*BlockSize:span.Offset+index*BlockSize+size])
		} else if err := t.readBytes(raw, span.Location+index*BlockSize); err != nil {
			return err
		}
		crc := span.CRC
		if span.Checksums != 0 {
			var b [4]byte
			if err := t.readBytes(b[:], span.Checksums+index*4); err != nil {
				return err
			}
			crc = diskOrder.Uint32(b[:])
		}
		if checksum(raw) != crc {
			return ErrCorrupt
		}
		for _, piece := range blocks[index] {
			at := piece.position - index*BlockSize
			copy(raw[at:], piece.data)
			if err := t.patch(span.Location+piece.position, piece.data); err != nil {
				return err
			}
		}
		crc = checksum(raw)
		if span.Checksums == 0 {
			span.CRC = crc
		} else {
			var b [4]byte
			diskOrder.PutUint32(b[:], crc)
			if err := t.patch(span.Checksums+index*4, b[:]); err != nil {
				return err
			}
		}
	}
	return nil
}
