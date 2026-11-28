// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
)

// ReadSegment is one contiguous piece of a logical object range. A nil Data
// denotes Length sparse zero bytes. Otherwise len(Data)==Length. Borrowed marks
// slices backed by the volume's mmap or immutable RAM overlay/page; compressed
// data and providers without borrowing use bounded owned buffers instead.
// Data is READ-ONLY, including when its Go type is []byte. It must never be
// modified or retained after WithReadView's callback. Linux mappings enforce
// read-only access; writing into one is a programming error, not a storage API.
type ReadSegment struct {
	Data     []byte
	Length   uint64
	Borrowed bool
}

// ReadView contains a bounded, checked range. A file may have several physical
// spans/checksum chunks or sparse gaps: it is never falsely presented as one
// contiguous slice. Existing Get/GetRange continue to return owned copies.
type ReadView struct {
	Offset   uint64
	Length   uint64
	Segments []ReadSegment
}

// One shared bounded zero block avoids replacing each former 1-MiB sparse
// write with 256 tiny Writer calls. It is never exposed as writable view data.
var readViewZeros [BlockSize]byte

// WriteTo sends the view without copying mapped payloads into a Go buffer.
// The io.Writer contract forbids modifying or retaining p after Write returns.
// It must be called inside the WithReadView callback which owns this view.
func (view ReadView) WriteTo(dst io.Writer) (int64, error) {
	if view.Length > MaxBufferedRead {
		return 0, ErrRange
	}
	var size uint64
	for _, segment := range view.Segments {
		if segment.Length > view.Length-size || segment.Data != nil && uint64(len(segment.Data)) != segment.Length {
			return 0, ErrRange
		}
		size += segment.Length
	}
	if size != view.Length {
		return 0, ErrRange
	}
	var total int64
	for _, segment := range view.Segments {
		left := segment.Length
		for left > 0 {
			data := segment.Data
			if data == nil {
				data = readViewZeros[:min(left, uint64(len(readViewZeros)))]
			}
			n, err := dst.Write(data)
			if n < 0 || n > len(data) {
				return total, io.ErrShortWrite
			}
			total += int64(n)
			left -= uint64(n)
			if err != nil {
				return total, err
			}
			if n != len(data) {
				return total, io.ErrShortWrite
			}
			if segment.Data != nil {
				break
			}
		}
	}
	return total, nil
}

// WithReadView lends checked, read-only segments during fn. The requested range
// is clipped at EOF and must fit MaxBufferedRead after clipping. Sparse runs do
// not allocate payload. Each compressed chunk decodes at most BlockSize bytes;
// a view may retain several owned chunks. Partial chunks retain only selected bytes.
//
// Software contract: fn must not modify/retain segment bytes, close/reenter this
// handle or call management methods on its volume. The handle's operation lock
// prevents a concurrent Close; KeepAlive in withHandle prevents cleanup while fn
// runs. The Volume RWLock pins the mapping and immutable overlay pages. Physical
// checkpoints may proceed: their published RAM pages override the physical map
// until the final fence has acquired the exclusive lock again. No object ID or
// version changes are introduced by this read capability.
func (h *Handle) WithReadView(k Key, offset, length uint64, fn func(ReadView) error) (Info, error) {
	if fn == nil {
		return Info{}, ErrRange
	}
	return withHandle(h, false, func(v *Volume) (Info, error) {
		object, err := v.loadObject(k)
		if err != nil {
			return Info{}, err
		}
		if object.Info.Deleted {
			return Info{}, ErrNotFound
		}
		view, err := v.objectReadView(object, offset, length)
		if err != nil {
			return object.Info, err
		}
		return object.Info, fn(view)
	})
}

// visibleRegion returns one immutable region. The caller holds the Volume lock.
// Always consult published staging/deferred/fence overlays before mmap bytes;
// a concurrent physical checkpoint is allowed to overwrite only those shadowed
// pages (plus reserved free WAL pages, which are never live object payload).
func (v *Volume) visibleRegion(off uint64, n int) ([]byte, int, bool, error) {
	if n < 0 || off > v.limit || uint64(n) > v.limit-off {
		return nil, 0, false, ErrCorrupt
	}
	if n == 0 {
		return nil, 0, false, nil
	}
	page, at := off/PageSize*PageSize, int(off%PageSize)
	if v.overlay != nil {
		data, err := v.overlay.page(page)
		if err != nil {
			return nil, 0, false, err
		}
		size := min(n, PageSize-at)
		return data[at : at+size : at+size], size, false, nil
	}
	if v.staging != nil || v.deferred != nil || v.fence != nil {
		n = min(n, PageSize-at)
		var data []byte
		if v.stageStore != nil {
			var err error
			data, _, err = v.stageStore.get(page)
			if err != nil {
				return nil, 0, false, err
			}
		} else {
			data = v.staging[page]
		}
		if data == nil && v.deferred != nil {
			var err error
			data, _, err = v.deferred.store.get(page)
			if err != nil {
				return nil, 0, false, err
			}
		}
		if data == nil && v.fence != nil {
			var err error
			data, _, err = v.fence.page(page)
			if err != nil {
				return nil, 0, false, err
			}
		}
		if data != nil {
			return data[at : at+n : at+n], n, true, nil
		}
	}
	data, read, err := v.dev.(*meteredIO).borrowAt(off, n)
	if err == nil {
		return data, read, true, nil
	}
	if !errors.Is(err, ErrUnsupported) {
		return nil, 0, false, err
	}
	// Custom/unsupported backends keep the existing device-aligned bounded path.
	raw := make([]byte, PageSize)
	if err = readFullAt(v.dev, raw[:min(uint64(PageSize), v.limit-page)], page); err != nil {
		return nil, 0, false, err
	}
	size := min(n, PageSize-at)
	return raw[at : at+size : at+size], size, false, nil
}

func (v *Volume) readOnlyPage(off uint64) ([]byte, error) {
	if off%PageSize != 0 || off >= v.limit || PageSize > v.limit-off {
		return nil, ErrCorrupt
	}
	data, n, _, err := v.visibleRegion(off, PageSize)
	if err != nil {
		return nil, err
	}
	if n != PageSize {
		return nil, ErrCorrupt
	}
	if data == nil {
		return readViewZeros[:PageSize:PageSize], nil
	}
	return data, nil
}

// physicalReadView follows the same in-progress defragmentation redirection as
// readBytes, while preserving separate regions at overlay and mapping borders.
func (v *Volume) physicalReadView(off, length uint64) ([]ReadSegment, error) {
	start := off
	var out []ReadSegment
	for length > 0 {
		actual, size := off, min(length, uint64(BlockSize))
		if v.sb.Moving {
			from, end := v.sb.MoveFrom, v.sb.MoveFrom+v.sb.MoveDone
			if off >= from && off < end {
				size = min(size, end-off)
				actual = v.sb.MoveTo + off - from
			} else if off < from {
				size = min(size, from-off)
			}
		}
		data, n, borrowed, err := v.visibleRegion(actual, int(size))
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, ErrCorrupt
		}
		out = append(out, ReadSegment{Data: data, Length: uint64(n), Borrowed: borrowed})
		off += uint64(n)
		length -= uint64(n)
	}
	if !v.sb.Moving {
		out = v.coalesceMappedSegments(start, out)
	}
	return out, nil
}

// Shadow lookup splits a read into pages even when all of its payload still
// resides in one mapped extent. Rejoin only slices proven to alias that exact
// mapping. An intervening accepted/fence page remains a separate segment.
// The extra borrow just obtains an alias: the original reads already account
// for these bytes, and no payload is read, copied, or counted again here.
func (v *Volume) coalesceMappedSegments(off uint64, parts []ReadSegment) []ReadSegment {
	if len(parts) < 2 {
		return parts
	}
	file, ok := v.dev.(*meteredIO).VolumeIO.(*FileIO)
	if !ok {
		return parts
	}
	var length uint64
	for _, part := range parts {
		if part.Length > BlockSize-length {
			return parts
		}
		length += part.Length
	}
	data, n, err := file.borrowAt(off, int(length))
	if err != nil || uint64(n) != length {
		return parts
	}
	out := parts[:0]
	at := uint64(0)
	lastMapped := false
	for _, part := range parts {
		mapped := part.Borrowed && len(part.Data) > 0 && &part.Data[0] == &data[at]
		if mapped && lastMapped {
			prev := &out[len(out)-1]
			prev.Length += part.Length
			prev.Data = data[at+part.Length-prev.Length : at+part.Length : at+part.Length]
		} else {
			out = append(out, part)
		}
		at += part.Length
		lastMapped = mapped
	}
	return out
}

func readSegmentsCRC(parts []ReadSegment) uint32 {
	var crc uint32
	for _, part := range parts {
		if part.Data != nil {
			crc = crc32.Update(crc, crcTable, part.Data)
			continue
		}
		for left := part.Length; left > 0; {
			n := min(left, uint64(PageSize))
			crc = crc32.Update(crc, crcTable, readViewZeros[:n])
			left -= n
		}
	}
	return crc
}

// A partial owned slice must not retain a whole decoded chunk/page. Many small
// imported slices can otherwise pin BlockSize bytes per returned byte. Whole
// chunks keep their existing buffer; borrowed mmap/RAM data is never copied.
func sliceOwnedReadData(data []byte, offset, length uint64) []byte {
	selected := data[offset : offset+length : offset+length]
	if offset == 0 && length == uint64(len(data)) {
		return selected
	}
	return bytes.Clone(selected)
}

func sliceReadSegments(parts []ReadSegment, offset, length uint64) []ReadSegment {
	var out []ReadSegment
	for _, part := range parts {
		if length == 0 {
			break
		}
		if offset >= part.Length {
			offset -= part.Length
			continue
		}
		n := min(length, part.Length-offset)
		if part.Data != nil {
			if part.Borrowed {
				part.Data = part.Data[offset : offset+n : offset+n]
			} else {
				part.Data = sliceOwnedReadData(part.Data, offset, n)
			}
		}
		part.Length = n
		out = append(out, part)
		length -= n
		offset = 0
	}
	return out
}

func (v *Volume) objectReadView(object *object, offset, length uint64) (ReadView, error) {
	view := ReadView{Offset: offset}
	if offset >= object.Info.Length || length == 0 {
		return view, nil
	}
	length = min(length, object.Info.Length-offset)
	if length > MaxBufferedRead {
		return view, ErrRange
	}
	view.Length = length
	end, at := offset+length, offset
	for _, span := range object.Spans {
		start, stop := max(offset, span.Offset), min(end, span.Offset+span.Length)
		if stop <= start {
			continue
		}
		if at < start {
			view.Segments = append(view.Segments, ReadSegment{Length: start - at})
		}
		segments, err := v.spanReadView(span, start-span.Offset, stop-start)
		if err != nil {
			return ReadView{}, err
		}
		view.Segments = append(view.Segments, segments...)
		at = stop
	}
	if at < end {
		view.Segments = append(view.Segments, ReadSegment{Length: end - at})
	}
	return view, nil
}

func (v *Volume) spanReadView(span dataSpan, offset, length uint64) ([]ReadSegment, error) {
	if offset > span.Length || length > span.Length-offset {
		return nil, ErrRange
	}
	if span.Direct {
		if span.Codec != Raw {
			raw, err := v.directChunk(span)
			if err != nil {
				return nil, err
			}
			data := sliceOwnedReadData(raw, span.Skip+offset, length)
			return []ReadSegment{{Data: data, Length: length}}, nil
		}
		decoded := uint64(span.Checksums >> 32)
		if decoded == 0 || decoded > BlockSize || span.Stored != decoded || span.Skip > decoded || span.Length > decoded-span.Skip {
			return nil, ErrCorrupt
		}
		parts, err := v.physicalReadView(span.Location, span.Stored)
		if err != nil {
			return nil, err
		}
		crc := readSegmentsCRC(parts)
		if crc != uint32(span.Checksums) || crc != span.CRC {
			return nil, ErrCorrupt
		}
		return sliceReadSegments(parts, span.Skip+offset, length), nil
	}
	if offset > ^uint64(0)-span.Skip {
		return nil, ErrCorrupt
	}
	if span.Codec == Raw && (span.Skip > span.Stored || span.Length > span.Stored-span.Skip) {
		return nil, ErrCorrupt
	}
	var chunks uint64
	if span.Codec != Raw {
		if span.Stored < 8 {
			return nil, ErrCorrupt
		}
		var header [8]byte
		if err := v.readBytes(header[:], span.Location); err != nil {
			return nil, err
		}
		chunks = diskOrder.Uint64(header[:])
		if chunks == 0 || chunks > (span.Stored-8)/32 {
			return nil, ErrCorrupt
		}
	}
	var out []ReadSegment
	for position := span.Skip + offset; length > 0; {
		index, within := position/BlockSize, position%BlockSize
		var parts []ReadSegment
		if span.Codec == Raw {
			if index > span.Stored/BlockSize || index*BlockSize >= span.Stored {
				return nil, ErrCorrupt
			}
			n := min(uint64(BlockSize), span.Stored-index*BlockSize)
			var err error
			parts, err = v.physicalReadView(span.Location+index*BlockSize, n)
			if err != nil {
				return nil, err
			}
			crc := span.CRC
			if span.Checksums != 0 {
				if span.Checksums > v.limit || index > (v.limit-span.Checksums)/4 || 4 > v.limit-span.Checksums-index*4 {
					return nil, ErrCorrupt
				}
				var raw [4]byte
				if err = v.readBytes(raw[:], span.Checksums+index*4); err != nil {
					return nil, err
				}
				crc = diskOrder.Uint32(raw[:])
			}
			if readSegmentsCRC(parts) != crc {
				return nil, ErrCorrupt
			}
		} else {
			if index >= chunks {
				return nil, ErrCorrupt
			}
			var entry [32]byte
			if err := v.readBytes(entry[:], span.Location+8+index*32); err != nil {
				return nil, err
			}
			off, n, decoded := diskOrder.Uint64(entry[:]), diskOrder.Uint32(entry[8:]), diskOrder.Uint32(entry[12:])
			if off < 8+chunks*32 || off > span.Stored || uint64(n) > span.Stored-off || decoded > BlockSize || n > BlockSize {
				return nil, ErrCorrupt
			}
			if Compression(entry[24]) == Raw {
				if n != decoded {
					return nil, ErrCorrupt
				}
				var err error
				parts, err = v.physicalReadView(span.Location+off, uint64(n))
				if err != nil {
					return nil, err
				}
				crc := readSegmentsCRC(parts)
				if crc != diskOrder.Uint32(entry[20:]) || crc != diskOrder.Uint32(entry[16:]) {
					return nil, ErrCorrupt
				}
			} else {
				encoded := make([]byte, n)
				if err := v.readBytes(encoded, span.Location+off); err != nil {
					return nil, err
				}
				if checksum(encoded) != diskOrder.Uint32(entry[20:]) {
					return nil, ErrCorrupt
				}
				raw, err := decompressBlock(Compression(entry[24]), encoded, decoded)
				if err != nil {
					return nil, err
				}
				if checksum(raw) != diskOrder.Uint32(entry[16:]) {
					return nil, ErrCorrupt
				}
				parts = []ReadSegment{{Data: raw, Length: uint64(len(raw))}}
			}
		}
		var size uint64
		for _, p := range parts {
			size += p.Length
		}
		if within >= size {
			return nil, ErrCorrupt
		}
		n := min(length, size-within)
		out = append(out, sliceReadSegments(parts, within, n)...)
		position += n
		length -= n
	}
	return out, nil
}
