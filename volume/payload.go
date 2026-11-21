// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"io"
)

const objectHeader = 40
const spanBytes = 64
const listBytes = 512
const listEntries = 7

func encodeSpan(b []byte, s dataSpan) {
	for i, x := range []uint64{s.Offset, s.Length, s.Location, s.Stored, s.Capacity, s.Checksums, s.Skip} {
		diskOrder.PutUint64(b[i*8:], x)
	}
	diskOrder.PutUint32(b[56:], s.CRC)
	b[60] = byte(s.Codec)
}
func decodeSpan(b []byte) dataSpan {
	var s dataSpan
	for i, p := range []*uint64{&s.Offset, &s.Length, &s.Location, &s.Stored, &s.Capacity, &s.Checksums, &s.Skip} {
		*p = diskOrder.Uint64(b[i*8:])
	}
	s.CRC = diskOrder.Uint32(b[56:])
	s.Codec = Compression(b[60])
	return s
}
func cloneObject(o *object) *object {
	if o == nil {
		return &object{}
	}
	n := *o
	n.dirtyFrom = -1
	n.dirtyUntil = 0
	n.Spans = append([]dataSpan(nil), o.Spans...)
	n.ListBlocks = append([]uint64(nil), o.ListBlocks...)
	return &n
}
func (v *Volume) loadObject(k Key) (*object, error) {
	if o := v.cache.get(k); o != nil {
		return o, nil
	}
	ptr, e := v.lookup(objectKey(k))
	if e != nil {
		return nil, e
	}
	if ptr == 0 {
		return nil, ErrNotFound
	}
	head := make([]byte, objectHeader)
	if e = v.readBytes(head, ptr); e != nil {
		return nil, e
	}
	n := diskOrder.Uint32(head)
	if n < objectHeader || uint64(n) > v.sb.Identity.Size || string(head[4:8]) != "OBJ2" {
		return nil, ErrCorrupt
	}
	b := make([]byte, n)
	if e = v.readBytes(b, ptr); e != nil {
		return nil, e
	}
	stored := diskOrder.Uint32(b[32:])
	diskOrder.PutUint32(b[32:], 0)
	if checksum(b) != stored {
		return nil, ErrCorrupt
	}
	o := &object{Base: ptr, Size: uint64(n), Info: Info{Version: diskOrder.Uint64(b[8:]), Length: diskOrder.Uint64(b[16:]), Compression: Compression(b[24]), Deleted: b[25]&1 != 0}, Segmented: b[25]&2 != 0}
	count := uint64(diskOrder.Uint32(b[28:]))
	if !o.Info.Compression.valid() {
		return nil, ErrCorrupt
	}
	if o.Segmented {
		if n != objectHeader+24 {
			return nil, ErrCorrupt
		}
		o.ListHead = diskOrder.Uint64(b[40:])
		o.ListTail = diskOrder.Uint64(b[48:])
		o.ListCount = diskOrder.Uint64(b[56:])
		off := o.ListHead
		seen := map[uint64]bool{}
		for off != 0 {
			if seen[off] || uint64(len(seen)) > v.sb.Identity.Size/listBytes {
				return nil, ErrCorrupt
			}
			seen[off] = true
			block := make([]byte, listBytes)
			if e = v.readBytes(block, off); e != nil {
				return nil, e
			}
			if string(block[:4]) != "SEG2" || checksum(block[:508]) != diskOrder.Uint32(block[508:]) {
				return nil, ErrCorrupt
			}
			used := int(diskOrder.Uint16(block[16:]))
			if used > listEntries {
				return nil, ErrCorrupt
			}
			o.ListBlocks = append(o.ListBlocks, off)
			for i := 0; i < used; i++ {
				o.Spans = append(o.Spans, decodeSpan(block[32+i*spanBytes:]))
			}
			next := diskOrder.Uint64(block[8:])
			if next == 0 && off != o.ListTail {
				return nil, ErrCorrupt
			}
			off = next
		}
		if uint64(len(o.Spans)) != o.ListCount || count != o.ListCount {
			return nil, ErrCorrupt
		}
	} else {
		if uint64(n) != objectHeader+count*spanBytes {
			return nil, ErrCorrupt
		}
		for i := uint64(0); i < count; i++ {
			o.Spans = append(o.Spans, decodeSpan(b[objectHeader+i*spanBytes:]))
		}
	}
	var last uint64
	for _, x := range o.Spans {
		if x.Length == 0 || x.Offset < last || x.Offset > o.Info.Length || x.Length > o.Info.Length-x.Offset || x.Location < dataStart || x.Capacity < x.Stored || x.Location > v.sb.Identity.Size || x.Capacity > v.sb.Identity.Size-x.Location || !x.Codec.valid() {
			return nil, ErrCorrupt
		}
		last = x.Offset + x.Length
	}
	v.cache.put(k, o)
	return o, nil
}
func (t *transaction) saveObject(k Key, o *object) error {
	count := len(o.Spans)
	n := objectHeader + count*spanBytes
	if o.Segmented {
		n = objectHeader + 24
		need := (count + listEntries - 1) / listEntries
		oldBlocks := len(o.ListBlocks)
		for len(o.ListBlocks) < need {
			off, e := t.alloc(listBytes, true)
			if e != nil {
				return e
			}
			o.ListBlocks = append(o.ListBlocks, off)
		}
		for len(o.ListBlocks) > need {
			last := len(o.ListBlocks) - 1
			t.release(o.ListBlocks[last], listBytes, true)
			o.ListBlocks = o.ListBlocks[:last]
		}
		o.ListHead = 0
		o.ListTail = 0
		o.ListCount = uint64(count)
		if need > 0 {
			o.ListHead = o.ListBlocks[0]
			o.ListTail = o.ListBlocks[need-1]
		}
		begin, endBlock := 0, need
		if oldBlocks > 0 && o.dirtyFrom >= 0 {
			begin = min(o.dirtyFrom/listEntries, need)
			endBlock = min((o.dirtyUntil+listEntries-1)/listEntries, need)
			if oldBlocks != need {
				begin = min(begin, max(0, min(oldBlocks, need)-1))
				endBlock = need
			}
		}
		for i := begin; i < endBlock; i++ {
			off := o.ListBlocks[i]
			b := make([]byte, listBytes)
			copy(b, "SEG2")
			if i+1 < need {
				diskOrder.PutUint64(b[8:], o.ListBlocks[i+1])
			}
			from := i * listEntries
			end := min(from+listEntries, count)
			diskOrder.PutUint16(b[16:], uint16(end-from))
			for j := from; j < end; j++ {
				encodeSpan(b[32+(j-from)*spanBytes:], o.Spans[j])
			}
			diskOrder.PutUint32(b[508:], checksum(b[:508]))
			old := make([]byte, listBytes)
			if e := t.readBytes(old, off); e != nil {
				return e
			}
			if !bytes.Equal(old, b) {
				if e := t.patch(off, b); e != nil {
					return e
				}
			}
		}
	}
	b := make([]byte, n)
	diskOrder.PutUint32(b, uint32(n))
	copy(b[4:], "OBJ2")
	diskOrder.PutUint64(b[8:], o.Info.Version)
	diskOrder.PutUint64(b[16:], o.Info.Length)
	b[24] = byte(o.Info.Compression)
	if o.Info.Deleted {
		b[25] |= 1
	}
	if o.Segmented {
		b[25] |= 2
	}
	diskOrder.PutUint32(b[28:], uint32(count))
	if o.Segmented {
		diskOrder.PutUint64(b[40:], o.ListHead)
		diskOrder.PutUint64(b[48:], o.ListTail)
		diskOrder.PutUint64(b[56:], o.ListCount)
	} else {
		for i, x := range o.Spans {
			encodeSpan(b[objectHeader+i*spanBytes:], x)
		}
	}
	diskOrder.PutUint32(b[32:], checksum(b))
	if o.Base == 0 || o.Size < uint64(n) {
		off, e := t.alloc(uint64(n), true)
		if e != nil {
			return e
		}
		if o.Base != 0 {
			t.release(o.Base, o.Size, true)
		}
		o.Base, o.Size = off, uint64(n)
	}
	if o.Size > uint64(n) {
		t.release(o.Base+uint64(n), o.Size-uint64(n), true)
		o.Size = uint64(n)
	}
	if e := t.patch(o.Base, b); e != nil {
		return e
	}
	return t.set(&t.sb.ObjectRoot, objectKey(k), o.Base)
}
func (t *transaction) readBytes(dst []byte, off uint64) error {
	for len(dst) > 0 {
		page := off / PageSize * PageSize
		b := t.pages[page]
		if b == nil {
			var e error
			b, e = t.v.readPage(page)
			if e != nil {
				return e
			}
		}
		at := int(off - page)
		n := min(len(dst), PageSize-at)
		copy(dst, b[at:at+n])
		dst = dst[n:]
		off += uint64(n)
	}
	return nil
}
func checksumsSize(s dataSpan) uint64 {
	if s.Checksums == 0 {
		return 0
	}
	return (s.Skip + s.Length + BlockSize - 1) / BlockSize * 4
}
func releaseSpan(t *transaction, s dataSpan) {
	t.release(s.Location, s.Capacity, false)
	if s.Checksums != 0 {
		t.release(s.Checksums, checksumsSize(s), true)
	}
}
func releaseData(t *transaction, o *object) {
	if o == nil {
		return
	}
	for _, s := range o.Spans {
		releaseSpan(t, s)
	}
	for _, off := range o.ListBlocks {
		t.release(off, listBytes, true)
	}
}

// storePacked reserves one contiguous allocation for all supplied logical data
// intervals, including a sparse object's compact physical representation.
type payloadPiece struct {
	offset, length uint64
	src            io.Reader
}

func (t *transaction) storePacked(pieces []payloadPiece, codec Compression) ([]dataSpan, error) {
	pieces = coalescePayloadPieces(pieces)
	if codec != Raw {
		return t.storeCompressed(pieces, codec)
	}
	var capacity uint64
	for _, p := range pieces {
		n := p.length
		if codec != Raw {
			chunks := (n + BlockSize - 1) / BlockSize
			if chunks > (^uint64(0)-8-n)/32 {
				return nil, ErrRange
			}
			n += 8 + chunks*32
		}
		var e error
		capacity, e = add(capacity, n)
		if e != nil {
			return nil, e
		}
	}
	if capacity == 0 {
		return nil, nil
	}
	base, e := t.alloc(capacity, false)
	if e != nil {
		return nil, e
	}
	at := base
	var spans []dataSpan
	buffer := make([]byte, BlockSize)
	for _, p := range pieces {
		if p.length == 0 {
			continue
		}
		s := dataSpan{Offset: p.offset, Length: p.length, Location: at, Codec: codec}
		chunks := (p.length + BlockSize - 1) / BlockSize
		table := []byte(nil)
		if codec != Raw {
			table = make([]byte, 8+chunks*32)
			diskOrder.PutUint64(table, chunks)
			at += uint64(len(table))
		} else if chunks > 1 {
			s.Checksums, e = t.alloc(chunks*4, true)
			if e != nil {
				return nil, e
			}
		}
		for i := uint64(0); i < chunks; i++ {
			n := min(uint64(BlockSize), p.length-i*BlockSize)
			raw := buffer[:n]
			if _, e = io.ReadFull(p.src, raw); e != nil {
				return nil, e
			}
			crc := checksum(raw)
			if codec == Raw {
				if e = t.writeBytes(at, raw); e != nil {
					return nil, e
				}
				if chunks == 1 {
					s.CRC = crc
				} else {
					var c [4]byte
					diskOrder.PutUint32(c[:], crc)
					if e = t.patch(s.Checksums+i*4, c[:]); e != nil {
						return nil, e
					}
				}
				at += n
			} else {
				encoded, e := compressBlock(codec, raw)
				if e != nil {
					return nil, e
				}
				actual := codec
				if len(encoded) >= len(raw) {
					encoded = raw
					actual = Raw
				}
				entry := table[8+i*32:]
				diskOrder.PutUint64(entry, at-s.Location)
				diskOrder.PutUint32(entry[8:], uint32(len(encoded)))
				diskOrder.PutUint32(entry[12:], uint32(n))
				diskOrder.PutUint32(entry[16:], crc)
				diskOrder.PutUint32(entry[20:], checksum(encoded))
				entry[24] = byte(actual)
				if e = t.writeBytes(at, encoded); e != nil {
					return nil, e
				}
				at += uint64(len(encoded))
			}
		}
		s.Stored = at - s.Location
		s.Capacity = s.Stored
		if codec != Raw {
			if e = t.patch(s.Location, table); e != nil {
				return nil, e
			}
		}
		spans = append(spans, s)
	}
	if used := at - base; used < capacity {
		t.release(at, capacity-used, false)
	}
	return spans, nil
}
func (v *Volume) readSpan(s dataSpan, dst []byte, offset uint64) error {
	if offset > s.Length || uint64(len(dst)) > s.Length-offset {
		return ErrRange
	}
	position := s.Skip + offset
	for len(dst) > 0 {
		index := position / BlockSize
		within := position % BlockSize
		var raw []byte
		if s.Codec == Raw {
			n := min(uint64(BlockSize), s.Stored-index*BlockSize)
			raw = make([]byte, n)
			if e := v.readBytes(raw, s.Location+index*BlockSize); e != nil {
				return e
			}
			crc := s.CRC
			if s.Checksums != 0 {
				var b [4]byte
				if e := v.readBytes(b[:], s.Checksums+index*4); e != nil {
					return e
				}
				crc = diskOrder.Uint32(b[:])
			}
			if checksum(raw) != crc {
				return ErrCorrupt
			}
		} else {
			entry := make([]byte, 32)
			if e := v.readBytes(entry, s.Location+8+index*32); e != nil {
				return e
			}
			off := diskOrder.Uint64(entry)
			n := diskOrder.Uint32(entry[8:])
			decoded := diskOrder.Uint32(entry[12:])
			if off > s.Stored || uint64(n) > s.Stored-off || decoded > BlockSize || n > BlockSize {
				return ErrCorrupt
			}
			encoded := make([]byte, n)
			if e := v.readBytes(encoded, s.Location+off); e != nil {
				return e
			}
			if checksum(encoded) != diskOrder.Uint32(entry[20:]) {
				return ErrCorrupt
			}
			var e error
			raw, e = decompressBlock(Compression(entry[24]), encoded, decoded)
			if e != nil {
				return e
			}
			if checksum(raw) != diskOrder.Uint32(entry[16:]) {
				return ErrCorrupt
			}
		}
		if within >= uint64(len(raw)) {
			return ErrCorrupt
		}
		n := min(len(dst), len(raw)-int(within))
		copy(dst, raw[within:within+uint64(n)])
		dst = dst[n:]
		position += uint64(n)
	}
	return nil
}
func (v *Volume) readObject(o *object, dst []byte, offset uint64) error {
	if offset > o.Info.Length || uint64(len(dst)) > o.Info.Length-offset {
		return ErrRange
	}
	clear(dst)
	end := offset + uint64(len(dst))
	for _, s := range o.Spans {
		start := max(offset, s.Offset)
		stop := min(end, s.Offset+s.Length)
		if stop <= start {
			continue
		}
		if e := v.readSpan(s, dst[start-offset:stop-offset], start-s.Offset); e != nil {
			return e
		}
	}
	return nil
}

type objectReader struct {
	v         *Volume
	o         *object
	off, left uint64
}

func (r *objectReader) Read(b []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	n := min(uint64(len(b)), r.left)
	if e := r.v.readObject(r.o, b[:n], r.off); e != nil {
		return 0, e
	}
	r.off += n
	r.left -= n
	return int(n), nil
}

func coalescePayloadPieces(pieces []payloadPiece) []payloadPiece {
	var out []payloadPiece
	for i := 0; i < len(pieces); {
		p := pieces[i]
		readers := []io.Reader{p.src}
		j := i + 1
		for j < len(pieces) && p.offset+p.length == pieces[j].offset {
			p.length += pieces[j].length
			readers = append(readers, pieces[j].src)
			j++
		}
		if len(readers) > 1 {
			p.src = io.MultiReader(readers...)
		}
		out = append(out, p)
		i = j
	}
	return out
}
