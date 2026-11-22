// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
)

const MaxBufferedRead = 16 << 20
const (
	opPut uint8 = iota + 1
	opPutRange
	opAppend
	opTrunc
	opDelete
	opCAS
	opSnapshot
	opDedupe
	opForget
)

func (v *Volume) Stat(k Key) (Info, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.statLocked(k)
}
func (v *Volume) statLocked(k Key) (Info, error) {
	if e := v.check(false); e != nil {
		return Info{}, e
	}
	o, e := v.loadObject(k)
	if e != nil {
		return Info{}, e
	}
	if o.Info.Deleted {
		return Info{}, ErrNotFound
	}
	return o.Info, nil
}

func (v *Volume) readAtLocked(k Key, dst []byte, offset uint64) (int, error) {
	if e := v.check(false); e != nil {
		return 0, e
	}
	o, e := v.loadObject(k)
	if e != nil {
		return 0, e
	}
	if o.Info.Deleted {
		return 0, ErrNotFound
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if offset >= o.Info.Length {
		return 0, io.EOF
	}
	n := min(uint64(len(dst)), o.Info.Length-offset)
	if e = v.readObject(o, dst[:n], offset); e != nil {
		return 0, e
	}
	if n < uint64(len(dst)) {
		return int(n), io.EOF
	}
	return int(n), nil
}
func (v *Volume) getRangeLocked(k Key, offset, length uint64) ([]byte, Info, error) {
	if e := v.check(false); e != nil {
		return nil, Info{}, e
	}
	o, e := v.loadObject(k)
	if e != nil {
		return nil, Info{}, e
	}
	if o.Info.Deleted {
		return nil, Info{}, ErrNotFound
	}
	if offset >= o.Info.Length {
		return []byte{}, o.Info, nil
	}
	length = min(length, o.Info.Length-offset)
	if length > MaxBufferedRead {
		return nil, Info{}, ErrRange
	}
	out := make([]byte, int(length))
	e = v.readObject(o, out, offset)
	return out, o.Info, e
}

func (v *Volume) extentsLocked(k Key) ([]Extent, error) {
	if e := v.check(false); e != nil {
		return nil, e
	}
	o, e := v.loadObject(k)
	if e != nil {
		return nil, e
	}
	if o.Info.Deleted {
		return nil, ErrNotFound
	}
	var out []Extent
	var at uint64
	for _, e := range o.Spans {
		if e.Offset > at {
			out = append(out, Extent{at, e.Offset - at, true})
		}
		if len(out) > 0 && !out[len(out)-1].Zero && out[len(out)-1].Offset+out[len(out)-1].Length == e.Offset {
			out[len(out)-1].Length += e.Length
		} else {
			out = append(out, Extent{e.Offset, e.Length, false})
		}
		at = e.Offset + e.Length
	}
	if at < o.Info.Length {
		out = append(out, Extent{at, o.Info.Length - at, true})
	}
	return out, nil
}
func precondition(old *object, o Options) error {
	exists := old != nil && !old.Info.Deleted
	if o.CreateOnly && o.ExpectedVersion != nil {
		return ErrRange
	}
	if o.CreateOnly && exists {
		return ErrPrecondition
	}
	var version, length uint64
	if old != nil {
		version = old.Info.Version
	}
	if exists {
		length = old.Info.Length
	}
	if o.ExpectedVersion != nil && *o.ExpectedVersion != version {
		return ErrPrecondition
	}
	if o.ExpectedLength != nil && *o.ExpectedLength != length {
		return ErrPrecondition
	}
	return nil
}
func fingerprint(op uint8, k Key, offset, length uint64, opts Options, expected []byte) hash.Hash {
	h := sha256.New()
	h.Write([]byte{op})
	h.Write(k.Bucket[:])
	h.Write(k.Object[:])
	var b [8]byte
	for _, x := range []uint64{offset, length} {
		binary.LittleEndian.PutUint64(b[:], x)
		h.Write(b[:])
	}
	for _, x := range []*uint64{opts.ExpectedVersion, opts.ExpectedLength} {
		if x == nil {
			h.Write([]byte{0})
		} else {
			h.Write([]byte{1})
			binary.LittleEndian.PutUint64(b[:], *x)
			h.Write(b[:])
		}
	}
	flags := byte(0)
	if opts.CreateOnly {
		flags |= 1
	}
	if opts.WholeObject {
		flags |= 2
	}
	if opts.restoreSameVersion {
		flags |= 4
	}
	h.Write([]byte{flags})
	if opts.Compression == nil {
		h.Write([]byte{0})
	} else {
		h.Write([]byte{1, byte(*opts.Compression)})
	}
	binary.LittleEndian.PutUint64(b[:], uint64(len(expected)))
	h.Write(b[:])
	h.Write(expected)
	return h
}
func validateCAS(offset uint64, expected, replacement []byte, opts Options) error {
	if len(replacement) > 1<<20 || len(expected) > 1<<20 {
		return ErrRange
	}
	if !opts.WholeObject && len(expected) != len(replacement) {
		return ErrRange
	}
	if opts.WholeObject && (offset != 0 || (!opts.CreateOnly && opts.ExpectedVersion == nil)) {
		return ErrRange
	}
	return nil
}

const receiptBytes = 144

func (v *Volume) loadDedupe(id ID) (dedupe, bool, error) {
	var d dedupe
	if id == (ID{}) {
		return d, false, nil
	}
	ptr, e := v.lookup(requestKey(id))
	if e != nil || ptr == 0 {
		return d, false, e
	}
	b := make([]byte, receiptBytes)
	if e = v.readBytes(b, ptr); e != nil {
		return d, false, e
	}
	if string(b[:4]) != "REQ2" || checksum(b[:140]) != diskOrder.Uint32(b[140:]) {
		return d, false, ErrCorrupt
	}
	copy(d.Key.Bucket[:], b[4:20])
	copy(d.Key.Object[:], b[20:52])
	copy(d.Fingerprint[:], b[52:84])
	d.Result = Result{Info: Info{Version: diskOrder.Uint64(b[84:]), Length: diskOrder.Uint64(b[92:]), Compression: Compression(b[100]), Deleted: b[101] != 0}, Offset: diskOrder.Uint64(b[104:])}
	return d, true, nil
}
func (t *transaction) saveReceipt(id ID, d dedupe) error {
	if id == (ID{}) {
		return nil
	}
	b := make([]byte, receiptBytes)
	copy(b, "REQ2")
	copy(b[4:20], d.Key.Bucket[:])
	copy(b[20:52], d.Key.Object[:])
	copy(b[52:84], d.Fingerprint[:])
	diskOrder.PutUint64(b[84:], d.Result.Info.Version)
	diskOrder.PutUint64(b[92:], d.Result.Info.Length)
	b[100] = byte(d.Result.Info.Compression)
	if d.Result.Info.Deleted {
		b[101] = 1
	}
	diskOrder.PutUint64(b[104:], d.Result.Offset)
	diskOrder.PutUint32(b[140:], checksum(b[:140]))
	off, e := t.alloc(receiptBytes, true)
	if e != nil {
		return e
	}
	if e = t.patch(off, b); e != nil {
		return e
	}
	return t.set(&t.sb.ObjectRoot, requestKey(id), off)
}
func (v *Volume) operationStatusLocked(id ID) (Result, bool, error) {
	d, found, e := v.loadDedupe(id)
	return d.Result, found, e
}
func (v *Volume) forgetRequestLocked(id ID) error {
	if id == (ID{}) {
		return ErrRange
	}
	off, e := v.lookup(requestKey(id))
	if e != nil || off == 0 {
		return e
	}
	t := v.transaction()
	t.release(off, receiptBytes, true)
	if e = t.remove(&t.sb.ObjectRoot, requestKey(id)); e != nil {
		return e
	}
	return t.commit(false)
}
func oldPieces(v *Volume, o *object, limit, cutStart, cutEnd uint64) []payloadPiece {
	var pieces []payloadPiece
	if o == nil || o.Info.Deleted {
		return pieces
	}
	for _, s := range o.Spans {
		end := min(s.Offset+s.Length, limit)
		start := s.Offset
		if start >= end {
			continue
		}
		if cutStart < cutEnd && start < cutEnd && cutStart < end {
			if start < cutStart {
				n := cutStart - start
				pieces = append(pieces, payloadPiece{start, n, &objectReader{v, o, start, n}})
			}
			start = max(start, cutEnd)
		}
		if start < end {
			pieces = append(pieces, payloadPiece{start, end - start, &objectReader{v, o, start, end - start}})
		}
	}
	return pieces
}
func (v *Volume) mutateWithRetryLocked(op uint8, k Key, offset, length uint64, src io.Reader, expected []byte, opts Options) (Result, error) {
	if e := v.check(true); e != nil {
		return Result{}, e
	}
	if opts.Compression != nil && !opts.Compression.valid() {
		return Result{}, ErrRange
	}
	payload := op != opTrunc && op != opDelete
	if payload && (length > uint64(^uint64(0)>>1) || length > 0 && src == nil) {
		return Result{}, ErrRange
	}
	if payload {
		if _, e := add(offset, length); e != nil {
			return Result{}, e
		}
	}
	h := fingerprint(op, k, offset, length, opts, expected)
	prior, retry, e := v.loadDedupe(opts.RequestID)
	if e != nil {
		return Result{}, e
	}
	if retry {
		if payload && length > 0 {
			if _, e = io.CopyN(h, src, int64(length)); e != nil {
				return Result{}, e
			}
		}
		var fp [32]byte
		copy(fp[:], h.Sum(nil))
		if prior.Key != k || prior.Fingerprint != fp {
			return Result{}, ErrRequestID
		}
		return prior.Result, nil
	}
	old, e := v.loadObject(k)
	if errors.Is(e, ErrNotFound) {
		old = nil
	} else if e != nil {
		return Result{}, e
	}
	if e = precondition(old, opts); e != nil {
		return Result{}, e
	}
	o := cloneObject(old)
	alive := old != nil && !old.Info.Deleted
	if !alive {
		o.Info.Length = 0
		o.Spans = nil
		o.ListBlocks = nil
		o.Segmented = false
	}
	codec := o.Info.Compression
	if opts.Compression != nil {
		codec = *opts.Compression
	}
	replace := op == opPut || op == opCAS && opts.WholeObject
	if alive && codec != old.Info.Compression && !replace {
		return Result{}, ErrPrecondition
	}
	if op == opAppend {
		offset = o.Info.Length
	}
	end, e := add(offset, length)
	if e != nil && payload {
		return Result{}, e
	}
	if op == opCAS && !opts.WholeObject {
		if !alive || end > o.Info.Length {
			return Result{}, ErrPrecondition
		}
		actual := make([]byte, len(expected))
		if e = v.readObject(o, actual, offset); e != nil {
			return Result{}, e
		}
		if !bytes.Equal(actual, expected) {
			return Result{}, ErrPrecondition
		}
	}
	version := o.Info.Version + 1
	if version == 0 {
		return Result{}, ErrRange
	}
	newLength := o.Info.Length
	if replace {
		newLength = length
	} else if payload {
		newLength = max(newLength, end)
	} else if op == opTrunc {
		newLength = length
	} else {
		newLength = 0
	}
	if op == opAppend && length == 0 && alive {
		version = o.Info.Version
	}
	t := v.transaction()
	defer t.close()
	reader := src
	if payload && length > 0 {
		reader = io.TeeReader(src, h)
	}
	if op == opAppend {
		o.Segmented = true
		o.dirtyFrom = max(0, len(o.Spans)-1)
		if length > 0 {
			if e = t.appendPayload(o, offset, reader, length); e != nil {
				return Result{}, e
			}
		}
		o.dirtyUntil = len(o.Spans)
	} else if op == opPutRange || op == opCAS && !opts.WholeObject {
		if e = t.writeRange(o, offset, length, reader, codec); e != nil {
			return Result{}, e
		}
	} else {
		var pieces []payloadPiece
		if replace {
			if length > 0 {
				pieces = []payloadPiece{{0, length, reader}}
			}
			o.Segmented = false
		} else if op == opDelete {
			o.Segmented = false
		} else if op == opTrunc {
			pieces = oldPieces(v, old, newLength, 0, 0)
		}
		spans, e := t.storePacked(pieces, codec)
		if e != nil {
			return Result{}, e
		}
		releaseData(t, old)
		o.Spans = spans
		o.ListBlocks = nil
		o.ListHead = 0
		o.ListTail = 0
	}
	o.Info = Info{Version: version, Length: newLength, Compression: codec, Deleted: op == opDelete}
	if old != nil && !old.Info.Deleted {
		t.sb.Objects--
		t.sb.LogicalBytes -= old.Info.Length
	}
	if !o.Info.Deleted {
		if newLength > ^uint64(0)-t.sb.LogicalBytes {
			return Result{}, ErrRange
		}
		t.sb.Objects++
		t.sb.LogicalBytes += newLength
	}
	t.sb.ArenaUnused = arenaAfter(t.sb.ArenaUnused, old, o)
	if e = t.saveObject(k, o); e != nil {
		return Result{}, e
	}
	result := Result{Info: o.Info, Offset: offset}
	var fp [32]byte
	copy(fp[:], h.Sum(nil))
	if e = t.saveReceipt(opts.RequestID, dedupe{k, fp, result}); e != nil {
		return Result{}, e
	}
	if e = t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance, opts.GuaranteedAllowance); e != nil {
		return Result{}, e
	}
	v.cache.put(k, o)
	return result, nil
}
func arenaUnused(o *object) uint64 {
	var n uint64
	if o != nil && o.Segmented {
		for _, s := range o.Spans {
			n += s.Capacity - s.Stored
		}
	}
	return n
}
func arenaAfter(total uint64, old, next *object) uint64 {
	return total - arenaUnused(old) + arenaUnused(next)
}
func (t *transaction) appendPayload(o *object, offset uint64, src io.Reader, length uint64) error {
	if len(o.Spans) > 0 {
		last := &o.Spans[len(o.Spans)-1]
		if !last.Direct && last.Codec == Raw && last.Skip == 0 && last.Offset+last.Length == offset && length <= last.Capacity-last.Stored {
			// Continue the stored CRC with only the new bytes. Reading the old
			// arena would turn each tiny append into an ever larger read.
			oldLength := last.Length
			oldCount := (oldLength + BlockSize - 1) / BlockSize
			count := (oldLength + length + BlockSize - 1) / BlockSize
			oldChecks := checksumsSize(*last)
			oldTable := last.Checksums
			if count > 1 && count*4 != oldChecks {
				next, e := t.alloc(count*4, true)
				if e != nil {
					return e
				}
				if oldChecks > 0 {
					buf := make([]byte, min(uint64(64<<10), oldChecks))
					for off := uint64(0); off < oldChecks; {
						n := min(uint64(len(buf)), oldChecks-off)
						if e = t.readBytes(buf[:n], oldTable+off); e != nil {
							return e
						}
						if e = t.patch(next+off, buf[:n]); e != nil {
							return e
						}
						off += n
					}
					t.release(oldTable, oldChecks, true)
				} else if oldCount == 1 {
					var b [4]byte
					diskOrder.PutUint32(b[:], last.CRC)
					if e = t.patch(next, b[:]); e != nil {
						return e
					}
				}
				last.Checksums = next
			}
			position := oldLength
			remaining := length
			buf := make([]byte, min(uint64(BlockSize), length))
			for remaining > 0 {
				index := position / BlockSize
				within := position % BlockSize
				crc := uint32(0)
				if within > 0 {
					if oldCount == 1 && index == 0 {
						crc = last.CRC
					} else {
						var b [4]byte
						if e := t.readBytes(b[:], last.Checksums+index*4); e != nil {
							return e
						}
						crc = diskOrder.Uint32(b[:])
					}
				}
				n := min(uint64(len(buf)), remaining, uint64(BlockSize)-within)
				raw := buf[:n]
				if _, e := io.ReadFull(src, raw); e != nil {
					return e
				}
				crc = crc32.Update(crc, crcTable, raw)
				if e := t.writeBytes(last.Location+position, raw); e != nil {
					return e
				}
				if count == 1 {
					last.CRC = crc
				} else {
					var b [4]byte
					diskOrder.PutUint32(b[:], crc)
					if e := t.patch(last.Checksums+index*4, b[:]); e != nil {
						return e
					}
				}
				position += n
				remaining -= n
			}
			last.Length += length
			last.Stored += length
			return nil
		}
	}
	capacity := align(length)
	if capacity < PageSize {
		capacity = PageSize
	}
	if n := len(o.Spans); n > 0 {
		capacity = max(capacity, min(o.Spans[n-1].Capacity*2, uint64(BlockSize)))
	}
	base, e := t.alloc(capacity, false)
	if e != nil {
		return e
	}
	s := dataSpan{Offset: offset, Length: length, Location: base, Stored: length, Capacity: capacity, Codec: Raw}
	count := (length + BlockSize - 1) / BlockSize
	if count > 1 {
		s.Checksums, e = t.alloc(count*4, true)
		if e != nil {
			return e
		}
	}
	buf := make([]byte, BlockSize)
	for i := uint64(0); i < count; i++ {
		n := min(uint64(BlockSize), length-i*BlockSize)
		raw := buf[:n]
		if _, e = io.ReadFull(src, raw); e != nil {
			return e
		}
		if e = t.writeBytes(base+i*BlockSize, raw); e != nil {
			return e
		}
		crc := checksum(raw)
		if count == 1 {
			s.CRC = crc
		} else {
			var b [4]byte
			diskOrder.PutUint32(b[:], crc)
			if e = t.patch(s.Checksums+i*4, b[:]); e != nil {
				return e
			}
		}
	}
	o.Spans = append(o.Spans, s)
	return nil
}
