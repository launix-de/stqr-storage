// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"sort"
)

const MaxBufferedRead = 16 << 20

func overlay(old []dataExtent, start, end uint64, fresh []dataExtent) []dataExtent {
	out := make([]dataExtent, 0, len(old)+len(fresh)+1)
	for _, e := range old {
		ee := e.Offset + e.Length
		if ee <= start || e.Offset >= end {
			out = append(out, e)
			continue
		}
		if e.Offset < start {
			left := e
			left.Length = start - e.Offset
			out = append(out, left)
		}
		if ee > end {
			right := e
			right.Skip += uint32(end - e.Offset)
			right.Offset = end
			right.Length = ee - end
			out = append(out, right)
		}
	}
	out = append(out, fresh...)
	sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
	return out
}
func applyObject(o *object, r record) error {
	if r.Op == opPut || r.Op == opSnapshot || r.Replace || r.Op == opDelete {
		o.Extents = nil
	}
	end, e := add(r.Offset, r.WriteLength)
	if e != nil {
		return ErrCorrupt
	}
	if r.Op == opSnapshot {
		o.Extents = append([]dataExtent(nil), r.Extents...)
	} else if r.Op != opTrunc && r.Op != opDelete {
		o.Extents = overlay(o.Extents, r.Offset, end, r.Extents)
	}
	o.Extents = overlay(o.Extents, r.Length, ^uint64(0), nil)
	o.Info = Info{Version: r.Version, Length: r.Length, Compression: r.Codec, Deleted: r.Deleted}
	o.Arena = r.Arena
	var last uint64
	for _, x := range o.Extents {
		if x.Offset < last || x.Length == 0 || x.Offset > o.Info.Length || x.Length > o.Info.Length-x.Offset {
			return ErrCorrupt
		}
		last = x.Offset + x.Length
	}
	return nil
}
func (v *Volume) readObject(o *object, dst []byte, offset uint64) error {
	if offset > o.Info.Length || uint64(len(dst)) > o.Info.Length-offset {
		return ErrRange
	}
	clear(dst)
	end := offset + uint64(len(dst))
	at := sort.Search(len(o.Extents), func(i int) bool { return o.Extents[i].Offset+o.Extents[i].Length > offset })
	for ; at < len(o.Extents) && o.Extents[at].Offset < end; at++ {
		e := o.Extents[at]
		raw, err := v.readBlock(v.sb, e)
		if err != nil {
			return err
		}
		start := max(offset, e.Offset)
		stop := min(end, e.Offset+e.Length)
		pos := uint64(e.Skip) + start - e.Offset
		if pos > uint64(len(raw)) || stop-start > uint64(len(raw))-pos {
			return ErrCorrupt
		}
		copy(dst[start-offset:stop-offset], raw[pos:pos+stop-start])
	}
	return nil
}

// Stat returns general object information. For metadata and payload protected by
// the same lock, use Open(false), Handle.Stat and Handle.CopyTo, then Close.
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

// ReadAt reads a bounded range. Sparse bytes are synthesized as zero. At/after
// EOF it returns io.EOF; callers wanting a clipped slice may use GetRange.
func (v *Volume) ReadAt(k Key, dst []byte, offset uint64) (int, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.readAtLocked(k, dst, offset)
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
func (v *Volume) GetRange(k Key, offset, length uint64) ([]byte, Info, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
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

// Get is a convenience for small objects. Large objects use CopyTo/ReadAt; the
// buffer limit does not limit logical object size or sparse offsets.
func (v *Volume) Get(k Key) ([]byte, Info, error) { return v.GetRange(k, 0, ^uint64(0)) }
func (v *Volume) Extents(k Key) ([]Extent, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
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
	for _, e := range o.Extents {
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
func (v *Volume) Put(k Key, data []byte, opts Options) (Info, error) {
	r, e := v.mutate(opPut, k, 0, uint64(len(data)), bytes.NewReader(data), nil, opts)
	return r.Info, e
}
func (v *Volume) PutReader(k Key, data io.Reader, length uint64, opts Options) (Info, error) {
	r, e := v.mutate(opPut, k, 0, length, data, nil, opts)
	return r.Info, e
}
func (v *Volume) PutRange(k Key, offset uint64, data []byte, opts Options) (Info, error) {
	r, e := v.mutate(opPutRange, k, offset, uint64(len(data)), bytes.NewReader(data), nil, opts)
	return r.Info, e
}
func (v *Volume) PutRangeReader(k Key, offset uint64, data io.Reader, length uint64, opts Options) (Info, error) {
	r, e := v.mutate(opPutRange, k, offset, length, data, nil, opts)
	return r.Info, e
}
func (v *Volume) Append(k Key, data []byte, opts Options) (Result, error) {
	return v.mutate(opAppend, k, 0, uint64(len(data)), bytes.NewReader(data), nil, opts)
}
func (v *Volume) AppendReader(k Key, data io.Reader, length uint64, opts Options) (Result, error) {
	return v.mutate(opAppend, k, 0, length, data, nil, opts)
}
func (v *Volume) Trunc(k Key, length uint64, opts Options) (Info, error) {
	r, e := v.mutate(opTrunc, k, 0, length, nil, nil, opts)
	return r.Info, e
}
func (v *Volume) Delete(k Key, opts Options) error {
	_, e := v.mutate(opDelete, k, 0, 0, nil, nil, opts)
	return e
}

// CAS compares and replaces an equal-length range. With WholeObject and an
// ExpectedVersion (or CreateOnly), it atomically replaces the entire object.
func (v *Volume) CAS(k Key, offset uint64, expected, replacement []byte, opts Options) (Info, error) {
	if e := validateCAS(offset, expected, replacement, opts); e != nil {
		return Info{}, e
	}
	r, e := v.mutate(opCAS, k, offset, uint64(len(replacement)), bytes.NewReader(replacement), expected, opts)
	return r.Info, e
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

func (v *Volume) mutate(op uint8, k Key, offset, length uint64, src io.Reader, expected []byte, opts Options) (Result, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.mutateWithRetryLocked(op, k, offset, length, src, expected, opts)
}

// mutateWithRetryLocked requires the volume's exclusive lock, also when reached
// through a write handle. Keep validation, compaction and retry identical.
func (v *Volume) mutateWithRetryLocked(op uint8, k Key, offset, length uint64, src io.Reader, expected []byte, opts Options) (Result, error) {
	if e := v.check(true); e != nil {
		return Result{}, e
	}
	if op != opTrunc && op != opDelete && length > 0 && (src == nil || length > uint64(^uint64(0)>>1)) {
		return Result{}, ErrRange
	}
	if opts.Compression != nil && !opts.Compression.valid() {
		return Result{}, ErrRange
	}
	var seeker io.Seeker
	var initial int64
	if x, ok := src.(io.Seeker); ok {
		seeker = x
		initial, _ = x.Seek(0, io.SeekCurrent)
	}
	if len(v.runs) >= maxIndexRuns {
		if e := v.compactLocked(); e != nil {
			return Result{}, e
		}
	}
	result, e := v.mutateLocked(op, k, offset, length, src, expected, opts)
	if errors.Is(e, ErrFull) && v.failed == nil && (src == nil || seeker != nil) {
		if err := v.compactLocked(); err != nil {
			return Result{}, err
		}
		if seeker != nil {
			if _, err := seeker.Seek(initial, io.SeekStart); err != nil {
				return Result{}, err
			}
		}
		return v.mutateLocked(op, k, offset, length, src, expected, opts)
	}
	return result, e
}
func (v *Volume) mutateLocked(op uint8, k Key, offset, length uint64, src io.Reader, expected []byte, opts Options) (Result, error) {
	h := fingerprint(op, k, offset, length, opts, expected)
	d, exists, e := v.loadDedupe(opts.RequestID)
	if e != nil {
		return Result{}, e
	}
	payload := op != opTrunc && op != opDelete
	if exists {
		if payload {
			if _, e = io.CopyN(h, src, int64min(length)); e != nil {
				return Result{}, e
			}
			if length > uint64(^uint64(0)>>1) {
				return Result{}, ErrRange
			}
		}
		var fp [32]byte
		copy(fp[:], h.Sum(nil))
		if d.Key != k || d.Fingerprint != fp {
			return Result{}, ErrRequestID
		}
		return d.Result, nil
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
	o := &object{}
	if old != nil {
		*o = *old
	}
	alive := old != nil && !old.Info.Deleted
	if !alive {
		o.Info.Length = 0
		o.Extents = nil
		o.Arena = arena{}
	}
	codec := o.Info.Compression
	if opts.Compression != nil {
		codec = *opts.Compression
	}
	if alive && codec != old.Info.Compression && op != opPut {
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
	replace := op == opPut || (op == opCAS && opts.WholeObject)
	if replace {
		newLength = length
		o.Arena = arena{}
	} else if payload {
		newLength = max(newLength, end)
	} else if op == opTrunc {
		newLength = length
	} else {
		newLength = 0
		o.Arena = arena{}
	}
	if op == opAppend && length == 0 && alive { // A no-op may still need a durable retry result.
		version = o.Info.Version
	}
	prev, e := v.lookup(objectKey(k))
	if e != nil {
		return Result{}, e
	}
	r := record{Op: op, Codec: codec, Key: k, Request: opts.RequestID, Version: version, Offset: offset, Length: newLength, Previous: prev, Replace: replace, Deleted: op == opDelete, Arena: o.Arena}
	if payload {
		r.WriteLength = length
	}
	s := v.sb
	buffer := make([]byte, BlockSize)
	remaining := uint64(0)
	if payload {
		remaining = length
	}
	for remaining > 0 {
		n := min(uint64(len(buffer)), remaining)
		if _, e = io.ReadFull(src, buffer[:n]); e != nil {
			return Result{}, e
		}
		h.Write(buffer[:n])
		ext, e := v.storeBlock(&s, &r.Arena, offset+(length-remaining), buffer[:n], codec, op == opAppend)
		if e != nil {
			return Result{}, e
		}
		r.Extents = append(r.Extents, ext)
		remaining -= n
	}
	if op == opTrunc && length < o.Info.Length {
		r.Arena = arena{}
	} // Retained data refs remain valid; drop spare append tail.
	copy(r.Fingerprint[:], h.Sum(nil))
	r.Result = Result{Info: Info{version, newLength, codec, r.Deleted}, Offset: offset}
	// Reserve includes payload, index delta flush and commit metadata. The final
	// check is repeated after journal/index staging inside commitWithReserve.
	if e = v.commitWithReserve(r, s, opts.UseGuaranteed); e != nil {
		return Result{}, e
	}
	return r.Result, nil
}
func int64min(n uint64) int64 {
	if n > uint64(^uint64(0)>>1) {
		return int64(^uint64(0) >> 1)
	}
	return int64(n)
}

// OperationStatus returns a stored successful result, if its RequestID has not
// been explicitly forgotten. Absence is not a proof that an operation never ran.
func (v *Volume) OperationStatus(id ID) (Result, bool, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return Result{}, false, e
	}
	d, ok, e := v.loadDedupe(id)
	return d.Result, ok, e
}
func (v *Volume) ForgetRequest(id ID) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
	}
	if id == (ID{}) {
		return ErrRange
	}
	if len(v.runs) >= maxIndexRuns {
		if e := v.compactLocked(); e != nil {
			return e
		}
	}
	return v.commitWithReserve(record{Op: opForget, Request: id}, v.sb, false)
}
