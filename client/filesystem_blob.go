// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"syscall"
)

func newFSID() ObjectID {
	var id ObjectID
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return id
}

// putImmutable allocates an identity for a genuinely new object or transient
// redo chunk. Publish its bytes durably before another object can reference it.
func (f *Filesystem) putImmutable(ctx context.Context, data []byte) (ObjectID, error) {
	ctx = context.WithValue(ctx, fsDurableKey{}, true)
	if f.readOnly {
		return ObjectID{}, syscall.EROFS
	}
	id := newFSID()
	_, err := f.store.CAS(ctx, id, 0, nil, data, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
	return id, err
}
func (f *Filesystem) index(ctx context.Context, s fsSegment, depth uint8) ([]fsSegment, error) {
	data, _, err := f.objectBytes(ctx, s.id)
	if err != nil {
		return nil, err
	}
	return decodeFSIndex(data, s.length, depth)
}
func (f *Filesystem) readSegment(ctx context.Context, s fsSegment, depth uint8, dst []byte, off uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if uint64(len(dst)) > s.length || off > s.length-uint64(len(dst)) {
		return ErrFSFormat
	}
	if s.id == (ObjectID{}) {
		clear(dst)
		return nil
	}
	if depth == 0 {
		if s.legacyVersion != 0 {
			return f.readLowerLegacy(ctx, s.id, dst, s.offset+off)
		}
		if s.lower {
			return f.readLower(ctx, s.id, dst, s.offset+off)
		}
		if writes, _ := ctx.Value(fsReadViewKey{}).(map[ObjectID]fsBlockWrite); len(writes) > 0 {
			data, _, e := f.objectBytes(ctx, s.id)
			if e != nil {
				return e
			}
			start := s.offset + off
			if start > uint64(len(data)) || uint64(len(dst)) > uint64(len(data))-start {
				if sparse, _ := ctx.Value(fsSparsePageKey{}).(bool); !sparse {
					return ErrFSFormat
				}
				clear(dst)
				if start < uint64(len(data)) {
					copy(dst, data[start:])
				}
				return nil
			}
			copy(dst, data[start:start+uint64(len(dst))])
			return nil
		}
		if ranged, ok := f.store.(interface {
			GetRange(context.Context, ObjectID, uint64, uint64) ([]byte, ObjectInfo, error)
		}); ok {
			data, _, err := ranged.GetRange(ctx, s.id, uint64(s.offset)+off, uint64(len(dst)))
			sparse, _ := ctx.Value(fsSparsePageKey{}).(bool)
			if sparse && errors.Is(err, ErrRange) {
				all, _, e := f.objectBytes(ctx, s.id)
				if e != nil {
					return e
				}
				at := s.offset + off
				clear(dst)
				if at < uint64(len(all)) {
					copy(dst, all[at:])
				}
				return nil
			}
			if err != nil {
				return err
			}
			if len(data) != len(dst) {
				if !sparse || len(data) > len(dst) {
					return ErrFSFormat
				}
				clear(dst)
			}
			copy(dst, data)
			return nil
		}
		data, _, err := f.store.Get(ctx, s.id)
		if err != nil {
			return err
		}
		start := uint64(s.offset) + off
		if start > uint64(len(data)) || uint64(len(dst)) > uint64(len(data))-start {
			if sparse, _ := ctx.Value(fsSparsePageKey{}).(bool); sparse {
				clear(dst)
				if start < uint64(len(data)) {
					copy(dst, data[start:])
				}
				return nil
			}
			return ErrFSFormat
		}
		copy(dst, data[start:start+uint64(len(dst))])
		return nil
	}
	children, err := f.index(ctx, s, depth)
	if err != nil {
		return err
	}
	for _, c := range children {
		if len(dst) == 0 {
			break
		}
		if off >= c.length {
			off -= c.length
			continue
		}
		n := min(uint64(len(dst)), c.length-off)
		if err = f.readSegment(ctx, c, depth-1, dst[:int(n)], off); err != nil {
			return err
		}
		dst = dst[int(n):]
		off = 0
	}
	return nil
}
func lowerDataFingerprint(n fsNode) [32]byte {
	e := Encoder{B: []byte("STQR readonly lower data v1\x00")}
	e.U64(n.mtime)
	e.U64(n.blob.length)
	e.B = append(e.B, n.blob.root[:]...)
	e.U8(n.blob.depth)
	e.Blob(n.blob.inline)
	return sha256.Sum256(e.B)
}
func (f *Filesystem) checkLowerAnchor(ctx context.Context, b fsBlob) error {
	if b.lowerOwner == (ObjectID{}) {
		return nil
	}
	if f.lower == nil {
		return ErrOverlayNeedsLower
	}
	n, _, e := f.lower.load(ctx, b.lowerOwner)
	if e != nil {
		return e
	}
	if lowerDataFingerprint(n) != b.lowerDigest {
		return syscall.ESTALE
	}
	return nil
}
func (f *Filesystem) readBlob(ctx context.Context, b fsBlob, dst []byte, off uint64) (int, error) {
	if e := f.checkLowerAnchor(ctx, b); e != nil {
		return 0, e
	}
	n, e := f.readBlobUnchecked(ctx, b, dst, off)
	if check := f.checkLowerAnchor(ctx, b); check != nil {
		return n, check
	}
	return n, e
}
func (f *Filesystem) readBlobUnchecked(ctx context.Context, b fsBlob, dst []byte, off uint64) (int, error) {
	if b.mutable {
		ctx = context.WithValue(ctx, fsSparsePageKey{}, true)
	}
	if b.view != nil {
		ctx = context.WithValue(ctx, fsReadViewKey{}, b.view)
	}
	if off >= b.length {
		return 0, io.EOF
	}
	n := int(min(uint64(len(dst)), b.length-off))
	if b.source != (ObjectID{}) {
		return n, f.readLowerLegacy(ctx, b.source, dst[:n], off)
	}
	if b.lower {
		if f.lower == nil {
			return 0, ErrOverlayNeedsLower
		}
		b.lower = false
		b.lowerOwner = ObjectID{}
		return f.lower.readBlob(ctx, b, dst, off)
	} else if b.root == (ObjectID{}) {
		copy(dst[:n], b.inline[off:off+uint64(n)])
	} else if err := f.readSegment(ctx, fsSegment{length: b.length, id: b.root}, b.depth, dst[:n], off); err != nil {
		return 0, err
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}
func (f *Filesystem) flatten(ctx context.Context, s fsSegment, depth uint8) ([]fsSegment, error) {
	if depth == 0 || s.id == (ObjectID{}) {
		return []fsSegment{s}, nil
	}
	children, err := f.index(ctx, s, depth)
	if err != nil {
		return nil, err
	}
	var out []fsSegment
	for _, c := range children {
		part, err := f.flatten(ctx, c, depth-1)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}
func sliceFSSegments(in []fsSegment, start, end uint64) []fsSegment {
	var out []fsSegment
	var pos uint64
	for _, s := range in {
		next := pos + s.length
		if next > start && pos < end {
			a, b := max(start, pos)-pos, min(end, next)-pos
			s.length = b - a
			if s.id != (ObjectID{}) {
				s.offset += a
			}
			out = append(out, s)
		}
		pos = next
		if pos >= end {
			break
		}
	}
	return out
}
func (f *Filesystem) readLowerLegacy(ctx context.Context, id ObjectID, dst []byte, offset uint64) error {
	if f.lower == nil {
		return ErrOverlayNeedsLower
	}
	n, _, err := f.lower.load(ctx, id)
	if err != nil {
		return err
	}
	if n.kind != fsFile || offset > n.blob.length || uint64(len(dst)) > n.blob.length-offset {
		return ErrFSFormat
	}
	_, err = f.lower.readBlob(ctx, n.blob, dst, offset)
	if err != nil {
		return err
	}
	return f.lower.checkNodeView(ctx, n)
}
func (f *Filesystem) readLower(ctx context.Context, id ObjectID, dst []byte, offset uint64) error {
	if f.lower == nil {
		return ErrOverlayNeedsLower
	}
	return f.lower.readSegment(ctx, fsSegment{id: id, offset: offset, length: uint64(len(dst))}, 0, dst, 0)
}

func (f *Filesystem) dataSegments(ctx context.Context, data []byte) ([]fsSegment, error) {
	var out []fsSegment
	for len(data) > 0 {
		n := min(len(data), FSObjectSize)
		id, err := f.putImmutable(ctx, data[:n])
		if err != nil {
			return nil, err
		}
		out = append(out, fsSegment{length: uint64(n), id: id})
		data = data[n:]
	}
	return out, nil
}
func (f *Filesystem) buildBlob(ctx context.Context, segments []fsSegment, length uint64) (fsBlob, error) {
	b := fsBlob{length: length}
	if length <= fsInlineSize {
		b.inline = make([]byte, int(length))
		pos := 0
		for _, s := range segments {
			if err := f.readSegment(ctx, s, 0, b.inline[pos:pos+int(s.length)], 0); err != nil {
				return fsBlob{}, err
			}
			pos += int(s.length)
		}
		return b, nil
	}
	depth := uint8(0)
	for {
		depth++
		if depth > fsMaxDepth {
			return fsBlob{}, ErrFSFormat
		}
		var next []fsSegment
		for start := 0; start < len(segments); start += fsFanout {
			part := segments[start:min(start+fsFanout, len(segments))]
			var n uint64
			for _, s := range part {
				n += s.length
			}
			id, err := f.putImmutable(ctx, encodeFSIndex(part))
			if err != nil {
				return fsBlob{}, err
			}
			next = append(next, fsSegment{length: n, id: id})
		}
		if len(next) == 1 {
			b.root = next[0].id
			b.depth = depth
			return b, nil
		}
		segments = next
	}
}
func (f *Filesystem) blobFromBytes(ctx context.Context, data []byte) (fsBlob, error) {
	if len(data) <= fsInlineSize {
		return fsBlob{length: uint64(len(data)), inline: append([]byte(nil), data...)}, nil
	}
	s, err := f.dataSegments(ctx, data)
	if err != nil {
		return fsBlob{}, err
	}
	return f.buildBlob(ctx, s, uint64(len(data)))
}

func (f *Filesystem) segments(ctx context.Context, b fsBlob) ([]fsSegment, error) {
	if b.length == 0 {
		return nil, nil
	}
	if b.root != (ObjectID{}) || b.source != (ObjectID{}) || b.lower {
		return f.segmentsReadOnly(ctx, b)
	}
	id, e := f.putImmutable(ctx, b.inline)
	if e != nil {
		return nil, e
	}
	return []fsSegment{{id: id, length: b.length}}, nil
}
