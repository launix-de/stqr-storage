// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/rand"
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
func (f *Filesystem) putImmutable(ctx context.Context, data []byte) (ObjectID, error) {
	if f.readOnly {
		return ObjectID{}, syscall.EROFS
	}
	id := newFSID()
	_, err := f.store.CAS(ctx, id, 0, nil, data, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
	return id, err
}
func (f *Filesystem) index(ctx context.Context, s fsSegment, depth uint8) ([]fsSegment, error) {
	data, _, err := f.store.Get(ctx, s.id)
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
		if s.lowerVersion != 0 {
			return f.readLower(ctx, s.id, s.lowerVersion, dst, s.offset+off)
		}
		if ranged, ok := f.store.(interface {
			GetRange(context.Context, ObjectID, uint64, uint64) ([]byte, ObjectInfo, error)
		}); ok {
			data, _, err := ranged.GetRange(ctx, s.id, uint64(s.offset)+off, uint64(len(dst)))
			if err != nil {
				return err
			}
			if len(data) != len(dst) {
				return ErrFSFormat
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
func (f *Filesystem) readBlob(ctx context.Context, b fsBlob, dst []byte, off uint64) (int, error) {
	if off >= b.length {
		return 0, io.EOF
	}
	n := int(min(uint64(len(dst)), b.length-off))
	if b.source != (ObjectID{}) {
		if err := f.readLower(ctx, b.source, b.sourceVersion, dst[:n], off); err != nil {
			return 0, err
		}
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
func (f *Filesystem) segments(ctx context.Context, b fsBlob) ([]fsSegment, error) {
	if b.length == 0 {
		return nil, nil
	}
	if b.source != (ObjectID{}) {
		return []fsSegment{{length: b.length, id: b.source, lowerVersion: b.sourceVersion}}, nil
	}
	if b.root != (ObjectID{}) {
		return f.flatten(ctx, fsSegment{length: b.length, id: b.root}, b.depth)
	}
	id, err := f.putImmutable(ctx, b.inline)
	if err != nil {
		return nil, err
	}
	return []fsSegment{{length: b.length, id: id}}, nil
}

func (f *Filesystem) readLower(ctx context.Context, id ObjectID, version uint64, dst []byte, offset uint64) error {
	if f.lower == nil {
		return ErrOverlayNeedsLower
	}
	n, info, err := f.lower.load(ctx, id)
	if err != nil {
		return err
	}
	if info.Version != version {
		return syscall.ESTALE
	}
	if n.kind != fsFile || offset > n.blob.length || uint64(len(dst)) > n.blob.length-offset {
		return ErrFSFormat
	}
	_, err = f.lower.readBlob(ctx, n.blob, dst, offset)
	return err
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
