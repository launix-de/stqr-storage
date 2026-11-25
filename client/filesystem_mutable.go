// SPDX-License-Identifier: GPL-3.0-only
package client

// Mutable filesystem blocks are protected by the inode's recoverable intent.
// The intent contains redo bytes, not a new persistent identity for the data.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"syscall"
)

type fsBlockWrite struct {
	id     ObjectID
	absent bool
	full   bool
	remove bool
	offset uint64
	data   []byte
	before []byte
}
type fsReadViewKey struct{}
type fsSparsePageKey struct{}

func fsStableID(owner ObjectID, kind byte, level uint8, slot uint64) ObjectID {
	b := append([]byte("STQR mutable filesystem block v1\x00"), owner[:]...)
	b = append(b, kind, level)
	b = binary.LittleEndian.AppendUint64(b, slot)
	return sha256.Sum256(b)
}
func (f *Filesystem) blockInfo(ctx context.Context, id ObjectID) (ObjectInfo, error) {
	if s, ok := f.store.(interface {
		Stat(context.Context, ObjectID) (ObjectInfo, error)
	}); ok {
		return s.Stat(ctx, id)
	}
	_, info, err := f.store.Get(ctx, id)
	return info, err
}
func fsWriteView(writes []fsBlockWrite) map[ObjectID]fsBlockWrite {
	out := make(map[ObjectID]fsBlockWrite, len(writes))
	for _, w := range writes {
		out[w.id] = w
	}
	return out
}
func (f *Filesystem) objectBytes(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	writes, _ := ctx.Value(fsReadViewKey{}).(map[ObjectID]fsBlockWrite)
	w, patch := writes[id]
	if patch && w.full {
		return bytes.Clone(w.data), ObjectInfo{Length: uint64(len(w.data))}, nil
	}
	data, info, err := f.store.Get(ctx, id)
	if patch && w.absent && errors.Is(err, ErrNotFound) {
		data = nil
		err = nil
	}
	if err != nil {
		return nil, info, err
	}
	if patch {
		if w.remove {
			return nil, info, ErrNotFound
		}
		end := w.offset + uint64(len(w.data))
		if end > uint64(len(data)) {
			data = append(data, make([]byte, end-uint64(len(data)))...)
		}
		copy(data[w.offset:], w.data)
		info.Length = uint64(len(data))
	}
	return data, info, nil
}
func (f *Filesystem) stageBlock(ctx context.Context, n *fsNode, id ObjectID, offset uint64, data []byte, full bool) error {
	var before []byte
	_, nativePut := f.store.(interface {
		Put(context.Context, ObjectID, []byte, PutOptions) (ObjectInfo, error)
	})
	var err error
	if nativePut && !full {
		_, err = f.blockInfo(ctx, id)
	} else {
		before, _, err = f.store.Get(ctx, id)
	}
	absent := errors.Is(err, ErrNotFound)
	if err != nil && !absent {
		return err
	}
	// Index pages and directory pages are compared before staging. A stable page
	// which did not change must cause neither a mutation nor another native version.
	if full && !absent {
		old := before
		if bytes.Equal(old, data) {
			return nil
		}
		if len(old) == len(data) {
			first, last := 0, len(data)
			for first < last && old[first] == data[first] {
				first++
			}
			for last > first && old[last-1] == data[last-1] {
				last--
			}
			offset = uint64(first)
			data = data[first:last]
			full = false
		}
	}
	if _, ranged := f.store.(interface {
		PutRange(context.Context, ObjectID, uint64, []byte, PutOptions) (ObjectInfo, error)
	}); !ranged && !full {
		raw, _, e := f.store.Get(ctx, id)
		if absent && errors.Is(e, ErrNotFound) {
			e = nil
			raw = nil
		}
		if e != nil {
			return e
		}
		end := offset + uint64(len(data))
		if end > uint64(len(raw)) {
			raw = append(raw, make([]byte, end-uint64(len(raw)))...)
		}
		copy(raw[offset:], data)
		data = raw
		offset = 0
		full = true
	}
	if nativePut {
		before = nil
	}
	n.edits = append(n.edits, fsBlockWrite{id: id, before: bytes.Clone(before), absent: absent, full: full, offset: offset, data: bytes.Clone(data)})
	return nil
}
func (f *Filesystem) stageRemoval(ctx context.Context, n *fsNode, id ObjectID) error {
	before, _, err := f.store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	n.edits = append(n.edits, fsBlockWrite{id: id, before: bytes.Clone(before), remove: true})
	return nil
}
func (f *Filesystem) indexIDs(ctx context.Context, b fsBlob) ([]ObjectID, error) {
	var out []ObjectID
	var walk func(ObjectID, uint64, uint8) error
	walk = func(id ObjectID, length uint64, depth uint8) error {
		if b.lower || b.source != (ObjectID{}) || depth == 0 || id == (ObjectID{}) {
			return nil
		}
		out = append(out, id)
		parts, e := f.index(ctx, fsSegment{id: id, length: length}, depth)
		if e != nil {
			return e
		}
		for _, part := range parts {
			if e = walk(part.id, part.length, depth-1); e != nil {
				return e
			}
		}
		return nil
	}
	err := walk(b.root, b.length, b.depth)
	return out, err
}
func (f *Filesystem) finishStableBlob(ctx context.Context, n *fsNode, old fsBlob, segments []fsSegment, length uint64) error {
	b := fsBlob{length: length, mutable: true}
	var logical uint64
	for i, part := range segments {
		if part.legacyVersion != 0 {
			data := make([]byte, int(part.length))
			if e := f.readSegment(ctx, part, 0, data, 0); e != nil {
				return e
			}
			id := fsStableID(n.identity, 2, 0, logical/4096)
			if e := f.stageBlock(ctx, n, id, 0, data, true); e != nil {
				return e
			}
			segments[i] = fsSegment{id: id, length: part.length}
		}
		logical += part.length
	}

	for _, s := range segments {
		if s.lower {
			b.lowerOwner = old.lowerOwner
			b.lowerDigest = old.lowerDigest
			break
		}
	}
	keep := map[ObjectID]bool{}
	for _, s := range segments {
		if s.id != (ObjectID{}) && !s.lower && s.legacyVersion == 0 {
			keep[s.id] = true
		}
	}
	if length <= fsInlineSize {
		b.inline = make([]byte, int(length))
		pos := 0
		view := context.WithValue(ctx, fsReadViewKey{}, fsWriteView(n.edits))
		for _, s := range segments {
			if e := f.readSegment(view, s, 0, b.inline[pos:pos+int(s.length)], 0); e != nil {
				return e
			}
			pos += int(s.length)
		}
		// Inline data no longer references any data block.
		keep = map[ObjectID]bool{}
	} else {
		level := uint8(0)
		for {
			level++
			if level > fsMaxDepth {
				return ErrFSFormat
			}
			var next []fsSegment
			for start := 0; start < len(segments); start += fsFanout {
				part := segments[start:min(start+fsFanout, len(segments))]
				var size uint64
				for _, s := range part {
					size += s.length
				}
				id := fsStableID(n.identity, 1, level, uint64(start/fsFanout))
				keep[id] = true
				if e := f.stageBlock(ctx, n, id, 0, encodeFSIndex(part), true); e != nil {
					return e
				}
				next = append(next, fsSegment{id: id, length: size})
			}
			if len(next) == 1 {
				b.root, b.depth = next[0].id, level
				break
			}
			segments = next
		}
	}
	// Only truly retired pages/blocks are reclaimed. Untouched blocks keep their
	// ID and version even when another part of the inode changes.
	owned, e := f.ownedBlockIDs(ctx, n.identity, old)
	if e != nil {
		return e
	}
	retired := map[ObjectID]bool{}
	for _, id := range owned {
		if !keep[id] {
			retired[id] = true
		}
	}
	for id := range retired {
		if e = f.stageRemoval(ctx, n, id); e != nil {
			return e
		}
	}
	n.blob = b
	return nil
}

// Reading an inline old blob must never allocate an "immutable" copy merely to
// edit it. Inline conversion is handled by the actual affected data page.
func (f *Filesystem) segmentsReadOnly(ctx context.Context, b fsBlob) ([]fsSegment, error) {
	if b.length == 0 || b.root == (ObjectID{}) && b.source == (ObjectID{}) && !b.lower {
		return nil, nil
	}
	if b.lower {
		if f.lower == nil {
			return nil, ErrOverlayNeedsLower
		}
		b.lower = false
		parts, e := f.lower.segmentsReadOnly(ctx, b)
		for i := range parts {
			if parts[i].id != (ObjectID{}) {
				parts[i].lower = true
			}
		}
		return parts, e
	}
	if b.source != (ObjectID{}) {
		return []fsSegment{{id: b.source, length: b.length, legacyVersion: b.legacyVersion}}, nil
	}
	parts, e := f.flatten(ctx, fsSegment{id: b.root, length: b.length}, b.depth)
	if e != nil {
		return nil, e
	}
	var converted []fsSegment
	for _, part := range parts {
		if part.legacyVersion == 0 {
			converted = append(converted, part)
			continue
		}
		if f.lower == nil {
			return nil, ErrOverlayNeedsLower
		}
		node, _, e := f.lower.load(ctx, part.id)
		if e != nil {
			return nil, e
		}
		refs, e := f.lower.segmentsReadOnly(ctx, node.blob)
		if e != nil {
			return nil, e
		}
		if node.blob.root == (ObjectID{}) {
			converted = append(converted, part)
			continue
		} // inline is materialized by the affected write
		refs = sliceFSSegments(refs, part.offset, part.offset+part.length)
		for i := range refs {
			if refs[i].id != (ObjectID{}) {
				refs[i].lower = true
			}
		}
		converted = append(converted, refs...)
	}
	return converted, nil
}
func (f *Filesystem) stableWriteNode(ctx context.Context, n *fsNode, data []byte, start uint64) error {
	if n.kind != fsFile {
		return syscall.EISDIR
	}
	if len(data) == 0 {
		return nil
	}
	end := start + uint64(len(data))
	if end < start || end > uint64(^uint64(0)>>1) {
		return syscall.EFBIG
	}
	old := n.blob
	length := max(old.length, end)
	if length <= fsInlineSize {
		body := make([]byte, int(length))
		if old.length > 0 {
			if _, e := f.readBlob(ctx, old, body[:old.length], 0); e != nil {
				return e
			}
		}
		copy(body[start:], data)
		n.blob = fsBlob{length: length, inline: body, mutable: true}
		n.mtime = n.ctime
		return nil
	}
	parts, e := f.segmentsReadOnly(ctx, old)
	if e != nil {
		return e
	}
	// Copy-up pages keep their 4-KiB identities even after the last lower
	// reference disappears. Finishing copy-up must not convert them to new IDs.
	smallPages := false
	var logical uint64
	for _, part := range parts {
		smallPages = smallPages || part.lower || part.legacyVersion != 0 || part.id == fsStableID(n.identity, 2, 0, logical/4096)
		logical += part.length
	}
	unit := uint64(FSObjectSize)
	if smallPages {
		unit = 4096
	}
	first := start / unit * unit
	stop := (end - 1) / unit * unit
	var written []fsSegment
	for at := first; at <= stop; at += unit {
		size := min(unit, length-at)
		a, z := max(start, at), min(end, at+size)
		id := fsStableID(n.identity, 0, 0, at/FSObjectSize)
		if smallPages {
			id = fsStableID(n.identity, 2, 0, at/unit)
		}
		// A lower/legacy block is copied up once, then subsequent writes use its
		// stable local address. Missing logical holes need no materialized prefix.
		canonical := false
		var pos uint64
		for _, s := range parts {
			if pos == at && s.id == id && s.offset == 0 && !s.lower && s.legacyVersion == 0 {
				canonical = true
				break
			}
			pos += s.length
		}
		if smallPages {
			if e = f.stageBlock(ctx, n, id, a-at, data[a-start:z-start], false); e != nil {
				return e
			}
			written = append(written, fsSegment{id: id, length: z - a, offset: a - at})
			continue
		} else if canonical {
			if e = f.stageBlock(ctx, n, id, a-at, data[a-start:z-start], false); e != nil {
				return e
			}
		} else {
			body := make([]byte, int(size))
			count := min(size, old.length-min(at, old.length))
			if count > 0 {
				if _, e = f.readBlob(ctx, old, body[:count], at); e != nil && e != io.EOF {
					return e
				}
			}
			copy(body[a-at:], data[a-start:z-start])
			if e = f.stageBlock(ctx, n, id, 0, body, true); e != nil {
				return e
			}
		}
		written = append(written, fsSegment{id: id, length: size})
	}
	prefixEnd := first
	suffixStart := stop + unit
	if smallPages {
		prefixEnd = start
		suffixStart = end
	}
	next := sliceFSSegments(parts, 0, min(prefixEnd, old.length))
	// Existing inline data becomes part of the first page above. For a write past
	// the first page it needs one stable page of its own, not a new random ID.
	if old.root == (ObjectID{}) && old.source == (ObjectID{}) && old.length > 0 && first > 0 {
		id := fsStableID(n.identity, 0, 0, 0)
		if e = f.stageBlock(ctx, n, id, 0, old.inline, true); e != nil {
			return e
		}
		next = []fsSegment{{id: id, length: old.length}}
	}
	prefix := min(prefixEnd, old.length)
	if prefixEnd > prefix {
		next = append(next, fsSegment{length: prefixEnd - prefix})
	}
	next = append(next, written...)
	next = append(next, sliceFSSegments(parts, suffixStart, old.length)...)
	n.mtime = n.ctime
	return f.finishStableBlob(ctx, n, old, next, length)
}
func (f *Filesystem) stableResize(ctx context.Context, n *fsNode, length uint64) error {
	if n.kind != fsFile {
		return syscall.EISDIR
	}
	if length > uint64(^uint64(0)>>1) {
		return syscall.EFBIG
	}
	old := n.blob
	if length == old.length {
		return nil
	}
	n.mtime = n.ctime
	if length <= fsInlineSize {
		body := make([]byte, int(length))
		count := min(length, old.length)
		if count > 0 {
			if _, e := f.readBlob(ctx, old, body[:count], 0); e != nil {
				return e
			}
		}
		// Plan retirement before publishing the new inline head.
		parts, e := f.segmentsReadOnly(ctx, old)
		if e != nil {
			return e
		}
		ids, e := f.indexIDs(ctx, old)
		if e != nil {
			return e
		}
		for _, s := range parts {
			if !s.lower && s.legacyVersion == 0 && s.id != (ObjectID{}) {
				ids = append(ids, s.id)
			}
		}
		seen := map[ObjectID]bool{}
		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				if e = f.stageRemoval(ctx, n, id); e != nil {
					return e
				}
			}
		}
		n.blob = fsBlob{length: length, inline: body, mutable: true}
		return nil
	}
	parts, e := f.segmentsReadOnly(ctx, old)
	if e != nil {
		return e
	}
	if old.root == (ObjectID{}) && old.source == (ObjectID{}) && old.length > 0 {
		id := fsStableID(n.identity, 0, 0, 0)
		if e = f.stageBlock(ctx, n, id, 0, old.inline, true); e != nil {
			return e
		}
		parts = []fsSegment{{id: id, length: old.length}}
	}
	parts = sliceFSSegments(parts, 0, min(length, old.length))
	if length < old.length && len(parts) > 0 {
		last := &parts[len(parts)-1]
		var at uint64
		for _, part := range parts[:len(parts)-1] {
			at += part.length
		}
		smallPage := last.id == fsStableID(n.identity, 2, 0, at/4096)
		if !last.lower && last.legacyVersion == 0 && smallPage {
			raw, _, e := f.objectBytes(ctx, last.id)
			if e != nil {
				return e
			}
			retained := last.offset + last.length
			if retained < uint64(len(raw)) {
				if e = f.stageBlock(ctx, n, last.id, 0, raw[:retained], true); e != nil {
					return e
				}
			}
		} else if !last.lower && last.legacyVersion == 0 && last.offset == 0 && last.id == fsStableID(n.identity, 0, 0, at/FSObjectSize) {
			body := make([]byte, int(last.length))
			if _, e = f.readBlob(ctx, old, body, at); e != nil {
				return e
			}
			if e = f.stageBlock(ctx, n, last.id, 0, body, true); e != nil {
				return e
			}
		}
	}

	if length > old.length {
		parts = append(parts, fsSegment{length: length - old.length})
	}
	return f.finishStableBlob(ctx, n, old, parts, length)
}
func (f *Filesystem) stableBody(ctx context.Context, n *fsNode, data []byte) error {
	old := n.blob
	if len(data) <= fsInlineSize {
		// Directory shrinking follows the same safe retirement rule as files.
		kind := n.kind
		n.kind = fsFile
		err := f.stableResize(ctx, n, uint64(len(data)))
		n.kind = kind
		if err != nil {
			return err
		}
		n.blob = fsBlob{length: uint64(len(data)), inline: bytes.Clone(data), mutable: true}
		return nil
	}
	var parts []fsSegment
	unit := FSObjectSize
	if n.kind == fsDirectory {
		unit = 4096
	}
	for at := 0; at < len(data); at += unit {
		piece := data[at:min(at+unit, len(data))]
		id := fsStableID(n.identity, 0, 0, uint64(at/unit))
		if e := f.stageBlock(ctx, n, id, 0, piece, true); e != nil {
			return e
		}
		parts = append(parts, fsSegment{id: id, length: uint64(len(piece))})
	}
	return f.finishStableBlob(ctx, n, old, parts, uint64(len(data)))
}
