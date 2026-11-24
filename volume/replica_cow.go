// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"io"
	"sort"
	"sync"
)

// Allocation planning and writes touch private transaction pages and free
// bytes only. Readers keep using the old index while payload is being streamed.
// Publication/WAL application reacquires the exclusive lock. A private I/O view
// keeps an ambiguous I/O failure from racing readers' check of v.failed.
func (v *Volume) stageReplicaPayload(t *transaction, pieces []payloadPiece, codec Compression) (spans []dataSpan, err error) {
	return v.stageReplicaData(t, func() ([]dataSpan, error) { return t.storePacked(pieces, codec) })
}

func (v *Volume) stageReplicaAppend(t *transaction, old *object, offset, length uint64, src io.Reader) ([]dataSpan, error) {
	return v.stageReplicaData(t, func() ([]dataSpan, error) {
		o := cloneObject(old)
		if old == nil || old.Info.Deleted {
			o.Spans = nil
		}
		if length == 0 {
			return o.Spans, nil
		}
		if len(o.Spans) > 0 {
			last := o.Spans[len(o.Spans)-1]
			if !last.Direct && last.Codec == Raw && last.Skip == 0 && last.Length == last.Stored && last.Capacity <= BlockSize && last.Checksums == 0 && last.Offset+last.Length == offset && length <= last.Capacity-last.Stored {
				// Only unused bytes beyond the visible CRC prefix are written. The
				// new prefix CRC lives in the private descriptor, not a shared table.
				if err := t.appendPayload(o, offset, src, length); err != nil {
					return nil, err
				}
				return o.Spans, nil
			}
		}
		// A sentinel supplies the arena growth hint without exposing a previous
		// multi-block checksum table to appendPayload's in-place fast path.
		capacity := uint64(0)
		if len(o.Spans) > 0 {
			capacity = o.Spans[len(o.Spans)-1].Capacity
		}
		tail := &object{Spans: []dataSpan{{Direct: true, Capacity: capacity}}}
		if err := t.appendPayload(tail, offset, src, length); err != nil {
			return nil, err
		}
		return append(o.Spans, tail.Spans[1:]...), nil
	})
}

func (v *Volume) stageReplicaData(t *transaction, produce func() ([]dataSpan, error)) (spans []dataSpan, err error) {
	if v.prepareCond == nil {
		v.prepareCond = sync.NewCond(&v.mu)
	}
	shadow := &Volume{dev: v.dev, sb: v.sb, limit: v.limit, cache: v.cache, nodes: v.nodes, state: v.state}
	v.preparing = true
	t.v = shadow
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		v.preparing = false
		t.v = v
		if shadow.failed != nil {
			v.failed = shadow.failed
		}
		v.prepareCond.Broadcast()
		if err == nil {
			err = v.check(true)
		}
	}()
	return produce()
}

// Prepared range writes retain unaffected extents. Allocation ownership is
// the union of referenced intervals: a prefix and suffix may share the original
// arena and checksum table. Retirement subtracts the new descriptor's intervals.
func objectAllocations(o *object) []allocation {
	if o == nil {
		return nil
	}
	var out []allocation
	for _, s := range o.Spans {
		out = append(out, allocation{s.Location, s.Capacity, false})
		if n := checksumsSize(s); n > 0 {
			out = append(out, allocation{s.Checksums, n, true})
		}
	}
	for _, at := range o.ListBlocks {
		out = append(out, allocation{at, listBytes, true})
	}
	return mergeAllocations(out)
}
func mergeAllocations(in []allocation) []allocation {
	sort.Slice(in, func(i, j int) bool {
		if in[i].meta != in[j].meta {
			return !in[i].meta
		}
		return in[i].off < in[j].off
	})
	var out []allocation
	for _, a := range in {
		if a.n == 0 {
			continue
		}
		if len(out) > 0 {
			last := &out[len(out)-1]
			if last.meta == a.meta && a.off <= last.off+last.n {
				last.n = max(last.off+last.n, a.off+a.n) - last.off
				continue
			}
		}
		out = append(out, a)
	}
	return out
}
func exclusiveAllocations(old, next *object) []allocation {
	keep := objectAllocations(next)
	var out []allocation
	for _, a := range objectAllocations(old) {
		cursor, end := a.off, a.off+a.n
		for _, b := range keep {
			if b.meta != a.meta || b.off+b.n <= cursor || b.off >= end {
				continue
			}
			if b.off > cursor {
				out = append(out, allocation{cursor, b.off - cursor, a.meta})
			}
			cursor = max(cursor, b.off+b.n)
			if cursor >= end {
				break
			}
		}
		if cursor < end {
			out = append(out, allocation{cursor, end - cursor, a.meta})
		}
	}
	return out
}
func releaseUnsharedData(t *transaction, old, next *object) {
	for _, a := range exclusiveAllocations(old, next) {
		t.release(a.off, a.n, a.meta)
	}
}
func objectAliases(o *object) bool {
	if o == nil {
		return false
	}
	var ranges []allocation
	for _, s := range o.Spans {
		ranges = append(ranges, allocation{s.Location, s.Capacity, false})
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].off < ranges[j].off })
	for i := 1; i < len(ranges); i++ {
		if ranges[i].off < ranges[i-1].off+ranges[i-1].n {
			return true
		}
	}
	return false
}
func (v *Volume) retainSpans(old *object, limit, cutStart, cutEnd uint64) ([]dataSpan, error) {
	if old == nil || old.Info.Deleted {
		return nil, nil
	}
	var out []dataSpan
	for _, original := range old.Spans {
		spans := []dataSpan{original}
		intersects := cutStart < cutEnd && original.Offset < cutEnd && cutStart < original.Offset+original.Length
		if !original.Direct && original.Codec != Raw && (intersects || original.Offset < limit && limit < original.Offset+original.Length) {
			var err error
			spans, err = v.packedChunkReferences(original)
			if err != nil {
				return nil, err
			}
		}
		for _, s := range spans {
			start, end := s.Offset, min(s.Offset+s.Length, limit)
			if start >= end {
				continue
			}
			retain := func(a, b uint64) {
				if a >= b {
					return
				}
				p := s
				p.Skip += a - s.Offset
				p.Offset = a
				p.Length = b - a
				out = append(out, p)
			}
			if cutStart < cutEnd && start < cutEnd && cutStart < end {
				retain(start, min(cutStart, end))
				retain(max(start, cutEnd), end)
			} else {
				retain(start, end)
			}
		}
	}
	return out, nil
}

// Read-only counterpart of unpackChunks: the old packed header and padding
// remain allocated until publication retires the old descriptor. Direct chunk
// references can then retain just the unchanged encoded bytes.
func (v *Volume) packedChunkReferences(s dataSpan) ([]dataSpan, error) {
	var head [8]byte
	if s.Stored < 8 {
		return nil, ErrCorrupt
	}
	if err := v.readBytes(head[:], s.Location); err != nil {
		return nil, err
	}
	count := diskOrder.Uint64(head[:])
	if s.Skip != 0 || count == 0 || count > (s.Stored-8)/32 || count != (s.Length+BlockSize-1)/BlockSize {
		return nil, ErrCorrupt
	}
	previous := uint64(8) + count*32
	var out []dataSpan
	for i := uint64(0); i < count; i++ {
		var b [32]byte
		if err := v.readBytes(b[:], s.Location+8+i*32); err != nil {
			return nil, err
		}
		off, n, decoded := diskOrder.Uint64(b[:]), uint64(diskOrder.Uint32(b[8:])), uint64(diskOrder.Uint32(b[12:]))
		codec := Compression(b[24])
		if off < previous || off > s.Stored || n == 0 || n > s.Stored-off || n > BlockSize || decoded != min(uint64(BlockSize), s.Length-i*BlockSize) || !codec.valid() {
			return nil, ErrCorrupt
		}
		out = append(out, dataSpan{Offset: s.Offset + i*BlockSize, Length: decoded, Location: s.Location + off, Stored: n, Capacity: n, Codec: codec, CRC: diskOrder.Uint32(b[16:]), Checksums: decoded<<32 | uint64(diskOrder.Uint32(b[20:])), Direct: true})
		previous = off + n
	}
	return out, nil
}
