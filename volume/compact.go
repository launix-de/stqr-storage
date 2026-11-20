// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
)

// Checkpoint flushes the bounded metadata delta into a sorted on-volume run.
// The journal remains the durable source of truth; checkpoints accelerate lookup.
func (v *Volume) Checkpoint() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	if len(v.delta) == 0 {
		return nil
	}
	if len(v.runs) >= maxIndexRuns {
		return v.compactLocked()
	}
	s := v.sb
	run, e := v.writeIndex(&s, sortedDelta(v.delta))
	if e != nil {
		return e
	}
	r := record{Op: opIndex, Offset: run.Location, Length: run.Count}
	b, _ := encodeRecord(r)
	if e = v.appendJournal(&s, b); e != nil {
		if !errors.Is(e, ErrFull) {
			v.failed = e
		}
		return e
	}
	if s.DataTop-s.metaPages()*2*PageSize < v.guaranteed {
		return ErrFull
	}
	if e = v.publish(s); e != nil {
		return e
	}
	v.runs = append(v.runs, run)
	clear(v.delta)
	return nil
}

// Compact copies only live object versions, tombstones, and retained retry
// results into the other bank, then atomically switches the superblock. The
// entire old bank stays intact until the new bank and root have synced.
// Half the volume is deliberately reserved as GC/recovery working space.
func (v *Volume) Compact() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	return v.compactLocked()
}
func (v *Volume) compactLocked() error {
	s := v.sb
	s.Bank = 1 - s.Bank
	s.Generation++
	s.BankEpoch = s.Generation
	s.JournalBytes = 0
	s.TailSlot = 0
	s.DataTop = s.bankSize()
	s.IndexBytes = 0
	s.LiveData = 0
	s.ArenaUnused = 0
	s.IndexCount = 0
	s.IndexLocation = 0
	s.ReplayFrom = 0
	dst := newVolume(v.dev, s, OpenOptions{CacheEntries: -1})
	dst.guaranteed = v.guaranteed
	var count uint64
	if e := v.walkIndex(func(indexEntry) error { count++; return nil }); e != nil {
		return e
	}
	writer, e := dst.newIndexWriter(&dst.sb, count)
	if e != nil {
		return e
	}
	err := v.walkIndex(func(entry indexEntry) error {
		var r record
		if entry.Key[0] == 0 {
			o, e := v.loadObject(entry.Key.object())
			if e != nil {
				return e
			}
			r = record{Op: opSnapshot, Key: entry.Key.object(), Version: o.Info.Version, Length: o.Info.Length, Codec: o.Info.Compression, Deleted: o.Info.Deleted, Replace: true}
			if !o.Info.Deleted {
				if e = v.copyExtents(dst, &r, o); e != nil {
					return e
				}
			}
			snapshot := &object{Info: o.Info, Extents: r.Extents, Arena: r.Arena}
			dst.sb.LiveData += liveAllocation(snapshot)
			dst.sb.ArenaUnused += arenaUnused(snapshot)
		} else {
			old, e := v.readRecord(v.sb, entry.Value-1)
			if e != nil {
				return e
			}
			r = record{Op: opDedupe, Key: old.Key, Request: old.Request, Fingerprint: old.Fingerprint, Codec: old.Result.Info.Compression, Result: old.Result, Length: old.Result.Info.Length, Version: old.Result.Info.Version, Deleted: old.Result.Info.Deleted}
		}
		ptr := dst.sb.JournalBytes + 1
		b, e := encodeRecord(r)
		if e != nil {
			return e
		}
		if e = dst.appendJournal(&dst.sb, b); e != nil {
			return e
		}
		return writer.append(indexEntry{entry.Key, ptr})
	})
	if err == nil {
		err = writer.finish()
	}
	if err != nil {
		if dst.failed != nil {
			v.failed = dst.failed
		}
		return err
	}
	if count > 0 {
		dst.runs = []indexRun{writer.run}
		dst.sb.IndexLocation = writer.run.Location
		dst.sb.IndexCount = count
	}
	dst.sb.ReplayFrom = dst.sb.JournalBytes
	dst.sb.Generation++
	if dst.sb.DataTop-dst.sb.metaPages()*2*PageSize < v.guaranteed {
		return ErrFull
	}
	if err = v.publish(dst.sb); err != nil {
		return err
	}
	v.delta = dst.delta
	v.runs = dst.runs
	v.cache.clear()
	return nil
}
func (v *Volume) copyExtents(dst *Volume, r *record, o *object) error {
	buffer := make([]byte, BlockSize)
	appendable := o.Arena.Pages != 0
	for i := 0; i < len(o.Extents); {
		start := o.Extents[i].Offset
		end := start + o.Extents[i].Length
		j := i + 1
		for j < len(o.Extents) && o.Extents[j].Offset == end {
			end += o.Extents[j].Length
			j++
		}
		for at := start; at < end; {
			n := min(uint64(len(buffer)), end-at)
			if e := v.readObject(o, buffer[:n], at); e != nil {
				return e
			}
			x, e := dst.storeBlock(&dst.sb, &r.Arena, at, buffer[:n], r.Codec, appendable)
			if e != nil {
				return e
			}
			r.Extents = append(r.Extents, x)
			at += n
		}
		i = j
	}
	return nil
}

// Scrub verifies index pages and all currently live payload checksums, including
// decompression limits. Historical dead bytes are deliberately not read.
func (v *Volume) Scrub() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return e
	}
	var objects, logical, live, unused uint64
	e := v.walkIndex(func(x indexEntry) error {
		if x.Key[0] == 1 {
			r, e := v.readRecord(v.sb, x.Value-1)
			if e != nil {
				return e
			}
			if requestKey(r.Request) != x.Key {
				return ErrCorrupt
			}
			return nil
		}
		o, e := v.loadObjectUncached(x.Key.object())
		if e != nil {
			return e
		}
		if o.Info.Deleted {
			return nil
		}
		objects++
		if ^uint64(0)-logical < o.Info.Length {
			return ErrCorrupt
		}
		logical += o.Info.Length
		live += liveAllocation(o)
		unused += arenaUnused(o)
		for _, ext := range o.Extents {
			if _, e = v.readBlock(v.sb, ext); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	if objects != v.sb.Objects || logical != v.sb.LogicalBytes || live != v.sb.LiveData || unused != v.sb.ArenaUnused {
		return ErrCorrupt
	}
	return nil
}

// scanLocked visits the hash-ordered bucket interval [start,end); end==zero means
// the end of hash space. The caller holds a read or write lease.
func (v *Volume) scanLocked(bucket ID, start, end ObjectID, fn func(Key, Info) error) error {
	if e := v.check(false); e != nil {
		return e
	}
	return v.walkIndex(func(x indexEntry) error {
		if x.Key[0] != 0 {
			return nil
		}
		k := x.Key.object()
		if k.Bucket != bucket || bytes.Compare(k.Object[:], start[:]) < 0 || (end != (ObjectID{}) && bytes.Compare(k.Object[:], end[:]) >= 0) {
			return nil
		}
		o, e := v.loadObject(k)
		if e != nil {
			return e
		}
		if o.Info.Deleted {
			return nil
		}
		return fn(k, o.Info)
	})
}

// recompressLocked changes physical representation while preserving logical versions,
// sparse holes, append offsets, and all retained RequestID results.
func (v *Volume) recompressLocked(k Key, codec Compression) error {
	if e := v.check(true); e != nil {
		return e
	}
	if !codec.valid() {
		return ErrRange
	}
	if len(v.runs) >= maxIndexRuns {
		if e := v.compactLocked(); e != nil {
			return e
		}
	}
	o, e := v.loadObject(k)
	if e != nil {
		return e
	}
	if o.Info.Deleted {
		return ErrNotFound
	}
	if o.Info.Compression == codec {
		return nil
	}
	s := v.sb
	dst := newVolume(v.dev, s, OpenOptions{CacheEntries: -1})
	r := record{Op: opSnapshot, Codec: codec, Key: k, Version: o.Info.Version, Length: o.Info.Length, Replace: true}
	if e = v.copyExtents(dst, &r, o); e != nil {
		if dst.failed != nil {
			v.failed = dst.failed
		}
		return e
	}
	r.Result = Result{Info: Info{Version: r.Version, Length: r.Length, Compression: codec}}
	return v.commitWithReserve(r, dst.sb, false)
}

// snapshotWire exports logical extents, not volume-specific physical offsets.
// Individual frames have their own checksum; holes never become zero-byte data.
const snapshotMagic = "STQRSNP1"

func (v *Volume) Export(dst io.Writer) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return e
	}
	if _, e := io.WriteString(dst, snapshotMagic); e != nil {
		return e
	}
	var count uint64
	if e := v.walkIndex(func(indexEntry) error { count++; return nil }); e != nil {
		return e
	}
	var countBytes [8]byte
	diskOrder.PutUint64(countBytes[:], count)
	if e := writeAll(dst, countBytes[:]); e != nil {
		return e
	}
	err := v.walkIndex(func(x indexEntry) error {
		r, e := v.readRecord(v.sb, x.Value-1)
		if e != nil {
			return e
		}
		if x.Key[0] == 1 {
			r.Op = opDedupe
			r.Arena = arena{}
			r.Extents = nil
			r.Updates = nil
			r.Previous = 0
			b, e := encodeRecord(r)
			if e != nil {
				return e
			}
			return writeFrame(dst, 2, b)
		}
		o, e := v.loadObject(x.Key.object())
		if e != nil {
			return e
		}
		r = record{Op: opSnapshot, Key: x.Key.object(), Version: o.Info.Version, Length: o.Info.Length, Codec: o.Info.Compression, Deleted: o.Info.Deleted}
		b, e := encodeRecord(r)
		if e != nil {
			return e
		}
		if e = writeFrame(dst, 1, b); e != nil {
			return e
		}
		for _, ext := range o.Extents {
			raw, e := v.readBlock(v.sb, ext)
			if e != nil {
				return e
			}
			buf := make([]byte, 8+ext.Length)
			diskOrder.PutUint64(buf, ext.Offset)
			copy(buf[8:], raw[ext.Skip:uint64(ext.Skip)+ext.Length])
			if e = writeFrame(dst, 3, buf); e != nil {
				return e
			}
		}
		return writeFrame(dst, 4, nil)
	})
	if err != nil {
		return err
	}
	return writeFrame(dst, 0, nil)
}
func writeFrame(w io.Writer, kind byte, b []byte) error {
	h := make([]byte, 13)
	h[0] = kind
	diskOrder.PutUint64(h[1:], uint64(len(b)))
	diskOrder.PutUint32(h[9:], checksum(b))
	if e := writeAll(w, h); e != nil {
		return e
	}
	return writeAll(w, b)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// Import restores a complete Export stream into an empty volume. The stream is
// staged without publishing any object until its end marker and all checksums
// validate. An optional codec transcodes all objects, preserving logical versions
// and retry results. Sparse gaps are represented only by offsets/lengths.
func (v *Volume) Import(src io.Reader, codec *Compression) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
	}
	if v.sb.JournalBytes != 0 {
		return ErrBusy
	}
	if codec != nil && !codec.valid() {
		return ErrRange
	}
	magic := make([]byte, len(snapshotMagic))
	if _, e := io.ReadFull(src, magic); e != nil {
		return e
	}
	if string(magic) != snapshotMagic {
		return ErrCorrupt
	}
	dst := newVolume(v.dev, v.sb, OpenOptions{CacheEntries: -1})
	var countBytes [8]byte
	if _, e := io.ReadFull(src, countBytes[:]); e != nil {
		return e
	}
	count := diskOrder.Uint64(countBytes[:])
	writer, e := dst.newIndexWriter(&dst.sb, count)
	if e != nil {
		return e
	}
	var current *record
	var last uint64
	stage := func(r record, key indexKey) error {
		ptr := dst.sb.JournalBytes + 1
		b, e := encodeRecord(r)
		if e != nil {
			return e
		}
		if e = dst.appendJournal(&dst.sb, b); e != nil {
			return e
		}
		return writer.append(indexEntry{key, ptr})
	}
	var prevKey indexKey
	haveKey := false
	for {
		kind, b, e := readFrame(src)
		if e != nil {
			if dst.failed != nil {
				v.failed = dst.failed
			}
			return e
		}
		switch kind {
		case 0:
			if len(b) != 0 || current != nil {
				return ErrCorrupt
			}
			if e = writer.finish(); e != nil {
				return e
			}
			if count > 0 {
				dst.runs = []indexRun{writer.run}
				dst.sb.IndexLocation = writer.run.Location
				dst.sb.IndexCount = count
			}
			dst.sb.ReplayFrom = dst.sb.JournalBytes
			dst.sb.Generation++
			if dst.sb.DataTop-dst.sb.metaPages()*2*PageSize < v.guaranteed {
				return ErrFull
			}
			if e = v.publish(dst.sb); e != nil {
				return e
			}
			v.delta = dst.delta
			v.runs = dst.runs
			v.cache.clear()
			return nil
		case 1:
			if current != nil {
				return ErrCorrupt
			}
			r, e := decodeRecord(b)
			if e != nil || r.Op != opSnapshot || r.Version == 0 || len(r.Extents) != 0 {
				return ErrCorrupt
			}
			k := objectKey(r.Key)
			if haveKey && !lessKey(prevKey, k) {
				return ErrCorrupt
			}
			prevKey = k
			haveKey = true
			if codec != nil {
				r.Codec = *codec
			}
			r.Previous = 0
			r.Arena = arena{}
			r.Replace = true
			current = &r
			last = 0
		case 2:
			if current != nil {
				return ErrCorrupt
			}
			r, e := decodeRecord(b)
			if e != nil || r.Op != opDedupe || r.Request == (ID{}) || len(r.Extents) != 0 {
				return ErrCorrupt
			}
			r.Arena = arena{}
			k := requestKey(r.Request)
			if haveKey && !lessKey(prevKey, k) {
				return ErrCorrupt
			}
			prevKey = k
			haveKey = true
			if e = stage(r, k); e != nil {
				return e
			}
		case 3:
			if current == nil || current.Deleted || len(b) <= 8 {
				return ErrCorrupt
			}
			off := diskOrder.Uint64(b)
			n := uint64(len(b) - 8)
			if n > BlockSize || off < last || off > current.Length || n > current.Length-off {
				return ErrCorrupt
			}
			ext, e := dst.storeBlock(&dst.sb, &current.Arena, off, b[8:], current.Codec, false)
			if e != nil {
				if dst.failed != nil {
					v.failed = dst.failed
				}
				return e
			}
			current.Extents = append(current.Extents, ext)
			last = off + n
		case 4:
			if current == nil || len(b) != 0 {
				return ErrCorrupt
			}
			snapshot := &object{Info: Info{Length: current.Length, Deleted: current.Deleted}, Extents: current.Extents, Arena: current.Arena}
			dst.sb.LiveData += liveAllocation(snapshot)
			dst.sb.ArenaUnused += arenaUnused(snapshot)
			if !current.Deleted {
				dst.sb.Objects++
				if ^uint64(0)-dst.sb.LogicalBytes < current.Length {
					return ErrRange
				}
				dst.sb.LogicalBytes += current.Length
			}
			if e = stage(*current, objectKey(current.Key)); e != nil {
				return e
			}
			current = nil
		default:
			return ErrCorrupt
		}
	}
}
func readFrame(r io.Reader) (byte, []byte, error) {
	h := make([]byte, 13)
	if _, e := io.ReadFull(r, h); e != nil {
		return 0, nil, e
	}
	n := diskOrder.Uint64(h[1:])
	if n > 2*BlockSize+4096 {
		return 0, nil, ErrCorrupt
	}
	b := make([]byte, int(n))
	if _, e := io.ReadFull(r, b); e != nil {
		return 0, nil, e
	}
	if checksum(b) != diskOrder.Uint32(h[9:]) {
		return 0, nil, ErrCorrupt
	}
	return h[0], b, nil
}
