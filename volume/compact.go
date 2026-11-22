// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

func (v *Volume) Checkpoint() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	return v.sync()
}
func (v *Volume) Compact() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(false); e != nil {
		return e
	}
	return v.compactLocked()
}

// Defragmentation moves one object per committed transaction. The source is
// released only after payload, index and byte-exact free ranges commit together.
// It requires a suitable free destination; no permanent second bank is reserved.
func (v *Volume) compactLocked() error {
	if v.sb.Moving {
		if e := v.resumeMoveLocked(); e != nil {
			return e
		}
	}
	for {
		moved := false
		var cursor *Key
		for {
			records, e := v.scanObjectPage(cursor, 128)
			if e != nil {
				return e
			}
			if len(records) == 0 {
				break
			}
			for _, r := range records {
				o, e := v.loadObject(r.Key)
				if e != nil {
					return e
				}
				if !o.Info.Deleted {
					for i := range o.Spans {
						target, found, e := v.moveTarget(o.Spans[i])
						if e != nil {
							return e
						}
						if found {
							if e = v.beginMove(r.Key, i, target); e != nil {
								return e
							}
							moved = true
							o, e = v.loadObject(r.Key)
							if e != nil {
								return e
							}
						}
					}
				}
				k := r.Key
				cursor = &k
			}
		}
		if !moved {
			return nil
		}
	}
}
func (v *Volume) scanObjectPage(after *Key, limit int) ([]ObjectRecord, error) {
	var out []ObjectRecord
	var cursor indexKey
	if after != nil {
		cursor = objectKey(*after)
	}
	e := v.walkIndexBounds(cursor, indexKey{1}, func(x indexEntry) error {
		if x.Key[0] != 0 || after != nil && !lessKey(cursor, x.Key) {
			return nil
		}
		k := x.Key.object()
		o, e := v.loadObject(k)
		if e != nil {
			return e
		}
		out = append(out, ObjectRecord{k, o.Info})
		if len(out) == limit {
			return stopTree
		}
		return nil
	})
	if e == stopTree {
		e = nil
	}
	return out, e
}
func (v *Volume) rewriteObject(k Key, old *object, codec Compression, opts Options) error {
	t := v.transaction()
	pieces := oldPieces(v, old, old.Info.Length, 0, 0)
	spans, e := t.storePacked(pieces, codec)
	if e != nil {
		return e
	}
	o := cloneObject(old)
	releaseData(t, old)
	o.Spans = spans
	o.ListBlocks = nil
	o.ListHead = 0
	o.ListTail = 0
	o.Info.Compression = codec
	t.sb.ArenaUnused = arenaAfter(t.sb.ArenaUnused, old, o)
	if e = t.saveObject(k, o); e != nil {
		return e
	}
	if e = t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance); e != nil {
		return e
	}
	v.cache.put(k, o)
	return nil
}
func (v *Volume) recompressLocked(k Key, codec Compression, opts Options) error {
	if !codec.valid() {
		return ErrRange
	}
	o, e := v.loadObject(k)
	if e != nil {
		return e
	}
	if o.Info.Deleted {
		return ErrNotFound
	}
	return v.rewriteObject(k, o, codec, opts)
}
func (v *Volume) scanLocked(bucket ID, start, end ObjectID, fn func(Key, Info) error) error {
	lower := objectKey(Key{Bucket: bucket, Object: start})
	upper := objectKey(Key{Bucket: bucket, Object: end})
	if end == (ObjectID{}) {
		for i := 16; i >= 0; i-- {
			upper[i]++
			if upper[i] != 0 {
				break
			}
		}
	}
	return v.walkIndexBounds(lower, upper, func(x indexEntry) error {
		if x.Key[0] != 0 {
			return nil
		}
		k := x.Key.object()
		if k.Bucket != bucket || bytes.Compare(k.Object[:], start[:]) < 0 || end != (ObjectID{}) && bytes.Compare(k.Object[:], end[:]) >= 0 {
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
func (v *Volume) Scrub() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return e
	}
	v.cache.clear()
	v.nodes.clear()
	return v.scrubLocked()
}
func (v *Volume) scrubLocked() error {
	buf := make([]byte, BlockSize)
	return v.walkIndex(func(x indexEntry) error {
		if x.Key[0] == 1 {
			var id ID
			copy(id[:], x.Key[1:17])
			_, _, e := v.loadDedupe(id)
			return e
		}
		o, e := v.loadObject(x.Key.object())
		if e != nil {
			return e
		}
		for _, s := range o.Spans {
			for off := uint64(0); off < s.Length; {
				n := min(uint64(len(buf)), s.Length-off)
				if e = v.readSpan(s, buf[:n], off); e != nil {
					return e
				}
				off += n
			}
		}
		return nil
	})
}

// Snapshot frames carry logical ranges, never physical offsets. Keeping a fixed
// chunk size also makes semantic export digests stable across format revisions.
type snapshotFrame struct {
	Kind      string
	Key       Key           `json:",omitzero"`
	Info      Info          `json:",omitzero"`
	Offset    uint64        `json:",omitzero"`
	Data      []byte        `json:",omitzero"`
	Request   RequestRecord `json:",omitzero"`
	Segmented bool          `json:",omitzero"`
}

func (v *Volume) Export(dst io.Writer) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return e
	}
	enc := json.NewEncoder(dst)
	if e := enc.Encode(snapshotFrame{Kind: "STQR-SNAPSHOT-2"}); e != nil {
		return e
	}
	e := v.walkIndex(func(x indexEntry) error {
		if x.Key[0] != 0 {
			return nil
		}
		k := x.Key.object()
		o, e := v.loadObject(k)
		if e != nil {
			return e
		}
		if e = enc.Encode(snapshotFrame{Kind: "object", Key: k, Info: o.Info, Segmented: o.Segmented}); e != nil {
			return e
		}
		buf := make([]byte, 64<<10)
		for _, s := range o.Spans {
			for off := uint64(0); off < s.Length; {
				n := min(uint64(len(buf)), s.Length-off)
				if e = v.readSpan(s, buf[:n], off); e != nil {
					return e
				}
				if e = enc.Encode(snapshotFrame{Kind: "data", Offset: s.Offset + off, Data: buf[:n]}); e != nil {
					return e
				}
				off += n
			}
		}
		return enc.Encode(snapshotFrame{Kind: "end"})
	})
	if e != nil {
		return e
	}
	if e = v.scanRequestsLocked(func(r RequestRecord) error { return enc.Encode(snapshotFrame{Kind: "request", Request: r}) }); e != nil {
		return e
	}
	return enc.Encode(snapshotFrame{Kind: "complete"})
}

// Import validates the complete stream in a temporary volume before publishing
// records to the empty destination. The destination transaction is staged until
// the stream's end marker, so malformed input cannot expose partial objects.
func (v *Volume) Import(src io.Reader, codec *Compression) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
	}
	if v.sb.Objects != 0 || v.sb.Sequence != 0 {
		return ErrBusy
	}
	if codec != nil && !codec.valid() {
		return ErrRange
	}
	d := json.NewDecoder(src)
	var f snapshotFrame
	if e := d.Decode(&f); e != nil || f.Kind != "STQR-SNAPSHOT-2" {
		return ErrCorrupt
	}
	file, e := os.CreateTemp("", "stqr-import-*")
	if e != nil {
		return e
	}
	path := file.Name()
	defer os.Remove(path)
	if e = file.Truncate(int64(v.sb.Identity.Size)); e != nil {
		file.Close()
		return e
	}
	file.Close()
	dev, e := OpenFile(path)
	if e != nil {
		return e
	}
	identity := v.sb.Identity
	temp, e := Format(dev, FormatOptions{ClusterID: identity.ClusterID, NodeID: identity.NodeID, VolumeID: identity.VolumeID, Class: identity.Class})
	if e != nil {
		dev.Close()
		return e
	}
	defer temp.Close()
	for {
		if e = d.Decode(&f); e != nil {
			return ErrCorrupt
		}
		if f.Kind == "complete" {
			var extra any
			if e = d.Decode(&extra); e != io.EOF {
				return ErrCorrupt
			}
			break
		}
		h, e := temp.Open(true)
		if e != nil {
			return e
		}
		switch f.Kind {
		case "object":
			key, info := f.Key, f.Info
			if codec != nil {
				info.Compression = *codec
			}
			e = h.Restore(key, info, func(yield func(uint64, io.Reader, uint64) error) error {
				for {
					var frame snapshotFrame
					if err := d.Decode(&frame); err != nil {
						return ErrCorrupt
					}
					if frame.Kind == "end" {
						return nil
					}
					if frame.Kind != "data" || len(frame.Data) == 0 {
						return ErrCorrupt
					}
					if err := yield(frame.Offset, bytes.NewReader(frame.Data), uint64(len(frame.Data))); err != nil {
						return err
					}
				}
			}, Options{restoreSegmented: f.Segmented})
		case "request":
			e = h.RestoreRequest(f.Request)
		default:
			e = ErrCorrupt
		}
		h.Close()
		if e != nil {
			return e
		}
	}
	// The destination is empty: its data area is entirely unreferenced.
	// Stream the validated allocated pages there without retaining an entire
	// snapshot/directory in RAM. Only its two existing root pages need redo.
	buffer := make([]byte, BlockSize)
	copiedUntil := dataStart
	copyRegion := func(begin, end uint64) error {
		start := max(begin/PageSize*PageSize, copiedUntil)
		stop := min(align(end), identity.Size)
		for start < stop {
			n := min(uint64(len(buffer)), stop-start)
			if err := readFullAt(temp.dev, buffer[:n], start); err != nil {
				return err
			}
			if err := v.write(buffer[:n], start); err != nil {
				return err
			}
			start += n
		}
		copiedUntil = max(copiedUntil, stop)
		return nil
	}
	previous := dataStart
	e = treeWalk(temp.node, temp.sb.FreeRoot, func(x treeEntry) error {
		off := keyOffset(x.key)
		if off > previous {
			if err := copyRegion(previous, off); err != nil {
				return err
			}
		}
		previous = off + x.value
		return nil
	})
	if e != nil {
		return e
	}
	if previous < identity.Size {
		if e = copyRegion(previous, identity.Size); e != nil {
			return e
		}
	}
	t := v.transaction()
	t.sb = temp.sb
	t.sb.Generation = max(v.sb.Generation, temp.sb.Generation)
	t.sb.Sequence = max(v.sb.Sequence, temp.sb.Sequence)
	for _, off := range []uint64{treeStart, treeStart + PageSize} {
		page, e := temp.readPage(off)
		if e != nil {
			return e
		}
		t.pages[off] = page
	}
	return t.commit(false)
}

func (v *Volume) copyToLocked(k Key, offset, length uint64, dst io.Writer) (uint64, error) {
	o, e := v.loadObject(k)
	if e != nil {
		return 0, e
	}
	if o.Info.Deleted {
		return 0, ErrNotFound
	}
	if offset > o.Info.Length {
		return 0, ErrRange
	}
	length = min(length, o.Info.Length-offset)
	buf := make([]byte, BlockSize)
	var done uint64
	for done < length {
		n := min(uint64(len(buf)), length-done)
		if e = v.readObject(o, buf[:n], offset+done); e != nil {
			return done, e
		}
		written, err := dst.Write(buf[:n])
		done += uint64(written)
		if err != nil {
			return done, err
		}
		if written != int(n) {
			return done, io.ErrShortWrite
		}
	}
	return done, nil
}
