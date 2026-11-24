// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"io"
	"os"
)

type ExtentSource func(yield func(offset uint64, data io.Reader, length uint64) error) error
type RequestRecord struct {
	ID          ID
	Key         Key
	Fingerprint [32]byte
	Result      Result
}

func (v *Volume) restoreLocked(k Key, info Info, source ExtentSource, opts Options) error {
	if busy, err := v.pending(k); err != nil {
		return err
	} else if busy {
		return ErrBusy
	}
	if info.Version == 0 || !info.Compression.valid() || info.Deleted && info.Length != 0 || opts.Compression != nil && *opts.Compression != info.Compression {
		return ErrRange
	}
	old, e := v.loadObject(k)
	if e == ErrNotFound {
		old = nil
	} else if e != nil {
		return e
	}
	prior, retry, e := v.loadDedupe(opts.RequestID)
	if e != nil {
		return e
	}
	if !retry {
		if e = precondition(old, opts); e != nil {
			return e
		}
		if old != nil && (old.Info.Version > info.Version || old.Info.Version == info.Version && !opts.restoreSameVersion) {
			return ErrPrecondition
		}
	}
	// Trusted migration sources need not be seekable. Stage logical payload only,
	// never sparse zero ranges, then allocate its final packed region exactly once.
	spool, e := os.CreateTemp("", "stqr-restore-*")
	if e != nil {
		return e
	}
	defer func() { name := spool.Name(); spool.Close(); os.Remove(name) }()
	h := fingerprint(opSnapshot, k, info.Version, info.Length, opts, nil)
	flags := byte(0)
	if info.Deleted {
		flags = 1
	}
	h.Write([]byte{flags, byte(info.Compression)})
	var pieces []payloadPiece
	var last, physical uint64
	if source != nil {
		e = source(func(off uint64, r io.Reader, n uint64) error {
			if info.Deleted || n == 0 || r == nil || off < last || off > info.Length || n > info.Length-off || n > uint64(^uint64(0)>>1) {
				return ErrRange
			}
			var b [16]byte
			diskOrder.PutUint64(b[:], off)
			diskOrder.PutUint64(b[8:], n)
			h.Write(b[:])
			if _, e := io.CopyN(io.MultiWriter(spool, h), r, int64(n)); e != nil {
				return e
			}
			pieces = append(pieces, payloadPiece{off, n, io.NewSectionReader(spool, int64(physical), int64(n))})
			last = off + n
			physical += n
			return nil
		})
		if e != nil {
			return e
		}
	}
	var fp [32]byte
	copy(fp[:], h.Sum(nil))
	if retry {
		if prior.Key != k || prior.Fingerprint != fp {
			return ErrRequestID
		}
		return nil
	}
	t := v.transaction()
	spans, e := t.storePacked(pieces, info.Compression)
	if e != nil {
		return e
	}
	o := &object{importShadow: opts.ImportShadow, Info: info, Spans: spans, Segmented: opts.restoreSegmented}
	if old != nil {
		o.Base, o.Size = old.Base, old.Size
	}
	releaseData(t, old)
	if old != nil && !old.Info.Deleted {
		t.sb.Objects--
		t.sb.LogicalBytes -= old.Info.Length
	}
	if !info.Deleted {
		t.sb.Objects++
		var err error
		t.sb.LogicalBytes, err = add(t.sb.LogicalBytes, info.Length)
		if err != nil {
			return err
		}
	}
	t.sb.ArenaUnused = arenaAfter(t.sb.ArenaUnused, old, o)
	if e = t.saveObject(k, o); e != nil {
		return e
	}
	if e = t.saveReceipt(opts.RequestID, dedupe{importShadow: opts.ImportShadow, Key: k, Fingerprint: fp, Result: Result{Info: info}}); e != nil {
		return e
	}
	if e = t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance, opts.GuaranteedAllowance); e != nil {
		return e
	}
	v.cache.put(k, o)
	return nil
}
func (v *Volume) restoreRequestLocked(x RequestRecord, opts Options) error {
	if x.ID == (ID{}) || !x.Result.Info.Compression.valid() {
		return ErrRange
	}
	old, ok, e := v.loadDedupe(x.ID)
	if e != nil {
		return e
	}
	if ok {
		if old.Key != x.Key || old.Fingerprint != x.Fingerprint || old.Result != x.Result {
			return ErrRequestID
		}
		return nil
	}
	t := v.transaction()
	if e = t.saveReceipt(x.ID, dedupe{importShadow: opts.ImportShadow, Key: x.Key, Fingerprint: x.Fingerprint, Result: x.Result}); e != nil {
		return e
	}
	return t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance, opts.GuaranteedAllowance)
}
func (v *Volume) scanRequestsLocked(fn func(RequestRecord) error) error {
	return v.walkIndex(func(x indexEntry) error {
		if x.Key[0] != 1 {
			return nil
		}
		var id ID
		copy(id[:], x.Key[1:17])
		d, found, e := v.loadDedupe(id)
		if e != nil {
			return e
		}
		if !found {
			return ErrCorrupt
		}
		return fn(RequestRecord{ID: id, Key: d.Key, Fingerprint: d.Fingerprint, Result: d.Result})
	})
}
