// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"io"
)

type ExtentSource func(yield func(offset uint64, data io.Reader, length uint64) error) error
type RequestRecord struct {
	ID          ID
	Key         Key
	Fingerprint [32]byte
	Result      Result
}

func (v *Volume) restoreLocked(k Key, info Info, source ExtentSource, opts Options, replayable bool) error {
	var prepared ReplicaRecord
	var havePrepared bool
	if opts.prepare != nil {
		if opts.RequestID == (ID{}) || opts.prepare.Epoch == 0 || opts.prepare.Sequence == 0 || opts.prepare.SetHash == ([32]byte{}) {
			return ErrRange
		}
		var err error
		prepared, havePrepared, err = v.loadReplica(opts.RequestID)
		if err != nil {
			return err
		}
	}

	if busy, err := v.pending(k); err != nil {
		return err
	} else if busy && !havePrepared {
		return ErrBusy
	}
	if !info.Compression.valid() || info.Deleted && info.Length != 0 || opts.Compression != nil && *opts.Compression != info.Compression {
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
	if !retry && !havePrepared {
		if e = v.precondition(old, opts); e != nil {
			return e
		}
	}
	// Trusted migration sources need not be seekable. Stage logical payload only,
	// never sparse zero ranges, then allocate its final packed region exactly once.
	var spool *encodedSpool
	if !replayable {
		spool = &encodedSpool{noSpill: v.sb.Identity.Class == RAM}
		defer spool.Close()
	}
	h := fingerprint(opSnapshot, k, 0, info.Length, opts, nil)
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
			if replayable {
				section, ok := r.(interface {
					io.Reader
					io.Seeker
				})
				if !ok {
					return ErrUnsupported
				}
				position, err := section.Seek(0, io.SeekCurrent)
				if err != nil {
					return err
				}
				if _, err = io.CopyN(h, section, int64(n)); err != nil {
					return err
				}
				if _, err = section.Seek(position, io.SeekStart); err != nil {
					return err
				}
				pieces = append(pieces, payloadPiece{off, n, section})
			} else {
				if _, e := io.CopyN(io.MultiWriter(spool, h), r, int64(n)); e != nil {
					return e
				}
				pieces = append(pieces, payloadPiece{off, n, io.NewSectionReader(spool, int64(physical), int64(n))})
			}
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
	if havePrepared {
		if prepared.Request.Key != k || prepared.Request.Fingerprint != fp || prepared.Binding != *opts.prepare || prepared.Request.Result.Info != info {
			return ErrRequestID
		}
		if prepared.State == ReplicaAborted {
			return &ReplicaAbortError{Status: prepared.AbortStatus}
		}
		return nil
	}
	if retry {
		if prior.Key != k || prior.Fingerprint != fp {
			return ErrRequestID
		}
		return nil
	}
	t := v.transaction()
	defer t.close()
	spans, e := t.storePacked(pieces, info.Compression)
	if e != nil {
		return e
	}
	o := &object{importShadow: opts.ImportShadow, Info: info, Spans: spans, Segmented: opts.restoreSegmented}
	if opts.prepare != nil {
		if e = t.writeObject(o); e != nil {
			return e
		}
		result := Result{Info: info}
		record := ReplicaRecord{Request: RequestRecord{ID: opts.RequestID, Key: k, Fingerprint: fp, Result: result}, Binding: *opts.prepare, State: ReplicaPrepared, object: o.Base, operation: opSnapshot}
		if old != nil && !old.Info.Deleted {
			record.PreviousLength = old.Info.Length
		}
		record.receipt, e = t.writeReceipt(dedupe{Key: k, Fingerprint: fp, Result: result})
		if e != nil {
			return e
		}
		if e = t.saveReplica(&record); e != nil {
			return e
		}
		if e = t.set(&t.sb.ObjectRoot, pendingKey(k), record.base); e != nil {
			return e
		}
		return t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance, opts.GuaranteedAllowance)
	}

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
	v.cache.remove(k)
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
	defer t.close()
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

// PrepareRestore stages a sparse trusted snapshot using the same durable ALL
// decision protocol as ordinary mutations. No content becomes visible at prepare.
func (h *Handle) PrepareRestore(k Key, info Info, source ExtentSource, opts Options, binding ReplicaBinding) (Result, error) {
	return withHandle(h, true, func(v *Volume) (Result, error) {
		opts.prepare = &binding
		opts.Accepted = false
		err := v.restoreLocked(k, info, source, opts, false)
		if errors.Is(err, ErrBusy) {
			return Result{}, err
		}
		return Result{Info: info}, err
	})
}
