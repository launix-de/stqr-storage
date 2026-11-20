// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"io"
)

// ExtentSource yields ascending, nonoverlapping logical data intervals. Holes
// are omitted, including arbitrarily large ZERO intervals. Each reader must
// supply exactly length bytes before the callback returns. Neither source nor
// its callbacks may reenter the destination Volume while Restore holds its lock.
type ExtentSource func(yield func(offset uint64, data io.Reader, length uint64) error) error

// Restore atomically installs a trusted migration/recovery snapshot in a volume
// which may already contain unrelated objects. It preserves the supplied logical
// version and tombstone. The server must validate source ownership/epoch before
// calling; this storage primitive does not grant cluster authority. Older/equal
// existing versions are rejected; a nonzero retry ID makes repeats idempotent.
func (v *Volume) Restore(k Key, info Info, source ExtentSource, opts Options) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
	}
	if (opts.Compression != nil && *opts.Compression != info.Compression) || info.Version == 0 || !info.Compression.valid() || (info.Deleted && info.Length != 0) {
		return ErrRange
	}
	if len(v.runs) >= maxIndexRuns {
		if e := v.compactLocked(); e != nil {
			return e
		}
	}
	h := fingerprint(opSnapshot, k, info.Version, info.Length, opts, nil)
	flags := byte(0)
	if info.Deleted {
		flags = 1
	}
	h.Write([]byte{flags, byte(info.Compression)})
	prior, retry, e := v.loadDedupe(opts.RequestID)
	if e != nil {
		return e
	}
	old, e := v.loadObject(k)
	if e != nil && !errors.Is(e, ErrNotFound) {
		return e
	}
	if !retry {
		if e = precondition(old, opts); e != nil {
			return e
		}
		if old != nil && (old.Info.Version > info.Version || (old.Info.Version == info.Version && !opts.restoreSameVersion)) {
			return ErrPrecondition
		}
	}
	s := v.sb
	r := record{Op: opSnapshot, Key: k, Codec: info.Compression, Version: info.Version, Length: info.Length, Deleted: info.Deleted, Replace: true, Request: opts.RequestID, Result: Result{Info: info}}
	var last uint64
	buffer := make([]byte, BlockSize)
	yield := func(off uint64, data io.Reader, length uint64) error {
		if info.Deleted || length == 0 || data == nil || off < last || off > info.Length || length > info.Length-off {
			return ErrRange
		}
		var header [16]byte
		diskOrder.PutUint64(header[:], off)
		diskOrder.PutUint64(header[8:], length)
		h.Write(header[:])
		var done uint64
		for done < length {
			n := min(uint64(len(buffer)), length-done)
			if _, e := io.ReadFull(data, buffer[:n]); e != nil {
				return e
			}
			h.Write(buffer[:n])
			if !retry {
				ext, e := v.storeBlock(&s, &r.Arena, off+done, buffer[:n], info.Compression, false)
				if e != nil {
					return e
				}
				r.Extents = append(r.Extents, ext)
			}
			done += n
		}
		last = off + length
		return nil
	}
	if source != nil {
		if e = source(yield); e != nil {
			return e
		}
	}
	copy(r.Fingerprint[:], h.Sum(nil))
	if retry {
		if prior.Key != k || prior.Fingerprint != r.Fingerprint {
			return ErrRequestID
		}
		return nil
	}
	return v.commitWithReserve(r, s, opts.UseGuaranteed)
}

// RequestRecord is the persisted successful result carried alongside a migrated
// object. It contains no pointers or physical locations from the source volume.
type RequestRecord struct {
	ID          ID
	Key         Key
	Fingerprint [32]byte
	Result      Result
}

// RestoreRequest imports original client retry state after its object snapshot.
// Trusted server code must transfer this before activating target ownership.
func (v *Volume) RestoreRequest(x RequestRecord) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
	}
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
	if len(v.runs) >= maxIndexRuns {
		if e := v.compactLocked(); e != nil {
			return e
		}
	}
	r := record{Op: opDedupe, Key: x.Key, Request: x.ID, Fingerprint: x.Fingerprint, Result: x.Result, Codec: x.Result.Info.Compression, Version: x.Result.Info.Version, Length: x.Result.Info.Length, Deleted: x.Result.Info.Deleted}
	return v.commitWithReserve(r, v.sb, false)
}

// ScanRequests streams retained retry records for migration. fn must not reenter
// this volume; it runs under its read lock. Entries are ordered by RequestID.
func (v *Volume) ScanRequests(fn func(RequestRecord) error) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return e
	}
	return v.walkIndex(func(x indexEntry) error {
		if x.Key[0] != 1 {
			return nil
		}
		r, e := v.readRecord(v.sb, x.Value-1)
		if e != nil {
			return e
		}
		return fn(RequestRecord{ID: r.Request, Key: r.Key, Fingerprint: r.Fingerprint, Result: r.Result})
	})
}

// Repair replaces a damaged object's physical data with a trusted replica of
// the same version. It requires that version still be current, preserves retry
// history and logical version, and is not a client mutation or an owner change.
func (v *Volume) Repair(k Key, info Info, source ExtentSource, opts Options) error {
	version := info.Version
	opts.ExpectedVersion = &version
	opts.restoreSameVersion = true
	return v.Restore(k, info, source, opts)
}
