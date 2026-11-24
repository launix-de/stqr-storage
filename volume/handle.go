// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"sync"
)

// Handle holds a volume's read or write lock until Close. It must not be copied.
// Use defer h.Close() immediately after a successful Open. CopyTo may be called
// repeatedly and does not close the handle, including on errors or panics.
//
// Object operations are exposed only on Handle; Volume provides management,
// Open and the standalone Stat query. Handle methods serialize with each other
// and Close. Callbacks must not reenter the handle or its volume. Volume's
// management methods and standalone Stat acquire locks and must be called
// outside a handle session.
// A write handle excludes other mutations; a read handle excludes publication.
// PrepareMutation alone admits readers during its private payload staging, so
// large uploads never hide the previous committed object behind a network read.
// Multiple mutations under a write handle are individually committed, not one
// transaction. AddCleanup is only a fallback for abandoned handles: the runtime
// does not guarantee when or whether a cleanup executes. Explicit Close releases
// the lock synchronously, exactly once, and cancels the cleanup.
type Handle struct {
	lease   *handleLease
	cleanup runtime.Cleanup
}

// The cleanup owns the lease, never the Handle. Keeping the Handle reachable
// from here (even through a closure) would prevent its cleanup from running.
type handleLease struct {
	mu     sync.Mutex
	v      *Volume
	write  bool
	closed bool
}

// Open acquires the volume lock. write=false allows concurrent readers;
// write=true also permits mutations and excludes every other volume operation.
// The returned handle is the only supported way to compose locked operations.
func (v *Volume) Open(write bool) (*Handle, error) {
	if write {
		v.mu.Lock()
	} else {
		v.mu.RLock()
	}
	l := &handleLease{v: v, write: write}
	if e := v.check(write); e != nil {
		l.release()
		return nil, e
	}
	h := &Handle{lease: l}
	h.cleanup = runtime.AddCleanup(h, (*handleLease).release, l)
	return h, nil
}

func (l *handleLease) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	if l.write {
		l.v.mu.Unlock()
	} else {
		l.v.mu.RUnlock()
	}
	l.v = nil
}

// Close waits for a running handle operation, cancels its cleanup, and releases
// the volume lock. It is idempotent, including for nil/zero-value handles.
func (h *Handle) Close() error {
	defer runtime.KeepAlive(h)
	if h != nil && h.lease != nil {
		h.cleanup.Stop()
		h.lease.release()
	}
	return nil
}

func withHandle[T any](h *Handle, write bool, f func(*Volume) (T, error)) (T, error) {
	// Keep the cleanup target alive through all I/O, including callback panics.
	defer runtime.KeepAlive(h)
	var zero T
	if h == nil || h.lease == nil {
		return zero, ErrClosed
	}
	l := h.lease
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return zero, ErrClosed
	}
	if write && !l.write {
		return zero, ErrReadOnly
	}
	if e := l.v.check(write); e != nil {
		return zero, e
	}
	return f(l.v)
}

// Stat returns metadata under the handle's existing lock. It remains consistent
// with subsequent reads until Close (or a mutation through this write handle).
func (h *Handle) Stat(k Key) (Info, error) {
	return withHandle(h, false, func(v *Volume) (Info, error) { return v.statLocked(k) })
}

// RecordInfo includes retained tombstones. The server uses it to keep routing
// retries/recreation to the same physical owner instead of treating a deleted
// object as a never-seen key. Client-facing Stat continues to hide tombstones.
func (h *Handle) RecordInfo(k Key) (Info, error) {
	return withHandle(h, false, func(v *Volume) (Info, error) {
		o, err := v.loadObject(k)
		if err != nil {
			return Info{}, err
		}
		return o.Info, nil
	})
}

// AllocatedPhysical reports this object's actual payload, descriptor, extension
// lists and checksum allocations. Shared B-tree pages and retry receipts are
// accounted separately; logical sparse gaps consume no payload allocation.
func (h *Handle) AllocatedPhysical(k Key) (uint64, error) {
	return withHandle(h, false, func(v *Volume) (uint64, error) {
		o, err := v.loadObject(k)
		if err != nil {
			return 0, err
		}
		total := o.Size
		for _, a := range objectAllocations(o) {
			total += a.n
		}
		return total, nil
	})
}

// CopyTo streams a range to dst under the existing lock, clipping at EOF.
// It never closes the handle. Both metadata and any further CopyTo calls remain
// protected until Close. This writer API allows future direct-transfer paths;
// the current engine reads/checks/decompresses bounded blocks.
func (h *Handle) CopyTo(k Key, offset, length uint64, dst io.Writer) (uint64, error) {
	return withHandle(h, false, func(v *Volume) (uint64, error) {
		return v.copyToLocked(k, offset, length, dst)
	})
}

// ReadAt reads a bounded range, synthesizing sparse zeros. A partial read at EOF
// returns io.EOF. It does not change the handle's lock lifetime.
func (h *Handle) ReadAt(k Key, dst []byte, offset uint64) (int, error) {
	return withHandle(h, false, func(v *Volume) (int, error) { return v.readAtLocked(k, dst, offset) })
}

func (h *Handle) mutate(op uint8, k Key, offset, length uint64, src io.Reader, expected []byte, opts Options) (Result, error) {
	return withHandle(h, true, func(v *Volume) (Result, error) {
		return v.mutateWithRetryLocked(op, k, offset, length, src, expected, opts)
	})
}

func (h *Handle) Put(k Key, data []byte, opts Options) (Info, error) {
	return h.PutReader(k, bytes.NewReader(data), uint64(len(data)), opts)
}

func (h *Handle) PutReader(k Key, src io.Reader, length uint64, opts Options) (Info, error) {
	r, e := h.mutate(opPut, k, 0, length, src, nil, opts)
	return r.Info, e
}

func (h *Handle) PutRange(k Key, offset uint64, data []byte, opts Options) (Info, error) {
	return h.PutRangeReader(k, offset, bytes.NewReader(data), uint64(len(data)), opts)
}

func (h *Handle) PutRangeReader(k Key, offset uint64, src io.Reader, length uint64, opts Options) (Info, error) {
	r, e := h.mutate(opPutRange, k, offset, length, src, nil, opts)
	return r.Info, e
}

func (h *Handle) Append(k Key, data []byte, opts Options) (Result, error) {
	return h.AppendReader(k, bytes.NewReader(data), uint64(len(data)), opts)
}

func (h *Handle) AppendReader(k Key, src io.Reader, length uint64, opts Options) (Result, error) {
	return h.mutate(opAppend, k, 0, length, src, nil, opts)
}

func (h *Handle) Trunc(k Key, length uint64, opts Options) (Info, error) {
	r, e := h.mutate(opTrunc, k, 0, length, nil, nil, opts)
	return r.Info, e
}

func (h *Handle) Delete(k Key, opts Options) error {
	_, e := h.mutate(opDelete, k, 0, 0, nil, nil, opts)
	return e
}

// Check compares application bytes/length while the caller retains this read
// or write handle. A following CopyTo therefore observes the checked content.
func (h *Handle) Check(k Key, opts Options) error {
	return h.do(false, func(v *Volume) error {
		o, err := v.loadObject(k)
		if err != nil {
			return err
		}
		return v.precondition(o, opts)
	})
}

// CAS compares bytes under the volume write lock. WholeObject also compares
// the complete old length before replacing the object; CreateOnly requires absence.
func (h *Handle) CAS(k Key, offset uint64, expected, replacement []byte, opts Options) (Info, error) {
	r, e := withHandle(h, true, func(v *Volume) (Result, error) {
		if e := validateCAS(offset, expected, replacement, opts); e != nil {
			return Result{}, e
		}
		return v.mutateWithRetryLocked(opCAS, k, offset, uint64(len(replacement)), bytes.NewReader(replacement), expected, opts)
	})
	return r.Info, e
}

func (h *Handle) do(write bool, f func(*Volume) error) error {
	_, e := withHandle(h, write, func(v *Volume) (struct{}, error) {
		if write {
			if err := v.flushDeferred(); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, f(v)
	})
	return e
}

// Get is a convenience for small objects. Large objects use CopyTo/ReadAt; the
// buffer limit does not limit logical object size or sparse offsets.
func (h *Handle) Get(k Key) ([]byte, Info, error) { return h.GetRange(k, 0, ^uint64(0)) }

// GetRange returns at most MaxBufferedRead bytes, clipping at EOF.
func (h *Handle) GetRange(k Key, offset, length uint64) ([]byte, Info, error) {
	type readResult struct {
		data []byte
		info Info
	}
	r, e := withHandle(h, false, func(v *Volume) (readResult, error) {
		data, info, e := v.getRangeLocked(k, offset, length)
		return readResult{data, info}, e
	})
	return r.data, r.info, e
}

// Extents returns logical data and sparse intervals, without backend offsets.
func (h *Handle) Extents(k Key) ([]Extent, error) {
	return withHandle(h, false, func(v *Volume) ([]Extent, error) { return v.extentsLocked(k) })
}

// Scan visits the hash-ordered interval [start,end) in a bucket; zero end means
// the end of hash space. fn must not reenter this handle or volume.
func (h *Handle) Scan(bucket ID, start, end ObjectID, fn func(Key, Info) error) error {
	return h.do(false, func(v *Volume) error { return v.scanLocked(bucket, start, end, fn) })
}

// OperationStatus returns a retained successful result. Absence is not proof
// that an operation never ran.
func (h *Handle) OperationStatus(id ID) (Result, bool, error) {
	type status struct {
		result Result
		found  bool
	}
	r, e := withHandle(h, false, func(v *Volume) (status, error) {
		result, found, e := v.operationStatusLocked(id)
		return status{result, found}, e
	})
	return r.result, r.found, e
}

func (h *Handle) ForgetRequest(id ID) error {
	return h.do(true, func(v *Volume) error { return v.forgetRequestLocked(id) })
}

// ForgetRequests atomically removes at most 4096 settled retry records in one
// durable commit. Active replica transactions remain protected.
func (h *Handle) ForgetRequests(ids []ID) error {
	return h.do(true, func(v *Volume) error { return v.forgetRequestsLocked(ids) })
}

// Recompress changes physical encoding, preserving object bytes and retries.
func (h *Handle) Recompress(k Key, codec Compression) error {
	return h.RecompressWithOptions(k, codec, Options{})
}

// RecompressWithOptions permits a trusted handoff to consume its own physical
// import allowance. Logical contents and original retry results stay unchanged;
// failed encodings leave the previous canonical object readable. Only reserve
// fields are accepted; logical mutation/precondition options return ErrRange.
func (h *Handle) RecompressWithOptions(k Key, codec Compression, opts Options) error {
	return h.do(true, func(v *Volume) error {
		if opts.RequestID != (ID{}) || opts.Compression != nil || opts.ExpectedLength != nil || opts.Compare != nil || opts.CreateOnly || opts.WholeObject {
			return ErrRange
		}
		return v.recompressLocked(k, codec, opts)
	})
}

// Restore installs trusted bytes and tombstones under the volume write lock.
// The server must fence source/destination ownership before calling. Byte/length
// predicates can guard installation; a retry ID makes repeats idempotent.
// Neither source nor its callbacks may reenter the handle or destination volume.
func (h *Handle) Restore(k Key, info Info, source ExtentSource, opts Options) error {
	return h.do(true, func(v *Volume) error { return v.restoreLocked(k, info, source, opts, false) })
}

// RestoreSeekable imports an immutable, replayable snapshot without another
// temporary payload copy. Every reader yielded by source must implement io.Seeker
// and remain open and unchanged until this call returns. Readers must be distinct
// for extents with independent cursors. Untrusted streams should use Restore.
func (h *Handle) RestoreSeekable(k Key, info Info, source ExtentSource, opts Options) error {
	return h.do(true, func(v *Volume) error { return v.restoreLocked(k, info, source, opts, true) })
}

// Repair replaces damaged physical data using a trusted replica of the current
// logical contents. It preserves retained retry history.
func (h *Handle) Repair(k Key, info Info, source ExtentSource, opts Options) error {
	return h.Restore(k, info, source, opts)
}

// ScanRequests streams retry records in RequestID order. fn must not reenter
// the handle or volume.
func (h *Handle) ScanRequests(fn func(RequestRecord) error) error {
	return h.do(false, func(v *Volume) error { return v.scanRequestsLocked(fn) })
}

// RestoreRequest imports original retry state before activating target ownership.
func (h *Handle) RestoreRequest(x RequestRecord) error {
	return h.RestoreRequestWithOptions(x, Options{})
}

// RestoreRequestWithOptions lets a trusted migration consume space reserved
// for its own import. It does not change the original retry fingerprint/result.
func (h *Handle) RestoreRequestWithOptions(x RequestRecord, opts Options) error {
	return h.do(true, func(v *Volume) error { return v.restoreRequestLocked(x, opts) })
}

// SyncThrough flushes the current object locally. It requires a write
// handle because a failed flush fences the volume. Replicated barriers are the
// server's responsibility.
func (h *Handle) SyncThrough(k Key) error {
	return h.do(true, func(v *Volume) error { return v.syncThroughLocked(k) })
}

// ObjectRecord is logical migration metadata, including retained tombstones.
type ObjectRecord struct {
	Key  Key
	Info Info
}

// ScanObjects returns at most limit records strictly after the optional cursor,
// ordered by (bucket, object). Unlike client-facing Scan, it includes tombstones
// and all buckets. It supports bounded-memory local evacuation/recovery. It does
// not grant placement authority. limit must be 1..4096; an empty page ends a scan.
func (h *Handle) ScanObjects(after *Key, limit int) ([]ObjectRecord, error) {
	return withHandle(h, false, func(v *Volume) ([]ObjectRecord, error) {
		if limit < 1 || limit > 4096 {
			return nil, ErrRange
		}
		out := make([]ObjectRecord, 0, limit)
		stop := errors.New("scan page complete")
		var cursor indexKey
		if after != nil {
			cursor = objectKey(*after)
		}
		upper := indexKey{1}
		e := v.walkIndexBounds(cursor, upper, func(x indexEntry) error {
			if x.Key[0] != 0 || (after != nil && !lessKey(cursor, x.Key)) {
				return nil
			}
			k := x.Key.object()
			o, e := v.loadObject(k)
			if e != nil {
				return e
			}
			out = append(out, ObjectRecord{Key: k, Info: o.Info})
			if len(out) == limit {
				return stop
			}
			return nil
		})
		if e == stop {
			e = nil
		}
		return out, e
	})
}

// Generation identifies the full local checkpoint covering the handle's snapshot.
func (h *Handle) Generation() (uint64, error) {
	return withHandle(h, false, func(v *Volume) (uint64, error) { return v.sb.Generation, nil })
}
