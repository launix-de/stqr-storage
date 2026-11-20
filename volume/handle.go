// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"io"
	"runtime"
	"sync"
)

// Handle holds a volume's read or write lock until Close. It must not be copied.
// Use defer h.Close() immediately after a successful Open. CopyTo may be called
// repeatedly and does not close the handle, including on errors or panics.
//
// Handle methods serialize with each other and Close. While holding a handle,
// use its methods rather than reentering Volume methods (which acquire the same
// lock). Writer/reader callbacks must not reenter the handle or its volume.
// A write handle excludes all other volume access; a read handle excludes writes.
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

// CopyTo streams a range to dst under the existing lock, clipping at EOF.
// It never closes the handle. Both metadata and any further CopyTo calls remain
// protected until Close. This writer API allows future direct-transfer paths;
// the current engine reads/checks/decompresses bounded blocks.
func (h *Handle) CopyTo(k Key, offset, length uint64, dst io.Writer) (uint64, error) {
	return withHandle(h, false, func(v *Volume) (uint64, error) {
		return v.copyToLocked(k, offset, length, dst)
	})
}

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

func (h *Handle) CAS(k Key, offset uint64, expected, replacement []byte, opts Options) (Info, error) {
	if e := validateCAS(offset, expected, replacement, opts); e != nil {
		return Info{}, e
	}
	r, e := h.mutate(opCAS, k, offset, uint64(len(replacement)), bytes.NewReader(replacement), expected, opts)
	return r.Info, e
}
