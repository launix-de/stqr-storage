// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func mustOpenHandle(t *testing.T, v *Volume, write bool) *Handle {
	t.Helper()
	h, e := v.Open(write)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func TestHandleMetadataAndRepeatedCopy(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k := testKey(1)
	data := bytes.Repeat([]byte("original"), BlockSize/4)
	want, e := v.Put(k, data, Options{Compression: Codec(LZ4)})
	if e != nil {
		t.Fatal(e)
	}
	h := mustOpenHandle(t, v, false)
	if v.mu.TryLock() {
		v.mu.Unlock()
		t.Fatal("read handle did not exclude writers")
	}
	if !v.mu.TryRLock() {
		t.Fatal("read handle did not allow another reader")
	}
	v.mu.RUnlock()
	info, e := h.Stat(k)
	if e != nil || info != want {
		t.Fatalf("stat: %+v %v", info, e)
	}
	writerDone := make(chan error, 1)
	go func() {
		_, e := v.Put(k, []byte("new"), Options{})
		writerDone <- e
	}()
	for _, offset := range []uint64{0, BlockSize - 3, uint64(len(data)) - 2} {
		var out bytes.Buffer
		n, e := h.CopyTo(k, offset, BlockSize, &out)
		end := min(uint64(len(data)), offset+BlockSize)
		if e != nil || n != end-offset || !bytes.Equal(out.Bytes(), data[offset:end]) {
			t.Fatalf("copy %d: %d %v", offset, n, e)
		}
	}
	info, e = h.Stat(k)
	if e != nil || info != want {
		t.Fatalf("metadata changed while open: %+v %v", info, e)
	}
	select {
	case e := <-writerDone:
		t.Fatalf("writer completed while handle open: %v", e)
	default:
	}
	h.Close()
	select {
	case e := <-writerDone:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release writer")
	}
	mustGet(t, v, k, []byte("new"))
	if _, e := h.CopyTo(k, 0, 1, io.Discard); !errors.Is(e, ErrClosed) {
		t.Fatalf("closed CopyTo: %v", e)
	}
	if _, e := h.Stat(k); !errors.Is(e, ErrClosed) {
		t.Fatalf("closed Stat: %v", e)
	}
}

type handleWriter func([]byte) (int, error)

func (f handleWriter) Write(b []byte) (int, error) { return f(b) }

func TestHandleCopyFailuresKeepSessionOpen(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	k := testKey(1)
	v.Put(k, []byte("abc"), Options{})
	h := mustOpenHandle(t, v, false)
	wantErr := errors.New("writer failed")
	if n, e := h.CopyTo(k, 0, 3, handleWriter(func([]byte) (int, error) { return 1, wantErr })); n != 1 || e != wantErr {
		t.Fatalf("writer error: %d %v", n, e)
	}
	if n, e := h.CopyTo(k, 0, 3, handleWriter(func([]byte) (int, error) { return 1, nil })); n != 1 || e != io.ErrShortWrite {
		t.Fatalf("short write: %d %v", n, e)
	}
	func() {
		defer func() {
			if recover() != "writer panic" {
				t.Error("missing writer panic")
			}
		}()
		h.CopyTo(k, 0, 3, handleWriter(func([]byte) (int, error) { panic("writer panic") }))
	}()
	if v.mu.TryLock() {
		v.mu.Unlock()
		t.Fatal("CopyTo released the session on failure")
	}
	var out bytes.Buffer
	if n, e := h.CopyTo(k, 0, 3, &out); n != 3 || e != nil || out.String() != "abc" {
		t.Fatalf("reuse after failures: %d %v %q", n, e, out.String())
	}
	if _, e := h.CopyTo(k, 4, 1, io.Discard); e != ErrRange {
		t.Fatalf("invalid range: %v", e)
	}
	if _, e := h.CopyTo(testKey(2), 0, 1, io.Discard); e != ErrNotFound {
		t.Fatalf("missing object: %v", e)
	}
	h.Close()
	if !v.mu.TryLock() {
		t.Fatal("Close did not release after errors")
	}
	v.mu.Unlock()
}

func TestWriteHandleMutations(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k := testKey(1)
	h := mustOpenHandle(t, v, true)
	if v.mu.TryRLock() {
		v.mu.RUnlock()
		t.Fatal("write handle allowed another reader")
	}
	if _, e := h.Put(k, []byte("abc"), Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e := h.PutRange(k, 1, []byte("B"), Options{}); e != nil {
		t.Fatal(e)
	}
	opts := Options{RequestID: testID(5)}
	r, e := h.Append(k, []byte("d"), opts)
	if e != nil || r.Offset != 3 {
		t.Fatalf("append: %+v %v", r, e)
	}
	r2, e := h.AppendReader(k, bytes.NewReader([]byte("d")), 1, opts)
	if e != nil || r2 != r {
		t.Fatalf("retry: %+v %v", r2, e)
	}
	if _, e := h.CAS(k, 2, []byte("c"), []byte("C"), Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e := h.CAS(k, 2, []byte("c"), []byte("c"), Options{}); e != ErrPrecondition {
		t.Fatalf("CAS conflict: %v", e)
	}
	if _, e := h.CAS(k, 0, nil, []byte("x"), Options{}); e != ErrRange {
		t.Fatalf("invalid CAS: %v", e)
	}
	if _, e := h.Trunc(k, 8, Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e := h.PutRangeReader(k, 7, bytes.NewReader([]byte("!")), 1, Options{}); e != nil {
		t.Fatal(e)
	}
	want := []byte{'a', 'B', 'C', 'd', 0, 0, 0, '!'}
	b := make([]byte, 10)
	if n, e := h.ReadAt(k, b, 0); n != 8 || e != io.EOF || !bytes.Equal(b[:n], want) {
		t.Fatalf("read: %d %v %q", n, e, b)
	}
	info, e := h.Stat(k)
	if e != nil || info.Length != 8 {
		t.Fatalf("stat: %+v %v", info, e)
	}
	h.Close()
	mustGet(t, v, k, want)
	h = mustOpenHandle(t, v, true)
	if e := h.Delete(k, Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e := h.Stat(k); e != ErrNotFound {
		t.Fatalf("deleted: %v", e)
	}
}

func TestReadHandleRejectsWrites(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	h := mustOpenHandle(t, v, false)
	k := testKey(1)
	ops := []func() error{
		func() error { _, e := h.Put(k, nil, Options{}); return e },
		func() error { _, e := h.PutRange(k, 0, nil, Options{}); return e },
		func() error { _, e := h.Append(k, nil, Options{}); return e },
		func() error { _, e := h.Trunc(k, 0, Options{}); return e },
		func() error { return h.Delete(k, Options{}) },
		func() error { _, e := h.CAS(k, 0, nil, nil, Options{}); return e },
	}
	for i, op := range ops {
		if e := op(); e != ErrReadOnly {
			t.Fatalf("write %d: %v", i, e)
		}
	}
}

func TestHandleCloseWaitsForCopy(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	k := testKey(1)
	v.Put(k, []byte("abc"), Options{})
	h := mustOpenHandle(t, v, true)
	entered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(finish) }) }
	defer unblock()
	copied := make(chan error, 1)
	go func() {
		_, e := h.CopyTo(k, 0, 3, handleWriter(func(b []byte) (int, error) {
			close(entered)
			<-finish
			return len(b), nil
		}))
		copied <- e
	}()
	<-entered
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.Close() }()
	}
	runtime.GC()
	if v.mu.TryRLock() {
		v.mu.RUnlock()
		t.Fatal("Close unlocked during CopyTo")
	}
	unblock()
	if e := <-copied; e != nil {
		t.Fatal(e)
	}
	wg.Wait()
	// Double/concurrent Close must not unlock a later, unrelated session.
	next := mustOpenHandle(t, v, true)
	h.Close()
	if v.mu.TryRLock() {
		v.mu.RUnlock()
		t.Fatal("old Close unlocked a new session")
	}
	next.Close()
}

//go:noinline
func abandonHandle(v *Volume, write bool) error {
	h, e := v.Open(write)
	runtime.KeepAlive(h)
	return e
}

func awaitHandleCleanup(t *testing.T, v *Volume) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if v.mu.TryLock() {
			v.mu.Unlock()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("abandoned handle cleanup not observed after repeated GC")
}

func TestAbandonedHandleCleanup(t *testing.T) {
	for _, write := range []bool{false, true} {
		v, _ := memoryVolume(t, 1<<20)
		if e := abandonHandle(v, write); e != nil {
			t.Fatal(e)
		}
		awaitHandleCleanup(t, v)
		v.Close()
	}
}

// No deferred Close or retained handle: only CopyTo itself keeps the target
// reachable. Its cleanup must not release the volume during a blocked callback.
//
//go:noinline
func copyAndAbandon(v *Volume, dst io.Writer) error {
	h, e := v.Open(false)
	if e != nil {
		return e
	}
	_, e = h.CopyTo(testKey(1), 0, 3, dst)
	return e
}

func TestHandleAliveThroughoutCopy(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	v.Put(testKey(1), []byte("abc"), Options{})
	entered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(finish) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- copyAndAbandon(v, handleWriter(func(b []byte) (int, error) {
			close(entered)
			<-finish
			return len(b), nil
		}))
	}()
	<-entered
	for i := 0; i < 5; i++ {
		runtime.GC()
		if v.mu.TryLock() {
			v.mu.Unlock()
			t.Fatal("cleanup ran while CopyTo was active")
		}
	}
	unblock()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	awaitHandleCleanup(t, v)
}

func TestHandleOpenFailureAndZeroValue(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	v.SetState(Draining)
	if h, e := v.Open(true); h != nil || e != ErrDraining {
		t.Fatalf("draining open: %v %v", h, e)
	}
	h := mustOpenHandle(t, v, false)
	h.Close()
	v.Close()
	for _, write := range []bool{false, true} {
		if h, e := v.Open(write); h != nil || e != ErrClosed {
			t.Fatalf("closed open: %v %v", h, e)
		}
		if !v.mu.TryLock() {
			t.Fatal("failed Open leaked lock")
		}
		v.mu.Unlock()
	}
	for _, h := range []*Handle{nil, {}} {
		if e := h.Close(); e != nil {
			t.Fatal(e)
		}
		if _, e := h.ReadAt(testKey(1), nil, 0); e != ErrClosed {
			t.Fatalf("zero read: %v", e)
		}
	}
}

func ExampleVolume_Open() {
	v, err := NewRAM(1 << 20)
	if err != nil {
		panic(err)
	}
	defer v.Close()
	h, err := v.Open(false)
	if err != nil {
		panic(err)
	}
	defer h.Close()
	key := Key{}
	info, err := h.Stat(key)
	if err != nil {
		return
	}
	// Headers can now use info.Length/Version without a race before streaming.
	h.CopyTo(key, 0, info.Length, io.Discard)
	h.CopyTo(key, 0, min(info.Length, 1024), io.Discard)
}
