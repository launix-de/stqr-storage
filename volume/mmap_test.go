// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

func readViewBytes(t *testing.T, h *Handle, key Key, off, length uint64) []byte {
	t.Helper()
	var out bytes.Buffer
	_, err := h.WithReadView(key, off, length, func(view ReadView) error { _, err := view.WriteTo(&out); return err })
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestReadViewSparseRAMAndOwnedCompatibility(t *testing.T) {
	v, mem := memoryVolume(t, 8<<20)
	key := testKey(1)
	if _, err := testPutRange(v, key, 2*BlockSize+7, []byte("tail"), Options{}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var sawZero, sawBorrowed bool
	_, err = h.WithReadView(key, 0, 3*BlockSize, func(view ReadView) error {
		if view.Length != 2*BlockSize+11 {
			t.Fatal("wrong clipped length", view.Length)
		}
		for _, part := range view.Segments {
			if part.Data == nil {
				sawZero = true
			} else if part.Borrowed {
				sawBorrowed = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawZero || !sawBorrowed {
		t.Fatal("RAM view copied its sparse/payload storage")
	}
	got := readViewBytes(t, h, key, 2*BlockSize, 20)
	if !bytes.Equal(got, append(make([]byte, 7), []byte("tail")...)) {
		t.Fatal("sparse clipped view", got)
	}
	owned, _, err := h.GetRange(key, 2*BlockSize+7, 4)
	if err != nil {
		t.Fatal(err)
	}
	owned[0] = 'X'
	if got := readViewBytes(t, h, key, 2*BlockSize+7, 4); string(got) != "tail" {
		t.Fatal("GetRange returned an alias")
	}
	// Sparse backend regions themselves expose zeros without allocating a page.
	before := len(mem.pages)
	data, n, err := mem.borrowAt(uint64(mem.size-PageSize), PageSize)
	if err != nil || n != PageSize || data != nil || len(mem.pages) != before {
		t.Fatal("borrowing an absent RAM page allocated it", err)
	}
}

func TestReadViewCompressionAndDirectRawChunks(t *testing.T) {
	for _, codec := range []Compression{Raw, Gzip, LZ4, XZ} {
		t.Run(codec.String(), func(t *testing.T) {
			v, _ := memoryVolume(t, 12<<20)
			key := testKey(1)
			want := bytes.Repeat([]byte("compressed and sparse views"), 50000)
			if _, err := testPut(v, key, want, Options{Compression: Codec(codec)}); err != nil {
				t.Fatal(err)
			}
			h, err := v.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			if got := readViewBytes(t, h, key, 17, uint64(len(want))); !bytes.Equal(got, want[17:]) {
				t.Fatal("compressed range differs")
			}
			h.Close()
			if codec != Raw {
				change := []byte("replacement")
				if _, err := testPutRange(v, key, 31, change, Options{}); err != nil {
					t.Fatal(err)
				}
				copy(want[31:], change)
				h, err = v.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				defer h.Close()
				if got := readViewBytes(t, h, key, 0, uint64(len(want))); !bytes.Equal(got, want) {
					t.Fatal("direct RAW replacement view differs")
				}
			}
		})
	}
}

func TestReadViewCloseWaitsForCallbackAndNoReentry(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("borrowed"), Options{}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := h.WithReadView(key, 0, 8, func(view ReadView) error {
			close(entered)
			<-release
			runtime.GC()
			if len(view.Segments) != 1 || string(view.Segments[0].Data) != "borrowed" {
				return errors.New("callback view changed")
			}
			return nil
		})
		done <- err
	}()
	<-entered
	closed := make(chan struct{})
	go func() { h.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close invalidated a running callback")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
	if _, err = h.WithReadView(key, 0, 8, func(ReadView) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal("closed view handle accepted", err)
	}
}

type readViewFallback struct{ VolumeIO }

func TestReadViewAlignedFallbackAndValidation(t *testing.T) {
	memory, err := NewMemory(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Format(readViewFallback{memory}, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	want := bytes.Repeat([]byte("fallback"), 1000)
	if _, err = testPut(v, key, want, Options{}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	_, err = h.WithReadView(key, 3, 100, func(view ReadView) error {
		for _, segment := range view.Segments {
			if segment.Borrowed {
				t.Fatal("fallback falsely reported a borrowed backend")
			}
		}
		var out bytes.Buffer
		_, err := view.WriteTo(&out)
		if !bytes.Equal(out.Bytes(), want[3:103]) {
			t.Fatal("fallback data differs")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.WithReadView(testKey(99), 0, 10, func(ReadView) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing view", err)
	}
	if _, err = h.WithReadView(key, 0, 10, nil); !errors.Is(err, ErrRange) {
		t.Fatal("nil callback", err)
	}
	if got := readViewBytes(t, h, key, uint64(len(want)), 100); len(got) != 0 {
		t.Fatal("EOF view not empty")
	}
	marker := errors.New("consumer failed")
	if _, err = h.WithReadView(key, 0, 1, func(ReadView) error { return marker }); !errors.Is(err, marker) {
		t.Fatal("consumer error lost", err)
	}
	if _, err = (ReadView{Length: 1, Segments: []ReadSegment{{Data: []byte("oversized"), Length: 1}}}).WriteTo(io.Discard); !errors.Is(err, ErrRange) {
		t.Fatal("invalid view accepted", err)
	}
}

// The RAM overlay/fence path must also remain correct without disk mmap.
func TestReadViewCheckpointOverlayStaysImmutable(t *testing.T) {
	v, d := newCheckpointGate(t)
	key := testKey(1)
	want := bytes.Repeat([]byte("new-data"), 4000)
	if _, err := testPut(v, key, bytes.Repeat([]byte("old-data"), 4000), Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := testPut(v, key, want, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	entered, release := d.arm(nil)
	defer release()
	fence := make(chan error, 1)
	go func() { fence <- v.Sync() }()
	<-entered
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.WithReadView(key, 0, uint64(len(want)), func(view ReadView) error {
		release()
		for i := 0; i < 10; i++ {
			var out bytes.Buffer
			if _, err := view.WriteTo(&out); err != nil {
				return err
			}
			if !bytes.Equal(out.Bytes(), want) {
				return errors.New("checkpoint changed a borrowed overlay")
			}
			runtime.Gosched()
		}
		return nil
	})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = <-fence; err != nil {
		t.Fatal(err)
	}
}

// Keep a writer with an invalid short-write count from corrupting accounting.
type badViewWriter struct{}

func (badViewWriter) Write(data []byte) (int, error) { return len(data) + 1, nil }
func TestReadViewWriterErrors(t *testing.T) {
	view := ReadView{Length: 3, Segments: []ReadSegment{{Data: []byte("abc"), Length: 3}}}
	if _, err := view.WriteTo(badViewWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}
