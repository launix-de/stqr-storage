// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Trusted encoded migration can provide many sub-page chunks. The accepted
// builder must bound their partial-page staging before metadata publication,
// just as it bounds a contiguous upload. Measure live payload retention while
// the real source callback is still active, not after the builder has closed.
func TestAcceptedSpillEncodedPartialPagesBoundedDuringImport(t *testing.T) {
	d := newCrash(64 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	const chunks = 8192
	chunk := make([]byte, PageSize-1)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}
	crc := checksum(chunk)
	passes := 0
	var retained uint64
	source := func(yield func(EncodedChunk) error) error {
		passes++
		var before runtime.MemStats
		if passes == 2 {
			runtime.GC()
			runtime.ReadMemStats(&before)
		}
		for i := 0; i < chunks; i++ {
			if err := yield(EncodedChunk{Offset: uint64(i * len(chunk)), Length: uint64(len(chunk)), Data: chunk, DecodedLength: uint32(len(chunk)), Codec: Raw, RawCRC: crc, EncodedCRC: crc}); err != nil {
				return err
			}
		}
		if passes == 2 {
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			if after.HeapAlloc > before.HeapAlloc {
				retained = after.HeapAlloc - before.HeapAlloc
			}
		}
		return nil
	}
	key := testKey(1)
	length := uint64(chunks * len(chunk))
	before := v.IOStats()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.RestoreEncoded(key, Info{Length: length, Compression: Raw}, source, Options{Accepted: true, ImportShadow: true})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("32-MiB partial-page import retained %d bytes in active source callback", retained)
	if passes != 2 || retained > 16<<20 {
		t.Fatal("encoded partial-page staging retained an object-sized page directory", passes, retained)
	}
	if v.deferred.store.data == nil {
		t.Fatal("large import did not reach disposable spill")
	}
	if after := v.IOStats(); after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
		t.Fatal("accepted encoded import wrote/synced volume", before, after)
	}
	for _, start := range []uint64{0, PageSize - 9, length / 2, length - 1024} {
		got, info, err := testGetRange(v, key, start, 1024)
		if err != nil || info.Length != length || len(got) != 1024 {
			t.Fatal("imported range", start, info, err)
		}
		for i, b := range got {
			if b != chunk[(start+uint64(i))%uint64(len(chunk))] {
				t.Fatal("partial-page import corrupted data", start, i)
			}
		}
	}
}

func TestAcceptedSpillEncodedPartialPagesRAMBackpressureBeforeFullImport(t *testing.T) {
	scratch := t.TempDir()
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(name, scratch)
	}
	v, _ := memoryVolume(t, 64<<20)
	defer v.Close()
	key := testKey(1)
	seed, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = seed.RestoreEncoded(key, Info{Length: 4, Compression: Raw}, func(yield func(EncodedChunk) error) error {
		data := []byte("keep")
		return yield(EncodedChunk{Length: 4, Data: data, DecodedLength: 4, Codec: Raw, RawCRC: checksum(data), EncodedCRC: checksum(data)})
	}, Options{ImportShadow: true})
	seed.Close()
	if err != nil {
		t.Fatal(err)
	}
	const chunks = 8192
	chunk := bytes.Repeat([]byte{'x'}, PageSize-1)
	crc := checksum(chunk)
	passes, consumed := 0, 0
	source := func(yield func(EncodedChunk) error) error {
		passes++
		for i := 0; i < chunks; i++ {
			if err := yield(EncodedChunk{Offset: uint64(i * len(chunk)), Length: uint64(len(chunk)), Data: chunk, DecodedLength: uint32(len(chunk)), Codec: Raw, RawCRC: crc, EncodedCRC: crc}); err != nil {
				return err
			}
			if passes == 2 {
				consumed++
			}
		}
		return nil
	}
	before, generation := v.IOStats(), v.Header().Generation
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.RestoreEncoded(key, Info{Length: uint64(chunks * len(chunk)), Compression: Raw}, source, Options{Accepted: true, ImportShadow: true})
	h.Close()
	if !errors.Is(err, ErrWritebackPressure) || passes != 2 || consumed >= chunks/2 {
		t.Fatal("RAM import did not backpressure during bounded partial-page staging", err, passes, consumed)
	}
	mustGet(t, v, key, []byte("keep"))
	if after := v.IOStats(); after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps || v.Header().Generation != generation {
		t.Fatal("rejected RAM import changed publication/durability", before, after)
	}
	files, err := os.ReadDir(scratch)
	if err != nil || len(files) != 0 {
		t.Fatal("RAM import used disk staging", files, err)
	}
	t.Logf("RAM backpressured after %d second-pass chunks without disk spill or publication", consumed)
}

// Use a real file and reopen it without a simulated power cut: even an orderly
// close must not publish an unrequested accepted tail through the page cache.
func TestAcceptedSpillFileOverwriteCASAndAppendRetry(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "discard", true: "explicit-fence"}[explicit], func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "volume")
			v, err := New(name, 48<<20, SSD)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			key := testKey(1)
			old := []byte("durable before accepted writes")
			if _, err = testPut(v, key, old, Options{}); err != nil {
				t.Fatal(err)
			}
			before := v.IOStats()
			for _, value := range []byte{'a', 'b'} {
				if _, err = testPut(v, key, bytes.Repeat([]byte{value}, 5<<20), Options{Accepted: true}); err != nil {
					t.Fatal(err)
				}
			}
			if v.deferred.store.data == nil {
				t.Fatal("mixed-operation fixture did not spill")
			}
			want := bytes.Repeat([]byte{'b'}, 5<<20)
			appendOptions := Options{Accepted: true, RequestID: testID(9001)}
			first, err := testAppend(v, key, []byte("append once"), appendOptions)
			if err != nil || first.Offset != uint64(len(want)) {
				t.Fatal("append", first, err)
			}
			want = append(want, []byte("append once")...)
			if retry, err := testAppend(v, key, []byte("append once"), appendOptions); err != nil || retry != first {
				t.Fatal("accepted append retry", retry, first, err)
			}
			if _, err = testAppend(v, key, []byte("different"), appendOptions); !errors.Is(err, ErrRequestID) {
				t.Fatal("conflicting append retry", err)
			}
			if _, err = testCAS(v, key, 17, []byte("bbbb"), []byte("CAS!"), Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			copy(want[17:], "CAS!")
			if _, err = testCAS(v, key, 17, []byte("bbbb"), []byte("bad!"), Options{Accepted: true}); !errors.Is(err, ErrPrecondition) {
				t.Fatal("mismatched CAS", err)
			}
			h, err := v.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			got := readViewBytes(t, h, key, 0, uint64(len(want)))
			h.Close()
			if !bytes.Equal(got, want) {
				t.Fatal("view did not contain the latest overwrite/CAS and exactly one append")
			}
			if after := v.IOStats(); after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
				t.Fatal("mixed accepted operations wrote/synced volume", before, after)
			}
			if explicit {
				if err = v.Sync(); err != nil {
					t.Fatal(err)
				}
				if after := v.IOStats(); after.WriteOps <= before.WriteOps || after.SyncOps-before.SyncOps != 4 {
					t.Fatal("explicit checkpoint did not perform the four-stage fence", before, after)
				}
			}
			beforeClose := v.IOStats()
			if err = v.Close(); err != nil {
				t.Fatal(err)
			}
			if after := v.IOStats(); after.WriteOps != beforeClose.WriteOps || after.SyncOps != beforeClose.SyncOps {
				t.Fatal("close wrote/synced an unrequested tail", beforeClose, after)
			}
			reopened, err := New(name, 0, SSD)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if !explicit {
				mustGet(t, reopened, key, old)
				return
			}
			mustGet(t, reopened, key, want)
			beforeRetry := reopened.IOStats()
			if retry, err := testAppend(reopened, key, []byte("append once"), appendOptions); err != nil || retry != first {
				t.Fatal("durable append receipt lost across file reopen", retry, first, err)
			}
			mustGet(t, reopened, key, want)
			if after := reopened.IOStats(); after.WriteOps != beforeRetry.WriteOps || after.SyncOps != beforeRetry.SyncOps {
				t.Fatal("accepted receipt retry requested persistence", beforeRetry, after)
			}
		})
	}
}

type spillReviewFenceIO struct {
	VolumeIO
	entered, release, finished chan struct{}
	calls                      int
}

func (d *spillReviewFenceIO) Sync() error {
	if d.entered != nil {
		d.calls++
		if d.calls == 1 {
			close(d.entered)
			<-d.release
		}
	}
	err := d.VolumeIO.Sync()
	if d.entered != nil && d.calls == 4 {
		close(d.finished)
	}
	return err
}

func TestAcceptedSpillReadViewPinsFilesUntilFencePublication(t *testing.T) {
	memory, err := NewMemory(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	d := &spillReviewFenceIO{VolumeIO: memory}
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	want := bytes.Repeat([]byte{'n'}, 64<<10)
	if _, err = testPut(v, key, bytes.Repeat([]byte{'o'}, len(want)), Options{}); err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	err = v.beginDeferred()
	if err == nil {
		v.deferred.store.limit = 2
	}
	v.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(v, key, want, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	spill := v.deferred.store.data
	if spill == nil {
		t.Fatal("view fixture did not spill")
	}
	d.entered, d.release, d.finished = make(chan struct{}), make(chan struct{}), make(chan struct{})
	fenced := make(chan error, 1)
	go func() { fenced <- v.Sync() }()
	select {
	case <-d.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint did not enter backend")
	}
	h, err := v.Open(false)
	if err != nil {
		close(d.release)
		t.Fatal(err)
	}
	_, err = h.WithReadView(key, 0, uint64(len(want)), func(view ReadView) error {
		close(d.release)
		select {
		case <-d.finished:
		case <-time.After(3 * time.Second):
			return errors.New("physical checkpoint did not finish while view was pinned")
		}
		if _, err := spill.Stat(); err != nil {
			return errors.New("fence closed the spill before read-view release")
		}
		select {
		case <-fenced:
			return errors.New("fence publication completed while its read view was pinned")
		default:
		}
		var got bytes.Buffer
		if _, err := view.WriteTo(&got); err != nil {
			return err
		}
		if !bytes.Equal(got.Bytes(), want) {
			return errors.New("spilled view changed across physical checkpoint")
		}
		return nil
	})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	awaitCheckpointTest(t, fenced)
	if _, err = spill.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("completed checkpoint retained its spill handle", err)
	}
	if _, err = os.Stat(spill.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("completed checkpoint retained its spill file", err)
	}
	mustGet(t, v, key, want)
}
