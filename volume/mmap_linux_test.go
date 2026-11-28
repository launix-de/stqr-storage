// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package volume

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func mappedTestVolume(t *testing.T, size int64) (*Volume, *FileIO) {
	t.Helper()
	v, err := New(filepath.Join(t.TempDir(), "mapped-volume"), size, SSD)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	file, ok := v.dev.(*meteredIO).VolumeIO.(*FileIO)
	if !ok || !file.hasMapping() || len(file.mapping) != int(size) {
		t.Fatal("file backend was not mapped")
	}
	return v, file
}

type mappedPointerWriter struct {
	pointer *byte
	length  int
	seen    bool
}

func (w *mappedPointerWriter) Write(data []byte) (int, error) {
	if w.seen || len(data) != w.length || len(data) > 0 && &data[0] != w.pointer {
		return 0, errors.New("CopyTo copied or fragmented a contiguous RAW extent")
	}
	w.seen = true
	return len(data), nil
}

func TestMmapPayloadViewAndCopyToBorrowMappedBytes(t *testing.T) {
	v, file := mappedTestVolume(t, 16<<20)
	key := testKey(1)
	want := bytes.Repeat([]byte("m"), BlockSize)
	if _, err := testPut(v, key, want, Options{}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	object, err := v.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	expected := &file.mapping[object.Spans[0].Location]
	before := v.ioStatsLocked()
	_, err = h.WithReadView(key, 0, BlockSize, func(view ReadView) error {
		if view.Length != BlockSize || len(view.Segments) != 1 {
			t.Fatal("unexpected contiguous RAW shape", view.Length, len(view.Segments))
		}
		segment := view.Segments[0]
		if !segment.Borrowed || &segment.Data[0] != expected || !bytes.Equal(segment.Data, want) {
			t.Fatal("payload was not borrowed directly from mmap")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	writer := &mappedPointerWriter{pointer: expected, length: BlockSize}
	if n, err := h.CopyTo(key, 0, BlockSize, writer); err != nil || n != BlockSize || !writer.seen {
		t.Fatal("direct CopyTo", n, err)
	}
	after := v.ioStatsLocked()
	if after.ReadOps != before.ReadOps || after.ViewReadBytes <= before.ViewReadBytes {
		t.Fatal("mmap access reported as positioned disk reads", before, after)
	}
	var small [7]byte
	if n, err := h.ReadAt(key, small[:], 19); err != nil || n != len(small) || !bytes.Equal(small[:], want[19:26]) {
		t.Fatal("mapped ranged read", n, err)
	}
}

func TestMmapDemandPagingDoesNotPopulateWholeVolume(t *testing.T) {
	_, file := mappedTestVolume(t, 128<<20)
	resident := make([]byte, (len(file.mapping)+os.Getpagesize()-1)/os.Getpagesize())
	_, _, errno := syscall.Syscall(syscall.SYS_MINCORE, uintptr(unsafe.Pointer(&file.mapping[0])), uintptr(len(file.mapping)), uintptr(unsafe.Pointer(&resident[0])))
	runtime.KeepAlive(file)
	if errno != 0 {
		t.Fatal(errno)
	}
	var pages int
	for _, state := range resident {
		if state&1 != 0 {
			pages++
		}
	}
	if pages >= len(resident)/8 {
		t.Fatalf("mapping eagerly populated %d/%d pages", pages, len(resident))
	}
}

func TestMmapGrowthRescanShrinkAndBorrowLifetime(t *testing.T) {
	v, file := mappedTestVolume(t, 2<<20)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("kept"), Options{}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	grew := make(chan error, 1)
	go func() { grew <- v.Grow(4 << 20) }()
	select {
	case err := <-grew:
		t.Fatal("Grow remapped while a read handle was alive", err)
	case <-time.After(20 * time.Millisecond):
	}
	if string(readViewBytes(t, h, key, 0, 4)) != "kept" {
		t.Fatal("pinned view changed")
	}
	h.Close()
	if err = <-grew; err != nil {
		t.Fatal(err)
	}
	if len(file.mapping) != 4<<20 {
		t.Fatal("grown mapping has wrong extent")
	}
	if err = file.file.Truncate(6 << 20); err != nil {
		t.Fatal(err)
	}
	if size, err := v.Rescan(); err != nil || size != 6<<20 || len(file.mapping) != 6<<20 {
		t.Fatal("rescan mapping", size, err)
	}
	if err = v.Shrink(2 << 20); err != nil {
		t.Fatal(err)
	}
	if len(file.mapping) != 2<<20 {
		t.Fatal("shrink retained inaccessible mapped tail")
	}
	mustGet(t, v, key, []byte("kept"))
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	if file.mapping != nil {
		t.Fatal("Close leaked mapping")
	}
}

func TestMmapRawTreeSearchBorrowsPagesAndRejectsCorruption(t *testing.T) {
	v, file := mappedTestVolume(t, 16<<20)
	for i := uint64(1); i <= 600; i++ {
		if _, err := testPut(v, testKey(i), []byte("x"), Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.Sync(); err != nil {
		t.Fatal(err)
	}
	v.SetCacheLimits(-1, 1)
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	key := objectKey(testKey(599))
	allocations := testing.AllocsPerRun(20, func() {
		if pointer, err := v.lookup(key); err != nil || pointer == 0 {
			t.Fatal("raw lookup", pointer, err)
		}
	})
	if allocations != 0 {
		t.Fatalf("mapped B-tree lookup allocated %.1f objects", allocations)
	}
	root, err := v.rawTreePage(v.sb.ObjectRoot)
	if err != nil {
		t.Fatal(err)
	}
	if &root.data[0] != &file.mapping[v.sb.ObjectRoot] {
		t.Fatal("B-tree page was copied")
	}
	lower, upper := objectKey(testKey(590)), objectKey(testKey(600))
	var count int
	if err = v.walkIndexBounds(lower, upper, func(e indexEntry) error {
		if lessKey(e.Key, lower) || !lessKey(e.Key, upper) {
			t.Fatal("raw bounds crossed")
		}
		count++
		return nil
	}); err != nil || count != 10 {
		t.Fatal("raw bounded traversal", count, err)
	}
	// Fault injection bypasses the exclusive-owner contract deliberately.
	saved := append([]byte(nil), file.mapping[v.sb.ObjectRoot:v.sb.ObjectRoot+PageSize]...)
	damaged := append([]byte(nil), saved...)
	damaged[32] ^= 1
	if _, err = file.WriteAt(damaged, int64(v.sb.ObjectRoot)); err != nil {
		t.Fatal(err)
	}
	if _, err = v.lookup(key); !errors.Is(err, ErrCorrupt) {
		t.Fatal("mapped corruption hidden by decoded cache", err)
	}
	if _, err = file.WriteAt(saved, int64(v.sb.ObjectRoot)); err != nil {
		t.Fatal(err)
	}
}

type mappedCheckpointGate struct {
	*FileIO
	enabled                        atomic.Bool
	calls                          atomic.Int32
	entered, release, physicalDone chan struct{}
	enterOnce, doneOnce            sync.Once
}

func (d *mappedCheckpointGate) Sync() error {
	if !d.enabled.Load() {
		return d.FileIO.Sync()
	}
	count := d.calls.Add(1)
	if count == 1 {
		d.enterOnce.Do(func() { close(d.entered) })
		<-d.release
	}
	err := d.FileIO.Sync()
	if count >= 4 {
		d.doneOnce.Do(func() { close(d.physicalDone) })
	}
	return err
}
func TestMmapReadViewCoherentDuringOutsideLockCheckpoint(t *testing.T) {
	file, err := CreateFile(filepath.Join(t.TempDir(), "checkpoint-map"), 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	d := &mappedCheckpointGate{FileIO: file, entered: make(chan struct{}), release: make(chan struct{}), physicalDone: make(chan struct{})}
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	want := bytes.Repeat([]byte("new"), 10000)
	if _, err = testPut(v, key, bytes.Repeat([]byte("old"), 10000), Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(v, key, want, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	d.enabled.Store(true)
	fence := make(chan error, 1)
	go func() { fence <- v.Sync() }()
	select {
	case <-d.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("fence did not start")
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(d.release) }) }
	defer release()
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.WithReadView(key, 0, uint64(len(want)), func(view ReadView) error {
		var first bytes.Buffer
		if _, err := view.WriteTo(&first); err != nil {
			return err
		}
		if !bytes.Equal(first.Bytes(), want) {
			return errors.New("view read stale mapped data instead of published overlay")
		}
		release()
		select {
		case <-d.physicalDone:
		case <-time.After(3 * time.Second):
			return errors.New("physical checkpoint blocked behind a borrowed read")
		}
		var second bytes.Buffer
		if _, err := view.WriteTo(&second); err != nil {
			return err
		}
		if !bytes.Equal(second.Bytes(), want) {
			return errors.New("physical overwrite tore borrowed data")
		}
		select {
		case <-fence:
			return errors.New("fence publication escaped an active handle")
		default:
		}
		return nil
	})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-fence:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fence did not finish after handle close")
	}
}

func TestMmapLoopBlockdeviceAndResize(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires an isolated root-owned loop-device fixture")
	}
	if _, err := exec.LookPath("losetup"); err != nil {
		t.Skip("losetup unavailable")
	}
	path := filepath.Join(t.TempDir(), "scratch-device")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Truncate(8 << 20); err != nil {
		file.Close()
		t.Fatal(err)
	}
	file.Close()
	output, err := exec.Command("losetup", "--find", "--show", path).CombinedOutput()
	if err != nil {
		t.Skipf("loop-device fixture unavailable: %v", err)
	}
	device := strings.TrimSpace(string(output))
	defer exec.Command("losetup", "--detach", device).Run()
	dev, err := OpenFile(device)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Format(dev, FormatOptions{Class: SSD})
	if err != nil {
		dev.Close()
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	if _, err = testPut(v, key, []byte("block"), Options{}); err != nil {
		t.Fatal(err)
	}
	if !dev.hasMapping() {
		t.Fatal("blockdevice was not mapped")
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	if got := readViewBytes(t, h, key, 0, 5); string(got) != "block" {
		t.Fatal("mapped blockdevice payload")
	}
	h.Close()
	if err = os.Truncate(path, 16<<20); err != nil {
		t.Fatal(err)
	}
	if output, err = exec.Command("losetup", "--set-capacity", device).CombinedOutput(); err != nil {
		t.Fatalf("resize fixture: %v %s", err, output)
	}
	if size, err := v.Rescan(); err != nil || size != 16<<20 || len(dev.mapping) != 16<<20 {
		t.Fatal("blockdevice mapping rescan", size, err)
	}
	mustGet(t, v, key, []byte("block"))
	if !errors.Is(v.Grow(24<<20), ErrUnsupported) {
		t.Fatal("blockdevice Grow resized physical media")
	}
}

var _ io.Writer = (*mappedPointerWriter)(nil)

type failedMappedSync struct {
	*FileIO
	fail atomic.Bool
}

func (d *failedMappedSync) Sync() error {
	if d.fail.Load() {
		return io.ErrClosedPipe
	}
	return d.FileIO.Sync()
}
func TestMmapCloseDoesNotStartFailedSyncAndStillUnmaps(t *testing.T) {
	file, err := CreateFile(filepath.Join(t.TempDir(), "failed-close"), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	dev := &failedMappedSync{FileIO: file}
	v, err := Format(dev, FormatOptions{Class: SSD})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	if _, err = testPut(v, testKey(1), []byte("accepted"), Options{Accepted: true}); err != nil {
		v.Close()
		t.Fatal(err)
	}
	dev.fail.Store(true)
	beforeClose := v.IOStats()
	if err = v.Close(); !errors.Is(err, ErrRetentionRequired) {
		t.Fatal("Close abandoned private pages", err)
	}
	if file.mapping == nil {
		t.Fatal("rejected Close released live mapping")
	}
	if err = v.Abort(); err != nil {
		t.Fatal("explicit abort failed", err)
	}
	if v.IOStats().SyncOps != beforeClose.SyncOps {
		t.Fatal("Close synced an accepted tail")
	}
	if file.mapping != nil {
		t.Fatal("failed Close leaked mapping")
	}
	if err = v.Close(); err != nil {
		t.Fatal("second Close not idempotent", err)
	}
}

func TestMmapCallbackPanicRetainsExplicitHandleContract(t *testing.T) {
	v, _ := mappedTestVolume(t, 4<<20)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("value"), Options{}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("callback panic swallowed")
			}
		}()
		h.WithReadView(key, 0, 5, func(ReadView) error { panic("consumer panic") })
	}()
	// The operation lock is released on panic, but the enclosing handle retains
	// its read lock until the caller's explicit/finally Close as documented.
	h.Close()
	if err = v.Grow(8 << 20); err != nil {
		t.Fatal("panic leaked handle mutex", err)
	}
}

func TestMmapCounterPositiveControlUsesRealReadAtFallback(t *testing.T) {
	file, err := CreateFile(filepath.Join(t.TempDir(), "positioned-control"), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Format(readAtOnlyFile{file: file}, FormatOptions{Class: SSD})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	if _, err = testPut(v, key, []byte("control"), Options{}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	before := v.ioStatsLocked()
	if got := readViewBytes(t, h, key, 0, 7); string(got) != "control" {
		t.Fatal("positioned fallback content")
	}
	after := v.ioStatsLocked()
	if after.ReadOps <= before.ReadOps || after.ReadBytes <= before.ReadBytes || after.ViewReadOps != before.ViewReadOps {
		t.Fatal("real positioned read was not distinguished from mapped access", before, after)
	}
}
