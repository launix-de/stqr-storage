// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package volume

import (
	"io"
	"path/filepath"
	"testing"
)

// Deliberately hide the internal borrow capability and use real positioned
// reads. This is a benchmark/control backend, never a production file fallback.
type readAtOnlyFile struct{ file *FileIO }

func (d readAtOnlyFile) ReadAt(data []byte, off int64) (int, error) {
	return d.file.file.ReadAt(data, off)
}
func (d readAtOnlyFile) WriteAt(data []byte, off int64) (int, error) {
	return d.file.WriteAt(data, off)
}
func (d readAtOnlyFile) Size() (int64, error) { return d.file.Size() }
func (d readAtOnlyFile) Sync() error          { return d.file.Sync() }
func (d readAtOnlyFile) Close() error         { return d.file.Close() }
func (d readAtOnlyFile) Alignment() uint32    { return d.file.Alignment() }

func benchmarkMappedTree(b *testing.B, positioned, decoded bool) {
	file, err := CreateFile(filepath.Join(b.TempDir(), "index"), 32<<20)
	if err != nil {
		b.Fatal(err)
	}
	var backend VolumeIO = file
	if positioned {
		backend = readAtOnlyFile{file}
	}
	v, err := Format(backend, FormatOptions{Class: SSD})
	if err != nil {
		file.Close()
		b.Fatal(err)
	}
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		b.Fatal(err)
	}
	for i := uint64(1); i <= 4096; i++ {
		if _, err = h.Put(testKey(i), []byte{1}, Options{Accepted: true}); err != nil {
			h.Close()
			b.Fatal(err)
		}
	}
	h.Close()
	if err = v.Sync(); err != nil {
		b.Fatal(err)
	}
	v.SetCacheLimits(-1, 1)
	h, err = v.Open(false)
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()
	// Both paths run over the same warm OS page cache, avoiding fictitious cold
	// disk-throughput comparisons. The baseline issues actual ReadAt syscalls.
	for i := uint64(1); i <= 4096; i++ {
		if _, err = v.lookup(objectKey(testKey(i))); err != nil {
			b.Fatal(err)
		}
	}
	before := v.ioStatsLocked()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := objectKey(testKey(uint64(i%4096 + 1)))
		var ptr uint64
		if decoded {
			ptr, err = treeLookup(v.node, v.sb.ObjectRoot, key)
		} else {
			ptr, err = v.lookup(key)
		}
		if err != nil || ptr == 0 {
			b.Fatal(ptr, err)
		}
	}
	b.StopTimer()
	after := v.ioStatsLocked()
	b.ReportMetric(float64(after.ReadOps-before.ReadOps)/float64(b.N), "ReadAt/op")
	b.ReportMetric(float64(after.ViewReadOps-before.ViewReadOps)/float64(b.N), "view/op")
}
func BenchmarkMmapTreeLookup(b *testing.B)          { benchmarkMappedTree(b, false, false) }
func BenchmarkReadAtTreeLookup(b *testing.B)        { benchmarkMappedTree(b, true, false) }
func BenchmarkReadAtDecodedTreeLookup(b *testing.B) { benchmarkMappedTree(b, true, true) }

func benchmarkMappedCopy(b *testing.B, positioned bool) {
	file, err := CreateFile(filepath.Join(b.TempDir(), "payload"), 16<<20)
	if err != nil {
		b.Fatal(err)
	}
	var backend VolumeIO = file
	if positioned {
		backend = readAtOnlyFile{file}
	}
	v, err := Format(backend, FormatOptions{Class: SSD})
	if err != nil {
		file.Close()
		b.Fatal(err)
	}
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		b.Fatal(err)
	}
	key := testKey(1)
	if _, err = h.Put(key, make([]byte, BlockSize), Options{}); err != nil {
		h.Close()
		b.Fatal(err)
	}
	h.Close()
	h, err = v.Open(false)
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()
	b.SetBytes(BlockSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = h.CopyTo(key, 0, BlockSize, io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkMmapCopyTo(b *testing.B)   { benchmarkMappedCopy(b, false) }
func BenchmarkReadAtCopyTo(b *testing.B) { benchmarkMappedCopy(b, true) }
