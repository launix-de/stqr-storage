// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"runtime"
	"testing"
)

func TestCASCheckedComparisonLayouts(t *testing.T) {
	for _, layout := range []string{"raw", "sparse", "gzip", "direct"} {
		t.Run(layout, func(t *testing.T) {
			v, _ := memoryVolume(t, 8<<20)
			defer v.Close()
			key := testKey(1)
			want := bytes.Repeat([]byte("a"), 8192)
			switch layout {
			case "sparse":
				want = make([]byte, 8192)
				copy(want[4096:], bytes.Repeat([]byte("s"), 4096))
				if _, err := testPutRange(v, key, 4096, want[4096:], Options{}); err != nil {
					t.Fatal(err)
				}
			case "direct":
				h, err := v.Open(true)
				if err != nil {
					t.Fatal(err)
				}
				err = h.RestoreEncoded(key, Info{Length: uint64(len(want)), Compression: Raw}, func(yield func(EncodedChunk) error) error {
					return yield(EncodedChunk{Length: uint64(len(want)), DecodedLength: uint32(len(want)), Codec: Raw, RawCRC: checksum(want), EncodedCRC: checksum(want), Data: want})
				}, Options{ImportShadow: true})
				h.Close()
				if err != nil {
					t.Fatal(err)
				}
			default:
				opt := Options{}
				if layout == "gzip" {
					opt.Compression = Codec(Gzip)
				}
				if _, err := testPut(v, key, want, opt); err != nil {
					t.Fatal(err)
				}
			}
			bad := bytes.Clone(want)
			bad[0] ^= 1
			before := v.IOStats().SyncOps
			if _, err := testCAS(v, key, 0, bad, want, Options{Accepted: true, WholeObject: true}); !errors.Is(err, ErrPrecondition) {
				t.Fatal("byte mismatch", err)
			}
			if _, err := testCAS(v, key, 0, want[:len(want)-1], want, Options{Accepted: true, WholeObject: true}); !errors.Is(err, ErrPrecondition) {
				t.Fatal("whole length mismatch", err)
			}
			next := bytes.Repeat([]byte("z"), len(want))
			if _, err := testCAS(v, key, 0, want, next, Options{Accepted: true, WholeObject: true}); err != nil {
				t.Fatal("whole comparison", err)
			}
			if _, err := testCAS(v, key, uint64(len(next)), nil, nil, Options{Accepted: true}); err != nil {
				t.Fatal("empty EOF range", err)
			}
			if info, err := testCAS(v, key, uint64(len(next)+1), nil, nil, Options{Accepted: true}); err != nil || info.Length != uint64(len(next)+1) {
				t.Fatal("zero-length sparse extension", info, err)
			}
			next = append(next, 0)
			if v.IOStats().SyncOps != before {
				t.Fatal("comparison changed accepted Sync semantics")
			}
			mustGet(t, v, key, next)
		})
	}
}
func TestCASCheckedComparisonValidatesBeforeMismatch(t *testing.T) {
	v, disk := memoryVolume(t, 8<<20)
	defer v.Close()
	key := testKey(1)
	raw := bytes.Repeat([]byte("v"), BlockSize+8192)
	if _, err := testPut(v, key, raw, Options{}); err != nil {
		t.Fatal(err)
	}
	object, err := v.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	span := object.Spans[0]
	badAt := span.Location + BlockSize + 5
	if _, err = disk.WriteAt([]byte("!"), int64(badAt)); err != nil {
		t.Fatal(err)
	}
	expected := bytes.Clone(raw[4096 : 4096+BlockSize])
	expected[0] ^= 1
	before := v.IOStats().SyncOps
	if _, err = testCAS(v, key, 4096, expected, bytes.Repeat([]byte("r"), len(expected)), Options{Accepted: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatal("later checksum corruption lost to earlier mismatch", err)
	}
	if _, err = disk.WriteAt([]byte("v"), int64(badAt)); err != nil {
		t.Fatal(err)
	}
	if _, err = testCAS(v, key, 4096, expected, expected, Options{Accepted: true}); !errors.Is(err, ErrPrecondition) {
		t.Fatal("clean mismatch result", err)
	}
	if v.IOStats().SyncOps != before {
		t.Fatal("failed comparison synced")
	}
	mustGet(t, v, key, raw)
}
func TestCASCheckedComparisonAvoidsFullActualCopy(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	defer v.Close()
	key := testKey(1)
	raw := bytes.Repeat([]byte("r"), 24+3218*81)
	if _, err := testPut(v, key, raw, Options{}); err != nil {
		t.Fatal(err)
	}
	wrong := bytes.Clone(raw)
	wrong[0] ^= 1
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	for i := 0; i < 4; i++ {
		if _, err = h.CAS(key, 0, wrong, raw, Options{Accepted: true, WholeObject: true}); !errors.Is(err, ErrPrecondition) {
			t.Fatal(err)
		}
	}
	var begin, end runtime.MemStats
	runtime.ReadMemStats(&begin)
	const repetitions = 64
	for i := 0; i < repetitions; i++ {
		if _, err = h.CAS(key, 0, wrong, raw, Options{Accepted: true, WholeObject: true}); !errors.Is(err, ErrPrecondition) {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&end)
	perOp := (end.TotalAlloc - begin.TotalAlloc) / repetitions
	t.Logf("260682-byte mismatching CAS allocated%d bytes/op", perOp)
	if perOp >= 128<<10 {
		t.Fatal("CAS copied the entire expected range", perOp)
	}
}
func BenchmarkCASWholeReferenceComparison(b *testing.B) {
	d, err := NewMemory(8 << 20)
	if err != nil {
		b.Fatal(err)
	}
	v, err := Format(d, FormatOptions{Class: RAM})
	if err != nil {
		b.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	raw := bytes.Repeat([]byte("r"), 24+3218*81)
	h, err := v.Open(true)
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()
	if _, err = h.Put(key, raw, Options{}); err != nil {
		b.Fatal(err)
	}
	wrong := bytes.Clone(raw)
	wrong[0] ^= 1
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = h.CAS(key, 0, wrong, raw, Options{Accepted: true, WholeObject: true}); !errors.Is(err, ErrPrecondition) {
			b.Fatal(err)
		}
	}
}

type comparisonReadFaultIO struct {
	VolumeIO
	off     int64
	enabled bool
}

var errComparisonRead = errors.New("injected later payload read failure")

func (d *comparisonReadFaultIO) ReadAt(p []byte, off int64) (int, error) {
	if d.enabled && off <= d.off && d.off < off+int64(len(p)) {
		return 0, errComparisonRead
	}
	return d.VolumeIO.ReadAt(p, off)
}
func TestCASCheckedComparisonReadErrorBeforeMismatch(t *testing.T) {
	mem, err := NewMemory(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	disk := &comparisonReadFaultIO{VolumeIO: mem}
	v, err := Format(disk, FormatOptions{Class: RAM})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	raw := bytes.Repeat([]byte("a"), BlockSize+8192)
	if _, err = testPut(v, key, raw, Options{}); err != nil {
		t.Fatal(err)
	}
	o, err := v.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	disk.off = int64(o.Spans[0].Location + BlockSize)
	disk.enabled = true
	expected := bytes.Clone(raw[4096 : 4096+BlockSize])
	expected[0] ^= 1
	if _, err = testCAS(v, key, 4096, expected, expected, Options{Accepted: true}); !errors.Is(err, errComparisonRead) {
		t.Fatal("later read error lost to mismatch", err)
	}
	disk.enabled = false
	mustGet(t, v, key, raw)
}

func BenchmarkCASWholeReferenceAccepted(b *testing.B) {
	d, err := NewMemory(8 << 20)
	if err != nil {
		b.Fatal(err)
	}
	v, err := Format(d, FormatOptions{Class: RAM})
	if err != nil {
		b.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	small := bytes.Repeat([]byte("r"), 24+3218*81)
	large := append(bytes.Clone(small), bytes.Repeat([]byte("x"), 81)...)
	raw := large
	h, err := v.Open(true)
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()
	if _, err = h.Put(key, raw, Options{Accepted: true}); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(small)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next := small
		if len(raw) == len(small) {
			next = large
		}
		if _, err = h.CAS(key, 0, raw, next, Options{Accepted: true, WholeObject: true}); err != nil {
			b.Fatal(err)
		}
		raw = next
	}
}
