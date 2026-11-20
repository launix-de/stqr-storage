// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type alignedIO struct {
	*MemoryIO
	violations int
	alignment  uint32
}

func (d *alignedIO) Alignment() uint32 { return d.alignment }
func (d *alignedIO) ReadAt(b []byte, off int64) (int, error) {
	if off%int64(d.alignment) != 0 || len(b)%int(d.alignment) != 0 {
		d.violations++
		return 0, ErrRange
	}
	return d.MemoryIO.ReadAt(b, off)
}
func (d *alignedIO) WriteAt(b []byte, off int64) (int, error) {
	if off%int64(d.alignment) != 0 || len(b)%int(d.alignment) != 0 {
		d.violations++
		return 0, ErrRange
	}
	return d.MemoryIO.WriteAt(b, off)
}
func TestAlignedDeviceIO(t *testing.T) {
	m, _ := NewMemory(4 << 20)
	d := &alignedIO{MemoryIO: m, alignment: 4096}
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	for i := uint64(0); i < 100; i++ {
		if _, e = testAppend(v, testKey(i%4), []byte("ten-bytes!"), Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if e = v.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	if e = v.Scrub(); e != nil {
		t.Fatal(e)
	}
	if e = v.Compact(); e != nil {
		t.Fatal(e)
	}
	if d.violations != 0 {
		t.Fatal("unaligned I/O")
	}
	d.alignment = 8192
	if _, e = Open(d, OpenOptions{}); !errors.Is(e, ErrUnsupported) {
		t.Fatal(e)
	}
}
func TestFormatRoundTripAndValidation(t *testing.T) {
	v, d := memoryVolume(t, 2<<20)
	h := v.Header()
	b, e := h.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	copyHeader, e := DecodeSuperblock(b)
	if e != nil || copyHeader != h {
		t.Fatalf("%+v %v", copyHeader, e)
	}
	if _, e = ReadHeader(d); e != nil {
		t.Fatal(e)
	}
	b[64] ^= 1
	if _, e = DecodeSuperblock(b); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	if _, e = DecodeSuperblock(nil); !errors.Is(e, ErrUnformatted) {
		t.Fatal(e)
	}
	bad := h
	bad.DataTop = ^uint64(0)
	if _, e = bad.MarshalBinary(); e == nil {
		t.Fatal("bad data top")
	}
	if _, e = Open(d, OpenOptions{VolumeID: testID(1)}); !errors.Is(e, ErrIdentity) {
		t.Fatal(e)
	}
}
func TestFutureFormatIsNotReformatted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	v, e := New(path, 1<<20, HDD)
	if e != nil {
		t.Fatal(e)
	}
	v.Close()
	f, e := os.OpenFile(path, os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	b := make([]byte, PageSize)
	f.ReadAt(b, 0)
	diskOrder.PutUint16(b[8:], FormatVersion+1)
	diskOrder.PutUint32(b[PageSize-4:], checksum(b[:PageSize-4]))
	f.WriteAt(b, 0)
	f.Close()
	if _, e = New(path, 1<<20, HDD); !errors.Is(e, ErrVersion) {
		t.Fatal(e)
	}
}
func TestReformatDoesNotReuseOldArenaPages(t *testing.T) {
	v, d := memoryVolume(t, 4<<20)
	for i := 0; i < 40; i++ {
		if _, e := testAppend(v, testKey(1), []byte("old-data"), Options{}); e != nil {
			t.Fatal(e)
		}
	}
	v, e := Format(d, FormatOptions{Class: RAM})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testAppend(v, testKey(1), []byte("fresh"), Options{}); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 100; i++ {
		if _, e = testTrunc(v, testKey(2), uint64(i), Options{}); e != nil {
			t.Fatal(e)
		}
	}
	mustGet(t, v, testKey(1), []byte("fresh"))
	v = reopen(t, v, d)
	mustGet(t, v, testKey(1), []byte("fresh"))
}
func TestInvalidOperationsLeaveStateUnchanged(t *testing.T) {
	v, _ := memoryVolume(t, 2<<20)
	k := testKey(1)
	if _, e := testPut(v, k, []byte("valid"), Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e := testPutRange(v, k, ^uint64(0), []byte("bad"), Options{}); !errors.Is(e, ErrRange) {
		t.Fatal(e)
	}
	if _, e := testCAS(v, k, 0, []byte("a"), []byte("long"), Options{}); !errors.Is(e, ErrRange) {
		t.Fatal(e)
	}
	if _, e := testPut(v, k, nil, Options{Compression: Codec(Compression(255))}); !errors.Is(e, ErrRange) {
		t.Fatal(e)
	}
	mustGet(t, v, k, []byte("valid"))
	if e := v.SetState(State(100)); !errors.Is(e, ErrRange) {
		t.Fatal(e)
	}
	if e := v.SetGuaranteedReserve(^uint64(0)); !errors.Is(e, ErrFull) {
		t.Fatal(e)
	}
	if e := testForgetRequest(v, ID{}); !errors.Is(e, ErrRange) {
		t.Fatal(e)
	}
	if e := v.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := testPut(v, k, nil, Options{}); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}
func TestDecodeCompressionBounds(t *testing.T) {
	for _, codec := range []Compression{Raw, Gzip, LZ4, XZ} {
		raw := bytes.Repeat([]byte{0}, 100)
		data, e := compressBlock(codec, raw)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = decompressBlock(codec, data, 1); !errors.Is(e, ErrCorrupt) {
			t.Fatalf("codec %s accepted wrong length %v", codec, e)
		}
		if _, e = decompressBlock(codec, data, BlockSize+1); !errors.Is(e, ErrCorrupt) {
			t.Fatal(e)
		}
	}
}
func FuzzDecodeRecord(f *testing.F) {
	b, _ := encodeRecord(record{Op: opPut, Key: testKey(1), Version: 1})
	f.Add(b)
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 2<<20 {
			return
		}
		if len(b) >= 4 {
			b = bytes.Clone(b)
			diskOrder.PutUint32(b[len(b)-4:], checksum(b[:len(b)-4]))
		}
		r, e := decodeRecord(b)
		if e == nil {
			again, e := encodeRecord(r)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = decodeRecord(again); e != nil {
				t.Fatal(e)
			}
		}
	})
}
func FuzzSuperblock(f *testing.F) {
	s := Superblock{Identity: Identity{VolumeID: testID(1), Size: MinVolumeSize}, Generation: 1, BankEpoch: 1}
	s.DataTop = s.bankSize()
	b, _ := s.MarshalBinary()
	f.Add(b)
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) == PageSize {
			b = bytes.Clone(b)
			diskOrder.PutUint32(b[PageSize-4:], checksum(b[:PageSize-4]))
		}
		s, e := DecodeSuperblock(b)
		if e == nil {
			b, e := s.MarshalBinary()
			if e != nil {
				t.Fatal(e)
			}
			if _, e = DecodeSuperblock(b); e != nil {
				t.Fatal(e)
			}
		}
	})
}

func TestRecognizableDamagedHeaderDoesNotAutoformat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	v, e := New(path, 1<<20, HDD)
	if e != nil {
		t.Fatal(e)
	}
	v.Close()
	f, e := os.OpenFile(path, os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	for _, off := range []int64{0, PageSize} {
		b := make([]byte, PageSize)
		f.ReadAt(b, off)
		b[6] ^= 1
		f.WriteAt(b, off)
	}
	f.Close()
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = New(path, 1<<20, HDD); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	after, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("damaged volume was reformatted")
	}
}
