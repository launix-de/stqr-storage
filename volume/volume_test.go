// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

func testKey(n uint64) Key {
	var k Key
	k.Bucket[0] = 1
	binary.BigEndian.PutUint64(k.Object[24:], n)
	return k
}
func testID(n uint64) ID { var id ID; binary.LittleEndian.PutUint64(id[:], n); return id }
func memoryVolume(t *testing.T, size int64) (*Volume, *MemoryIO) {
	t.Helper()
	d, e := NewMemory(size)
	if e != nil {
		t.Fatal(e)
	}
	v, e := Format(d, FormatOptions{Class: RAM})
	if e != nil {
		t.Fatal(e)
	}
	return v, d
}
func mustGet(t *testing.T, v *Volume, k Key, want []byte) {
	t.Helper()
	got, _, e := v.Get(k)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatalf("Get(%x): got %q, error %v, want %q", k.Object[24:], got, e, want)
	}
}
func reopen(t *testing.T, v *Volume, d VolumeIO) *Volume {
	t.Helper()
	w, e := Open(d, OpenOptions{CacheEntries: 2, CacheBytes: 1024})
	if e != nil {
		t.Fatal(e)
	}
	return w
}

func TestObjectLifecycleAndRecovery(t *testing.T) {
	v, d := memoryVolume(t, 4<<20)
	k := testKey(1)
	a, e := v.Append(k, []byte("abc"), Options{RequestID: testID(1)})
	if e != nil || a.Offset != 0 || a.Info.Length != 3 {
		t.Fatalf("append %+v %v", a, e)
	}
	b, e := v.Append(k, []byte("de"), Options{RequestID: testID(2)})
	if e != nil || b.Offset != 3 {
		t.Fatalf("append %+v %v", b, e)
	}
	mustGet(t, v, k, []byte("abcde"))
	v = reopen(t, v, d)
	mustGet(t, v, k, []byte("abcde"))
	again, e := v.Append(k, []byte("de"), Options{RequestID: testID(2)})
	if e != nil || again != b {
		t.Fatalf("retry: %+v %v", again, e)
	}
	if _, e = v.Append(k, []byte("different"), Options{RequestID: testID(2)}); !errors.Is(e, ErrRequestID) {
		t.Fatalf("conflicting ID: %v", e)
	}
	if _, e = v.PutRange(k, 1, []byte("XY"), Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, []byte("aXYde"))
	if _, e = v.Trunc(k, 2, Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e = v.Append(k, []byte("!"), Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, []byte("aX!"))
	if e = v.Delete(k, Options{RequestID: testID(3)}); e != nil {
		t.Fatal(e)
	}
	if _, e = v.Stat(k); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	if _, e = v.Stat(k); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	if _, e = v.Put(k, []byte("new"), Options{}); e != nil {
		t.Fatal(e)
	}
	if e = v.Delete(k, Options{RequestID: testID(3)}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, []byte("new"))
	if e = v.Scrub(); e != nil {
		t.Fatal(e)
	}
}
func TestSparseAndPartialBlocks(t *testing.T) {
	v, d := memoryVolume(t, 4<<20)
	k := testKey(1)
	const hole = uint64(1) << 42
	if _, e := v.PutRange(k, hole, []byte("tail"), Options{}); e != nil {
		t.Fatal(e)
	}
	st := v.Stats()
	if st.AllocatedPhysical > 64<<10 || st.LogicalBytes != hole+4 {
		t.Fatalf("sparse stats %+v", st)
	}
	b, info, e := v.GetRange(k, hole-2, 6)
	if e != nil || !bytes.Equal(b, []byte{0, 0, 't', 'a', 'i', 'l'}) || info.Length != hole+4 {
		t.Fatalf("%q %+v %v", b, info, e)
	}
	if _, e = v.Trunc(k, hole+100, Options{}); e != nil {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	b, _, e = v.GetRange(k, hole+2, 6)
	if e != nil || !bytes.Equal(b, []byte{'i', 'l', 0, 0, 0, 0}) {
		t.Fatalf("%v %v", b, e)
	}
	ext, e := v.Extents(k)
	if e != nil || len(ext) != 3 || !ext[0].Zero || !ext[2].Zero {
		t.Fatalf("%+v %v", ext, e)
	}
	if _, e = v.Trunc(k, hole+2, Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e = v.Trunc(k, hole+6, Options{}); e != nil {
		t.Fatal(e)
	}
	b, _, e = v.GetRange(k, hole, 6)
	if e != nil || !bytes.Equal(b, []byte{'t', 'a', 0, 0, 0, 0}) {
		t.Fatalf("%v %v", b, e)
	}
	if e = v.Compact(); e != nil {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	b, _, e = v.GetRange(k, hole, 6)
	if e != nil || !bytes.Equal(b, []byte{'t', 'a', 0, 0, 0, 0}) {
		t.Fatalf("after GC: %v %v", b, e)
	}
}
func TestCodecsRangesAndRecompress(t *testing.T) {
	for _, c := range []Compression{Raw, Gzip, LZ4, XZ} {
		t.Run(c.String(), func(t *testing.T) {
			v, d := memoryVolume(t, 8<<20)
			k := testKey(1)
			want := bytes.Repeat([]byte("abcdef"), 24000)
			info, e := v.Put(k, want, Options{Compression: Codec(c)})
			if e != nil {
				t.Fatal(e)
			}
			change := []byte("OVERLAPPING")
			if _, e = v.PutRange(k, BlockSize-4, change, Options{}); e != nil {
				t.Fatal(e)
			}
			copy(want[BlockSize-4:], change)
			if _, e = v.Append(k, []byte("end"), Options{}); e != nil {
				t.Fatal(e)
			}
			want = append(want, []byte("end")...)
			mustGet(t, v, k, want)
			v = reopen(t, v, d)
			mustGet(t, v, k, want)
			before, _ := v.Stat(k)
			target := Gzip
			if c == Gzip {
				target = Raw
			}
			if e = v.Recompress(k, target); e != nil {
				t.Fatal(e)
			}
			after, _ := v.Stat(k)
			if after.Version != before.Version || after.Length != before.Length || after.Compression != target || info.Compression != c {
				t.Fatalf("before %+v after %+v", before, after)
			}
			mustGet(t, v, k, want)
			if e = v.Compact(); e != nil {
				t.Fatal(e)
			}
			v = reopen(t, v, d)
			mustGet(t, v, k, want)
			if e = v.Scrub(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestCASAndPreconditions(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k := testKey(1)
	info, e := v.Put(k, []byte("abcd"), Options{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = v.CAS(k, 1, []byte("bc"), []byte("XY"), Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, []byte("aXYd"))
	if _, e = v.CAS(k, 1, []byte("bc"), []byte("ZZ"), Options{}); !errors.Is(e, ErrPrecondition) {
		t.Fatal(e)
	}
	if _, e = v.Put(k, []byte("stale"), Options{ExpectedVersion: &info.Version}); !errors.Is(e, ErrPrecondition) {
		t.Fatal(e)
	}
	info, _ = v.Stat(k)
	if _, e = v.CAS(k, 0, nil, []byte("replacement"), Options{WholeObject: true, ExpectedVersion: &info.Version}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, []byte("replacement"))
	if _, e = v.CAS(testKey(2), 0, nil, []byte("created"), Options{WholeObject: true, CreateOnly: true}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, testKey(2), []byte("created"))
}
func TestIndexSpillAndCompaction(t *testing.T) {
	v, d := memoryVolume(t, 16<<20)
	v.SetCacheLimits(2, 512)
	for i := 0; i < 900; i++ {
		if _, e := v.Put(testKey(uint64(i)), []byte(fmt.Sprintf("object-%d", i)), Options{}); e != nil {
			t.Fatalf("put %d: %v", i, e)
		}
	}
	for i := 0; i < 900; i += 17 {
		mustGet(t, v, testKey(uint64(i)), []byte(fmt.Sprintf("object-%d", i)))
	}
	if len(v.cache.items) > 2 || len(v.delta) > deltaLimit || len(v.runs) > maxIndexRuns {
		t.Fatalf("cache %d delta %d runs %d", len(v.cache.items), len(v.delta), len(v.runs))
	}
	if e := v.Compact(); e != nil {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	for i := 0; i < 900; i += 19 {
		mustGet(t, v, testKey(uint64(i)), []byte(fmt.Sprintf("object-%d", i)))
	}
	if v.Stats().Objects != 900 {
		t.Fatal(v.Stats())
	}
}
func TestConcurrentAppendAndReaders(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	k := testKey(1)
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	var offsets sync.Map
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := v.Append(k, []byte{byte(i)}, Options{RequestID: testID(uint64(i + 1))})
			if e == nil {
				if _, dup := offsets.LoadOrStore(r.Offset, true); dup {
					e = fmt.Errorf("duplicate offset %d", r.Offset)
				}
			}
			if e != nil {
				errs <- e
			}
			_, _ = v.Stat(k)
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	info, e := v.Stat(k)
	if e != nil || info.Length != 100 {
		t.Fatalf("%+v %v", info, e)
	}
}
func TestFileConstructorsAndExclusiveLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	v, e := New(path, 4<<20, SSD)
	if e != nil {
		t.Fatal(e)
	}
	id := v.Identity()
	if _, e = New(path, 0, HDD); e == nil {
		t.Fatal("double open allowed")
	}
	if _, e = v.Put(testKey(1), []byte("persist"), Options{}); e != nil {
		t.Fatal(e)
	}
	if e = v.Close(); e != nil {
		t.Fatal(e)
	}
	v, e = New(path, 0, HDD)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	if v.Identity() != id {
		t.Fatal("identity changed")
	}
	mustGet(t, v, testKey(1), []byte("persist"))
}
func TestRandomModel(t *testing.T) {
	v, d := memoryVolume(t, 8<<20)
	model := map[Key][]byte{}
	rng := rand.New(rand.NewSource(71))
	for i := 0; i < 500; i++ {
		k := testKey(uint64(rng.Intn(12)))
		old, exists := model[k]
		data := bytes.Repeat([]byte{byte(i)}, rng.Intn(35))
		var e error
		switch rng.Intn(5) {
		case 0:
			_, e = v.Put(k, data, Options{})
			model[k] = append([]byte{}, data...)
		case 1:
			_, e = v.Append(k, data, Options{})
			model[k] = append(old, data...)
		case 2:
			n := rng.Intn(80)
			_, e = v.Trunc(k, uint64(n), Options{})
			b := make([]byte, n)
			copy(b, old)
			model[k] = b
		case 3:
			off := rng.Intn(80)
			_, e = v.PutRange(k, uint64(off), data, Options{})
			b := make([]byte, max(len(old), off+len(data)))
			copy(b, old)
			copy(b[off:], data)
			model[k] = b
		case 4:
			e = v.Delete(k, Options{})
			delete(model, k)
		}
		_ = exists
		if e != nil {
			t.Fatalf("step %d: %v", i, e)
		}
		if i%41 == 0 {
			v = reopen(t, v, d)
		}
		if i%97 == 0 {
			if e = v.Compact(); e != nil {
				t.Fatalf("compact %d: %v", i, e)
			}
		}
		for key, want := range model {
			mustGet(t, v, key, want)
		}
	}
}
func TestStreamingAndEOF(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k := testKey(1)
	data := bytes.Repeat([]byte("data"), 50000)
	if _, e := v.PutReader(k, bytes.NewReader(data), uint64(len(data)), Options{}); e != nil {
		t.Fatal(e)
	}
	var out bytes.Buffer
	n, e := v.CopyTo(k, 0, uint64(len(data)), &out)
	if e != nil || n != uint64(len(data)) || !bytes.Equal(data, out.Bytes()) {
		t.Fatalf("copy %d %v", n, e)
	}
	b := make([]byte, 3)
	if n, e := v.ReadAt(k, b, uint64(len(data)-1)); n != 1 || e != io.EOF {
		t.Fatalf("%d %v", n, e)
	}
	before, _ := v.Stat(k)
	if _, e = v.PutReader(k, bytes.NewReader([]byte("short")), 100, Options{}); e == nil {
		t.Fatal("short accepted")
	}
	after, _ := v.Stat(k)
	if before != after {
		t.Fatal("failed put changed object")
	}
}
