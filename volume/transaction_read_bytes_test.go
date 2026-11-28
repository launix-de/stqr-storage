// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
)

func BenchmarkAcceptedObjectMutation(b *testing.B) {
	for _, mapped := range []bool{false, true} {
		for _, workload := range []string{"put-128", "range-4k-in-1m"} {
			b.Run(fmt.Sprintf("mapped=%v/%s", mapped, workload), func(b *testing.B) {
				var v *Volume
				var err error
				if mapped {
					v, err = New(filepath.Join(b.TempDir(), "volume"), 32<<20, SSD)
				} else {
					v, err = NewRAM(32 << 20)
				}
				if err != nil {
					b.Fatal(err)
				}
				defer v.Close()
				key := testKey(1)
				original := bytes.Repeat([]byte("a"), 128)
				if workload == "range-4k-in-1m" {
					original = bytes.Repeat([]byte("a"), BlockSize)
				}
				if _, err = testPut(v, key, original, Options{Accepted: true}); err != nil {
					b.Fatal(err)
				}
				change := bytes.Repeat([]byte("b"), 128)
				if workload == "range-4k-in-1m" {
					change = bytes.Repeat([]byte("b"), PageSize)
				}
				before := v.IOStats().SyncOps
				b.ReportAllocs()
				b.SetBytes(int64(len(change)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					change[0] = byte(i)
					if workload == "put-128" {
						_, err = testPut(v, key, change, Options{Accepted: true})
					} else {
						_, err = testPutRange(v, key, BlockSize/2, change, Options{Accepted: true})
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if v.IOStats().SyncOps != before {
					b.Fatal("ordinary accepted writes synced")
				}
			})
		}
	}
}

func TestTransactionReadBytesBorrowedAllocation(t *testing.T) {
	v, err := NewRAM(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	v.mu.Lock()
	defer v.mu.Unlock()
	off := uint64(2 << 20)
	want := bytes.Repeat([]byte("r"), BlockSize)
	if _, err = v.dev.WriteAt(want, int64(off)); err != nil {
		t.Fatal(err)
	}
	tx := v.transaction()
	defer tx.close()
	dst := make([]byte, len(want))
	before := v.ioStatsLocked().SyncOps
	allocated := testing.AllocsPerRun(30, func() {
		if err := tx.readBytes(dst, off); err != nil {
			panic(err)
		}
	})
	if allocated != 0 {
		t.Fatalf("read into caller-owned buffer allocated %.0f page copies", allocated)
	}
	if !bytes.Equal(dst, want) {
		t.Fatal("borrowed copy changed payload")
	}
	if v.ioStatsLocked().SyncOps != before {
		t.Fatal("read requested persistence")
	}
	dst[0] = 'x'
	var got [1]byte
	if err = tx.readBytes(got[:], off); err != nil || got[0] != 'r' {
		t.Fatal("read destination aliased storage", err)
	}
}

func TestTransactionReadBytesLayerPrecedence(t *testing.T) {
	for _, mode := range []string{"base", "hot", "spilled", "accepted", "accepted-hot", "accepted-absent", "staging", "deferred", "fence"} {
		t.Run(mode, func(t *testing.T) {
			v, err := NewRAM(8 << 20)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			v.mu.Lock()
			defer v.mu.Unlock()
			off := uint64(2 << 20)
			want := make([]byte, 4*PageSize)
			for i := range want {
				want[i] = byte('a' + i/PageSize)
			}
			if _, err = v.dev.WriteAt(want, int64(off)); err != nil {
				t.Fatal(err)
			}
			tx := v.transaction()
			defer tx.close()
			var stores []*dirtyStore
			defer func() {
				v.staging = nil
				v.deferred = nil
				v.fence = nil
				for _, s := range stores {
					s.close()
				}
			}()
			data := bytes.Repeat([]byte("X"), PageSize)
			page := off + PageSize
			store := func() *dirtyStore {
				s := newDirtyStore(2, true)
				stores = append(stores, s)
				if err := s.put(page, data, 1); err != nil {
					t.Fatal(err)
				}
				return s
			}
			switch mode {
			case "hot":
				tx.pages[page] = data
			case "spilled":
				tx.spool = &encodedSpool{}
				if _, err = tx.spool.Write(data); err != nil {
					t.Fatal(err)
				}
				tx.spilled = map[uint64]int64{page: 0}
			case "accepted", "accepted-hot":
				tx.acceptedPages = store()
				if mode == "accepted-hot" {
					data = bytes.Repeat([]byte("H"), PageSize)
					tx.pages[page] = data
				}
			case "accepted-absent":
				tx.acceptedPages = newDirtyStore(2, true)
			case "staging":
				v.staging = map[uint64][]byte{page: data}
			case "deferred":
				v.deferred = &deferredGroup{store: store()}
			case "fence":
				v.fence = &writeFence{group: &deferredGroup{store: store()}}
			}
			if mode != "base" && mode != "accepted-absent" {
				copy(want[PageSize:2*PageSize], data)
			}
			// Cross both page boundaries and read only part of the first and last page.
			start, length := PageSize-7, 2*PageSize+14
			got := make([]byte, length)
			before := v.ioStatsLocked().SyncOps
			if err = tx.readBytes(got, off+uint64(start)); err != nil || !bytes.Equal(got, want[start:start+length]) {
				t.Fatal("wrong visible layer", err)
			}
			if v.ioStatsLocked().SyncOps != before {
				t.Fatal("read requested persistence")
			}
			got[0] ^= 1
			var again [1]byte
			if err = tx.readBytes(again[:], off+uint64(start)); err != nil || again[0] != want[start] {
				t.Fatal("destination aliased layer", err)
			}
		})
	}
}

func TestTransactionReadBytesFailuresAndFallback(t *testing.T) {
	for _, mode := range []string{"range", "spilled-short", "accepted-closed", "backend-error", "aligned-fallback"} {
		t.Run(mode, func(t *testing.T) {
			v, mem := memoryVolume(t, 8<<20)
			defer v.Close()
			v.mu.Lock()
			defer v.mu.Unlock()
			tx := v.transaction()
			defer tx.close()
			off := uint64(2 << 20)
			dst := make([]byte, 19)
			var want error
			switch mode {
			case "range":
				off = v.limit - 3
				want = ErrCorrupt
			case "spilled-short":
				tx.spool = &encodedSpool{}
				tx.spilled = map[uint64]int64{off: PageSize}
				want = io.EOF
			case "accepted-closed":
				tx.acceptedPages = newDirtyStore(1, true)
				tx.acceptedPages.close()
				want = ErrClosed
			case "backend-error":
				want = errors.New("injected payload read failure")
				v.dev.(*meteredIO).VolumeIO = treeViewReadError{VolumeIO: mem, err: want}
			case "aligned-fallback":
				d := &alignedIO{MemoryIO: mem, alignment: PageSize}
				v.dev.(*meteredIO).VolumeIO = d
				off += 17
				defer func() {
					if d.violations != 0 {
						t.Fatal("unaligned fallback")
					}
				}()
			}
			if err := tx.readBytes(dst, off); !errors.Is(err, want) {
				t.Fatalf("got %v want %v", err, want)
			}
		})
	}
}
