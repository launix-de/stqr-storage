// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

type scratchObservingReader struct {
	src      io.Reader
	capacity int
}

func (r *scratchObservingReader) Read(p []byte) (int, error) {
	r.capacity = max(r.capacity, cap(p))
	return r.src.Read(p)
}
func TestPackedPayloadScratchBoundedByActualChunk(t *testing.T) {
	for _, size := range []int{0, 1, 81, 4097, BlockSize - 1, BlockSize, BlockSize + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			v, _ := memoryVolume(t, 8<<20)
			defer v.Close()
			raw := bytes.Repeat([]byte{byte(size % 251)}, size)
			r := &scratchObservingReader{src: bytes.NewReader(raw)}
			if _, err := testPutReader(v, testKey(1), r, uint64(size), Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			if r.capacity > min(size, BlockSize) {
				t.Fatalf("payload size%d obtained%d-byte scratch, want<=%d", size, r.capacity, min(size, BlockSize))
			}
			mustGet(t, v, testKey(1), raw)
		})
	}
}
func TestPackedPayloadScratchUnequalSparsePieces(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	tnx := v.transaction()
	defer tnx.close()
	a := bytes.Repeat([]byte("a"), 81)
	b := bytes.Repeat([]byte("b"), 4097)
	left := &scratchObservingReader{src: bytes.NewReader(a)}
	right := &scratchObservingReader{src: bytes.NewReader(b)}
	spans, err := tnx.storePacked([]payloadPiece{{offset: 0, length: 81, src: left}, {offset: 1 << 20, length: 4097, src: right}}, Raw)
	if err != nil {
		t.Fatal(err)
	}
	if left.capacity != 4097 || right.capacity != 4097 || len(spans) != 2 {
		t.Fatal("scratch is not bounded by largest piece", left.capacity, right.capacity, len(spans))
	}
	for i, want := range [][]byte{a, b} {
		got := make([]byte, len(want))
		if err := tnx.readBytes(got, spans[i].Location); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("scratch reuse aliased prior payload", i)
		}
	}
}
func BenchmarkPackedPayloadScratch(b *testing.B) {
	for _, size := range []int{81, 36 << 10, BlockSize} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			d, err := NewMemory(8 << 20)
			if err != nil {
				b.Fatal(err)
			}
			v, err := Format(d, FormatOptions{Class: RAM})
			if err != nil {
				b.Fatal(err)
			}
			defer v.Close()
			h, err := v.Open(true)
			if err != nil {
				b.Fatal(err)
			}
			defer h.Close()
			raw := bytes.Repeat([]byte("x"), size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tnx := v.transaction()
				_, err = tnx.storePacked([]payloadPiece{{length: uint64(size), src: bytes.NewReader(raw)}}, Raw)
				tnx.close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
