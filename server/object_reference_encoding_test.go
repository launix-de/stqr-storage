// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Keep the old streaming encoder as an independent wire-format oracle and
// controlled allocation baseline. Production must not adopt its buffer growth.
func legacyReferenceEncoding(entries []objectReferenceEntry, bucket volume.ID) []byte {
	e := client.Encoder{B: []byte("ORF1")}
	e.ID(client.ID(bucket))
	e.U32(uint32(len(entries)))
	for _, r := range entries {
		e.B = append(e.B, r.object[:]...)
		e.B = append(e.B, r.proof[:]...)
		e.ID(client.ID(r.account))
		flags := uint8(0)
		if r.claim {
			flags = 1
		}
		if r.dependency {
			flags = 2
		}
		e.U8(flags)
	}
	return e.B
}
func referenceEncodingEntries(n int) []objectReferenceEntry {
	entries := make([]objectReferenceEntry, n)
	for i := range entries {
		entries[i].object[0] = byte(i)
		entries[i].object[31] = byte(i >> 8)
		entries[i].proof[7] = byte(i * 7)
		entries[i].account[15] = byte(i * 3)
		entries[i].claim = i%3 == 1
		entries[i].dependency = i%3 == 2
	}
	return entries
}

var referenceEncodingSink []byte

func TestObjectReferenceEncodingExactBytesAndAllocation(t *testing.T) {
	bucket := volume.ID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	for _, n := range []int{0, 1, 512, maxObjectReferences} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			entries := referenceEncodingEntries(n)
			want := legacyReferenceEncoding(entries, bucket)
			got := encodeObjectReferences(entries, bucket)
			if !bytes.Equal(got, want) || len(got) != 24+81*n {
				t.Fatal("reference wire format changed")
			}
			decoded, err := decodeObjectReferences(got, bucket)
			if err != nil || len(decoded) != n {
				t.Fatal("round trip failed", err, len(decoded))
			}
			for i := range entries {
				if decoded[i] != entries[i] {
					t.Fatal("entry changed", i)
				}
			}
			other := bucket
			other[0]++
			if foreign, err := decodeObjectReferences(got, other); err != nil || len(foreign) != 0 {
				t.Fatal("bucket ownership boundary changed", err)
			}
			allocations := testing.AllocsPerRun(10, func() { referenceEncodingSink = encodeObjectReferences(entries, bucket) })
			if allocations > 1 {
				t.Fatalf("reference encoding allocated%.1f times, expected one exact buffer", allocations)
			}
		})
	}
}
func BenchmarkObjectReferenceEncoding(b *testing.B) {
	for _, n := range []int{0, 1, 512, maxObjectReferences} {
		for _, old := range []bool{false, true} {
			name := fmt.Sprint(n)
			encode := encodeObjectReferences
			if old {
				name += "/legacy"
				encode = legacyReferenceEncoding
			}
			b.Run(name, func(b *testing.B) {
				entries := referenceEncodingEntries(n)
				bucket := volume.ID{1}
				b.ReportAllocs()
				b.SetBytes(int64(24 + 81*n))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					referenceEncodingSink = encode(entries, bucket)
				}
			})
		}
	}
}
