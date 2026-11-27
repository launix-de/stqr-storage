// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// Includes actual transaction projection, quota admission and observer images.
// Setup is excluded; no per-object metric or synthetic prefilled accounting map.
func BenchmarkStatisticsAcceptedMutation(b *testing.B) {
	for _, count := range []int{1, 128, 1024} {
		b.Run(fmt.Sprintf("buckets-%d", count), func(b *testing.B) {
			v, err := NewRAM(32 << 20)
			if err != nil {
				b.Fatal(err)
			}
			defer v.Close()
			h, err := v.Open(true)
			if err != nil {
				b.Fatal(err)
			}
			var key Key
			key.Object[0] = 1
			for i := 0; i < count; i++ {
				binary.BigEndian.PutUint64(key.Bucket[8:], uint64(i+1))
				if _, err = h.Put(key, []byte{0}, Options{Accepted: true}); err != nil {
					b.Fatal(err)
				}
			}
			h.Close()
			if err = v.SetAdmission(func(StorageChange) (uint64, error) { return 0, nil }, func(StorageSnapshot) {}); err != nil {
				b.Fatal(err)
			}
			if err = v.SetOwnershipFilter(1, func(Key, bool) bool { return true }); err != nil {
				b.Fatal(err)
			}
			h, err = v.Open(true)
			if err != nil {
				b.Fatal(err)
			}
			defer h.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err = h.Put(key, []byte{byte(i + 1)}, Options{Accepted: true}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
