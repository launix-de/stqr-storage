// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"fmt"
	"reflect"
	"testing"
)

func decodePageFixture(kind, level uint8) (treeNode, []byte) {
	n := treeNode{kind: kind, level: level}
	for i := 0; i < n.capacity(); i++ {
		// Index keys sort lexicographically, independently of on-disk byte order.
		key := spaceKey(uint64(i))
		n.entries = append(n.entries, treeEntry{key: key, value: uint64(i + 1), maximum: uint64(i + 2), aligned: uint64(i + 3)})
		if kind != treeFree || level == 0 {
			n.entries[i].maximum = 0
			n.entries[i].aligned = 0
		}
	}
	return n, encodeNode(n, testID(1), 1)
}

func TestDecodeTreePageShapes(t *testing.T) {
	for _, kind := range []uint8{treeObjects, treeFree} {
		for _, level := range []uint8{0, 1, 32} {
			t.Run(fmt.Sprintf("kind%d/level%d", kind, level), func(t *testing.T) {
				want, page := decodePageFixture(kind, level)
				got, err := decodeNode(page, testID(1))
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("roundtrip: %v", err)
				}
				invalid := append([]byte(nil), page...)
				diskOrder.PutUint16(invalid[6:], uint16(want.capacity()+1))
				diskOrder.PutUint32(invalid[PageSize-4:], checksum(invalid[:PageSize-4]))
				if _, err = decodeNode(invalid, testID(1)); err != ErrCorrupt {
					t.Fatalf("oversized count: %v", err)
				}
				invalid = append([]byte(nil), page...)
				invalid[32] ^= 1
				if _, err = decodeNode(invalid, testID(1)); err != ErrCorrupt {
					t.Fatalf("bad CRC: %v", err)
				}
				invalid = append([]byte(nil), page...)
				stride := 57
				if kind == treeFree {
					stride = 16
					if level > 0 {
						stride = 32
					}
				}
				copy(invalid[32+stride:32+stride+8], invalid[32:40])
				diskOrder.PutUint32(invalid[PageSize-4:], checksum(invalid[:PageSize-4]))
				if _, err = decodeNode(invalid, testID(1)); err != ErrCorrupt {
					t.Fatalf("duplicate key: %v", err)
				}
			})
		}
	}
}

var decodedPageSink treeNode

func TestDecodeTreePageAllocations(t *testing.T) {
	for _, kind := range []uint8{treeObjects, treeFree} {
		for _, level := range []uint8{0, 1} {
			_, page := decodePageFixture(kind, level)
			allocs := testing.AllocsPerRun(100, func() {
				var err error
				decodedPageSink, err = decodeNode(page, testID(1))
				if err != nil {
					panic(err)
				}
			})
			if allocs > 1 {
				t.Errorf("kind%d level%d: %.0f allocations; want <=1", kind, level, allocs)
			}
		}
	}
}

func BenchmarkDecodeTreePage(b *testing.B) {
	for _, kind := range []uint8{treeObjects, treeFree} {
		for _, level := range []uint8{0, 1} {
			b.Run(fmt.Sprintf("kind%d/level%d", kind, level), func(b *testing.B) {
				_, page := decodePageFixture(kind, level)
				b.ReportAllocs()
				b.SetBytes(PageSize)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err error
					decodedPageSink, err = decodeNode(page, testID(1))
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
