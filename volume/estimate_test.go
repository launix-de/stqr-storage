// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"fmt"
	"testing"
)

// Compare the conservative planning budget to actual admission consumption,
// including descriptors, receipts, B+-tree splits and exact payload allocation.
func TestEstimatedPutOverheadMatchesStorageFormat(t *testing.T) {
	for _, size := range []int{512, 3 << 10, 12 << 10, 64 << 10, 512 << 10, 1 << 20, 4 << 20} {
		t.Run(stringSize(size), func(t *testing.T) {
			v, e := NewRAM(int64(size*160 + 8<<20))
			if e != nil {
				t.Fatal(e)
			}
			defer v.Close()
			before := v.Stats().AdmissionFreePhysical
			data := bytes.Repeat([]byte{7}, size)
			for i := 0; i < 64; i++ {
				if _, e := testPut(v, testKey(uint64(i+1)), data, Options{RequestID: testID(uint64(i + 1))}); e != nil {
					t.Fatal(e)
				}
			}
			physical := before - v.Stats().AdmissionFreePhysical
			logical := uint64(64 * size)
			measured := (1000000*(physical-logical) + logical - 1) / logical
			estimate, e := EstimatePutOverheadPPM(uint64(size))
			if e != nil || uint64(estimate) < measured {
				t.Fatalf("planning budget %d below measured %d (%d physical / %d logical): %v", estimate, measured, physical, logical, e)
			}
			t.Logf("size=%d physical overhead per object=%d, ppm=%d", size, (physical-logical)/64, measured)
		})
	}
}
func stringSize(n int) string { return fmt.Sprintf("%d", n) }
