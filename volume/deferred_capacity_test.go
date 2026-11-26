// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

func TestAcceptedJournalPreservesContiguousCapacity(t *testing.T) {
	for _, fixture := range []struct {
		name    string
		size    int64
		payload int
	}{
		{"small-volume", 1 << 20, 900 << 10},
		{"large-volume", 16 << 20, 10 << 20},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			v, disk := memoryVolume(t, fixture.size)
			small := bytes.Repeat([]byte("s"), 8192)
			if _, err := testPut(v, testKey(1), small, Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			if err := v.Sync(); err != nil {
				t.Fatal(err)
			}
			// This fits in the volume, but cannot fit on either side of a small
			// object stranded behind a temporary journal at the front.
			large := bytes.Repeat([]byte("L"), fixture.payload)
			if _, err := testPut(v, testKey(2), large, Options{}); err != nil {
				t.Fatal(err)
			}
			v = reopen(t, v, disk)
			mustGet(t, v, testKey(1), small)
			mustGet(t, v, testKey(2), large)
			assertFreeTree(t, v)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
