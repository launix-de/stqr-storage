// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestWholeRawReplacementRepacksPriorLayouts(t *testing.T) {
	for _, layout := range []string{"append-shrink", "append-grow", "sparse", "direct-raw"} {
		t.Run(layout, func(t *testing.T) {
			v, _ := memoryVolume(t, 4<<20)
			defer v.Close()
			k := testKey(1)
			switch layout {
			case "append-shrink", "append-grow":
				if _, err := testAppend(v, k, bytes.Repeat([]byte("a"), 128), Options{}); err != nil {
					t.Fatal(err)
				}
				if v.Stats().ArenaUnusedPhysical == 0 {
					t.Fatal("fixture has no reserved append arena")
				}
			case "sparse":
				if _, err := testPutRange(v, k, 512<<10, []byte("tail"), Options{}); err != nil {
					t.Fatal(err)
				}
			case "direct-raw":
				raw := bytes.Repeat([]byte("d"), BlockSize)
				h, err := v.Open(true)
				if err != nil {
					t.Fatal(err)
				}
				err = h.RestoreEncoded(k, Info{Length: uint64(len(raw)), Compression: Raw}, func(yield func(EncodedChunk) error) error {
					return yield(EncodedChunk{Length: uint64(len(raw)), DecodedLength: uint32(len(raw)), Codec: Raw, RawCRC: checksum(raw), EncodedCRC: checksum(raw), Data: raw})
				}, Options{ImportShadow: true})
				h.Close()
				if err != nil {
					t.Fatal(err)
				}
				object, err := v.loadObject(k)
				if err != nil || len(object.Spans) != 1 || !object.Spans[0].Direct {
					t.Fatal("fixture is not direct RAW", object, err)
				}
			}
			size := 6000
			if layout == "append-shrink" {
				size = 2
			}
			next := bytes.Repeat([]byte("n"), size)
			if _, err := testPut(v, k, next, Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			assertPackedRaw(t, v, k, uint64(size))
			mustGet(t, v, k, next)
			if v.Stats().ArenaUnusedPhysical != 0 {
				t.Fatal("whole PUT retained an append arena", v.Stats())
			}
			assertFreeTree(t, v)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestWholeRawReplacementReaderFailurePreservesObject(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "accepted"}[accepted], func(t *testing.T) {
			v, _ := memoryVolume(t, 4<<20)
			defer v.Close()
			k := testKey(1)
			old := bytes.Repeat([]byte("o"), BlockSize+(128<<10))
			if _, err := testPut(v, k, old, Options{}); err != nil {
				t.Fatal(err)
			}
			at := assertPackedRaw(t, v, k, uint64(len(old)))
			before := v.Stats()
			ioBefore := v.IOStats()
			_, err := testPutReader(v, k, bytes.NewReader(bytes.Repeat([]byte("n"), BlockSize)), uint64(len(old)+81), Options{Accepted: accepted})
			if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
				t.Fatal("short replacement reader accepted", err)
			}
			mustGet(t, v, k, old)
			if got := assertPackedRaw(t, v, k, uint64(len(old))); got != at {
				t.Fatal("failed replacement relocated old payload")
			}
			after := v.Stats()
			if after.DataPhysical != before.DataPhysical || after.Objects != before.Objects {
				t.Fatal("failed replacement leaked payload", before, after)
			}
			if accepted && v.IOStats().SyncOps != ioBefore.SyncOps {
				t.Fatal("failed accepted write forced Sync")
			}
			assertFreeTree(t, v)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
			if _, err := testPut(v, k, []byte("still usable"), Options{}); err != nil {
				t.Fatal("failure poisoned future storage", err)
			}
		})
	}
}
func TestWholeRawNearFullGrowthFailsWithoutChangingOldPayload(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "accepted"}[accepted], func(t *testing.T) {
			v, _ := memoryVolume(t, 2<<20)
			defer v.Close()
			k := testKey(1)
			old := bytes.Repeat([]byte("o"), 1500<<10)
			if _, err := testPut(v, k, old, Options{}); err != nil {
				t.Fatal(err)
			}
			at := assertPackedRaw(t, v, k, uint64(len(old)))
			before := v.Stats()
			next := append(bytes.Clone(old), bytes.Repeat([]byte("n"), 81)...)
			if before.AdmissionFreePhysical <= 81 || before.AdmissionFreePhysical >= uint64(len(next)) {
				t.Fatal("fixture lacks delta-only free space", before)
			}
			if _, err := testPut(v, k, next, Options{Accepted: accepted}); !errors.Is(err, ErrFull) {
				t.Fatal("contiguous growth should require full temporary space", err)
			}
			mustGet(t, v, k, old)
			if got := assertPackedRaw(t, v, k, uint64(len(old))); got != at {
				t.Fatal("failed growth relocated old payload")
			}
			if after := v.Stats(); after.DataPhysical != before.DataPhysical || after.Objects != before.Objects {
				t.Fatal("failed growth leaked payload", before, after)
			}
			assertFreeTree(t, v)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
