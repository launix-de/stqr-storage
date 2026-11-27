// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

func assertPackedRaw(t testing.TB, v *Volume, k Key, length uint64) uint64 {
	t.Helper()
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	o, err := v.loadObject(k)
	if err != nil {
		t.Fatal(err)
	}
	if o.Info.Length != length || o.Segmented || len(o.ListBlocks) != 0 {
		t.Fatalf("whole replacement retained segmented history: length=%d segmented=%v listblocks=%d", o.Info.Length, o.Segmented, len(o.ListBlocks))
	}
	if length == 0 {
		if len(o.Spans) != 0 {
			t.Fatal("empty object allocated payload")
		}
		return 0
	}
	if len(o.Spans) != 1 || o.Spans[0].Offset != 0 || o.Spans[0].Length != length || o.Spans[0].Skip != 0 || o.Spans[0].Direct {
		t.Fatalf("whole RAW payload not packed: %d spans for %d bytes", len(o.Spans), length)
	}
	return o.Spans[0].Location
}

func TestWholeRawGrowthStaysPacked(t *testing.T) {
	for _, cas := range []bool{false, true} {
		name := "PUT"
		if cas {
			name = "CAS"
		}
		t.Run(name, func(t *testing.T) {
			v, _ := memoryVolume(t, 16<<20)
			defer v.Close()
			k := testKey(1)
			raw := bytes.Repeat([]byte("a"), 16<<10)
			if _, err := testPut(v, k, raw, Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			before := v.IOStats()
			for i := 0; i < 300; i++ {
				next := append(bytes.Clone(raw), bytes.Repeat([]byte{byte(i)}, 81)...)
				var err error
				if cas {
					_, err = testCAS(v, k, 0, raw, next, Options{WholeObject: true, Accepted: true})
				} else {
					_, err = testPut(v, k, next, Options{Accepted: true})
				}
				if err != nil {
					t.Fatal(i, err)
				}
				raw = next
				assertPackedRaw(t, v, k, uint64(len(raw)))
			}
			if after := v.IOStats(); after.SyncOps != before.SyncOps {
				t.Fatal("accepted whole replacement started Sync")
			}
			mustGet(t, v, k, raw)
			if v.Stats().Objects != 1 {
				t.Fatal("stable object ID became multiple objects")
			}
		})
	}
}

func TestWholeRawSameSizeAndShrinkReuseExistingSpan(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	defer v.Close()
	k := testKey(1)
	old := bytes.Repeat([]byte("x"), 64<<10)
	if _, err := testPut(v, k, old, Options{}); err != nil {
		t.Fatal(err)
	}
	at := assertPackedRaw(t, v, k, uint64(len(old)))
	for _, size := range []int{len(old), len(old) - 1, 5, 1, 0} {
		next := bytes.Repeat([]byte("n"), size)
		if _, err := testPut(v, k, next, Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
		got := assertPackedRaw(t, v, k, uint64(size))
		if size > 0 && got != at {
			t.Fatal("fitting whole replacement moved its allocation")
		}
		mustGet(t, v, k, next)
	}
}

func TestWholeRawReplacementCrashBoundary(t *testing.T) {
	d := newCrash(8 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	k := testKey(1)
	old := bytes.Repeat([]byte("old"), 6000)
	if _, err = testPut(v, k, old, Options{}); err != nil {
		t.Fatal(err)
	}
	next := append(bytes.Clone(old), []byte("whole-grow")...)
	if _, err = testCAS(v, k, 0, old, next, Options{WholeObject: true, Accepted: true}); err != nil {
		t.Fatal(err)
	}
	assertPackedRaw(t, v, k, uint64(len(next)))
	before, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, before, k, old)
	before.Close()
	if err = v.CheckpointThrough(v.Stats().Generation); err != nil {
		t.Fatal(err)
	}
	after, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	mustGet(t, after, k, next)
	assertPackedRaw(t, after, k, uint64(len(next)))
}
