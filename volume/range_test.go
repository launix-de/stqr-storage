// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func TestRangePUTKeepsRawAllocationAndBoundsIO(t *testing.T) {
	v, d := memoryVolume(t, 24<<20)
	key := testKey(1)
	payload := bytes.Repeat([]byte("abcd"), 2<<20)
	if _, err := testPut(v, key, payload, Options{}); err != nil {
		t.Fatal(err)
	}
	old, _ := v.loadObject(key)
	location := old.Spans[0].Location
	before := v.IOStats()
	update := bytes.Repeat([]byte("!"), PageSize)
	if _, err := testPutRange(v, key, BlockSize+17, update, Options{RequestID: testID(2)}); err != nil {
		t.Fatal(err)
	}
	after := v.IOStats()
	next, _ := v.loadObject(key)
	if next.Spans[0].Location != location || next.Segmented || len(next.Spans) != 1 {
		t.Fatal("ranged PUT relocated raw payload")
	}
	if after.WriteBytes-before.WriteBytes > 20*PageSize {
		t.Fatal("ranged PUT wrote untouched payload", after.WriteBytes-before.WriteBytes)
	}
	if testReadWorkBytes(after)-testReadWorkBytes(before) > BlockSize+20*PageSize {
		t.Fatal("ranged PUT read unrelated checksum blocks", testReadWorkBytes(after)-testReadWorkBytes(before))
	}
	copy(payload[BlockSize+17:], update)
	mustGet(t, reopen(t, v, d), key, payload)
	assertFreeTree(t, v)
}
func TestRangePUTNearFullWithoutObjectCopy(t *testing.T) {
	v, _ := memoryVolume(t, 2<<20)
	key := testKey(1)
	payload := bytes.Repeat([]byte("a"), 1500<<10)
	if _, err := testPut(v, key, payload, Options{}); err != nil {
		t.Fatal(err)
	}
	if v.Stats().AdmissionFreePhysical >= uint64(len(payload)) {
		t.Fatal("fixture has space for duplicate")
	}
	if _, err := testPutRange(v, key, 70, []byte("new"), Options{}); err != nil {
		t.Fatal(err)
	}
	copy(payload[70:], "new")
	mustGet(t, v, key, payload)
	assertFreeTree(t, v)
}
func TestRangePUTCompressedChunksKeepUnchangedPayload(t *testing.T) {
	for _, codec := range []Compression{Gzip, LZ4, XZ} {
		t.Run(codec.String(), func(t *testing.T) {
			v, d := memoryVolume(t, 8<<20)
			key := testKey(1)
			payload := bytes.Repeat([]byte("abcd"), 3*BlockSize/4)
			if _, err := testPut(v, key, payload, Options{Compression: &codec}); err != nil {
				t.Fatal(err)
			}
			old, _ := v.loadObject(key)
			s := old.Spans[0]
			table := make([]byte, 8+3*32)
			v.readBytes(table, s.Location)
			unchanged := []uint64{s.Location + diskOrder.Uint64(table[8:]), s.Location + diskOrder.Uint64(table[8+2*32:])}
			update := []byte("changed")
			if _, err := testPutRange(v, key, BlockSize+31, update, Options{}); err != nil {
				t.Fatal(err)
			}
			next, _ := v.loadObject(key)
			if len(next.Spans) != 3 || next.Spans[0].Location != unchanged[0] || next.Spans[2].Location != unchanged[1] {
				t.Fatal("unchanged compressed chunks moved", next.Spans)
			}
			copy(payload[BlockSize+31:], update)
			mustGet(t, reopen(t, v, d), key, payload)
			// A second update exercises the standalone chunk layout, followed by APPEND.
			if _, err := testPutRange(v, key, BlockSize+45, []byte("again"), Options{}); err != nil {
				t.Fatal(err)
			}
			copy(payload[BlockSize+45:], "again")
			testAppend(v, key, []byte("tail"), Options{})
			payload = append(payload, []byte("tail")...)
			mustGet(t, reopen(t, v, d), key, payload)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
			assertFreeTree(t, v)
			if err := testDelete(v, key, Options{}); err != nil {
				t.Fatal(err)
			}
			if v.Stats().DataPhysical != 0 {
				t.Fatal("leaked compressed payload", v.Stats())
			}
			assertFreeTree(t, v)
		})
	}
}
func TestRangePUTSparseAndSegmented(t *testing.T) {
	v, d := memoryVolume(t, 4<<20)
	key := testKey(1)
	testAppend(v, key, []byte("abc"), Options{})
	testAppend(v, key, []byte("def"), Options{})
	if _, err := testPutRange(v, key, 2, []byte("012345"), Options{}); err != nil {
		t.Fatal(err)
	}
	mustGet(t, reopen(t, v, d), key, []byte("ab012345"))
	if _, err := testPutRange(v, key, 100, []byte("end"), Options{}); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 103)
	copy(want, "ab012345")
	copy(want[100:], "end")
	mustGet(t, reopen(t, v, d), key, want)
	assertFreeTree(t, v)
	before := append([]byte(nil), want...)
	if _, err := testPutRangeReader(v, key, 0, bytes.NewReader([]byte("short")), 10, Options{}); err == nil {
		t.Fatal("short stream accepted")
	}
	mustGet(t, reopen(t, v, d), key, before)
}
func TestRangePUTCrashIsAtomic(t *testing.T) {
	for _, codec := range []Compression{Raw, Gzip} {
		t.Run(codec.String(), func(t *testing.T) {
			base := newCrash(2 << 20)
			v, err := Format(base, FormatOptions{Class: SSD})
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("abcd"), 3000)
			key := testKey(1)
			testPut(v, key, payload, Options{Compression: &codec})
			replacement := bytes.Repeat([]byte("!"), 5000)
			want := append([]byte(nil), payload...)
			copy(want[2000:], replacement)
			for _, through := range []bool{false, true} {
				for _, persist := range []bool{false, true} {
					for step := 1; step < 160; step++ {
						d := base.crash()
						d.fail = step
						d.writeThrough = through
						d.persistFailedSync = persist
						w, err := Open(d, OpenOptions{})
						if err != nil {
							t.Fatal(err)
						}
						_, writeErr := testPutRange(w, key, 2000, replacement, Options{RequestID: testID(1)})
						steps := d.steps
						recovered, err := Open(d.crash(), OpenOptions{})
						if err != nil {
							t.Fatalf("step %d: %v", step, err)
						}
						got, _, err := testGet(recovered, key)
						if err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(got, payload) && !bytes.Equal(got, want) {
							t.Fatalf("step %d: partially changed payload", step)
						}
						if writeErr == nil && !bytes.Equal(got, want) {
							t.Fatal("acknowledged range lost")
						}
						_, found, receiptErr := recovered.operationStatusLocked(testID(1))
						if receiptErr != nil || found != bytes.Equal(got, want) {
							t.Fatal("range and receipt disagree", found, receiptErr)
						}
						assertFreeTree(t, recovered)
						if writeErr == nil && step > steps {
							break
						}
						if step == 159 {
							t.Fatal("fault traversal did not finish")
						}
					}
				}
			}
		})
	}
}

func TestLargeRangePUTSpillsAndRecovers(t *testing.T) {
	base := newCrash(24 << 20)
	v, err := Format(base, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	original := bytes.Repeat([]byte("a"), 8<<20)
	if _, err = testPut(v, key, original, Options{}); err != nil {
		t.Fatal(err)
	}
	patch := bytes.Repeat([]byte("b"), 6<<20)
	want := append([]byte(nil), original...)
	copy(want[19:], patch)
	probe := base.crash()
	w, err := Open(probe, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = testPutRange(w, key, 19, patch, Options{RequestID: testID(1)}); err != nil {
		t.Fatal(err)
	}
	steps := probe.steps
	for _, step := range []int{1, 1025, 1550, 2000, steps - 2, steps} {
		if step <= 0 || step > steps {
			continue
		}
		t.Run(fmt.Sprint(step), func(t *testing.T) {
			d := base.crash()
			d.fail = step
			w, err := Open(d, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := testPutRange(w, key, 19, patch, Options{RequestID: testID(1)})
			recovered, err := Open(d.crash(), OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := testGet(recovered, key)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, original) && !bytes.Equal(got, want) {
				t.Fatal("partial spilled range visible")
			}
			if writeErr == nil && !bytes.Equal(got, want) {
				t.Fatal("acknowledged spilled range lost")
			}
			assertFreeTree(t, recovered)
		})
	}
}

func TestCompressedRangeShrinksAndGrowsOnlyItsChunk(t *testing.T) {
	v, d := memoryVolume(t, 12<<20)
	key := testKey(1)
	codec := Gzip
	payload := make([]byte, 3*BlockSize)
	rand.New(rand.NewSource(44)).Read(payload)
	if _, err := testPut(v, key, payload, Options{Compression: &codec}); err != nil {
		t.Fatal(err)
	}
	before := v.Stats().DataPhysical
	if _, err := testPutRange(v, key, BlockSize, make([]byte, BlockSize), Options{}); err != nil {
		t.Fatal(err)
	}
	clear(payload[BlockSize : 2*BlockSize])
	o, _ := v.loadObject(key)
	addresses := []uint64{o.Spans[0].Location, o.Spans[1].Location, o.Spans[2].Location}
	if v.Stats().DataPhysical >= before-BlockSize/2 {
		t.Fatal("compressed chunk tail was not freed")
	}
	mustGet(t, reopen(t, v, d), key, payload)
	rand.New(rand.NewSource(45)).Read(payload[BlockSize : 2*BlockSize])
	if _, err := testPutRange(v, key, BlockSize, payload[BlockSize:2*BlockSize], Options{}); err != nil {
		t.Fatal(err)
	}
	o, _ = v.loadObject(key)
	if o.Spans[0].Location != addresses[0] || o.Spans[2].Location != addresses[2] || o.Spans[1].Location == addresses[1] {
		t.Fatal("chunk growth moved unrelated payload")
	}
	mustGet(t, reopen(t, v, d), key, payload)
	assertFreeTree(t, v)
}
