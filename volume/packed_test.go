// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestPackedPUTClaimsOneExactRegion(t *testing.T) {
	v, _ := memoryVolume(t, 16<<20)
	var payload uint64
	for i, size := range []int{0, 1, 2, 3, 4, 5, 4095, 4096, 4097, 1 << 20, 3<<20 + 17} {
		key := testKey(uint64(i))
		want := bytes.Repeat([]byte{byte(i + 1)}, size)
		if _, e := testPut(v, key, want, Options{}); e != nil {
			t.Fatal(e)
		}
		o, e := v.loadObject(key)
		if e != nil {
			t.Fatal(e)
		}
		if o.Segmented {
			t.Fatal("PUT used segmented layout")
		}
		if size == 0 {
			if len(o.Spans) != 0 {
				t.Fatal("zero-byte object allocated data")
			}
		} else {
			if len(o.Spans) != 1 {
				t.Fatalf("%d-byte PUT has %d allocations", size, len(o.Spans))
			}
			s := o.Spans[0]
			if s.Stored != uint64(size) || s.Capacity != uint64(size) {
				t.Fatal("payload rounded up", s)
			}
			if size >= PageSize && s.Location%PageSize != 0 {
				t.Fatal("large PUT is not page aligned")
			}
		}
		mustGet(t, v, key, want)
		payload += uint64(size)
	}
	if got := v.Stats().DataPhysical; got != payload {
		t.Fatalf("physical payload %d, exact data %d", got, payload)
	}
	assertFreeTree(t, v)
}
func TestAppendSwitchesLayoutWithoutCopyingPUT(t *testing.T) {
	v, _ := memoryVolume(t, 2<<20)
	k := testKey(1)
	testPut(v, k, []byte("first"), Options{})
	o, _ := v.loadObject(k)
	original := o.Spans[0].Location
	testAppend(v, k, []byte("second"), Options{})
	o, _ = v.loadObject(k)
	if !o.Segmented || o.Spans[0].Location != original || len(o.Spans) != 2 {
		t.Fatal("APPEND copied PUT or failed to switch layout")
	}
	free := v.Stats().AdmissionFreePhysical
	testAppend(v, k, []byte("!"), Options{})
	if v.Stats().AdmissionFreePhysical != free {
		t.Fatal("tiny append allocated storage despite available arena")
	}
	mustGet(t, v, k, []byte("firstsecond!"))
	testPut(v, k, []byte("new"), Options{})
	o, _ = v.loadObject(k)
	if o.Segmented || len(o.Spans) != 1 || len(o.ListBlocks) != 0 || v.Stats().DataPhysical != 3 {
		t.Fatal("PUT did not discard segmented storage")
	}
	assertFreeTree(t, v)
}
func assertFreeTree(t *testing.T, v *Volume) {
	t.Helper()
	var sum, last uint64
	e := treeWalk(v.node, v.sb.FreeRoot, func(x treeEntry) error {
		off := keyOffset(x.key)
		if off < dataStart || off+x.value > v.sb.Identity.Size || x.value == 0 || sum > 0 && off <= last {
			t.Fatalf("uncoalesced/invalid free span %d + %d (previous end %d)", off, x.value, last)
		}
		sum += x.value
		last = off + x.value
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	s := v.Stats()
	if sum != s.AdmissionFreePhysical+s.GuaranteedRemainingPhysical || sum+s.MetadataPhysical+s.DataPhysical+s.OperatingReservePhysical != s.CapacityPhysical {
		t.Fatalf("unaccounted bytes: sum=%d stats=%+v", sum, s)
	}
}
func TestBTreeUpdatesOnlyAffectedPages(t *testing.T) {
	v, d := memoryVolume(t, 4<<20)
	for i := uint64(0); i < 1000; i++ {
		if _, e := testPut(v, testKey(i), nil, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	root := make([]byte, PageSize)
	if e := readFullAt(d, root, v.sb.ObjectRoot); e != nil {
		t.Fatal(e)
	}
	before := v.IOStats()
	if _, e := testPut(v, testKey(500), nil, Options{}); e != nil {
		t.Fatal(e)
	}
	after := v.IOStats()
	now := make([]byte, PageSize)
	readFullAt(d, now, v.sb.ObjectRoot)
	if !bytes.Equal(root, now) {
		t.Fatal("unchanged B+-tree root was rewritten")
	}
	if after.WriteOps-before.WriteOps > 12 {
		t.Fatal("one PUT rewrote an index directory", after.WriteOps-before.WriteOps)
	}
	w, e := Open(d, OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if w.IOStats().ReadOps > 12 {
		t.Fatal("Open scanned the object directory", w.IOStats())
	}
	for i := uint64(0); i < 1000; i += 7 {
		if e := testDelete(w, testKey(i), Options{}); e != nil {
			t.Fatal(e)
		}
	}
	assertFreeTree(t, w)
}
func TestCrashDuringSharedPageUpdateAndTreeSplit(t *testing.T) {
	for _, count := range []int{2, 71} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			base := newCrash(1 << 20)
			v, e := Format(base, FormatOptions{Class: SSD})
			if e != nil {
				t.Fatal(e)
			}
			for i := 0; i < count; i++ {
				if _, e = testPut(v, testKey(uint64(i)), []byte{byte(i)}, Options{}); e != nil {
					t.Fatal(e)
				}
			}
			for _, through := range []bool{false, true} {
				for _, persist := range []bool{false, true} {
					for step := 1; step < 60; step++ {
						d := base.crash()
						d.fail = step
						d.writeThrough = through
						d.persistFailedSync = persist
						w, e := Open(d, OpenOptions{})
						if e != nil {
							t.Fatal(e)
						}
						key := testKey(uint64(count))
						_, err := testPut(w, key, []byte("new"), Options{})
						steps := d.steps
						recovered, e := Open(d.crash(), OpenOptions{})
						if e != nil {
							t.Fatalf("step %d: %v", step, e)
						}
						for i := 0; i < count; i++ {
							mustGet(t, recovered, testKey(uint64(i)), []byte{byte(i)})
						}
						got, _, e := testGet(recovered, key)
						if e != nil && !errors.Is(e, ErrNotFound) {
							t.Fatal(e)
						}
						if e == nil && string(got) != "new" {
							t.Fatal("partial PUT visible")
						}
						if err == nil && step > steps {
							break
						}
						if step == 59 {
							t.Fatal("fault traversal did not end")
						}
					}
				}
			}
		})
	}
}

func defragFixture(t *testing.T, d VolumeIO) (*Volume, []byte) {
	t.Helper()
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	tx := v.transaction()
	gap, e := tx.alloc(PageSize, false)
	if e != nil {
		t.Fatal(e)
	}
	want := bytes.Repeat([]byte("move"), 5*PageSize/4)
	at, e := tx.alloc(uint64(len(want)), false)
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.writeBytes(at, want); e != nil {
		t.Fatal(e)
	}
	o := &object{Info: Info{Length: uint64(len(want)), Compression: Raw}, Spans: []dataSpan{{Offset: 0, Length: uint64(len(want)), Location: at, Stored: uint64(len(want)), Capacity: uint64(len(want)), Codec: Raw, CRC: checksum(want)}}}
	if e = tx.saveObject(testKey(1), o); e != nil {
		t.Fatal(e)
	}
	tx.sb.Objects = 1
	tx.sb.LogicalBytes = uint64(len(want))
	tx.release(gap, PageSize, false)
	if e = tx.commit(false); e != nil {
		t.Fatal(e)
	}
	return v, want
}
func TestDefragOverlappingMoveAndShrink(t *testing.T) {
	m, _ := NewMemory(512 << 10)
	v, want := defragFixture(t, m)
	before, _ := v.loadObject(testKey(1))
	source := before.Spans[0].Location
	if e := v.Compact(); e != nil {
		t.Fatal(e)
	}
	after, _ := v.loadObject(testKey(1))
	if after.Spans[0].Location >= source || after.Spans[0].Location != dataStart {
		t.Fatal("adjacent one-page hole did not move a five-page object")
	}
	mustGet(t, v, testKey(1), want)
	assertFreeTree(t, v)
	if e := v.Shrink(256 << 10); e != nil {
		t.Fatal(e)
	}
	if v.Size() != 256<<10 {
		t.Fatal("shrink not committed")
	}
	assertFreeTree(t, v)
	w, e := Open(m, OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	mustGet(t, w, testKey(1), want)
}
func TestCrashResumesOverlappingDefrag(t *testing.T) {
	base := newCrash(512 << 10)
	_, want := defragFixture(t, base)
	for _, through := range []bool{false, true} {
		for _, persist := range []bool{false, true} {
			for step := 1; step < 160; step++ {
				d := base.crash()
				d.fail = step
				d.writeThrough = through
				d.persistFailedSync = persist
				v, e := Open(d, OpenOptions{})
				if e != nil {
					t.Fatal(e)
				}
				err := v.Compact()
				used := d.steps
				recovered, e := Open(d.crash(), OpenOptions{})
				if e != nil {
					t.Fatalf("step=%d through=%v persist=%v: %v", step, through, persist, e)
				}
				mustGet(t, recovered, testKey(1), want)
				assertFreeTree(t, recovered)
				if err == nil && step > used {
					break
				}
				if step == 159 {
					t.Fatal("fault traversal did not finish")
				}
			}
		}
	}
}

func TestDamagedCommittedRootAndAnchorRejectRollback(t *testing.T) {
	d := newCrash(1 << 20)
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	testPut(v, testKey(1), []byte("first"), Options{})
	testPut(v, testKey(2), []byte("second"), Options{})
	generation := v.Header().Generation
	root := generation % 2 * PageSize
	anchor := (2 + generation%2) * PageSize
	d.live[root+PageSize-4] ^= 1
	d.live[anchor+PageSize-4] ^= 1
	if _, e = Open(d, OpenOptions{}); !errors.Is(e, ErrCorrupt) {
		t.Fatal("damaged latest commit silently rolled back", e)
	}
}

func TestTinyAppendDoesNotRereadExistingPayload(t *testing.T) {
	v, d := memoryVolume(t, 8<<20)
	k := testKey(1)
	payload := bytes.Repeat([]byte("a"), 700000)
	if _, e := testAppend(v, k, payload, Options{}); e != nil {
		t.Fatal(e)
	}
	before := v.IOStats()
	if _, e := testAppend(v, k, []byte("small"), Options{}); e != nil {
		t.Fatal(e)
	}
	after := v.IOStats()
	if after.ReadBytes-before.ReadBytes > 16*PageSize {
		t.Fatal("small APPEND reread its existing payload", after.ReadBytes-before.ReadBytes)
	}
	mustGet(t, v, k, append(payload, []byte("small")...))
	k = testKey(2)
	payload = bytes.Repeat([]byte("b"), 2*BlockSize+10)
	if _, e := testAppend(v, k, payload, Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e := testAppend(v, k, []byte("more"), Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, append(payload, []byte("more")...))
	w := reopen(t, v, d)
	mustGet(t, w, testKey(1), append(bytes.Repeat([]byte("a"), 700000), []byte("small")...))
	mustGet(t, w, k, append(payload, []byte("more")...))
}

func TestAppendUpdatesLinkedSegmentTail(t *testing.T) {
	v, d := memoryVolume(t, 32<<20)
	k := testKey(1)
	var want []byte
	for i := 0; i < 18; i++ {
		var size uint64 = 1
		if i > 0 {
			o, err := v.loadObject(k)
			if err != nil {
				t.Fatal(err)
			}
			tail := o.Spans[len(o.Spans)-1]
			size = tail.Capacity - tail.Stored + 1
		}
		chunk := bytes.Repeat([]byte{byte(i + 1)}, int(size))
		if _, err := testAppend(v, k, chunk, Options{}); err != nil {
			t.Fatal(err)
		}
		want = append(want, chunk...)
	}
	o, err := v.loadObject(k)
	if err != nil || len(o.ListBlocks) < 3 {
		t.Fatal("expected multiple linked segment blocks", err)
	}
	if _, err = testAppend(v, k, []byte("tail"), Options{}); err != nil {
		t.Fatal(err)
	}
	want = append(want, []byte("tail")...)
	mustGet(t, reopen(t, v, d), k, want)
	assertFreeTree(t, v)
}
