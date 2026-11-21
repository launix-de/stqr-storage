// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestGrowAndRescanPreserveCommittedLayout(t *testing.T) {
	for _, bankOne := range []bool{false, true} {
		p := filepath.Join(t.TempDir(), "grow")
		v, e := New(p, 512<<10, SSD)
		if e != nil {
			t.Fatal(e)
		}
		k := testKey(1)
		info, e := testPutRange(v, k, 1<<40, []byte("far"), Options{RequestID: testID(1)})
		if e != nil {
			t.Fatal(e)
		}
		testPut(v, testKey(2), []byte("deleted"), Options{})
		testDelete(v, testKey(2), Options{})
		if bankOne {
			if e = v.Compact(); e != nil {
				t.Fatal(e)
			}
		}
		identity := v.Identity()
		if e = v.Grow(640 << 10); e != nil {
			t.Fatal(e)
		}
		if v.Size() != 640<<10 || v.Identity().VolumeID != identity.VolumeID {
			t.Fatal("grow changed identity or lost capacity")
		}
		got, gotInfo, e := testGetRange(v, k, 1<<40, 3)
		if e != nil || !bytes.Equal(got, []byte("far")) || gotInfo != info {
			t.Fatalf("grew data: %q %+v %v", got, gotInfo, e)
		}
		if _, found, e := testOperationStatus(v, testID(1)); e != nil || !found {
			t.Fatalf("retry record: %v %v", found, e)
		}
		if e = os.Truncate(p, 768<<10); e != nil {
			t.Fatal(e)
		}
		v.Close()
		v, e = New(p, 0, HDD)
		if e != nil {
			t.Fatal(e)
		}
		if v.Size() != 640<<10 {
			t.Fatal("open silently adopted uncommitted capacity")
		}
		n, e := v.Rescan()
		if e != nil || n != 768<<10 {
			t.Fatalf("rescan: %d %v", n, e)
		}
		if e = v.Grow(512 << 10); !errors.Is(e, ErrRange) {
			t.Fatalf("shrink: %v", e)
		}
		if e = v.Scrub(); e != nil {
			t.Fatal(e)
		}
		v.Close()
		v, e = New(p, 0, HDD)
		if e != nil {
			t.Fatal(e)
		}
		got, _, e = testGetRange(v, k, 1<<40, 3)
		if e != nil || string(got) != "far" {
			t.Fatal("reopen lost sparse data", e)
		}
		v.Close()
	}
	m, _ := memoryVolume(t, 512<<10)
	if e := m.Grow(1 << 20); e != nil {
		t.Fatal(e)
	}
	if _, e := m.Rescan(); e != ErrUnsupported {
		t.Fatal(e)
	}
}
func TestCrashAtEveryRescanWriteAndSync(t *testing.T) {
	for _, bankOne := range []bool{false, true} {
		for _, through := range []bool{false, true} {
			for _, persist := range []bool{false, true} {
				base := newCrash(512 << 10)
				v, e := Format(base, FormatOptions{Class: SSD})
				if e != nil {
					t.Fatal(e)
				}
				k := testKey(1)
				want := bytes.Repeat([]byte("confirmed-"), 300)
				result, e := testAppend(v, k, want, Options{RequestID: testID(3)})
				if e != nil {
					t.Fatal(e)
				}
				if bankOne {
					if e = v.Compact(); e != nil {
						t.Fatal(e)
					}
				}
				base.live = append(base.live, make([]byte, 128<<10)...)
				base.stable = append(base.stable, make([]byte, 128<<10)...)
				for step := 1; step < 100; step++ {
					d := base.crash()
					d.writeThrough = through
					d.persistFailedSync = persist
					d.fail = step
					w, e := Open(d, OpenOptions{})
					if e != nil {
						t.Fatal(e)
					}
					_, growErr := w.Rescan()
					used := d.steps
					recovered, e := Open(d.crash(), OpenOptions{})
					if e != nil {
						t.Fatalf("bank=%v writeThrough=%v persist=%v step=%d: %v", bankOne, through, persist, step, e)
					}
					if recovered.Size() != 512<<10 && recovered.Size() != 640<<10 {
						t.Fatal("torn size adopted")
					}
					mustGet(t, recovered, k, want)
					again, e := testAppend(recovered, k, want, Options{RequestID: testID(3)})
					if e != nil || again != result {
						t.Fatalf("lost dedupe: %+v %v", again, e)
					}
					mustGet(t, recovered, k, want)
					if growErr == nil && step > used {
						break
					}
					if step == 99 {
						t.Fatal("fault traversal did not terminate")
					}
				}
			}
		}
	}
}
func TestScanObjectsPaginationIncludesTombstonesAndBuckets(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	for i := uint64(0); i < 270; i++ {
		k := testKey(i)
		k.Bucket[0] = byte(i % 3)
		if _, e := testPut(v, k, []byte("x"), Options{}); e != nil {
			t.Fatal(e)
		}
		if i%7 == 0 {
			if e := testDelete(v, k, Options{}); e != nil {
				t.Fatal(e)
			}
		}
	}
	h, e := v.Open(false)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	var cursor *Key
	count, deleted := 0, 0
	for {
		page, e := h.ScanObjects(cursor, 31)
		if e != nil {
			t.Fatal(e)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			if cursor != nil && !lessKey(objectKey(*cursor), objectKey(r.Key)) {
				t.Fatal("cursor order")
			}
			k := r.Key
			cursor = &k
			count++
			if r.Info.Deleted {
				deleted++
			}
		}
	}
	if count != 270 || deleted != 39 {
		t.Fatalf("scan dropped records: %d %d", count, deleted)
	}
	if _, e := h.ScanObjects(nil, 0); e != ErrRange {
		t.Fatal(e)
	}
}

func TestRAMGrowPreservesObjectsAndRetryRecords(t *testing.T) {
	v, _ := memoryVolume(t, 512<<10)
	k := testKey(9)
	before, e := testPutRange(v, k, 1<<40, []byte("sparse"), Options{RequestID: testID(9)})
	if e != nil {
		t.Fatal(e)
	}
	identity := v.Identity()
	if e = v.Grow(2 << 20); e != nil {
		t.Fatal(e)
	}
	got, after, e := testGetRange(v, k, 1<<40, 6)
	if e != nil || string(got) != "sparse" || after != before || v.Identity().VolumeID != identity.VolumeID {
		t.Fatal("RAM growth lost data or identity", e)
	}
	retry, e := testPutRange(v, k, 1<<40, []byte("sparse"), Options{RequestID: testID(9)})
	if e != nil || retry != before {
		t.Fatal("RAM growth lost retry record", e)
	}
	if e = v.Grow(1 << 20); !errors.Is(e, ErrRange) {
		t.Fatal("RAM shrink accepted", e)
	}
	if e = v.Scrub(); e != nil {
		t.Fatal(e)
	}
}
