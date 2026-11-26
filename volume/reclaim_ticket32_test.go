// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// Check actual allocator locations, independently of payload contents and IDs.
// A nearly full volume still needs bounded WAL/metadata working space; the
// checks below distinguish payload address reuse from that temporary journal.
func TestTicket32EqualSizeReuseNearCapacity(t *testing.T) {
	for _, operation := range []string{"put", "cas-range", "cas-object"} {
		t.Run(operation, func(t *testing.T) {
			v, disk := memoryVolume(t, 1<<20)
			key := testKey(32)
			data := bytes.Repeat([]byte("a"), 8192)
			if _, err := testPut(v, key, data, Options{}); err != nil {
				t.Fatal(err)
			}
			if _, err := testPut(v, testKey(33), bytes.Repeat([]byte("f"), 900<<10), Options{}); err != nil {
				t.Fatal(err)
			}
			initial, err := v.loadObject(key)
			if err != nil {
				t.Fatal(err)
			}
			baseline := v.Stats()
			if baseline.AdmissionFreePhysical >= 128<<10 {
				t.Fatal("fixture is not near capacity")
			}
			for i := 0; i < 100; i++ {
				next := bytes.Repeat([]byte{byte(i)}, len(data))
				if operation == "put" {
					_, err = testPut(v, key, next, Options{})
				} else {
					wrong := bytes.Clone(data)
					wrong[0] ^= 1
					if _, e := testCAS(v, key, 0, wrong, next, Options{WholeObject: operation == "cas-object"}); !errors.Is(e, ErrPrecondition) {
						t.Fatalf("mismatched CAS: %v", e)
					}
					_, err = testCAS(v, key, 0, data, next, Options{WholeObject: operation == "cas-object"})
				}
				if err != nil {
					t.Fatalf("iteration %d: %v", i, err)
				}
				state, e := v.loadObject(key)
				if e != nil {
					t.Fatal(e)
				}
				if len(state.Spans) != len(initial.Spans) {
					t.Fatal("span count changed")
				}
				for j, span := range state.Spans {
					if span.Location != initial.Spans[j].Location || span.Capacity != initial.Spans[j].Capacity {
						t.Fatalf("iteration %d span %d relocated", i, j)
					}
				}
				stats := v.Stats()
				if stats.Objects != 2 || stats.DataPhysical != baseline.DataPhysical || stats.AdmissionFreePhysical+8*PageSize < baseline.AdmissionFreePhysical {
					t.Fatalf("unbounded allocation after %d: baseline=%+v now=%+v", i, baseline, stats)
				}
				data = next
				if i%25 == 24 {
					v = reopen(t, v, disk)
					mustGet(t, v, key, data)
				}
			}
			if err := testDelete(v, key, Options{}); err != nil {
				t.Fatal(err)
			}
			if stats := v.Stats(); stats.Objects != 1 || stats.DataPhysical != baseline.DataPhysical-uint64(len(data)) {
				t.Fatalf("delete retained live data: %+v", stats)
			}
			assertFreeTree(t, v)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTicket32InPlaceCrashRecovery(t *testing.T) {
	for _, whole := range []bool{false, true} {
		for _, through := range []bool{false, true} {
			t.Run(fmt.Sprintf("whole=%t/write-through=%t", whole, through), func(t *testing.T) {
				base := newCrash(2 << 20)
				v, err := Format(base, FormatOptions{Class: SSD})
				if err != nil {
					t.Fatal(err)
				}
				key := testKey(32)
				before, after := bytes.Repeat([]byte("a"), 8192), bytes.Repeat([]byte("b"), 8192)
				if _, err = testPut(v, key, before, Options{}); err != nil {
					t.Fatal(err)
				}
				initial, err := v.loadObject(key)
				if err != nil {
					t.Fatal(err)
				}
				failures := 0
				for cut := 1; cut < 64; cut++ {
					d := base.crash()
					w, err := Open(d, OpenOptions{})
					if err != nil {
						t.Fatal(err)
					}
					d.fail, d.writeThrough = cut, through
					_, opErr := testCAS(w, key, 0, before, after, Options{RequestID: testID(32), WholeObject: whole})
					if opErr != nil {
						failures++
						if !errors.Is(opErr, ErrRecovery) {
							t.Fatal(opErr)
						}
					}
					recovered, err := Open(d.crash(), OpenOptions{})
					if err != nil {
						t.Fatalf("cut %d: %v", cut, err)
					}
					got, _, err := testGet(recovered, key)
					if err != nil || !bytes.Equal(got, before) && !bytes.Equal(got, after) {
						t.Fatalf("cut %d: torn data: %v", cut, err)
					}
					if opErr == nil && !bytes.Equal(got, after) {
						t.Fatal("acknowledged CAS lost")
					}
					if _, err := testCAS(recovered, key, 0, before, after, Options{RequestID: testID(32), WholeObject: whole}); err != nil {
						t.Fatalf("cut %d retry: %v", cut, err)
					}
					current, err := recovered.loadObject(key)
					if err != nil || current.Spans[0].Location != initial.Spans[0].Location {
						t.Fatalf("cut %d relocated: %v", cut, err)
					}
					mustGet(t, recovered, key, after)
					assertFreeTree(t, recovered)
					if opErr == nil {
						break
					}
				}
				if failures < 5 {
					t.Fatalf("only %d fault points", failures)
				}
			})
		}
	}
}
