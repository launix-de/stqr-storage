// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"testing"
)

func TestAcceptedVisibleAndRetryUpgrade(t *testing.T) {
	d := newCrash(16 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	k := testKey(1)
	syncs := v.IOStats().SyncOps
	writes := v.IOStats().WriteOps
	info, err := testPut(v, k, []byte("visible"), Options{RequestID: testID(1), Accepted: true})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, v, k, []byte("visible"))
	if v.IOStats().SyncOps != syncs || v.IOStats().WriteOps != writes {
		t.Fatal("accepted mutation touched durable backend")
	}
	duplicate, err := testPut(v, k, []byte("visible"), Options{RequestID: testID(1), Accepted: true})
	if err != nil || duplicate.Version != info.Version {
		t.Fatal(duplicate, err)
	}
	if _, err = testPut(v, k, []byte("changed"), Options{RequestID: testID(1), Accepted: true}); !errors.Is(err, ErrRequestID) {
		t.Fatal(err)
	}
	duplicate, err = testPut(v, k, []byte("visible"), Options{RequestID: testID(1)})
	if err != nil || duplicate.Version != info.Version {
		t.Fatal(duplicate, err)
	}
	if v.IOStats().SyncOps-syncs != 4 {
		t.Fatal("retry did not make accepted group durable", v.IOStats())
	}
	reboot, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, reboot, k, []byte("visible"))
}
func TestAcceptedGroupCrashAtEveryStep(t *testing.T) {
	base := newCrash(16 << 20)
	v, err := Format(base, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(v, testKey(1), []byte("old-a"), Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(v, testKey(2), []byte("old-b"), Options{}); err != nil {
		t.Fatal(err)
	}
	for _, through := range []bool{false, true} {
		for _, persist := range []bool{false, true} {
			failures := 0
			for fail := 1; fail < 160; fail++ {
				d := base.crash()
				w, err := Open(d, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = testPut(w, testKey(1), bytes.Repeat([]byte("A"), 6000), Options{RequestID: testID(100), Accepted: true}); err != nil {
					t.Fatal(err)
				}
				if _, err = testPut(w, testKey(2), []byte("new-b"), Options{RequestID: testID(101), Accepted: true}); err != nil {
					t.Fatal(err)
				}
				mustGet(t, w, testKey(2), []byte("new-b"))
				d.steps = 0
				d.fail = fail
				d.writeThrough = through
				d.persistFailedSync = persist
				commitErr := w.Sync()
				if commitErr != nil {
					failures++
					if !errors.Is(commitErr, ErrRecovery) {
						t.Fatal(commitErr)
					}
				}
				recovered, err := Open(d.crash(), OpenOptions{})
				if err != nil {
					t.Fatalf("through=%v persist=%v step=%d: %v", through, persist, fail, err)
				}
				a, _, err := testGet(recovered, testKey(1))
				if err != nil {
					t.Fatal(err)
				}
				b, _, err := testGet(recovered, testKey(2))
				if err != nil {
					t.Fatal(err)
				}
				old := bytes.Equal(a, []byte("old-a")) && bytes.Equal(b, []byte("old-b"))
				next := bytes.Equal(a, bytes.Repeat([]byte("A"), 6000)) && bytes.Equal(b, []byte("new-b"))
				if !old && !next || commitErr == nil && !next {
					t.Fatalf("step=%d inconsistent group a=%d b=%q error=%v", fail, len(a), b, commitErr)
				}
				if commitErr == nil {
					break
				}
			}
			if failures < 8 {
				t.Fatalf("only %d failure steps", failures)
			}
		}
	}
}
func TestAcceptedAbortAndCleanClose(t *testing.T) {
	d := newCrash(16 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(v, testKey(1), []byte("keep"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	// Truncated input is rejected without changing a preceding accepted object.
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.PutReader(testKey(1), bytes.NewReader([]byte("x")), 100, Options{Accepted: true})
	h.Close()
	if err == nil {
		t.Fatal("accepted truncated payload")
	}
	mustGet(t, v, testKey(1), []byte("keep"))
	generation := v.Header().Generation
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	if err = v.CheckpointThrough(generation); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, recovered, testKey(1), []byte("keep"))
}

// Dirty allocator state can reuse pages which are still live in the durable
// checkpoint. Such pages must go through redo, never the direct-free fast path.
func TestAcceptedDeleteReuseCrash(t *testing.T) {
	base := newCrash(16 << 20)
	v, err := Format(base, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	old := bytes.Repeat([]byte("o"), 8192)
	next := bytes.Repeat([]byte("n"), 8192)
	if _, err = testPut(v, testKey(1), old, Options{}); err != nil {
		t.Fatal(err)
	}
	for fail := 1; fail < 100; fail++ {
		d := base.crash()
		w, err := Open(d, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err = testDelete(w, testKey(1), Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
		if _, err = testPut(w, testKey(2), next, Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
		d.steps = 0
		d.fail = fail
		d.writeThrough = true
		err = w.Sync()
		recovered, e := Open(d.crash(), OpenOptions{})
		if e != nil {
			t.Fatal(fail, e)
		}
		a, _, ae := testGet(recovered, testKey(1))
		b, _, be := testGet(recovered, testKey(2))
		before := ae == nil && bytes.Equal(a, old) && errors.Is(be, ErrNotFound)
		after := errors.Is(ae, ErrNotFound) && be == nil && bytes.Equal(b, next)
		if !before && !after {
			t.Fatalf("step %d corrupted durable reused pages: a=%d/%v b=%d/%v", fail, len(a), ae, len(b), be)
		}
		if err == nil {
			if !after {
				t.Fatal("successful checkpoint missing")
			}
			break
		}
	}
}
