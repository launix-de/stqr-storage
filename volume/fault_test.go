// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// crashIO models a power cut with a separately tracked durable image. Writes may
// reach stable storage before Sync (writeThrough), and a failing write may tear.
// A failing Sync may have persisted everything before reporting failure.
type crashIO struct {
	live, stable                    []byte
	steps, fail                     int
	writeThrough, persistFailedSync bool
}

func newCrash(size int) *crashIO {
	return &crashIO{live: make([]byte, size), stable: make([]byte, size)}
}
func (d *crashIO) Size() (int64, error) { return int64(len(d.live)), nil }
func (d *crashIO) Close() error         { return nil }
func (d *crashIO) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(d.live)) {
		return 0, io.EOF
	}
	n := copy(b, d.live[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}
func (d *crashIO) WriteAt(b []byte, off int64) (int, error) {
	d.steps++
	if off < 0 || off > int64(len(d.live)) || int64(len(b)) > int64(len(d.live))-off {
		return 0, ErrFull
	}
	if d.fail == d.steps {
		n := len(b) / 2
		copy(d.live[off:], b[:n])
		if d.writeThrough {
			copy(d.stable[off:], b[:n])
		}
		return n, io.ErrShortWrite
	}
	copy(d.live[off:], b)
	if d.writeThrough {
		copy(d.stable[off:], b)
	}
	return len(b), nil
}
func (d *crashIO) Sync() error {
	d.steps++
	if d.steps == d.fail {
		if d.persistFailedSync {
			copy(d.stable, d.live)
		}
		return io.ErrClosedPipe
	}
	copy(d.stable, d.live)
	return nil
}
func (d *crashIO) crash() *crashIO {
	return &crashIO{live: bytes.Clone(d.stable), stable: bytes.Clone(d.stable)}
}

func TestCrashAtEveryCommitStep(t *testing.T) {
	for _, indexBoundary := range []bool{false, true} {
		for _, writeThrough := range []bool{false, true} {
			for _, persistSync := range []bool{false, true} {
				base := newCrash(4 << 20)
				v, e := Format(base, FormatOptions{Class: SSD})
				if e != nil {
					t.Fatal(e)
				}
				k := testKey(1)
				if _, e = testAppend(v, k, []byte("confirmed-"), Options{RequestID: testID(1)}); e != nil {
					t.Fatal(e)
				}
				if indexBoundary {
					for i := uint64(2); i < 64; i++ {
						if _, e = testPut(v, testKey(i), []byte("confirmed"), Options{}); e != nil {
							t.Fatal(e)
						}
					}
				}
				seenFailures := 0
				for fail := 1; fail < 40; fail++ {
					d := base.crash()
					w, e := Open(d, OpenOptions{})
					if e != nil {
						t.Fatal(e)
					}
					d.fail = fail
					d.writeThrough = writeThrough
					d.persistFailedSync = persistSync
					_, opErr := testAppend(w, k, []byte("new"), Options{RequestID: testID(1000)})
					if opErr != nil {
						seenFailures++
						if !errors.Is(opErr, ErrRecovery) {
							t.Fatalf("step %d: unexpected %v", fail, opErr)
						}
					}
					reboot := d.crash()
					recovered, e := Open(reboot, OpenOptions{})
					if e != nil {
						t.Fatalf("index=%v through=%v sync=%v step=%d reopen: %v", indexBoundary, writeThrough, persistSync, fail, e)
					}
					got, _, e := testGet(recovered, k)
					if e != nil || (!bytes.Equal(got, []byte("confirmed-")) && !bytes.Equal(got, []byte("confirmed-new"))) {
						t.Fatalf("step %d: corrupt result %q %v", fail, got, e)
					}
					if _, e = testAppend(recovered, k, []byte("new"), Options{RequestID: testID(1000)}); e != nil {
						t.Fatalf("step %d retry %v", fail, e)
					}
					mustGet(t, recovered, k, []byte("confirmed-new"))
					if indexBoundary {
						mustGet(t, recovered, testKey(63), []byte("confirmed"))
					}
					if opErr == nil {
						break
					}
				}
				if seenFailures < 5 {
					t.Fatalf("only %d failure points", seenFailures)
				}
			}
		}
	}
}
func TestCrashDuringCompaction(t *testing.T) {
	base := newCrash(4 << 20)
	v, e := Format(base, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	for i := uint64(0); i < 6; i++ {
		if _, e = testPut(v, testKey(i), bytes.Repeat([]byte{byte(i)}, 9000), Options{RequestID: testID(i + 1)}); e != nil {
			t.Fatal(e)
		}
	}
	for step := 1; step < 100; step++ {
		d := base.crash()
		w, e := Open(d, OpenOptions{})
		if e != nil {
			t.Fatal(e)
		}
		d.fail = step
		d.writeThrough = true
		d.persistFailedSync = step%2 == 0
		err := w.Compact()
		reboot := d.crash()
		r, e := Open(reboot, OpenOptions{})
		if e != nil {
			t.Fatalf("step %d: %v", step, e)
		}
		for i := uint64(0); i < 6; i++ {
			mustGet(t, r, testKey(i), bytes.Repeat([]byte{byte(i)}, 9000))
			if _, ok, e := testOperationStatus(r, testID(i+1)); e != nil || !ok {
				t.Fatalf("step %d lost retry %d: %v", step, i, e)
			}
		}
		if err == nil {
			return
		}
	}
	t.Fatal("no successful compaction reached")
}
func TestCommittedCorruptionIsNotSilentlyTruncated(t *testing.T) {
	d := newCrash(2 << 20)
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testPut(v, testKey(1), []byte("good"), Options{}); e != nil {
		t.Fatal(e)
	}
	s := v.Header()
	pos := s.ObjectRoot + 40
	d.live[pos] ^= 1
	if _, e = Open(d, OpenOptions{}); !errors.Is(e, ErrCorrupt) {
		t.Fatalf("corruption silently ignored: %v", e)
	}
}
func TestPayloadCorruptionScrub(t *testing.T) {
	d := newCrash(2 << 20)
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testPut(v, testKey(1), []byte("payload"), Options{}); e != nil {
		t.Fatal(e)
	}
	o, e := v.loadObject(testKey(1))
	if e != nil {
		t.Fatal(e)
	}
	d.live[o.Spans[0].Location] ^= 1
	if e = v.Scrub(); !errors.Is(e, ErrCorrupt) {
		t.Fatalf("%v", e)
	}
}

func TestScrubDoesNotTrustCachedMetadata(t *testing.T) {
	d := newCrash(2 << 20)
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testPut(v, testKey(1), []byte("good"), Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, testKey(1), []byte("good"))
	o, err := v.loadObject(testKey(1))
	if err != nil {
		t.Fatal(err)
	}
	d.live[o.Base+8] ^= 1
	if e = v.Scrub(); !errors.Is(e, ErrCorrupt) {
		t.Fatalf("cache hid metadata corruption: %v", e)
	}
}
