// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
)

func TestRestoreDeltaKeepsAllocationAndExactBaseBytes(t *testing.T) {
	v, d := memoryVolume(t, 8<<20)
	key := testKey(1)
	payload := bytes.Repeat([]byte("p"), BlockSize)
	info := Info{Length: uint64(len(payload)), Compression: Raw}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	source := func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader(payload), uint64(len(payload)))
	}
	if err = h.RestoreSeekable(key, info, source, Options{ImportShadow: true}); err != nil {
		t.Fatal(err)
	}
	old, _ := v.loadObject(key)
	location := old.Spans[0].Location
	newer := info

	h.Close()
	before := v.IOStats()
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.RestoreDelta(key, sha256.Sum256(payload), newer, []RestorePatch{{Offset: 97, Data: []byte("changed")}}, Options{ImportShadow: true}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	after := v.IOStats()
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	next, _ := v.loadObject(key)
	if next.Spans[0].Location != location || next.Info != newer || after.WriteBytes-before.WriteBytes > 128<<10 {
		t.Fatal("delta copied payload or lost source metadata", next.Info, after.WriteBytes-before.WriteBytes)
	}
	if err = h.RestoreDelta(key, sha256.Sum256(payload), newer, nil, Options{ImportShadow: true}); !errors.Is(err, ErrPrecondition) {
		t.Fatal("stale base accepted", err)
	}
	h.Close()
	copy(payload[97:], "changed")
	mustGet(t, reopen(t, v, d), key, payload)
}

func TestRestoreDeltaRejectsAuthoritativeObjects(t *testing.T) {
	v, _ := memoryVolume(t, 2<<20)
	key := testKey(1)
	info, err := testPut(v, key, []byte("keep"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	next := info

	if err = h.RestoreDelta(key, sha256.Sum256([]byte("keep")), next, []RestorePatch{{Data: []byte("bad!")}}, Options{ImportShadow: true}); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
}

func TestRestoreDeltaScatteredPagesReadChecksumBlockOnce(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	key := testKey(1)
	payload := bytes.Repeat([]byte("x"), BlockSize)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	info := Info{Length: BlockSize}
	if err = h.RestoreSeekable(key, info, func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader(payload), BlockSize)
	}, Options{ImportShadow: true}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	before := v.IOStats()
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	baseDigest := sha256.Sum256(payload)
	var patches []RestorePatch
	for i := 0; i < 16; i++ {
		off := uint64(i * 64 * 1024)
		patches = append(patches, RestorePatch{Offset: off, Data: []byte("z")})
		payload[off] = 'z'
	}
	next := info

	if err = h.RestoreDelta(key, baseDigest, next, patches, Options{ImportShadow: true}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	after := v.IOStats()
	if after.ReadBytes-before.ReadBytes > BlockSize+(128<<10) || after.WriteBytes-before.WriteBytes > 256<<10 {
		t.Fatal("scattered delta reread or rewrote unchanged data", after.ReadBytes-before.ReadBytes, after.WriteBytes-before.WriteBytes)
	}
	mustGet(t, v, key, payload)
}

func TestRestoreDeltaCrashAtEveryCommitStep(t *testing.T) {
	base := newCrash(4 << 20)
	v, err := Format(base, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	original := bytes.Repeat([]byte("x"), BlockSize)
	info := Info{Length: BlockSize}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.RestoreSeekable(key, info, func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader(original), BlockSize)
	}, Options{ImportShadow: true}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	patches := []RestorePatch{{Offset: 1, Data: []byte("first")}, {Offset: 65537, Data: []byte("middle")}, {Offset: 999001, Data: []byte("last")}}
	expected := bytes.Clone(original)
	for _, p := range patches {
		copy(expected[p.Offset:], p.Data)
	}
	next := info

	for _, through := range []bool{false, true} {
		for _, persist := range []bool{false, true} {
			failures := 0
			for step := 1; step <= 40; step++ {
				d := base.crash()
				w, e := Open(d, OpenOptions{})
				if e != nil {
					t.Fatal(e)
				}
				d.steps = 0
				d.fail = step
				d.writeThrough = through
				d.persistFailedSync = persist
				h, e := w.Open(true)
				if e != nil {
					t.Fatal(e)
				}
				e = h.RestoreDelta(key, sha256.Sum256(original), next, patches, Options{ImportShadow: true})
				h.Close()
				if e != nil {
					failures++
					if !errors.Is(e, ErrRecovery) {
						t.Fatalf("step=%d: %v", step, e)
					}
				}
				recovered, re := Open(d.crash(), OpenOptions{})
				if re != nil {
					t.Fatalf("step=%d reopen: %v", step, re)
				}
				got, actual, re := testGet(recovered, key)
				if re != nil || !(bytes.Equal(got, original) || bytes.Equal(got, expected)) {
					t.Fatalf("step=%d torn delta, info=%+v err=%v", step, actual, re)
				}
				if e == nil && !bytes.Equal(got, expected) {
					t.Fatal("acknowledged delta lost")
				}
				assertFreeTree(t, recovered)
				if e == nil {
					break
				}
			}
			if failures == 0 {
				t.Fatal("no crash cutpoint exercised")
			}
		}
	}
}
