// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestRangeCASZeroExtendedGrowth(t *testing.T) {
	for _, codec := range []Compression{Raw, Gzip, LZ4, XZ} {
		t.Run(codec.String(), func(t *testing.T) {
			v, d := memoryVolume(t, 8<<20)
			key := testKey(1)
			if _, err := testPut(v, key, []byte("abc"), Options{Compression: Codec(codec)}); err != nil {
				t.Fatal(err)
			}
			before := v.IOStats().SyncOps
			if _, err := testCAS(v, key, 1, []byte{'b', 'c', 0, 1}, []byte("WXYZ"), Options{Accepted: true}); !errors.Is(err, ErrPrecondition) {
				t.Fatal("nonzero virtual tail matched", err)
			}
			mustGet(t, v, key, []byte("abc"))
			info, err := testCAS(v, key, 1, []byte{'b', 'c', 0, 0}, []byte("WXYZ"), Options{Accepted: true})
			if err != nil || info.Length != 5 {
				t.Fatal(info, err)
			}
			request := Options{Accepted: true, RequestID: testID(20)}
			info, err = testCAS(v, key, 100, make([]byte, 3), []byte("end"), request)
			if err != nil || info.Length != 103 {
				t.Fatal(info, err)
			}
			if _, err = testCAS(v, key, 100, make([]byte, 3), []byte("end"), request); err != nil {
				t.Fatal("successful growth retry", err)
			}
			want := make([]byte, 103)
			copy(want, "aWXYZ")
			copy(want[100:], "end")
			mustGet(t, v, key, want)
			oldLength := uint64(5)
			if _, err = testCAS(v, key, 100, []byte("end"), []byte("END"), Options{Accepted: true, ExpectedLength: &oldLength}); !errors.Is(err, ErrPrecondition) {
				t.Fatal("expected length ignored", err)
			}
			if _, err = testCAS(v, key, math.MaxUint64, []byte{0}, []byte{1}, Options{Accepted: true}); !errors.Is(err, ErrRange) {
				t.Fatal("overflow accepted", err)
			}
			if v.IOStats().SyncOps != before {
				t.Fatal("range CAS implicitly synchronized")
			}
			if err = v.Sync(); err != nil {
				t.Fatal(err)
			}
			mustGet(t, reopen(t, v, d), key, want)
		})
	}
}
func TestRangeCASMissingAndDeletedZeroExtension(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "deleted"}[deleted], func(t *testing.T) {
			v, _ := memoryVolume(t, 4<<20)
			key := testKey(1)
			if deleted {
				testPut(v, key, []byte("old"), Options{})
				if err := testDelete(v, key, Options{}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := testCAS(v, key, 7, []byte{1}, []byte{'A'}, Options{Accepted: true}); !errors.Is(err, ErrPrecondition) {
				t.Fatal(err)
			}
			if _, _, err := testGet(v, key); !errors.Is(err, ErrNotFound) {
				t.Fatal("mismatch created object", err)
			}
			info, err := testCAS(v, key, 7, []byte{0}, []byte{'A'}, Options{Accepted: true})
			if err != nil || info.Length != 8 {
				t.Fatal(info, err)
			}
			want := make([]byte, 8)
			want[7] = 'A'
			mustGet(t, v, key, want)
		})
	}
}
func TestRangeCASLargeHoleStaysSparse(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	key := testKey(1)
	testPut(v, key, []byte("head"), Options{})
	const off = uint64(1) << 40
	info, err := testCAS(v, key, off, make([]byte, 4), []byte("tail"), Options{Accepted: true})
	if err != nil || info.Length != off+4 {
		t.Fatal(info, err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := h.GetRange(key, off-4, 8)
	h.Close()
	if err != nil || !bytes.Equal(got, append(make([]byte, 4), []byte("tail")...)) {
		t.Fatal(got, err)
	}
	if v.Stats().DataPhysical > 1<<20 {
		t.Fatal("hole materialized", v.Stats())
	}
}
func TestReplicaRangeCASZeroExtendedGrowth(t *testing.T) {
	v, d := memoryVolume(t, 4<<20)
	key := testKey(1)
	testPut(v, key, []byte("head"), Options{})
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	id := testID(21)
	binding := ReplicaBinding{Epoch: 2, Sequence: 1, SetHash: [32]byte{2}}
	r, err := h.PrepareMutation(opCAS, key, 20, 4, bytes.NewBufferString("tail"), make([]byte, 4), Options{RequestID: id}, binding)
	h.Close()
	if err != nil || r.Info.Length != 24 {
		t.Fatal(r, err)
	}
	mustGet(t, v, key, []byte("head"))
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.DecideReplica(id, binding); err == nil {
		_, err = h.PublishReplica(id, binding)
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 24)
	copy(want, "head")
	copy(want[20:], "tail")
	mustGet(t, reopen(t, v, d), key, want)
}
