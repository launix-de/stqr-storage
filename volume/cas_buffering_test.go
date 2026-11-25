// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"testing"
)

func TestAcceptedCASPreservesBufferedGroup(t *testing.T) {
	v, _ := newCheckpointGate(t)
	k := testKey(1)
	if _, err := testPut(v, k, []byte("first"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	if _, err := testCAS(v, k, 0, []byte("first"), []byte("later"), Options{Accepted: true, WholeObject: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := testCAS(v, k, 0, []byte("wrong"), []byte("error"), Options{Accepted: true}); !errors.Is(err, ErrPrecondition) {
		t.Fatal("failed comparison", err)
	}
	if after := v.IOStats(); after.SyncOps != before.SyncOps || after.WriteOps != before.WriteOps {
		t.Fatal("accepted CAS forced physical I/O", before, after)
	}
	mustGet(t, v, k, []byte("later"))
	if err := v.CheckpointThrough(v.Header().Generation); err != nil {
		t.Fatal(err)
	}
	if !v.IsDurableThrough(v.Header().Generation) {
		t.Fatal("explicit fence did not confirm CAS")
	}
}
