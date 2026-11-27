//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
//
package volume

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestCASCheckedComparisonReadOnlyMmap(t *testing.T) {
	v, err := New(filepath.Join(t.TempDir(), "cas-view"), 8<<20, HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	old := bytes.Repeat([]byte("o"), 24+3218*81)
	next := bytes.Repeat([]byte("n"), len(old))
	if _, err = testPut(v, key, old, Options{}); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats().SyncOps
	if _, err = testCAS(v, key, 0, old, next, Options{Accepted: true, WholeObject: true}); err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != before {
		t.Fatal("accepted comparison synced")
	}
	mustGet(t, v, key, next)
}
