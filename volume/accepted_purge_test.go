// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"testing"
)

func TestAcceptedDeletePurgeWaitsForExplicitCheckpoint(t *testing.T) {
	d := newCrash(16 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(17)
	if _, err = testPut(v, key, []byte("old durable content"), Options{}); err != nil {
		t.Fatal(err)
	}
	baseline := v.IOStats().SyncOps
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Delete(key, Options{Accepted: true}); err != nil {
		h.Close()
		t.Fatal(err)
	}
	generation, err := h.PurgeAccepted(key)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != baseline || v.IsDurableThrough(generation) {
		t.Fatal("accepted cleanup performed implicit physical checkpoint")
	}
	if _, _, err = testGet(v, key); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted key still visible", err)
	}
	before, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, before, key, []byte("old durable content"))
	if err = v.CheckpointThrough(generation); err != nil {
		t.Fatal(err)
	}
	after, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = testGet(after, key); !errors.Is(err, ErrNotFound) {
		t.Fatal("successful fence lost deletion", err)
	}
	h, err = after.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err = h.RecordInfo(key); !errors.Is(err, ErrNotFound) {
		t.Fatal("checkpoint retained physical tombstone", err)
	}
}
