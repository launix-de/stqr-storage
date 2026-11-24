// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestForgetRequestsBatchesCommitsAndRejectsPartialCleanup(t *testing.T) {
	v, err := New(filepath.Join(t.TempDir(), "batch"), 4<<20, HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	h, _ := v.Open(true)
	key := testKey(1)
	data := bytes.Repeat([]byte("payload"), 32)
	var ids []ID
	for i := 1; i <= 64; i++ {
		id := testID(uint64(i))
		ids = append(ids, id)
		if _, err = h.Put(key, data, Options{RequestID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err = h.ForgetRequests([]ID{ids[0], {}}); !errors.Is(err, ErrRange) {
		t.Fatal(err)
	}
	if _, ok, err := h.OperationStatus(ids[0]); err != nil || !ok {
		t.Fatal("failed batch removed a receipt", ok, err)
	}
	if err = h.ForgetRequests([]ID{ids[0], ids[0]}); !errors.Is(err, ErrRange) {
		t.Fatal("duplicate ID", err)
	}
	h.Close()
	before := v.IOStats()
	h, _ = v.Open(true)
	if err = h.ForgetRequests(ids); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, ok, err := h.OperationStatus(id); err != nil || ok {
			t.Fatal("receipt retained", id, err)
		}
	}
	got, _, err := h.Get(key)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("cleanup changed payload", err)
	}
	h.Close()
	after := v.IOStats()
	if after.SyncOps-before.SyncOps > 8 {
		t.Fatal("cleanup still commits per receipt", after.SyncOps-before.SyncOps)
	}
	if err = v.Scrub(); err != nil {
		t.Fatal(err)
	}
}
