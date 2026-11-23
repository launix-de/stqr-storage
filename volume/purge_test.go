// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"testing"
)

func TestPurgeRemovesOnlyConfirmedPhysicalVersion(t *testing.T) {
	v, device := memoryVolume(t, 4<<20)
	key := testKey(1)
	request := testID(77)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	info, err := h.Put(key, bytes.Repeat([]byte{1}, 1<<20), Options{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Purge(key, info.Version+1); !errors.Is(err, ErrPrecondition) {
		t.Fatal("stale cleanup removed newer version", err)
	}
	h.Close()
	before := v.Stats().AllocatedPhysical
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Purge(key, info.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RecordInfo(key); !errors.Is(err, ErrNotFound) {
		t.Fatal("purge invented a tombstone", err)
	}
	if _, found, err := h.OperationStatus(request); err != nil || !found {
		t.Fatal("cleanup discarded unconfirmed receipt", err)
	}
	if err := h.Purge(key, info.Version); err != nil {
		t.Fatal("cleanup retry", err)
	}
	h.Close()
	if v.Stats().AllocatedPhysical >= before || v.Stats().LogicalBytes != 0 {
		t.Fatal("physical data was not reclaimed")
	}
	v = reopen(t, v, device)
	defer v.Close()
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.RecordInfo(key); !errors.Is(err, ErrNotFound) {
		t.Fatal("purged record returned after reopen", err)
	}
}

func TestPurgeBatchRejectsWholeBatchOnVersionConflict(t *testing.T) {
	v, device := memoryVolume(t, 4<<20)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]ObjectRecord, 24)
	for i := range records {
		key := testKey(uint64(i + 1))
		info, err := h.Put(key, bytes.Repeat([]byte{byte(i)}, 73+i), Options{})
		if err != nil {
			t.Fatal(err)
		}
		records[i] = ObjectRecord{Key: key, Info: info}
	}
	invalid := append([]ObjectRecord(nil), records...)
	invalid[len(invalid)-1].Info.Version++
	if err := h.PurgeBatch(invalid); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	for _, record := range records {
		if _, err := h.RecordInfo(record.Key); err != nil {
			t.Fatal("partially removed rejected batch", err)
		}
	}
	if err := h.PurgeBatch([]ObjectRecord{records[0], records[0]}); !errors.Is(err, ErrRange) {
		t.Fatal(err)
	}
	if err := h.PurgeBatch(records); err != nil {
		t.Fatal(err)
	}
	h.Close()
	v = reopen(t, v, device)
	defer v.Close()
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	for _, record := range records {
		if _, err := h.RecordInfo(record.Key); !errors.Is(err, ErrNotFound) {
			t.Fatal("purged batch reappeared", err)
		}
	}
}
