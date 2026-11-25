// SPDX-License-Identifier: GPL-3.0-only
package volume

import "testing"

type ownershipTestStamp struct {
	sequence int
	bucket   ID
	known    bool
}

func (s ownershipTestStamp) ChangedOwnershipBuckets(any) ([]ID, bool) {
	if s.bucket == (ID{}) {
		return nil, s.known
	}
	return []ID{s.bucket}, s.known
}

func TestOwnershipDeltaSkipsUnrelatedRecordsAndInstallsNewPredicate(t *testing.T) {
	v, err := New("ownership", 4<<20, RAM)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	old, added := testID(1), testID(2)
	h, _ := v.Open(true)
	for i := byte(1); i <= 32; i++ {
		if _, err = h.Put(Key{Bucket: old, Object: ObjectID{i}}, []byte("data"), Options{RequestID: testID(uint64(i) + 40)}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	calls := 0
	include := func(k Key, _ bool) bool { calls++; return k.Bucket == old }
	if err = v.SetOwnershipFilter(ownershipTestStamp{sequence: 1}, include); err != nil || calls == 0 {
		t.Fatal("initial ownership must inspect records", calls, err)
	}
	for _, stamp := range []ownershipTestStamp{
		{sequence: 2, known: true},
		{sequence: 3, bucket: added, known: true},
	} {
		calls = 0
		before := v.IOStats()
		if err = v.SetOwnershipFilter(stamp, func(Key, bool) bool { calls++; return true }); err != nil {
			t.Fatal(err)
		}
		if after := v.IOStats(); calls != 0 || after.ReadOps != before.ReadOps || after.WriteOps != before.WriteOps {
			t.Fatal("metadata/empty bucket scanned existing storage", calls, before, after)
		}
	}
	// The new empty bucket must be included by future writes, even though its
	// addition did not trigger a recount of the old bucket's records.
	h, _ = v.Open(true)
	if _, err = h.Put(Key{Bucket: added, Object: ObjectID{1}}, []byte("new"), Options{RequestID: testID(100)}); err != nil {
		t.Fatal(err)
	}
	usage, err := h.OwnedStorageUsage()
	h.Close()
	if err != nil || len(usage) != 2 {
		t.Fatal("new predicate not used by mutations", usage, err)
	}
	for _, known := range []bool{true, false} {
		calls = 0
		stamp := ownershipTestStamp{sequence: 4, bucket: old, known: known}
		if !known {
			stamp.sequence++
		}
		if err = v.SetOwnershipFilter(stamp, func(k Key, _ bool) bool { calls++; return k.Bucket != old }); err != nil || calls == 0 {
			t.Fatal("nonempty/unknown change skipped recount", known, calls, err)
		}
		h, _ = v.Open(false)
		usage, err = h.OwnedStorageUsage()
		h.Close()
		if err != nil || len(usage) != 1 || usage[0].Bucket != added || usage[0].Logical != 3 {
			t.Fatal("changed ownership not applied", usage, err)
		}
	}
}
