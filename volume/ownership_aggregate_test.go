// SPDX-License-Identifier: GPL-3.0-only
package volume

import "testing"

type ownershipAggregateStamp struct {
	ownershipTestStamp
	owned, exact bool
}

func (s ownershipAggregateStamp) OwnershipForEntireBucket(ID) (bool, bool) { return s.owned, s.exact }

func TestOwnershipAggregatePopulatedBucketNeedsNoIO(t *testing.T) {
	v, err := New("aggregate", 8<<20, RAM)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	bucket, other := testID(1), testID(2)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 64; i++ {
		for _, id := range []ID{bucket, other} {
			if _, err = h.Put(Key{Bucket: id, Object: ObjectID{byte(i)}}, []byte("payload"), Options{RequestID: testID(i*3 + uint64(id[0]) + 100)}); err != nil {
				h.Close()
				t.Fatal(err)
			}
		}
	}
	h.Close()
	observed := 0
	if err = v.SetAdmission(nil, func(StorageSnapshot) { observed++ }); err != nil {
		t.Fatal(err)
	}
	if err = v.SetOwnershipFilter(ownershipAggregateStamp{ownershipTestStamp: ownershipTestStamp{sequence: 1}}, func(Key, bool) bool { return true }); err != nil {
		t.Fatal(err)
	}
	for i, own := range []bool{false, true, false, true} {
		calls := 0
		before, notifications := v.IOStats(), observed
		stamp := ownershipAggregateStamp{ownershipTestStamp: ownershipTestStamp{sequence: i + 2, bucket: bucket, known: true}, owned: own, exact: true}
		if err = v.SetOwnershipFilter(stamp, func(k Key, _ bool) bool { calls++; return own || k.Bucket != bucket }); err != nil {
			t.Fatal(err)
		}
		after := v.IOStats()
		if calls != 0 || testReadWorkOps(after) != testReadWorkOps(before) || after.WriteOps != before.WriteOps {
			t.Fatal("whole-bucket transition scanned storage", calls, before, after)
		}
		if observed != notifications+1 {
			t.Fatal("ownership observer not notified")
		}
		if !own {
			h, err = v.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.Put(Key{Bucket: bucket, Object: ObjectID{byte(100 + i)}}, []byte("later"), Options{RequestID: testID(uint64(1000 + i))})
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		h, err = v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		selected, e1 := h.OwnedStorageUsage()
		all, e2 := h.StorageUsage()
		h.Close()
		if e1 != nil || e2 != nil {
			t.Fatal(e1, e2)
		}
		expected := map[ID]BucketStorage{}
		for _, row := range all {
			if own || row.Bucket != bucket {
				expected[row.Bucket] = row
			}
		}
		if len(selected) != len(expected) {
			t.Fatal("wrong ownership totals", selected, expected)
		}
		for _, row := range selected {
			if row != expected[row.Bucket] {
				t.Fatal("cached accounting differs from exact physical totals", row, expected[row.Bucket])
			}
		}
	}
	// A mixed predicate cannot use whole-bucket counters, even with a known delta.
	calls := 0
	stamp := ownershipAggregateStamp{ownershipTestStamp: ownershipTestStamp{sequence: 10, bucket: bucket, known: true}}
	if err = v.SetOwnershipFilter(stamp, func(k Key, _ bool) bool { calls++; return k.Bucket != bucket || k.Object[0] == 1 }); err != nil || calls == 0 {
		t.Fatal("mixed ownership skipped exact recount", calls, err)
	}
	h, _ = v.Open(false)
	rows, err := h.OwnedStorageUsage()
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Bucket == bucket && (row.Objects != 1 || row.Logical != 7) {
			t.Fatal("partial recount inaccurate", row)
		}
	}
}
