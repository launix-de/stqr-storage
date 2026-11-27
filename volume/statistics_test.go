// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"reflect"
	"slices"
	"testing"
)

func TestStatisticsOwnershipRoundingAndIndependentSlices(t *testing.T) {
	first, second := ID{1}, ID{2}
	all := map[ID]bucketAllocation{
		first:  {logical: 1000, payload: 1000, metadata: 100, records: 1, objects: 1},
		second: {logical: 3000, payload: 3000, metadata: 300, records: 3, objects: 3},
	}
	owned := map[ID]bucketAllocation{
		first:  all[first],
		second: {logical: 500, payload: 700, metadata: 50, records: 1, objects: 1},
	}
	v := &Volume{}
	sb := Superblock{MetadataBytes: 6*PageSize + 400 + 10}
	wantAll := []BucketStorage{{Bucket: first, Logical: 1000, Physical: 1103, Objects: 1}, {Bucket: second, Logical: 3000, Physical: 3307, Objects: 3}}
	wantOwned := []BucketStorage{wantAll[0], {Bucket: second, Logical: 500, Physical: 752, Objects: 1}}
	got := v.storageSnapshot(all, owned, sb)
	if !reflect.DeepEqual(got.All, wantAll) || !reflect.DeepEqual(got.Owned, wantOwned) {
		t.Fatal("shared metadata rounding changed", got)
	}
	for _, selected := range []map[ID]bucketAllocation{nil, all} {
		snapshot := v.storageSnapshot(all, selected, sb)
		if !reflect.DeepEqual(snapshot.All, wantAll) || !reflect.DeepEqual(snapshot.Owned, wantAll) {
			t.Fatal(snapshot)
		}
		snapshot.All[0].Physical++
		if snapshot.Owned[0].Physical != wantAll[0].Physical {
			t.Fatal("callback slices alias")
		}
	}
}

func TestStatisticsAdmissionCannotAlterPublishedSnapshot(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "accepted"}[accepted], func(t *testing.T) {
			v, _ := memoryVolume(t, 8<<20)
			defer v.Close()
			var publication StorageSnapshot
			publications := 0
			reject := false
			if err := v.SetAdmission(func(change StorageChange) (uint64, error) {
				if reject {
					return 0, ErrFull
				}
				for i := range change.After {
					change.After[i].Logical = 999999
				}
				for i := range change.AfterOwned {
					change.AfterOwned[i].Physical = 999999
				}
				return 0, nil
			}, func(snapshot StorageSnapshot) { publication = snapshot; publications++ }); err != nil {
				t.Fatal(err)
			}
			h, err := v.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			key := testKey(1)
			if _, err := h.Put(key, []byte("hello"), Options{Accepted: accepted}); err != nil {
				t.Fatal(err)
			}
			all, err := h.StorageUsage()
			if err != nil {
				t.Fatal(err)
			}
			owned, err := h.OwnedStorageUsage()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(publication.All, all) || !reflect.DeepEqual(publication.Owned, owned) {
				t.Fatal("callback corrupted publication", publication, all, owned)
			}
			previous := publication
			previousAll, previousOwned := slices.Clone(previous.All), slices.Clone(previous.Owned)
			reject = true
			before := publications
			if _, err := h.Append(key, []byte("rejected"), Options{Accepted: accepted}); !errors.Is(err, ErrFull) {
				t.Fatal(err)
			}
			if publications != before {
				t.Fatal("rejected mutation published accounting")
			}
			reject = false
			if _, err := h.Append(key, []byte("!"), Options{Accepted: accepted}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(previous.All, previousAll) || !reflect.DeepEqual(previous.Owned, previousOwned) {
				t.Fatal("later write changed retained immutable publication")
			}
		})
	}
}
