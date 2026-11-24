// SPDX-License-Identifier: GPL-3.0-only
package volume

import "testing"

func TestScanBucketRangeBoundsAndCursor(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	for _, bucket := range []ID{{}, testID(1), {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}} {
		for _, first := range []byte{0, 1, 2, 127, 128, 255} {
			key := Key{Bucket: bucket, Object: ObjectID{first}}
			if _, err := testPut(v, key, []byte{first}, Options{}); err != nil {
				t.Fatal(err)
			}
		}
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := h.ScanBucketRange(bucket, ObjectID{1}, ObjectID{128}, false, nil, 2)
		if err != nil || len(rows) != 2 || rows[0].Key.Object[0] != 1 || rows[1].Key.Object[0] != 2 {
			t.Fatal(rows, err)
		}
		cursor := rows[1].Key.Object
		rows, err = h.ScanBucketRange(bucket, ObjectID{1}, ObjectID{128}, false, &cursor, 2)
		if err != nil || len(rows) != 1 || rows[0].Key.Object[0] != 127 {
			t.Fatal(rows, err)
		}
		rows, err = h.ScanBucketRange(bucket, ObjectID{255}, ObjectID{}, true, nil, 2)
		if err != nil || len(rows) != 1 || rows[0].Key.Bucket != bucket || rows[0].Key.Object[0] != 255 {
			t.Fatal(rows, err)
		}
		h.Close()
	}
}
