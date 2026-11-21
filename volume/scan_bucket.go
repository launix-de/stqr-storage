// SPDX-License-Identifier: GPL-3.0-only
package volume

import "errors"

// ScanBucketObjects pages live objects and tombstones in one bucket, strictly
// after the optional cursor. A nil cursor includes the zero object address.
func (h *Handle) ScanBucketObjects(bucket ID, after *ObjectID, limit int) ([]ObjectRecord, error) {
	return withHandle(h, false, func(v *Volume) ([]ObjectRecord, error) {
		if limit < 1 || limit > 4096 {
			return nil, ErrRange
		}
		out := make([]ObjectRecord, 0, limit)
		stop := errors.New("bucket scan page complete")
		var cursor indexKey
		if after != nil {
			cursor = objectKey(Key{Bucket: bucket, Object: *after})
		}
		err := v.walkIndex(func(x indexEntry) error {
			if x.Key[0] != 0 {
				return nil
			}
			k := x.Key.object()
			if k.Bucket != bucket || after != nil && !lessKey(cursor, x.Key) {
				return nil
			}
			o, err := v.loadObject(k)
			if err != nil {
				return err
			}
			out = append(out, ObjectRecord{Key: k, Info: o.Info})
			if len(out) == limit {
				return stop
			}
			return nil
		})
		if errors.Is(err, stop) {
			err = nil
		}
		return out, err
	})
}
