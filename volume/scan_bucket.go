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
		cursor := objectKey(Key{Bucket: bucket})
		upper := cursor
		for i := 16; i >= 0; i-- {
			upper[i]++
			if upper[i] != 0 {
				break
			}
		}
		if after != nil {
			cursor = objectKey(Key{Bucket: bucket, Object: *after})
		}
		err := v.walkIndexBounds(cursor, upper, func(x indexEntry) error {
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

// ScanRequestRecords pages retry metadata without scanning preceding requests
// or the payload-object namespace. The optional cursor is strictly exclusive.
func (h *Handle) ScanRequestRecords(after *ID, limit int) ([]RequestRecord, error) {
	return withHandle(h, false, func(v *Volume) ([]RequestRecord, error) {
		if limit < 1 || limit > 4096 {
			return nil, ErrRange
		}
		lower, upper := indexKey{1}, indexKey{2}
		if after != nil {
			lower = requestKey(*after)
		}
		var out []RequestRecord
		stop := errors.New("request scan page complete")
		err := v.walkIndexBounds(lower, upper, func(x indexEntry) error {
			if after != nil && !lessKey(lower, x.Key) {
				return nil
			}
			var id ID
			copy(id[:], x.Key[1:17])
			receipt, found, err := v.loadDedupe(id)
			if err != nil {
				return err
			}
			if !found {
				return ErrCorrupt
			}
			out = append(out, RequestRecord{ID: id, Key: receipt.Key, Fingerprint: receipt.Fingerprint, Result: receipt.Result})
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

func (h *Handle) ScanReplicaRecords(after *ID, limit int) ([]ReplicaRecord, error) {
	return withHandle(h, false, func(v *Volume) ([]ReplicaRecord, error) {
		if limit < 1 || limit > 4096 {
			return nil, ErrRange
		}
		lower, upper := indexKey{2}, indexKey{3}
		if after != nil {
			lower = replicaKey(*after)
		}
		var out []ReplicaRecord
		stop := errors.New("replica scan page complete")
		err := v.walkIndexBounds(lower, upper, func(x indexEntry) error {
			if after != nil && !lessKey(lower, x.Key) {
				return nil
			}
			record, err := v.replicaAt(x.Value)
			if err != nil {
				return err
			}
			out = append(out, record)
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
