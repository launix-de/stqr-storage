// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"testing"
	"time"
)

type multipartPageStore struct {
	S3Store
	records []ObjectEntry
	data    map[ObjectID][]byte
	reads   []ObjectID
	scans   int
}

func (s *multipartPageStore) ScanObjects(_ context.Context, after *ObjectID, limit uint32) ([]ObjectEntry, *ObjectID, error) {
	s.scans++
	start := 0
	for start < len(s.records) && after != nil && bytes.Compare(s.records[start].ID[:], after[:]) <= 0 {
		start++
	}
	end := min(start+int(limit), len(s.records))
	var next *ObjectID
	if end < len(s.records) {
		id := s.records[end-1].ID
		next = &id
	}
	return s.records[start:end], next, nil
}
func (s *multipartPageStore) GetRange(_ context.Context, id ObjectID, off, n uint64) ([]byte, ObjectInfo, error) {
	s.reads = append(s.reads, id)
	if b, ok := s.data[id]; ok {
		if off > uint64(len(b)) {
			return nil, ObjectInfo{}, ErrRange
		}
		return append([]byte(nil), b[off:min(uint64(len(b)), off+n)]...), ObjectInfo{Length: uint64(len(b))}, nil
	}
	return bytes.Repeat([]byte("N"), int(n)), ObjectInfo{Length: 1 << 20}, nil
}
func TestS3MultipartPageBudgetAndLateUpload(t *testing.T) {
	ctx := context.Background()
	store := &multipartPageStore{data: map[ObjectID][]byte{}}
	for i := 1; i <= 300; i++ {
		var id ObjectID
		binary.BigEndian.PutUint64(id[24:], uint64(i))
		store.records = append(store.records, ObjectEntry{ID: id, Info: ObjectInfo{Length: 1 << 20}})
	}
	upload := S3Upload{ID: "ffffffffffffffffffffffffffffffff", Key: "late", Owner: "owner", Created: time.Now(), Updated: time.Now(), Headers: map[string]string{}, Metadata: map[string]string{}}
	raw, err := encodeS3Upload(upload)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := s3UploadID(upload.ID)
	store.data[id] = raw
	store.records = append(store.records, ObjectEntry{ID: id, Info: ObjectInfo{Length: uint64(len(raw))}})
	sort.Slice(store.records, func(i, j int) bool { return bytes.Compare(store.records[i].ID[:], store.records[j].ID[:]) < 0 })
	api := NewS3(store)
	var after *ObjectID
	found := 0
	seen := map[ObjectID]bool{}
	for calls := 0; ; calls++ {
		if calls > 100 {
			t.Fatal("cursor failed to finish")
		}
		before := len(store.reads)
		scans := store.scans
		page, next, e := api.MultipartUploadsPage(ctx, after, 128, 8<<20)
		if e != nil {
			t.Fatal(e)
		}
		if store.scans != scans+1 {
			t.Fatal("one step scanned multiple native pages")
		}
		// Eight full checksum blocks plus a small upload header and its metadata.
		if len(store.reads)-before > 10 {
			t.Fatal("unbounded discovery", len(store.reads)-before)
		}
		for _, id := range store.reads[before:] {
			seen[id] = true
		}
		for _, u := range page {
			if u.ID != upload.ID {
				t.Fatal(u.ID)
			}
			found++
		}
		if next == nil {
			break
		}
		if after != nil && bytes.Compare(next[:], after[:]) <= 0 {
			t.Fatal("cursor did not advance")
		}
		after = next
	}
	if found != 1 || len(seen) != len(store.records) {
		t.Fatal("budget skipped objects or an upload", found, len(seen), len(store.records))
	}
}
func TestS3MultipartPageRejectsUnboundedCount(t *testing.T) {
	api := NewS3(&multipartPageStore{})
	for _, limit := range []uint32{0, 1001} {
		if _, _, err := api.MultipartUploadsPage(context.Background(), nil, limit, 1); !errors.Is(err, ErrRange) {
			t.Fatal(limit, err)
		}
	}
}
