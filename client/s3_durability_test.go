// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"
)

// Visibility and durable publication are separate here: only an explicit Sync
// copies accepted writes into persisted. This catches S3 ACKs sent too early.
type fencedS3Store struct {
	mu                       sync.Mutex
	data, persisted          map[ObjectID][]byte
	accepted, legacy, fences int
	syncErr                  error
	entered, release         chan struct{}
}

func newFencedS3Store() *fencedS3Store {
	return &fencedS3Store{data: map[ObjectID][]byte{}, persisted: map[ObjectID][]byte{}}
}
func (s *fencedS3Store) Get(_ context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[id]
	if !ok {
		return nil, ObjectInfo{}, ErrNotFound
	}
	return bytes.Clone(b), ObjectInfo{Length: uint64(len(b))}, nil
}
func (s *fencedS3Store) GetRange(ctx context.Context, id ObjectID, off, n uint64) ([]byte, ObjectInfo, error) {
	b, i, e := s.Get(ctx, id)
	if e != nil {
		return nil, i, e
	}
	if off > uint64(len(b)) {
		return nil, i, ErrRange
	}
	return b[off:min(off+n, uint64(len(b)))], i, nil
}
func (s *fencedS3Store) GetRangeToMatching(ctx context.Context, id ObjectID, off, n uint64, c ReadCondition, w io.Writer) (ObjectInfo, error) {
	b, i, e := s.Get(ctx, id)
	if e != nil {
		return i, e
	}
	if c.Length != nil && *c.Length != i.Length || c.Offset > i.Length || uint64(len(c.Bytes)) > i.Length-c.Offset || !bytes.Equal(b[c.Offset:c.Offset+uint64(len(c.Bytes))], c.Bytes) {
		return i, ErrPrecondition
	}
	if off > i.Length || n > i.Length-off {
		return i, ErrRange
	}
	_, e = w.Write(b[off : off+n])
	return i, e
}
func (s *fencedS3Store) CAS(_ context.Context, id ObjectID, off uint64, before, after []byte, opt CASOptions) (ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.data[id]
	if opt.Absent && exists || !opt.Absent && !exists || opt.ExpectedLength != nil && *opt.ExpectedLength != uint64(len(old)) {
		return ObjectInfo{}, ErrPrecondition
	}
	if opt.Scope == CASObject {
		if !opt.Absent && !bytes.Equal(old, before) {
			return ObjectInfo{}, ErrPrecondition
		}
		s.data[id] = bytes.Clone(after)
	} else {
		if off > uint64(len(old)) || uint64(len(before)) > uint64(len(old))-off || !bytes.Equal(old[off:off+uint64(len(before))], before) || len(before) != len(after) {
			return ObjectInfo{}, ErrPrecondition
		}
		next := bytes.Clone(old)
		copy(next[off:], after)
		s.data[id] = next
	}
	if opt.Accepted {
		s.accepted++
	} else {
		s.legacy++
	}
	return ObjectInfo{Length: uint64(len(s.data[id]))}, nil
}
func (s *fencedS3Store) PutFrom(ctx context.Context, id ObjectID, r io.Reader, n uint64, opt PutOptions) (ObjectInfo, error) {
	b, e := io.ReadAll(io.LimitReader(r, int64(n)+1))
	if e != nil {
		return ObjectInfo{}, e
	}
	if uint64(len(b)) != n {
		return ObjectInfo{}, io.ErrUnexpectedEOF
	}
	old, _, e := s.Get(ctx, id)
	if e != nil && !errors.Is(e, ErrNotFound) {
		return ObjectInfo{}, e
	}
	return s.CAS(ctx, id, 0, old, b, CASOptions{Scope: CASObject, MutationOptions: opt})
}
func (s *fencedS3Store) Delete(_ context.Context, id ObjectID, opt DeleteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[id]
	if !ok {
		return ErrNotFound
	}
	if opt.ExpectedLength != nil && *opt.ExpectedLength != uint64(len(b)) || uint64(len(opt.Compare)) > uint64(len(b)) || !bytes.Equal(b[:len(opt.Compare)], opt.Compare) {
		return ErrPrecondition
	}
	delete(s.data, id)
	if opt.Accepted {
		s.accepted++
	} else {
		s.legacy++
	}
	return nil
}
func (s *fencedS3Store) ScanObjects(_ context.Context, after *ObjectID, limit uint32) ([]ObjectEntry, *ObjectID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []ObjectID
	for id := range s.data {
		if after == nil || bytes.Compare(id[:], after[:]) > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	var next *ObjectID
	if len(ids) > int(limit) {
		ids = ids[:limit]
		id := ids[len(ids)-1]
		next = &id
	}
	var out []ObjectEntry
	for _, id := range ids {
		out = append(out, ObjectEntry{ID: id, Info: ObjectInfo{Length: uint64(len(s.data[id]))}})
	}
	return out, next, nil
}
func (s *fencedS3Store) Sync(ctx context.Context) error {
	s.mu.Lock()
	s.fences++
	entered, release, err := s.entered, s.release, s.syncErr
	s.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persisted = map[ObjectID][]byte{}
	for id, b := range s.data {
		s.persisted[id] = bytes.Clone(b)
	}
	return nil
}
func TestS3PublicationWaitsForExplicitSync(t *testing.T) {
	store := newFencedS3Store()
	store.entered = make(chan struct{}, 1)
	store.release = make(chan struct{})
	api := NewS3(store)
	done := make(chan error, 1)
	go func() {
		_, e := api.Put(context.Background(), "held", bytes.NewBufferString("data"), 4, S3PutOptions{})
		done <- e
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("S3 did not explicitly request Sync")
	}
	select {
	case err := <-done:
		t.Fatal("S3 ACK escaped its fence", err)
	default:
	}
	store.mu.Lock()
	if store.accepted != 1 || store.legacy != 0 || len(store.persisted) != 0 {
		t.Error("publication was implicitly durable", store.accepted, store.legacy, len(store.persisted))
	}
	store.mu.Unlock()
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.fences != 1 || len(store.persisted) != 1 {
		t.Fatal("missing explicit durable publication", store.fences, len(store.persisted))
	}
}
func TestS3PublicationSyncFailurePreventsSuccess(t *testing.T) {
	store := newFencedS3Store()
	failure := errors.New("fence failed")
	store.syncErr = failure
	_, err := NewS3(store).Put(context.Background(), "failed", bytes.NewBufferString("data"), 4, S3PutOptions{})
	if !errors.Is(err, failure) || store.fences != 1 || len(store.persisted) != 0 {
		t.Fatal("S3 acknowledged failed durability", err, store.fences)
	}
}
func TestS3MultipartUsesOneExplicitSyncPerPublication(t *testing.T) {
	ctx := context.Background()
	store := newFencedS3Store()
	api := NewS3(store)
	u, err := api.CreateMultipart(ctx, "multi", "owner", "", "", S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if store.fences != 1 {
		t.Fatal("create upload", store.fences)
	}
	p, err := api.UploadPart(ctx, u.ID, 1, bytes.NewBufferString("payload"), 7, S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if store.fences != 2 {
		t.Fatal("part ACK", store.fences)
	}
	_, err = api.CompleteMultipart(ctx, u.ID, []S3CompletedPart{{Number: 1, ETag: p.ETag}}, S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if store.fences != 3 || store.legacy != 0 {
		t.Fatal("nested publication did not share explicit fence", store.fences, store.legacy)
	}
	if _, ok := store.persisted[S3MetadataID("multi")]; !ok {
		t.Fatal("published object missing from durable cut")
	}
	if _, err = api.Head(ctx, "multi"); err != nil {
		t.Fatal(err)
	}
	if store.fences != 3 {
		t.Fatal("read implicitly synced", store.fences)
	}
	if err = api.Delete(ctx, "absent"); err != nil {
		t.Fatal(err)
	}
	if store.fences != 4 {
		t.Fatal("idempotent write ACK lacked observed-state fence", store.fences)
	}
	if err = api.SetPublicRead(ctx, "multi", true); err != nil {
		t.Fatal(err)
	}
	if store.fences != 5 {
		t.Fatal("ACL ACK lacked fence", store.fences)
	}
	if err = api.Delete(ctx, "multi"); err != nil {
		t.Fatal(err)
	}
	if store.fences != 6 {
		t.Fatal("delete ACK lacked fence", store.fences)
	}
}

func TestS3MultipartSyncRetryFencesVisibleReceipt(t *testing.T) {
	ctx := context.Background()
	store := newFencedS3Store()
	api := NewS3(store)
	u, err := api.CreateMultipart(ctx, "retry", "owner", "", "", S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := api.UploadPart(ctx, u.ID, 1, bytes.NewBufferString("payload"), 7, S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("lost publication fence")
	store.syncErr = failure
	parts := []S3CompletedPart{{Number: 1, ETag: p.ETag}}
	if _, err = api.CompleteMultipart(ctx, u.ID, parts, S3PutOptions{}); !errors.Is(err, failure) {
		t.Fatal("fence failure lost", err)
	}
	if _, ok := store.persisted[S3MetadataID("retry")]; ok {
		t.Fatal("failed fence published durable object")
	}
	store.syncErr = nil
	if _, err = api.CompleteMultipart(ctx, u.ID, parts, S3PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if store.fences != 4 {
		t.Fatal("visible receipt retry skipped explicit fence", store.fences)
	}
	if _, ok := store.persisted[S3MetadataID("retry")]; !ok {
		t.Fatal("retry ACK did not persist earlier publication")
	}
}
