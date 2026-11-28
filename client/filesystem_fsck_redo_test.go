// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// Model the actual offline server contract: PUT and PUT_RANGE are rejected;
// atomic CAS and DELETE remain available after exclusive maintenance admission.
type restrictedFsckStore struct {
	*fsckMemory
	put, ranged int
	applied     map[ObjectID][][]byte
	requests    map[ObjectID][]CASOptions
}

func (s *restrictedFsckStore) Put(context.Context, ObjectID, []byte, PutOptions) (ObjectInfo, error) {
	s.put++
	return ObjectInfo{}, &StatusError{Status: 409, Message: "restricted maintenance permits only CAS and DELETE"}
}
func (s *restrictedFsckStore) PutRange(context.Context, ObjectID, uint64, []byte, PutOptions) (ObjectInfo, error) {
	s.ranged++
	return ObjectInfo{}, &StatusError{Status: 409, Message: "restricted maintenance permits only CAS and DELETE"}
}
func (s *restrictedFsckStore) CAS(ctx context.Context, id ObjectID, off uint64, before, next []byte, opt CASOptions) (ObjectInfo, error) {
	info, err := s.fsckMemory.CAS(ctx, id, off, before, next, opt)
	if err == nil {
		if s.requests == nil {
			s.requests = map[ObjectID][]CASOptions{}
		}
		s.requests[id] = append(s.requests[id], opt)
		s.applied[id] = append(s.applied[id], bytes.Clone(next))
	}
	return info, err
}
func TestFilesystemFsckRecoversNativeRedoWithRestrictedMaintenance(t *testing.T) {
	ctx := context.Background()
	base := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	if _, err := OpenFilesystem(ctx, base, FSOptions{Create: true}); err != nil {
		t.Fatal(err)
	}
	root := FilesystemRootID()
	raw, _, err := base.Get(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	first, second, third := ObjectID{40}, ObjectID{41}, ObjectID{42}
	for id, data := range map[ObjectID][]byte{first: []byte("abcdefgh"), second: []byte("old contents")} {
		if _, err = base.CAS(ctx, id, 0, nil, data, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true}}); err != nil {
			t.Fatal(err)
		}
	}
	txID := ObjectID{90}
	tx := fsTransaction{state: fsTxCommitted, members: []fsTxMember{{id: root, before: bytes.Clone(raw), after: bytes.Clone(raw), writes: []fsBlockWrite{
		{id: first, offset: 2, data: []byte("XY")},
		{id: second, full: true, data: []byte("new")},
		{id: third, absent: true, offset: 3, data: []byte("z")},
	}}}}
	if _, err = base.CAS(ctx, txID, 0, nil, encodeFSTransaction(tx), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err = base.CAS(ctx, root, 0, raw, fsLock(txID), CASOptions{Scope: CASObject}); err != nil {
		t.Fatal(err)
	}
	s := &restrictedFsckStore{fsckMemory: base, applied: map[ObjectID][][]byte{}}
	if _, err = checkFilesystem(ctx, s); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[ObjectID][]byte{first: []byte("abXYefgh"), second: []byte("new"), third: {0, 0, 0, 'z'}} {
		got := s.applied[id]
		if len(got) != 1 || !bytes.Equal(got[0], want) {
			t.Fatalf("REDO %s: %q, want %q", id, got, want)
		}
	}
	got, _, err := base.Get(ctx, root)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("root recovery: %x %v", got, err)
	}
	if s.put != 0 || s.ranged != 0 {
		t.Fatalf("forbidden native mutations: PUT%d RANGE%d", s.put, s.ranged)
	}
}
func TestFilesystemOfflineRedoMissingAndAlreadyApplied(t *testing.T) {
	ctx := context.WithValue(context.Background(), fsOfflineRecoveryKey{}, true)
	s := &restrictedFsckStore{fsckMemory: &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}, applied: map[ObjectID][][]byte{}}
	f := &Filesystem{store: s}
	tx := ObjectID{91}
	id := ObjectID{92}
	w := fsBlockWrite{id: id, offset: 3, data: []byte("z")}
	if err := f.applyFSWrite(ctx, tx, 0, w); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing existing block silently recreated: %v", err)
	}
	w.absent = true
	if err := f.applyFSWrite(ctx, tx, 0, w); err != nil {
		t.Fatal(err)
	}
	if err := f.applyFSWrite(ctx, tx, 0, w); err != nil {
		t.Fatal(err)
	}
	empty := ObjectID{93}
	if err := f.applyFSWrite(ctx, tx, 1, fsBlockWrite{id: empty, absent: true, full: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(ctx, empty); err != nil {
		t.Fatalf("empty object was not created: %v", err)
	}
	if len(s.applied[id]) != 1 {
		t.Fatal("already applied REDO was submitted again")
	}
	for object, index := range map[ObjectID]uint64{id: 0, empty: 1} {
		options := s.requests[object]
		if len(options) != 1 || options[0].Scope != CASObject || !options[0].Absent || !options[0].Accepted || options[0].RequestID == (RequestID{}) || options[0].RequestID == fsRedoRequest(tx, index) {
			t.Fatalf("offline CAS did not use separate accepted whole-object creation: %s %+v", object, options)
		}
	}
}

func TestFilesystemOfflineRedoGrowthPreservesOnlineFastPaths(t *testing.T) {
	ctx := context.Background()
	s := &restrictedFsckStore{fsckMemory: &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}, applied: map[ObjectID][][]byte{}}
	f := &Filesystem{store: s}
	id, tx := ObjectID{94}, ObjectID{95}
	if _, err := s.fsckMemory.CAS(ctx, id, 0, nil, []byte("ab"), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true}}); err != nil {
		t.Fatal(err)
	}
	for index, write := range []fsBlockWrite{{id: id, full: true, data: []byte("next")}, {id: id, offset: 5, data: []byte("z")}} {
		err := f.applyFSWrite(ctx, tx, uint64(index), write)
		var status *StatusError
		if !errors.As(err, &status) || status.Status != 409 {
			t.Fatalf("ordinary online REDO stopped using native PUT path: %v", err)
		}
	}
	if s.put != 1 || s.ranged != 1 {
		t.Fatalf("online fast paths: PUT%d RANGE%d", s.put, s.ranged)
	}
	ctx = context.WithValue(ctx, fsOfflineRecoveryKey{}, true)
	write := fsBlockWrite{id: id, offset: 5, data: []byte("z")}
	for i := 0; i < 2; i++ {
		if err := f.applyFSWrite(ctx, tx, 2, write); err != nil {
			t.Fatal(err)
		}
	}
	got, _, err := s.Get(ctx, id)
	if err != nil || !bytes.Equal(got, []byte{'a', 'b', 0, 0, 0, 'z'}) || len(s.applied[id]) != 1 {
		t.Fatalf("existing-block growth or replay changed data: %q writes=%d err=%v", got, len(s.applied[id]), err)
	}
	opt := s.requests[id][0]
	if opt.Absent || opt.Scope != CASObject || !opt.Accepted || opt.RequestID == fsRedoRequest(tx, 2) {
		t.Fatalf("existing block CAS options: %+v", opt)
	}
}
