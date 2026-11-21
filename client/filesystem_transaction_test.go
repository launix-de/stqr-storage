// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
)

func pendingTestTransaction(t *testing.T, f *Filesystem) (ObjectID, []fsTxMember) {
	t.Helper()
	ctx := context.Background()
	root := FilesystemRootID()
	s, err := f.snapshot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	next := s.node
	next.mode = 0711
	members := []fsTxMember{fsMember(s, next)}
	id, err := f.putImmutable(ctx, encodeFSTransaction(fsTransaction{members: members}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.CAS(ctx, root, 0, nil, fsLock(id), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), ExpectedVersion: &s.info.Version}}); err != nil {
		t.Fatal(err)
	}
	return id, members
}

func TestFilesystemTransactionConcurrentHelpers(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	_, _ = pendingTestTransaction(t, f)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			other, err := OpenFilesystem(ctx, store, FSOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			info, err := other.Stat(ctx, FilesystemRootID())
			if err != nil || info.Mode != 0711 {
				t.Error(info, err)
			}
		}()
	}
	wg.Wait()
	if err := f.recoverNamespace(ctx); err != nil {
		t.Fatal(err)
	}
}

type fsReadOnlyStore struct{ FilesystemStore }

func (s fsReadOnlyStore) CAS(context.Context, ObjectID, uint64, []byte, []byte, CASOptions) (ObjectInfo, error) {
	return ObjectInfo{}, ErrUnauthorized
}
func TestFilesystemReadOnlyClientCanReadPendingIntent(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	root := FilesystemRootID()
	_, _ = pendingTestTransaction(t, f)
	reader, err := OpenFilesystem(ctx, fsReadOnlyStore{store}, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	info, err := reader.Stat(ctx, root)
	if err != nil || info.Mode != 0755 {
		t.Fatal(info, err)
	}
	mode := uint32(0777)
	if _, err = reader.SetAttributes(ctx, root, FSAttributes{Mode: &mode}); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("read-only writer bypassed marker", err)
	}
	before, _, _ := store.Get(ctx, root)
	if !bytes.HasPrefix(before, []byte("STFL")) {
		t.Fatal("read-only client changed marker")
	}
	if _, err = f.Stat(ctx, root); err != nil {
		t.Fatal(err)
	}
	info, err = reader.Stat(ctx, root)
	if err != nil || info.Mode != 0711 {
		t.Fatal(info, err)
	}
}

func TestFilesystemFsckRecoversIntentBeforeInventory(t *testing.T) {
	ctx := context.Background()
	store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = pendingTestTransaction(t, f)
	result, err := checkFilesystem(ctx, store)
	if err != nil || result.Reachable != 1 || result.Deleted != 1 {
		t.Fatal(result, err)
	}
	info, err := f.Stat(ctx, FilesystemRootID())
	if err != nil || info.Mode != 0711 {
		t.Fatal(info, err)
	}
}

func TestFilesystemTransactionAbortPreservesConcurrentChanges(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	root := FilesystemRootID()
	dir := testDirectory(t, f, root, "dir")
	a, err := f.snapshot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.snapshot(ctx, dir.ID)
	if err != nil {
		t.Fatal(err)
	}
	na, nb := a.node, b.node
	na.mode = 0711
	nb.mode = 0700
	members := []fsTxMember{fsMember(a, na), fsMember(b, nb)}
	sort.Slice(members, func(i, j int) bool { return bytes.Compare(members[i].id[:], members[j].id[:]) < 0 })
	// First participant locked, second participant already changed by a writer.
	id, err := f.putImmutable(ctx, encodeFSTransaction(fsTransaction{members: members}))
	if err != nil {
		t.Fatal(err)
	}
	m := members[0]
	if _, err = store.CAS(ctx, m.id, 0, nil, fsLock(id), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{ExpectedVersion: &m.version}}); err != nil {
		t.Fatal(err)
	}
	m = members[1]
	changed, err := decodeFSNode(m.before)
	if err != nil {
		t.Fatal(err)
	}
	changed.mode = 0770
	if _, err = store.CAS(ctx, m.id, 0, nil, encodeFSNode(changed), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{ExpectedVersion: &m.version}}); err != nil {
		t.Fatal(err)
	}
	committed, err := f.helpTransaction(ctx, id, 0)
	if err != nil || committed {
		t.Fatal(committed, err)
	}
	first, err := f.Stat(ctx, members[0].id)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := decodeFSNode(members[0].before)
	second, err := f.Stat(ctx, members[1].id)
	if err != nil || first.Mode != old.mode || second.Mode != 0770 {
		t.Fatal(first, second, err)
	}
}
