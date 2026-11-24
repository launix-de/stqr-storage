// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"syscall"
	"testing"
)

type fsckMemory struct {
	*MemoryFilesystemStore
	failAfter int
	deletes   int
}

func (m *fsckMemory) scanFilesystemObjects(ctx context.Context, after *ObjectID, limit uint32) ([]fsCheckObject, *ObjectID, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := []ObjectID{}
	for id := range m.objects {
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
	page := []fsCheckObject{}
	for _, id := range ids {
		o := m.objects[id]
		page = append(page, fsCheckObject{id, ObjectInfo{Length: uint64(len(o.data))}})
	}
	return page, next, nil
}
func (m *fsckMemory) Delete(ctx context.Context, id ObjectID, opt DeleteOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.failAfter > 0 && m.deletes == m.failAfter {
		return context.Canceled
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[id]
	if !ok {
		return ErrNotFound
	}
	delete(m.objects, id)
	m.deletes++
	return nil
}

func TestFilesystemFsckCollectsOrphansAndPreservesSharedSparseData(t *testing.T) {
	ctx := context.Background()
	store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	root := FilesystemRootID()
	file, err := f.Create(ctx, root, "kept", 0600, 5, 7, false)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("segment"), 20000)
	if _, err = f.WriteAt(ctx, file.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	// Distinct files own distinct mutable blocks. A normal copy writes the
	// content once; subsequent edits must not alter the other file.
	alias, err := f.Create(ctx, root, "shared", 0644, 5, 7, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, alias.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, file.ID, []byte("NEW"), 12); err != nil {
		t.Fatal(err)
	}
	old := append([]byte(nil), want...)
	copy(want[12:], "NEW")
	sparse, err := f.Create(ctx, root, "sparse", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, sparse.ID, []byte("tail"), 1<<40); err != nil {
		t.Fatal(err)
	}
	removed, err := f.Create(ctx, root, "removed", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, removed.ID, bytes.Repeat([]byte("garbage"), 200000), 0); err != nil {
		t.Fatal(err)
	}
	if err = f.Unlink(ctx, root, "removed"); err != nil {
		t.Fatal(err)
	}
	// Failed namespace publication leaves another inode to collect.
	if _, err = f.Create(ctx, root, "kept", 0600, 0, 0, false); !errors.Is(err, syscall.EEXIST) {
		t.Fatal(err)
	}
	// Force paging and include the zero object address in the inventory.
	for i := 0; i < 520; i++ {
		var id ObjectID
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		if _, err = store.CAS(ctx, id, 0, nil, []byte("orphan"), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true}}); err != nil {
			t.Fatal(err)
		}
	}
	before := len(store.objects)
	result, err := checkFilesystem(ctx, store)
	if err != nil || result.Scanned != uint64(before) || result.Deleted < 523 || result.DeletedBytes < 1400000 {
		t.Fatalf("%+v %v", result, err)
	}
	if result.Reachable+result.Deleted != result.Scanned || uint64(len(store.objects)) != result.Reachable {
		t.Fatal(result)
	}
	for _, test := range []struct {
		id   ObjectID
		want []byte
	}{{file.ID, want}, {alias.ID, old}} {
		got := make([]byte, len(test.want))
		if _, err = f.ReadAt(ctx, test.id, got, 0); err != nil || !bytes.Equal(got, test.want) {
			t.Fatalf("live data damaged: %v", err)
		}
	}
	got := make([]byte, 8)
	if _, err = f.ReadAt(ctx, sparse.ID, got, (1<<40)-4); err != nil || !bytes.Equal(got, []byte{0, 0, 0, 0, 't', 'a', 'i', 'l'}) {
		t.Fatalf("sparse data damaged: %q %v", got, err)
	}
	info, err := f.Stat(ctx, file.ID)
	if err != nil || info.Mode != 0600 || info.UID != 5 || info.GID != 7 {
		t.Fatal(info, err)
	}
	result, err = checkFilesystem(ctx, store)
	if err != nil || result.Deleted != 0 {
		t.Fatal(result, err)
	}
}

func TestFilesystemFsckNeverSweepsBrokenGraphs(t *testing.T) {
	for _, damage := range []string{"missing-root", "bad-root", "missing-inode", "cycle", "missing-segment"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}
			f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			file, err := f.Create(ctx, FilesystemRootID(), "file", 0600, 0, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.WriteAt(ctx, file.ID, bytes.Repeat([]byte("x"), 8192), 0); err != nil {
				t.Fatal(err)
			}
			if _, err = f.putImmutable(ctx, []byte("orphan")); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing-root":
				delete(store.objects, FilesystemRootID())
			case "bad-root":
				o := store.objects[FilesystemRootID()]
				o.data = []byte("broken")
				store.objects[FilesystemRootID()] = o
			case "missing-inode":
				delete(store.objects, file.ID)
			case "cycle":
				err = f.editDirectory(ctx, FilesystemRootID(), func(entries []FSEntry) ([]FSEntry, error) {
					return append(entries, FSEntry{Name: "cycle", ID: FilesystemRootID()}), nil
				})
				if err != nil {
					t.Fatal(err)
				}
			case "missing-segment":
				n, _, _ := f.load(ctx, file.ID)
				delete(store.objects, n.blob.root)
			}
			before := len(store.objects)
			result, err := checkFilesystem(ctx, store)
			if err == nil || result.Deleted != 0 || len(store.objects) != before {
				t.Fatalf("swept damaged graph: %+v %v", result, err)
			}
		})
	}
}

func TestFilesystemFsckResumesPartialSweep(t *testing.T) {
	ctx := context.Background()
	store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore(), failAfter: 2}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err = f.putImmutable(ctx, []byte("garbage")); err != nil {
			t.Fatal(err)
		}
	}
	result, err := checkFilesystem(ctx, store)
	if !errors.Is(err, context.Canceled) || result.Deleted != 2 {
		t.Fatal(result, err)
	}
	store.failAfter = 0
	result, err = checkFilesystem(ctx, store)
	if err != nil || result.Deleted != 3 || result.Reachable != 1 {
		t.Fatal(result, err)
	}
}
