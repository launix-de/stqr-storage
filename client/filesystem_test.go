// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

func newTestFS(t *testing.T) (*Filesystem, *MemoryFilesystemStore) {
	t.Helper()
	store := NewMemoryFilesystemStore()
	f, err := OpenFilesystem(context.Background(), store, FSOptions{Create: true, UID: 42, GID: 43})
	if err != nil {
		t.Fatal(err)
	}
	return f, store
}
func createTestFile(t *testing.T, f *Filesystem, name string) FSInfo {
	t.Helper()
	info, err := f.Create(context.Background(), FilesystemRootID(), name, 0640, 42, 43, false)
	if err != nil {
		t.Fatal(err)
	}
	return info
}
func readTestFile(t *testing.T, f *Filesystem, id ObjectID) []byte {
	t.Helper()
	info, err := f.Stat(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, int(info.Size))
	n, err := f.ReadAt(context.Background(), id, data, 0)
	if err != nil || n != len(data) {
		t.Fatalf("read: %d/%d %v", n, len(data), err)
	}
	return data
}
func TestFilesystemTreeAndReopen(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	dir, err := f.Create(ctx, FilesystemRootID(), "folder", 0750, 42, 43, true)
	if err != nil {
		t.Fatal(err)
	}
	file, err := f.Create(ctx, dir.ID, "payload", 0640, 42, 43, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, file.ID, []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFilesystem(ctx, store, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Lookup(ctx, dir.ID, "payload")
	if err != nil || got.ID != file.ID || got.Size != 5 {
		t.Fatalf("lookup: %+v %v", got, err)
	}
	if string(readTestFile(t, reopened, file.ID)) != "hello" {
		t.Fatal("payload lost")
	}
	if err = f.Rename(ctx, dir.ID, "payload", dir.ID, "renamed"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Lookup(ctx, dir.ID, "payload"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	if err = f.Unlink(ctx, dir.ID, "renamed"); err != nil {
		t.Fatal(err)
	}
	if string(readTestFile(t, reopened, file.ID)) != "hello" {
		t.Fatal("unlink invalidated open inode")
	}
	if err = f.Rename(ctx, FilesystemRootID(), "folder", dir.ID, "bad"); !errors.Is(err, syscall.EXDEV) {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "a/b", "a\x00b"} {
		if _, err = f.Create(ctx, dir.ID, name, 0600, 0, 0, false); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("name %q: %v", name, err)
		}
	}
}
func TestFilesystemLargeSparseAndTruncate(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	file := createTestFile(t, f, "large")
	data := bytes.Repeat([]byte("abcdefgh"), 3*FSObjectSize/8+5)
	if _, err := f.WriteAt(ctx, file.ID, data, 0); err != nil {
		t.Fatal(err)
	}
	patch := []byte("boundary-overwrite")
	offset := int64(FSObjectSize - 5)
	if _, err := f.WriteAt(ctx, file.ID, patch, offset); err != nil {
		t.Fatal(err)
	}
	copy(data[int(offset):], patch)
	if !bytes.Equal(readTestFile(t, f, file.ID), data) {
		t.Fatal("cross-segment overwrite changed other bytes")
	}
	if _, err := f.Truncate(ctx, file.ID, FSObjectSize+2); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Truncate(ctx, file.ID, FSObjectSize+30); err != nil {
		t.Fatal(err)
	}
	expected := append(append([]byte(nil), data[:FSObjectSize+2]...), make([]byte, 28)...)
	if !bytes.Equal(readTestFile(t, f, file.ID), expected) {
		t.Fatal("truncated bytes resurrected")
	}
	sparseOffset := int64(1) << 40
	before := len(store.objects)
	if _, err := f.WriteAt(ctx, file.ID, []byte("tail"), sparseOffset); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 12)
	n, err := f.ReadAt(ctx, file.ID, buf, sparseOffset-8)
	if err != nil || n != 12 || !bytes.Equal(buf, append(make([]byte, 8), []byte("tail")...)) {
		t.Fatalf("sparse tail %q %d %v", buf, n, err)
	}
	if len(store.objects)-before > 8 {
		t.Fatal("sparse write materialized its hole")
	}
	if _, err = f.Truncate(ctx, file.ID, 3); err != nil {
		t.Fatal(err)
	}
	if string(readTestFile(t, f, file.ID)) != "abc" {
		t.Fatal("shrink to inline failed")
	}
	n, err = f.ReadAt(ctx, file.ID, make([]byte, 4), 3)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("EOF %d %v", n, err)
	}
	for _, o := range store.objects {
		if len(o.data) > FSObjectSize {
			t.Fatalf("oversized object %d", len(o.data))
		}
	}
}
func TestFilesystemLargeDirectory(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	entries := make([]FSEntry, 22000)
	// Seed initialized empty inodes in bulk so this checks directory segmentation
	// without paying for 22,000 successive full directory edits.
	body := encodeFSNode(fsNode{kind: fsFile, mode: 0600})
	for i := range entries {
		id, err := f.putImmutable(ctx, body)
		if err != nil {
			t.Fatal(err)
		}
		entries[i] = FSEntry{Name: fmt.Sprintf("entry-%016d", i), ID: id}
	}
	if err := f.editDirectory(ctx, FilesystemRootID(), func([]FSEntry) ([]FSEntry, error) { return entries, nil }); err != nil {
		t.Fatal(err)
	}
	root, _, err := f.load(ctx, FilesystemRootID())
	if err != nil {
		t.Fatal(err)
	}
	if root.blob.length <= FSObjectSize || root.blob.root == (ObjectID{}) {
		t.Fatal("directory not segmented")
	}
	reopened, err := OpenFilesystem(ctx, store, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.ReadDir(ctx, FilesystemRootID())
	if err != nil || len(got) != len(entries) {
		t.Fatalf("readdir %d %v", len(got), err)
	}
	for _, i := range []int{0, len(entries) / 2, len(entries) - 1} {
		if got[i] != entries[i] {
			t.Fatalf("entry %d mismatch", i)
		}
	}
	if err = f.Rename(ctx, FilesystemRootID(), entries[100].Name, FilesystemRootID(), "a"); err != nil {
		t.Fatal(err)
	}
	if info, err := f.Lookup(ctx, FilesystemRootID(), "a"); err != nil || info.ID != entries[100].ID {
		t.Fatal(info, err)
	}
	for _, o := range store.objects {
		if len(o.data) > FSObjectSize {
			t.Fatal("directory exceeded per-object bound")
		}
	}
}
func TestFilesystemNestedSegmentLists(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestFS(t)
	file := createTestFile(t, f, "deep")
	leaf := bytes.Repeat([]byte("x"), 4096)
	id, err := f.putImmutable(ctx, leaf)
	if err != nil {
		t.Fatal(err)
	}
	segments := make([]fsSegment, fsFanout+1)
	for i := range segments {
		segments[i] = fsSegment{length: 4096, id: id}
	}
	b, err := f.buildBlob(ctx, segments, uint64(len(segments))*4096)
	if err != nil {
		t.Fatal(err)
	}
	if b.depth != 2 {
		t.Fatalf("depth %d", b.depth)
	}
	if _, err = f.mutate(ctx, file.ID, func(n *fsNode) error { n.blob = b; return nil }); err != nil {
		t.Fatal(err)
	}
	boundary := int64(fsFanout*4096 - 8)
	patch := []byte("0123456789abcdef")
	if _, err = f.WriteAt(ctx, file.ID, patch, boundary); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(patch)+2)
	if _, err = f.ReadAt(ctx, file.ID, got, boundary-1); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append(append([]byte{'x'}, patch...), 'x')) {
		t.Fatal(string(got))
	}
}
func TestFilesystemConcurrentMounts(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	other, err := OpenFilesystem(ctx, store, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	file := createTestFile(t, f, "parallel")
	const writers = 16
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fs := f
			if i%2 != 0 {
				fs = other
			}
			_, err := fs.Create(ctx, FilesystemRootID(), fmt.Sprintf("file-%02d", i), 0600, 0, 0, false)
			if err == nil {
				_, err = fs.WriteAt(ctx, file.ID, []byte{byte(i + 1)}, int64(i*2))
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := f.ReadDir(ctx, FilesystemRootID())
	if err != nil || len(entries) != writers+1 {
		t.Fatalf("lost directory updates %d %v", len(entries), err)
	}
	written := readTestFile(t, f, file.ID)
	expected := make([]byte, writers*2-1)
	for i := 0; i < writers; i++ {
		expected[i*2] = byte(i + 1)
	}
	if !bytes.Equal(written, expected) {
		t.Fatal("lost disjoint writes")
	}
	// Append offsets must also be chosen inside each CAS attempt.
	log := createTestFile(t, f, "log")
	errs = make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fs := f
			if i%2 != 0 {
				fs = other
			}
			_, err := fs.Append(ctx, log.ID, bytes.Repeat([]byte{byte(i)}, 32))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got := readTestFile(t, f, log.ID)
	if len(got) != writers*32 {
		t.Fatal("lost append")
	}
	seen := make(map[byte]bool)
	for start := 0; start < len(got); start += 32 {
		part := got[start : start+32]
		if !bytes.Equal(part, bytes.Repeat(part[:1], 32)) || seen[part[0]] {
			t.Fatal("interleaved or duplicated append")
		}
		seen[part[0]] = true
	}
}
func TestFilesystemRandomWritesAgainstByteSlice(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestFS(t)
	file := createTestFile(t, f, "model")
	rng := rand.New(rand.NewSource(9))
	var model []byte
	for step := 0; step < 200; step++ {
		if rng.Intn(4) == 0 {
			size := rng.Intn(10000)
			if _, err := f.Truncate(ctx, file.ID, uint64(size)); err != nil {
				t.Fatal(err)
			}
			if size < len(model) {
				model = model[:size]
			} else {
				model = append(model, make([]byte, size-len(model))...)
			}
		} else {
			offset := rng.Intn(10000)
			payload := make([]byte, rng.Intn(500)+1)
			rng.Read(payload)
			end := offset + len(payload)
			if end > len(model) {
				model = append(model, make([]byte, end-len(model))...)
			}
			copy(model[offset:], payload)
			if _, err := f.WriteAt(ctx, file.ID, payload, int64(offset)); err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(readTestFile(t, f, file.ID), model) {
			t.Fatalf("model mismatch at step %d", step)
		}
	}
}

type failFSStore struct {
	FilesystemStore
	id   ObjectID
	fail atomic.Bool
}

func (s *failFSStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	if id == s.id && s.fail.Load() {
		return ObjectInfo{}, ErrFull
	}
	return s.FilesystemStore.CAS(ctx, id, off, expected, replacement, opt)
}
func TestFilesystemFailedPublishKeepsOldSnapshot(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	file := createTestFile(t, f, "atomic")
	original := bytes.Repeat([]byte("old"), 4000)
	if _, err := f.WriteAt(ctx, file.ID, original, 0); err != nil {
		t.Fatal(err)
	}
	failing := &failFSStore{FilesystemStore: store, id: file.ID}
	failing.fail.Store(true)
	broken, err := OpenFilesystem(ctx, failing, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = broken.WriteAt(ctx, file.ID, bytes.Repeat([]byte("new"), 500000), 0); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}
	if !bytes.Equal(readTestFile(t, f, file.ID), original) {
		t.Fatal("failed CAS exposed a partial version")
	}
	snapshot, _, err := f.load(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, file.ID, []byte("changed"), 0); err != nil {
		t.Fatal(err)
	}
	old := make([]byte, len(original))
	if _, err = f.readBlob(ctx, snapshot.blob, old, 0); err != nil || !bytes.Equal(old, original) {
		t.Fatal("new version corrupted open snapshot", err)
	}
}
func TestFilesystemMalformedAndCanceled(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryFilesystemStore()
	_, err := store.CAS(ctx, FilesystemRootID(), 0, nil, []byte("invalid"), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenFilesystem(ctx, store, FSOptions{Create: true}); !errors.Is(err, ErrFSFormat) {
		t.Fatal(err)
	}
	f, _ := newTestFS(t)
	file := createTestFile(t, f, "cancel")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = f.WriteAt(canceled, file.ID, []byte("no"), 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(readTestFile(t, f, file.ID)) != 0 {
		t.Fatal("canceled write published")
	}
}
func FuzzFilesystemDecoders(f *testing.F) {
	f.Add(encodeFSNode(fsNode{kind: fsFile, mode: 0644}))
	dir, _ := encodeFSDirectory([]FSEntry{{Name: "test", ID: ObjectID{1}}})
	f.Add(dir)
	f.Add(encodeFSIndex([]fsSegment{{length: 4, id: ObjectID{1}}}))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > FSObjectSize {
			return
		}
		decodeFSNode(data)
		decodeFSDirectory(data)
		decodeFSIndex(data, 4096, 1)
	})
}
func ExampleOpenFilesystem() {
	ctx := context.Background()
	store := NewMemoryFilesystemStore()
	filesystem, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		panic(err)
	}
	file, err := filesystem.Create(ctx, FilesystemRootID(), "hello.txt", 0644, 0, 0, false)
	if err != nil {
		panic(err)
	}
	if _, err = filesystem.WriteAt(ctx, file.ID, []byte("hello"), 0); err != nil {
		panic(err)
	}
	data := make([]byte, 5)
	if _, err = filesystem.ReadAt(ctx, file.ID, data, 0); err != nil {
		panic(err)
	}
	fmt.Println(string(data))
	// Output: hello
}

func TestFilesystemDirectoryBudget(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	constrained, err := OpenFilesystem(ctx, store, FSOptions{DirectoryMemoryLimit: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = constrained.Create(ctx, FilesystemRootID(), "first", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err = constrained.Create(ctx, FilesystemRootID(), "second", 0600, 0, 0, false); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("directory budget not enforced: %v", err)
	}
	if _, err = f.Lookup(ctx, FilesystemRootID(), "second"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal("failed directory publication changed root", err)
	}
	if _, err = f.Create(ctx, FilesystemRootID(), "second", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err = constrained.ReadDir(ctx, FilesystemRootID()); !errors.Is(err, syscall.ENOMEM) {
		t.Fatal("oversized directory materialized", err)
	}
}
