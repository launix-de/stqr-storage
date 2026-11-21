// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
)

func testDirectory(t *testing.T, f *Filesystem, parent ObjectID, name string) FSInfo {
	t.Helper()
	info, err := f.Create(context.Background(), parent, name, 0750, 1, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	return info
}
func TestFilesystemRmdir(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	other, err := OpenFilesystem(ctx, store, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := FilesystemRootID()
	dir := testDirectory(t, f, root, "dir")
	file, err := f.Create(ctx, dir.ID, "file", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Rmdir(ctx, root, "dir"); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal(err)
	}
	if err = f.Rmdir(ctx, dir.ID, "file"); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatal(err)
	}
	if err = f.Unlink(ctx, dir.ID, "file"); err != nil {
		t.Fatal(err)
	}
	if err = f.Rmdir(ctx, root, "dir"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Lookup(ctx, root, "dir"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	if _, err = other.Create(ctx, dir.ID, "stale", 0600, 0, 0, false); !errors.Is(err, syscall.ENOENT) {
		t.Fatal("stale directory accepted create", err)
	}
	if info, err := other.Stat(ctx, dir.ID); err != nil || !info.Removed {
		t.Fatal(info, err)
	}
	if _, err = other.Stat(ctx, file.ID); err != nil {
		t.Fatal("unlinked inode was reclaimed early", err)
	}
}

func TestFilesystemRmdirAgainstConcurrentCreate(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	other, err := OpenFilesystem(ctx, store, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 100; iteration++ {
		name := fmt.Sprintf("dir-%d", iteration)
		dir := testDirectory(t, f, FilesystemRootID(), name)
		start := make(chan struct{})
		var createErr, removeErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, createErr = other.Create(ctx, dir.ID, "file", 0600, 0, 0, false) }()
		go func() { defer wg.Done(); <-start; removeErr = f.Rmdir(ctx, FilesystemRootID(), name) }()
		close(start)
		wg.Wait()
		if createErr == nil {
			if !errors.Is(removeErr, syscall.ENOTEMPTY) {
				t.Fatalf("created file hidden: create=%v rmdir=%v", createErr, removeErr)
			}
			if _, err = f.Lookup(ctx, dir.ID, "file"); err != nil {
				t.Fatal(err)
			}
		} else if removeErr == nil {
			if !errors.Is(createErr, syscall.ENOENT) {
				t.Fatal(createErr)
			}
		} else {
			t.Fatalf("no operation completed: %v %v", createErr, removeErr)
		}
	}
}

func TestFilesystemRenameReplacementAndExchange(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestFS(t)
	root := FilesystemRootID()
	a := testDirectory(t, f, root, "a")
	b := testDirectory(t, f, root, "b")
	src, err := f.Create(ctx, a.ID, "source", 0600, 12, 34, false)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := f.Create(ctx, b.ID, "target", 0640, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []struct {
		id   ObjectID
		data string
	}{{src.ID, "source-data"}, {dst.ID, "open-old-target"}} {
		if _, err = f.WriteAt(ctx, x.id, []byte(x.data), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.RenameWithFlags(ctx, a.ID, "source", b.ID, "target", FSRenameNoReplace); !errors.Is(err, syscall.EEXIST) {
		t.Fatal(err)
	}
	if err = f.Rename(ctx, a.ID, "source", b.ID, "target"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Lookup(ctx, a.ID, "source"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	if info, err := f.Lookup(ctx, b.ID, "target"); err != nil || info.ID != src.ID || info.Mode != 0600 || info.UID != 12 {
		t.Fatal(info, err)
	}
	if got := readTestFile(t, f, dst.ID); string(got) != "open-old-target" {
		t.Fatal("replacement invalidated open target", string(got))
	}
	dir := testDirectory(t, f, a.ID, "directory")
	if _, err = f.Create(ctx, dir.ID, "nested", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if err = f.RenameWithFlags(ctx, a.ID, "directory", b.ID, "target", FSRenameExchange); err != nil {
		t.Fatal(err)
	}
	if info, err := f.Lookup(ctx, a.ID, "directory"); err != nil || info.ID != src.ID {
		t.Fatal(info, err)
	}
	if info, err := f.Lookup(ctx, b.ID, "target"); err != nil || info.ID != dir.ID {
		t.Fatal(info, err)
	}
	if err = f.RenameWithFlags(ctx, a.ID, "directory", a.ID, "missing", FSRenameExchange); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	if err = f.RenameWithFlags(ctx, a.ID, "directory", a.ID, "anything", 3); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
}

func TestFilesystemRenameDirectories(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	other, _ := OpenFilesystem(ctx, store, FSOptions{})
	root := FilesystemRootID()
	a := testDirectory(t, f, root, "a")
	b := testDirectory(t, f, root, "b")
	child := testDirectory(t, f, a.ID, "child")
	if err := f.Rename(ctx, root, "a", child.ID, "cycle"); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if err := f.RenameWithFlags(ctx, root, "a", a.ID, "child", FSRenameExchange); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if _, err := f.Create(ctx, b.ID, "content", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if err := f.Rename(ctx, root, "a", root, "b"); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal(err)
	}
	if err := f.Unlink(ctx, b.ID, "content"); err != nil {
		t.Fatal(err)
	}
	if err := f.Rename(ctx, root, "a", root, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Lookup(ctx, a.ID, "child"); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Create(ctx, b.ID, "stale", 0600, 0, 0, false); !errors.Is(err, syscall.ENOENT) {
		t.Fatal("replaced directory still writable", err)
	}
	if err := f.Rename(ctx, a.ID, "child", root, "moved"); err != nil {
		t.Fatal(err)
	}
	if info, err := f.Lookup(ctx, root, "moved"); err != nil || info.ID != child.ID {
		t.Fatal(info, err)
	}
}

func TestFilesystemConcurrentDirectoryMovesCannotFormCycle(t *testing.T) {
	ctx := context.Background()
	for iteration := 0; iteration < 40; iteration++ {
		f, store := newTestFS(t)
		other, _ := OpenFilesystem(ctx, store, FSOptions{})
		root := FilesystemRootID()
		a := testDirectory(t, f, root, "a")
		b := testDirectory(t, f, root, "b")
		start := make(chan struct{})
		var x, y error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; x = f.Rename(ctx, root, "a", b.ID, "a") }()
		go func() { defer wg.Done(); <-start; y = other.Rename(ctx, root, "b", a.ID, "b") }()
		close(start)
		wg.Wait()
		if (x == nil) == (y == nil) {
			t.Fatalf("expected exactly one move: %v %v", x, y)
		}
		for _, e := range []error{x, y} {
			if e != nil && !errors.Is(e, syscall.EINVAL) && !errors.Is(e, syscall.ENOENT) {
				t.Fatal(e)
			}
		}
		if err := f.recoverNamespace(ctx); err != nil {
			t.Fatal(err)
		}
		entries, err := f.ReadDir(ctx, root)
		if err != nil || len(entries) != 1 {
			t.Fatal(entries, err)
		}
		entries, err = f.ReadDir(ctx, entries[0].ID)
		if err != nil || len(entries) != 1 {
			t.Fatal(entries, err)
		}
	}
}

type fsCrashStore struct {
	FilesystemStore
	at, calls int
	after     bool
}

func (s *fsCrashStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	s.calls++
	if s.calls == s.at && !s.after {
		return ObjectInfo{}, context.Canceled
	}
	i, err := s.FilesystemStore.CAS(ctx, id, off, expected, replacement, opt)
	if s.calls == s.at && s.after && err == nil {
		return ObjectInfo{}, context.Canceled
	}
	return i, err
}

func TestFilesystemNamespaceCrashRecovery(t *testing.T) {
	for _, operation := range []string{"rmdir", "rename", "replace-directory"} {
		for _, after := range []bool{false, true} {
			for at := 1; at <= 12; at++ {
				t.Run(fmt.Sprintf("%s/after-%t/cas-%d", operation, after, at), func(t *testing.T) {
					ctx := context.Background()
					f, store := newTestFS(t)
					root := FilesystemRootID()
					a := testDirectory(t, f, root, "a")
					b := testDirectory(t, f, root, "b")
					file, err := f.Create(ctx, a.ID, "file", 0600, 0, 0, false)
					if err != nil {
						t.Fatal(err)
					}
					if operation == "replace-directory" {
						if err = f.Unlink(ctx, a.ID, "file"); err != nil {
							t.Fatal(err)
						}
					}
					crash := &fsCrashStore{FilesystemStore: store, at: at, after: after}
					f.store = crash
					switch operation {
					case "rmdir":
						err = f.Rmdir(ctx, root, "b")
					case "rename":
						err = f.Rename(ctx, a.ID, "file", b.ID, "file")
					case "replace-directory":
						err = f.Rename(ctx, root, "b", root, "a")
					}
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					recovered, e := OpenFilesystem(ctx, store, FSOptions{})
					if e != nil {
						t.Fatal(e)
					}
					if e = recovered.recoverNamespace(ctx); e != nil {
						t.Fatal(e)
					}
					if operation == "rmdir" {
						_, existsErr := recovered.Lookup(ctx, root, "b")
						info, e := recovered.Stat(ctx, b.ID)
						if e != nil {
							t.Fatal(e)
						}
						if info.Removed != errors.Is(existsErr, syscall.ENOENT) {
							t.Fatal("partially applied rmdir", info, existsErr)
						}
					} else if operation == "replace-directory" {
						left, le := recovered.Lookup(ctx, root, "a")
						right, re := recovered.Lookup(ctx, root, "b")
						if le != nil {
							t.Fatal(le)
						}
						if left.ID == a.ID {
							if re != nil || right.ID != b.ID {
								t.Fatal("partial replacement", right, re)
							}
						} else if left.ID != b.ID || !errors.Is(re, syscall.ENOENT) {
							t.Fatal("partial replacement", left, right, re)
						}
						old, e := recovered.Stat(ctx, a.ID)
						if e != nil || old.Removed != (left.ID == b.ID) {
							t.Fatal(old, e)
						}
					} else {
						left, le := recovered.Lookup(ctx, a.ID, "file")
						right, re := recovered.Lookup(ctx, b.ID, "file")
						if le == nil && re == nil || le != nil && re != nil {
							t.Fatalf("non-atomic move: %v %v", le, re)
						}
						if le == nil && left.ID != file.ID || re == nil && right.ID != file.ID {
							t.Fatal("wrong inode")
						}
					}
					for _, o := range store.objects {
						if bytes.HasPrefix(o.data, []byte("STFL")) {
							t.Fatal("reachable markers were not recovered")
						}
					}
				})
			}
		}
	}
}
