// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package client

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// Opt in on a host with /dev/fuse and fusermount. Unlike the callback tests,
// this exercises kernel path lookup, file descriptors and syscall dispatch.
func TestFilesystemKernelMount(t *testing.T) {
	if os.Getenv("STQR_TEST_FUSE") != "1" {
		t.Skip("set STQR_TEST_FUSE=1 to exercise a real kernel mount")
	}
	filesystem, err := OpenFilesystem(context.Background(), NewMemoryFilesystemStore(), FSOptions{Create: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	if err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := filesystem.Mount(mountpoint, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mounted.Unmount(); err != nil {
			t.Error(err)
			return
		}
		mounted.Wait()
	})
	path := filepath.Join(mountpoint, "hello")
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fh.Close() })
	if _, err = fh.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err = fh.Sync(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "hello" {
		t.Fatalf("read: %q, %v", got, err)
	}
	renamed := filepath.Join(mountpoint, "renamed")
	if err = os.Rename(path, renamed); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(renamed); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err = fh.ReadAt(buf, 0); err != nil || string(buf) != "hello" {
		t.Fatalf("unlinked descriptor: %q, %v", buf, err)
	}
	if _, err = os.Stat(renamed); !os.IsNotExist(err) {
		t.Fatalf("removed name still accessible: %v", err)
	}
	a, b := filepath.Join(mountpoint, "a"), filepath.Join(mountpoint, "b")
	for _, dir := range []string{a, b} {
		if err = os.Mkdir(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	source, target := filepath.Join(a, "new"), filepath.Join(b, "existing")
	if err = os.WriteFile(source, []byte("new-data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(target, []byte("old-data"), 0600); err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err = unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE); err != unix.EEXIST {
		t.Fatal("NOREPLACE", err)
	}
	if err = os.Rename(source, target); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "new-data" {
		t.Fatal(string(got), err)
	}
	buf = make([]byte, 8)
	if _, err = old.ReadAt(buf, 0); err != nil || string(buf) != "old-data" {
		t.Fatal("open replaced file", string(buf), err)
	}
	if err = os.WriteFile(source, []byte("exchange"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_EXCHANGE); err != nil {
		t.Fatal("EXCHANGE", err)
	}
	if got, err := os.ReadFile(source); err != nil || string(got) != "new-data" {
		t.Fatal(string(got), err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "exchange" {
		t.Fatal(string(got), err)
	}
	moved := filepath.Join(a, "moved")
	if err = os.Rename(b, moved); err != nil {
		t.Fatal("move directory", err)
	}
	if err = os.Remove(moved); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal("nonempty rmdir", err)
	}
	if err = os.RemoveAll(a); err != nil {
		t.Fatal("recursive remove", err)
	}
	if _, err = os.Stat(a); !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestFilesystemInodeIdentityCleanup(t *testing.T) {
	f, _ := newTestFS(t)
	root := f.FUSERoot()
	table := root.numbers
	liveID := ObjectID{1}
	live := table.acquire(liveID)
	for i := 0; i < 1000; i++ {
		id := ObjectID{2, byte(i), byte(i >> 8)}
		table.acquire(id)
	}
	runtime.GC()
	deadline := time.Now().Add(3 * time.Second)
	for {
		table.mu.Lock()
		for id, entry := range table.ids {
			if entry.Value() == nil {
				delete(table.ids, id)
			}
		}
		count := len(table.ids)
		table.mu.Unlock()
		if count <= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead identities retained: %d", count)
		}
		runtime.GC()
		runtime.Gosched()
	}
	if again := table.acquire(liveID); again != live {
		t.Fatal("live inode identity changed")
	}
	runtime.KeepAlive(live)
	runtime.KeepAlive(root)
}

func TestOverlayReadonlyAppendFlagDoesNotCopyUp(t *testing.T) {
	ctx := context.Background()
	_, o, guard, upper, _, _ := overlayFixture(t, nil)
	root := NewFUSERoot(o)
	fusefs.NewNodeFS(root, &fusefs.Options{NullPermissions: true})
	var entry fuse.EntryOut
	child, errno := root.Lookup(ctx, "conf.json", &entry)
	if errno != 0 {
		t.Fatal(errno)
	}
	before := len(upper.objects)
	if _, _, errno = child.Operations().(*FSNode).Open(ctx, syscall.O_RDONLY|syscall.O_APPEND); errno != 0 {
		t.Fatal(errno)
	}
	if len(upper.objects) != before || guard.writes != 0 {
		t.Fatal("readonly open caused copy-up")
	}
}

// Exercise the real FUSE callbacks/bridge without mounting or requiring root,
// fusermount, or /dev/fuse. Core tests separately cover independent mounts.
func TestFilesystemFUSECallbacks(t *testing.T) {
	ctx := context.Background()
	filesystem, _ := newTestFS(t)
	root := filesystem.FUSERoot()
	fusefs.NewNodeFS(root, &fusefs.Options{NullPermissions: true})
	var entry fuse.EntryOut
	child, handle, flags, errno := root.Create(ctx, "hello", syscall.O_RDWR, 0644, &entry)
	if errno != 0 || flags&fuse.FOPEN_DIRECT_IO == 0 || entry.Mode&syscall.S_IFMT != syscall.S_IFREG {
		t.Fatalf("create %v %d %+v", errno, flags, entry)
	}
	node := child.Operations().(*FSNode)
	if n, errno := node.Write(ctx, handle, []byte("hello"), 0); errno != 0 || n != 5 {
		t.Fatalf("write %d %v", n, errno)
	}
	appendHandle, _, errno := node.Open(ctx, syscall.O_WRONLY|syscall.O_APPEND)
	if errno != 0 {
		t.Fatal(errno)
	}
	if n, errno := node.Write(ctx, appendHandle, []byte("!"), 0); errno != 0 || n != 1 {
		t.Fatalf("append %d %v", n, errno)
	}
	result, errno := node.Read(ctx, handle, make([]byte, 32), 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	data, status := result.Bytes(make([]byte, 32))
	if status != fuse.OK || !bytes.Equal(data, []byte("hello!")) {
		t.Fatalf("read %q %v", data, status)
	}
	result.Done()
	again, errno := root.Lookup(ctx, "hello", &entry)
	if errno != 0 || again.StableAttr() != child.StableAttr() {
		t.Fatalf("lookup changed inode identity %v", errno)
	}
	stream, errno := root.Readdir(ctx)
	if errno != 0 {
		t.Fatal(errno)
	}
	if !stream.HasNext() {
		t.Fatal("empty readdir")
	}
	dirent, errno := stream.Next()
	stream.Close()
	if errno != 0 || dirent.Name != "hello" {
		t.Fatal(dirent, errno)
	}
	in := fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE | fuse.FATTR_MODE
	in.Size = 2
	in.Mode = 0600
	var attr fuse.AttrOut
	if errno = node.Setattr(ctx, handle, &in, &attr); errno != 0 || attr.Size != 2 || attr.Mode&0777 != 0600 {
		t.Fatal(attr, errno)
	}
	if errno = node.Fsync(ctx, handle, 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno = root.Rename(ctx, "hello", root, "renamed", 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno = root.Unlink(ctx, "renamed"); errno != 0 {
		t.Fatal(errno)
	}
	if errno = node.Getattr(ctx, handle, &attr); errno != 0 || attr.Size != 2 {
		t.Fatalf("unlinked open handle %v", errno)
	}
	if _, errno = root.Mkdir(ctx, "dir", 0755, &entry); errno != 0 {
		t.Fatal(errno)
	}
	if errno = root.Unlink(ctx, "dir"); errno != syscall.EISDIR {
		t.Fatal(errno)
	}
	if errno = root.Rmdir(ctx, "dir"); errno != 0 {
		t.Fatal(errno)
	}
	if _, errno = root.Lookup(ctx, "dir", &entry); errno != syscall.ENOENT {
		t.Fatal(errno)
	}
	if _, errno = root.Lookup(ctx, "missing", &entry); errno != syscall.ENOENT {
		t.Fatal(errno)
	}
}
