// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package client

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

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
	if errno = root.Rmdir(ctx, "dir"); errno != syscall.ENOTSUP {
		t.Fatalf("unsafe rmdir accepted %v", errno)
	}
	if errno = root.Unlink(ctx, "dir"); errno != syscall.EISDIR {
		t.Fatal(errno)
	}
	if _, errno = root.Lookup(ctx, "missing", &entry); errno != syscall.ENOENT {
		t.Fatal(errno)
	}
}
