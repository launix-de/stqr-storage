// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package client

import (
	"context"
	"errors"
	"syscall"
	"testing"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestFUSECreationInheritsSetgidDirectory(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestFS(t)
	mode, group := uint32(02777), uint32(45678)
	if _, err := f.SetAttributes(ctx, FilesystemRootID(), FSAttributes{Mode: &mode, GID: &group}); err != nil {
		t.Fatal(err)
	}
	root := f.FUSERoot()
	fusefs.NewNodeFS(root, &fusefs.Options{NullPermissions: true})
	operations := []struct {
		name      string
		create    func(*fuse.EntryOut) syscall.Errno
		directory bool
	}{
		{"file", func(out *fuse.EntryOut) syscall.Errno {
			_, h, _, err := root.Create(ctx, "file", syscall.O_RDWR, 0644, out)
			if release, ok := h.(interface {
				Release(context.Context) syscall.Errno
			}); ok {
				if err := release.Release(ctx); err != 0 {
					return err
				}
			}
			return err
		}, false},
		{"directory", func(out *fuse.EntryOut) syscall.Errno { _, err := root.Mkdir(ctx, "directory", 0755, out); return err }, true},
		{"symlink", func(out *fuse.EntryOut) syscall.Errno {
			_, err := root.Symlink(ctx, "file", "symlink", out)
			return err
		}, false},
		{"fifo", func(out *fuse.EntryOut) syscall.Errno {
			_, err := root.Mknod(ctx, "fifo", syscall.S_IFIFO|0600, 0, out)
			return err
		}, false},
		{"socket", func(out *fuse.EntryOut) syscall.Errno {
			_, err := root.Mknod(ctx, "socket", syscall.S_IFSOCK|0600, 0, out)
			return err
		}, false},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			var out fuse.EntryOut
			if err := op.create(&out); err != 0 {
				t.Fatal(err)
			}
			if out.Gid != group || op.directory && out.Mode&syscall.S_ISGID == 0 {
				t.Fatalf("lost inherited group/mode: gid=%d mode=%o", out.Gid, out.Mode)
			}
		})
	}
}

type removedDirectoryEntryView struct {
	FilesystemView
	victim ObjectID
	err    error
}

func (v removedDirectoryEntryView) Stat(ctx context.Context, id ObjectID) (FSInfo, error) {
	if id == v.victim {
		return FSInfo{}, v.err
	}
	return v.FilesystemView.Stat(ctx, id)
}

func TestFUSEReaddirToleratesConcurrentUnlink(t *testing.T) {
	ctx := context.Background()
	for _, err := range []error{ErrNotFound, syscall.ENOENT, ErrUnauthorized} {
		t.Run(err.Error(), func(t *testing.T) {
			f, _ := newTestFS(t)
			removed, e := f.Create(ctx, FilesystemRootID(), "gone", 0600, 1, 1, false)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = f.Create(ctx, FilesystemRootID(), "live", 0600, 1, 1, false); e != nil {
				t.Fatal(e)
			}
			root := NewFUSERoot(removedDirectoryEntryView{f, removed.ID, err})
			stream, status := root.Readdir(ctx)
			if errors.Is(err, ErrUnauthorized) {
				if status != syscall.EACCES {
					t.Fatal("hid a storage permission failure", status)
				}
				return
			}
			if status != 0 {
				t.Fatal("concurrent unlink failed directory enumeration", status)
			}
			defer stream.Close()
			var names []string
			for stream.HasNext() {
				entry, status := stream.Next()
				if status != 0 {
					t.Fatal(status)
				}
				names = append(names, entry.Name)
			}
			if len(names) != 1 || names[0] != "live" {
				t.Fatal(names)
			}
		})
	}
}
