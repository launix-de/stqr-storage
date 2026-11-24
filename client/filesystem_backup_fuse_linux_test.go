//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func TestFilesystemRsyncLinkDest(t *testing.T) {
	if os.Getenv("STQR_TEST_FUSE") != "1" {
		t.Skip("set STQR_TEST_FUSE=1 for real rsync/FUSE backups")
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip(err)
	}
	ctx := context.Background()
	store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	if err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := f.Mount(mountpoint, false)
	if err != nil {
		t.Fatal(err)
	}
	active := true
	defer func() {
		if active {
			if e := mounted.Unmount(); e != nil {
				t.Error(e)
			} else {
				mounted.Wait()
			}
		}
	}()
	source := t.TempDir()
	write := func(name string, data []byte) {
		t.Helper()
		if e := os.WriteFile(filepath.Join(source, name), data, 0640); e != nil {
			t.Fatal(e)
		}
	}
	large := bytes.Repeat([]byte("unchanged"), 150000)
	write("large", large)
	write("changed", []byte("before"))
	write("mode", []byte("permissions"))
	write("deleted", []byte("old only"))
	if err = os.Link(filepath.Join(source, "large"), filepath.Join(source, "alias")); err != nil {
		t.Fatal(err)
	}
	first, second := filepath.Join(mountpoint, "day1"), filepath.Join(mountpoint, "day2")
	rsync := func(dest string, extra ...string) {
		t.Helper()
		args := append([]string{"-aH"}, extra...)
		args = append(args, source+"/", dest+"/")
		if out, e := exec.Command("rsync", args...).CombinedOutput(); e != nil {
			t.Fatalf("rsync: %v %s", e, out)
		}
	}
	rsync(first)
	write("changed", []byte("after, a different length"))
	if err = os.Chmod(filepath.Join(source, "mode"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(source, "deleted")); err != nil {
		t.Fatal(err)
	}
	rsync(second, "--link-dest="+first)
	stat := func(path string) os.FileInfo {
		t.Helper()
		i, e := os.Stat(path)
		if e != nil {
			t.Fatal(e)
		}
		return i
	}
	a, b := stat(filepath.Join(first, "large")), stat(filepath.Join(second, "large"))
	if !os.SameFile(a, b) || b.Sys().(*syscall.Stat_t).Nlink != 4 {
		t.Fatal("unchanged files/source aliases were not hardlinked across snapshots")
	}
	if os.SameFile(stat(filepath.Join(first, "changed")), stat(filepath.Join(second, "changed"))) {
		t.Fatal("changed file reused previous inode")
	}
	if data, e := os.ReadFile(filepath.Join(first, "changed")); e != nil || string(data) != "before" {
		t.Fatal("previous backup changed", e)
	}
	if stat(filepath.Join(first, "mode")).Mode().Perm() != 0640 || stat(filepath.Join(second, "mode")).Mode().Perm() != 0600 {
		t.Fatal("metadata-only change affected previous snapshot")
	}
	if _, e := os.Stat(filepath.Join(second, "deleted")); !os.IsNotExist(e) {
		t.Fatal("deleted source present in new snapshot", e)
	}
	if err = os.RemoveAll(first); err != nil {
		t.Fatal(err)
	}
	if data, e := os.ReadFile(filepath.Join(second, "large")); e != nil || !bytes.Equal(data, large) {
		t.Fatal("deleting previous snapshot damaged remaining backup", e)
	}
	if stat(filepath.Join(second, "large")).Sys().(*syscall.Stat_t).Nlink != 2 {
		t.Fatal("link count after pruning")
	}
	if err = mounted.Unmount(); err != nil {
		t.Fatal(err)
	}
	mounted.Wait()
	active = false
	if _, err = checkFilesystem(ctx, store); err != nil {
		t.Fatal("offline collection of backup graph", err)
	}
	root, err := f.Lookup(ctx, FilesystemRootID(), "day2")
	if err != nil {
		t.Fatal(err)
	}
	file, err := f.Lookup(ctx, root.ID, "large")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(large))
	if n, err := f.ReadAt(ctx, file.ID, got, 0); err != nil || n != len(got) || !bytes.Equal(got, large) {
		t.Fatal("offline collection damaged surviving backup", n, err)
	}
}
