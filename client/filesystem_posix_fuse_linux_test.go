//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestFilesystemPOSIXKernelMount(t *testing.T) {
	if os.Getenv("STQR_TEST_FUSE") != "1" {
		t.Skip("set STQR_TEST_FUSE=1 for real FUSE mounts")
	}
	ctx := context.Background()
	store := NewMemoryFilesystemStore()
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	if err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := f.Mount(mountpoint, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := mounted.Unmount(); err != nil {
			t.Error(err)
		} else {
			mounted.Wait()
		}
	}()
	path := func(name string) string { return filepath.Join(mountpoint, name) }
	if err = os.WriteFile(path("file"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Link(path("file"), path("alias")); err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(path("file"))
	if err != nil {
		t.Fatal(err)
	}
	alias, err := os.Stat(path("alias"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(original, alias) || original.Sys().(*syscall.Stat_t).Nlink != 2 {
		t.Fatal("hardlinks do not share an inode", original, alias)
	}
	if err = os.Symlink("alias", path("symlink")); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path("symlink")); err != nil || string(data) != "data" {
		t.Fatal(string(data), err)
	}
	if value, err := os.Readlink(path("symlink")); err != nil || value != "alias" {
		t.Fatal(value, err)
	}
	value := bytes.Repeat([]byte{0, 0xff, 0x42}, 20000)
	if err = unix.Setxattr(path("file"), "user.binary", value, unix.XATTR_CREATE); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(value))
	if n, err := unix.Getxattr(path("alias"), "user.binary", buffer); err != nil || n != len(value) || !bytes.Equal(buffer, value) {
		t.Fatal(n, err)
	}
	if n, err := unix.Listxattr(path("file"), nil); err != nil || n != len("user.binary")+1 {
		t.Fatal(n, err)
	}
	if err = os.Remove(path("file")); err != nil {
		t.Fatal(err)
	}
	alias, err = os.Stat(path("alias"))
	if err != nil || alias.Sys().(*syscall.Stat_t).Nlink != 1 {
		t.Fatal(alias, err)
	}
	if err = unix.Mkfifo(path("fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(path("fifo"), unix.O_RDWR|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if n, err := unix.Write(fd, []byte("pipe")); err != nil || n != 4 {
		t.Fatal(n, err)
	}
	pipe := make([]byte, 4)
	if n, err := unix.Read(fd, pipe); err != nil || n != 4 || string(pipe) != "pipe" {
		t.Fatal(n, string(pipe), err)
	}
	listener, err := net.Listen("unix", path("socket"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	connection, err := net.Dial("unix", path("socket"))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err = connection.Write([]byte("unix")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4)
	if n, err := peer.Read(data); err != nil || n != 4 || string(data) != "unix" {
		t.Fatal(n, string(data), err)
	}
	t.Run("git", func(t *testing.T) {
		git, err := exec.LookPath("git")
		if err != nil {
			t.Skip("git is not installed")
		}
		run := func(args ...string) {
			t.Helper()
			// Enter the self-served FUSE filesystem after exec, in git itself.
			// Fork/exec must not chdir there while waiting on this Go process.
			base := []string{"-C", mountpoint, "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}
			cmd := exec.Command(git, append(base, args...)...)
			for _, env := range os.Environ() {
				if !strings.HasPrefix(env, "GIT_") {
					cmd.Env = append(cmd.Env, env)
				}
			}
			cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=STQR Test", "GIT_AUTHOR_EMAIL=test@example.invalid", "GIT_COMMITTER_NAME=STQR Test", "GIT_COMMITTER_EMAIL=test@example.invalid", "GIT_TERMINAL_PROMPT=0")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v\n%s", args, err, out)
			}
		}
		run("init", "repository")
		if err = os.WriteFile(path("repository/source.txt"), []byte("source code\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if err = os.Symlink("source.txt", path("repository/source-link")); err != nil {
			t.Fatal(err)
		}
		run("-C", "repository", "add", ".")
		run("-C", "repository", "commit", "-m", "filesystem integration")
		run("-C", "repository", "gc")
		run("clone", "--local", "repository", "clone")
		run("-C", "clone", "fsck", "--full")
		if data, err := os.ReadFile(path("clone/source-link")); err != nil || string(data) != "source code\n" {
			t.Fatal(string(data), err)
		}
	})
	if _, err = checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store}); err != nil {
		t.Fatal(err)
	}
}
