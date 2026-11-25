// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

// Full kernel -> FUSE -> native WebSocket -> persistent volume path. Opt in on
// a host with /dev/fuse; the default suite also covers the bridge without a mount.
func TestBucketFilesystemKernelNamespace(t *testing.T) {
	if os.Getenv("STQR_TEST_FUSE") != "1" {
		t.Skip("set STQR_TEST_FUSE=1 for a real kernel mount")
	}
	s := testServer(t)
	addTestVolume(t, s, "kernel-fuse", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "kernel-fuse")
	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	f, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	if err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mounted, err := f.Mount(mountpoint, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := mounted.Unmount(); e != nil {
			t.Error(e)
			return
		}
		mounted.Wait()
	})
	a, bb := filepath.Join(mountpoint, "a"), filepath.Join(mountpoint, "b")
	var space syscall.Statfs_t
	if err := syscall.Statfs(mountpoint, &space); err != nil || space.Blocks == 0 || space.Bavail == 0 {
		t.Fatalf("statfs: %+v %v", space, err)
	}
	for _, path := range []string{a, bb} {
		if err = os.Mkdir(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	source, target := filepath.Join(a, "new"), filepath.Join(bb, "existing")
	want := bytes.Repeat([]byte("native-persisted-payload"), 4096)
	if err = os.WriteFile(source, want, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(target, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if err = os.Rename(source, target); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(target); err != nil || !bytes.Equal(data, want) {
		t.Fatal("moved data differs", err)
	}
	buf := make([]byte, 3)
	if _, err = old.ReadAt(buf, 0); err != nil || string(buf) != "old" {
		t.Fatal("open old target", string(buf), err)
	}
	moved := filepath.Join(a, "moved")
	if err = os.Rename(bb, moved); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(moved); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal(err)
	}
	if err = os.RemoveAll(a); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(a); !os.IsNotExist(err) {
		t.Fatal(err)
	}
}
