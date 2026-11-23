//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestSubscribedFilesystemKernelInvalidation(t *testing.T) {
	if os.Getenv("STQR_TEST_FUSE") != "1" {
		t.Skip("set STQR_TEST_FUSE=1")
	}
	_, reader, writer, ctx := subscriptionClients(t)
	lower, err := client.OpenFilesystem(ctx, writer, client.FSOptions{Create: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	if err != nil {
		t.Fatal(err)
	}
	file, err := lower.Create(ctx, client.FilesystemRootID(), "code.php", 0600, uint32(os.Getuid()), uint32(os.Getgid()), false)
	if err != nil {
		t.Fatal(err)
	}
	old := bytes.Repeat([]byte("a"), 8192)
	newData := bytes.Repeat([]byte("b"), len(old))
	if _, err = lower.WriteAt(ctx, file.ID, old, 0); err != nil {
		t.Fatal(err)
	}
	cache := client.NewSubscriptionStore(reader, 1<<20)
	t.Cleanup(cache.Close)
	view, err := client.OpenFilesystem(ctx, cache, client.FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	mountpoint := t.TempDir()
	mount, err := client.MountFilesystem(view, mountpoint, client.FSMountOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mount.Unmount(); err != nil {
			t.Error(err)
		}
		mount.Wait()
	})
	fd, err := os.Open(filepath.Join(mountpoint, "code.php"))
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	read := func() []byte {
		t.Helper()
		buf := make([]byte, len(old))
		if _, err := fd.ReadAt(buf, 0); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		return buf
	}
	if got := read(); !bytes.Equal(got, old) {
		t.Fatal("initial read")
	}
	if _, err = lower.WriteAt(ctx, file.ID, newData, 0); err != nil {
		t.Fatal(err)
	}
	for !bytes.Equal(read(), newData) {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("open descriptor page cache stayed stale")
		}
	}
	missing := filepath.Join(mountpoint, "new.php")
	if _, err = os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	created, err := lower.Create(ctx, client.FilesystemRootID(), "new.php", 0600, uint32(os.Getuid()), uint32(os.Getgid()), false)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(missing); err == nil {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("negative directory entry stayed stale")
		}
	}
	if err = lower.Unlink(ctx, client.FilesystemRootID(), "new.php"); err != nil {
		t.Fatal(err)
	}
	_ = created
	for {
		if _, err = os.Stat(missing); os.IsNotExist(err) {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("deleted directory entry stayed visible")
		}
	}
	if _, err = lower.SetAttributes(ctx, file.ID, client.FSAttributes{Mode: func() *uint32 { m := uint32(0640); return &m }()}); err != nil {
		t.Fatal(err)
	}
	for {
		st, err := fd.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() == 0640 {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("permissions stayed stale")
		}
	}
}

func TestSubscribedOverlayKernelCopyUpInvalidation(t *testing.T) {
	if os.Getenv("STQR_TEST_FUSE") != "1" {
		t.Skip("set STQR_TEST_FUSE=1")
	}
	s, lowerConn, baseWriter, ctx := subscriptionClients(t)
	base, err := client.OpenFilesystem(ctx, baseWriter, client.FSOptions{Create: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	if err != nil {
		t.Fatal(err)
	}
	file, err := base.Create(ctx, client.FilesystemRootID(), "code.php", 0600, uint32(os.Getuid()), uint32(os.Getgid()), false)
	if err != nil {
		t.Fatal(err)
	}
	initial := bytes.Repeat([]byte("a"), 8192)
	changed := bytes.Repeat([]byte("z"), len(initial))
	if _, err = base.WriteAt(ctx, file.ID, initial, 0); err != nil {
		t.Fatal(err)
	}
	b, _ := s.BucketByName("subscribed")
	upper, _ := createProtocolBucket(t, s, "subscription-upper")
	admin := s.cfg.Auth.BootstrapAdmin
	// Reuse the HTTP endpoint already represented by the test client.
	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	uc, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(upper.ID()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { uc.Close() })
	writer, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(upper.ID()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	lc := client.NewSubscriptionStore(lowerConn, 4<<20)
	up := client.NewSubscriptionStore(uc, 4<<20)
	t.Cleanup(lc.Close)
	t.Cleanup(up.Close)
	view, err := client.OpenOverlayFilesystem(ctx, lc, up, client.OverlayOptions{Create: true, LowerBucketID: client.BucketID(b.ID())})
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.OpenOverlayFilesystem(ctx, lowerConn, writer, client.OverlayOptions{LowerBucketID: client.BucketID(b.ID())})
	if err != nil {
		t.Fatal(err)
	}
	mp := t.TempDir()
	mount, err := client.MountFilesystem(view, mp, client.FSMountOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mount.Unmount(); err != nil {
			t.Error(err)
		}
		mount.Wait()
	})
	fd, err := os.Open(filepath.Join(mp, "code.php"))
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	buf := make([]byte, len(initial))
	if _, err = fd.ReadAt(buf, 0); err != nil || !bytes.Equal(buf, initial) {
		t.Fatal(err)
	}
	if _, err = other.WriteAt(ctx, file.ID, changed, 0); err != nil {
		t.Fatal(err)
	}
	wait := func() {
		for {
			if _, err = fd.ReadAt(buf, 0); err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(buf, changed) {
				return
			}
			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				t.Fatal("overlay page cache hid copy-up")
			}
		}
	}
	wait()
	if err = other.Unlink(ctx, client.FilesystemRootID(), "code.php"); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(filepath.Join(mp, "code.php")); os.IsNotExist(err) {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("overlay whiteout stayed invisible")
		}
	}
	wait() // The still-open upper inode must remain readable after unlink.
}
