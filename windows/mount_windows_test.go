// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package winfs

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/winfsp/cgofuse/fuse"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This opt-in test exercises Windows file APIs through the actual WinFsp driver.
func TestWindowsMountedFilesystem(t *testing.T) {
	if os.Getenv("STQR_TEST_WINFSP") != "1" {
		t.Skip("set STQR_TEST_WINFSP=1 on disposable Windows with WinFsp")
	}
	f, store := fixture(t)
	ctx, root := context.Background(), client.FilesystemRootID()
	info, err := f.Volume.Create(ctx, root, "from-go", 0644, 42, 43, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Volume.WriteAt(ctx, info.ID, []byte("shared format"), 0); err != nil {
		t.Fatal(err)
	}
	if err = f.Volume.SetXattr(ctx, info.ID, "user.preserved", []byte("linux metadata"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Volume.Link(ctx, root, "hardlink", info.ID); err != nil {
		t.Fatal(err)
	}
	host := fuse.NewFileSystemHost(&Adapter{FS: f})
	host.SetCapCaseInsensitive(false)
	host.SetCapReaddirPlus(true)
	drive := os.Getenv("STQR_TEST_WINFSP_DRIVE")
	if drive == "" {
		drive = "S:"
	}
	if _, err = os.Stat(drive + `\`); err == nil {
		t.Fatal("test drive already exists", drive)
	}
	done := make(chan bool, 1)
	go func() {
		done <- host.Mount(drive, []string{"-o", "volname=STQR-audit,uid=-1,gid=-1,umask=077,FileInfoTimeout=0,DirInfoTimeout=0,attr_timeout=0,entry_timeout=0,negative_timeout=0"})
	}()
	defer func() {
		host.Unmount()
		select {
		case ok := <-done:
			if !ok {
				t.Error("WinFsp mount failed")
			}
		case <-time.After(15 * time.Second):
			t.Error("WinFsp unmount timed out")
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err = os.Stat(drive + `\from-go`); err == nil {
			break
		}
		select {
		case ok := <-done:
			done <- ok
			t.Fatalf("WinFsp exited before mounting: %v", ok)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("mount unavailable", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	p := func(name string) string { return filepath.Join(drive+`\`, name) }
	if got, err := os.ReadFile(p("from-go")); err != nil || string(got) != "shared format" {
		t.Fatal(string(got), err)
	}
	if err = os.WriteFile(p("hardlink"), []byte("from Windows"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(p("from-go")); err != nil || string(got) != "from Windows" {
		t.Fatal("existing Linux hardlink", string(got), err)
	}
	if value, err := f.Volume.GetXattr(ctx, info.ID, "user.preserved"); err != nil || string(value) != "linux metadata" {
		t.Fatal("lost Linux xattr", string(value), err)
	}
	if err = os.Mkdir(p("directory"), 0755); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(p("directory/sparse"), os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data := bytes.Repeat([]byte("native Windows"), 8192)
	const offset = 2<<20 | 17
	if n, err := file.WriteAt(data, offset); err != nil || n != len(data) {
		t.Fatal(n, err)
	}
	if err = file.Sync(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, offset+len(data))
	if n, err := file.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got[:offset], make([]byte, offset)) || !bytes.Equal(got[offset:], data) {
		t.Fatal("sparse I/O", n, err)
	}
	if err = file.Truncate(100); err != nil {
		t.Fatal(err)
	}
	if err = file.Truncate(8192); err != nil {
		t.Fatal(err)
	}
	zero := make([]byte, 8192)
	if n, err := file.ReadAt(zero, 0); err != nil || n != len(zero) || !bytes.Equal(zero, make([]byte, len(zero))) {
		t.Fatal("truncate resurrected tail", n, err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(p("directory/sparse"), p("directory/renamed")); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(p("directory")); err != nil || len(entries) != 1 || entries[0].Name() != "renamed" {
		t.Fatal(entries, err)
	}
	if err = os.Rename(p("directory"), p("moved")); err != nil {
		t.Fatal(err)
	}
	check, err := client.OpenFilesystem(ctx, store, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := check.Lookup(ctx, root, "moved")
	if err != nil {
		t.Fatal(err)
	}
	saved, err := check.Lookup(ctx, dir.ID, "renamed")
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 8193)
	if n, err := check.ReadAt(ctx, saved.ID, buffer, 0); err != io.EOF || n != 8192 {
		t.Fatal("Go reopening of Windows graph", n, err)
	}
	if err = os.RemoveAll(p("moved")); err != nil {
		t.Fatal(err)
	}
	t.Log("native WinFsp mount: Go format, existing hardlinks/xattrs, sparse I/O, sync, truncate/regrow, rename, readdir, removal")
}
