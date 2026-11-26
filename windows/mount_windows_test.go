// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package winfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/launix-de/stqr-storage/client"
	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/windows"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// WinFsp queries capacity while mounting. The unbounded memory fixture supplies
// synthetic capacity; native server capacity is checked by the server suite.
type nativeMountStore struct{ *client.MemoryFilesystemStore }

func (*nativeMountStore) Space(context.Context) (client.BucketSpace, error) {
	return client.BucketSpace{Total: 1 << 30, Free: 1 << 30, Available: 1 << 30}, nil
}

// This opt-in test exercises Windows file APIs through the actual WinFsp driver.
func TestWindowsMountedFilesystem(t *testing.T) {
	if os.Getenv("STQR_TEST_WINFSP") != "1" {
		t.Skip("set STQR_TEST_WINFSP=1 on disposable Windows with WinFsp")
	}
	ctx, root := context.Background(), client.FilesystemRootID()
	store := &nativeMountStore{client.NewMemoryFilesystemStore()}
	volume, err := client.OpenFilesystem(ctx, store, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	f := New(ctx, volume, false)
	var capacity fuse.Statfs_t
	if status := (&Adapter{FS: f}).Statfs("/", &capacity); status != 0 || capacity.Bavail == 0 {
		t.Fatal("invalid native mount fixture capacity", status, capacity)
	}
	info, err := f.Volume.Create(ctx, root, "from-go", 0644, 42, 43, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Volume.WriteAt(ctx, info.ID, []byte("shared format"), 0); err != nil {
		t.Fatal(err)
	}
	if err = volume.SetXattr(ctx, info.ID, "user.preserved", []byte("linux metadata"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err = volume.Link(ctx, root, "hardlink", info.ID); err != nil {
		t.Fatal(err)
	}
	drive := mountTestFilesystem(t, f, "from-go")
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
	if value, err := volume.GetXattr(ctx, info.ID, "user.preserved"); err != nil || string(value) != "linux metadata" {
		t.Fatal("lost Linux xattr", string(value), err)
	}
	testNativeExtendedAttributes(t, p("from-go"), volume, info.ID)
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

func mountTestFilesystem(t *testing.T, f *Filesystem, ready string) string {
	t.Helper()
	var err error
	host := fuse.NewFileSystemHost(&Adapter{FS: f})
	host.SetCapCaseInsensitive(false)
	host.SetCapReaddirPlus(true)
	host.SetCapDeleteAccess(true)
	drive := os.Getenv("STQR_TEST_WINFSP_DRIVE")
	if drive == "" {
		drive = "S:"
	}
	if _, err = os.Stat(drive + `\`); err == nil {
		t.Fatal("test drive already exists", drive)
	}
	done := make(chan bool, 1)
	go func() {
		opts := []string{"-o", "volname=STQR-audit,uid=-1,gid=-1,umask=077,FileInfoTimeout=0,DirInfoTimeout=0,attr_timeout=0,entry_timeout=0,negative_timeout=0"}
		if f.ReadOnly {
			opts = append(opts, "-o", "ro")
		}
		done <- host.Mount(drive, opts)
	}()
	t.Cleanup(func() {
		host.Unmount()
		select {
		case ok := <-done:
			if !ok {
				t.Error("WinFsp mount failed")
			}
		case <-time.After(15 * time.Second):
			t.Error("WinFsp unmount timed out")
		}
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err = os.Stat(drive + `\` + ready); err == nil {
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

	return drive
}

// Exercise the actual NT extended-attribute API rather than only adapter calls.
func testNativeExtendedAttributes(t *testing.T, path string, volume *client.Filesystem, id client.ObjectID) {
	t.Helper()
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(name, windows.FILE_READ_EA|windows.FILE_WRITE_EA, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(h)
	attribute, value := "user.native", []byte{0, 255, 42}
	raw := make([]byte, 8+len(attribute)+1+len(value))
	raw[5] = byte(len(attribute))
	binary.LittleEndian.PutUint16(raw[6:8], uint16(len(value)))
	copy(raw[8:], attribute)
	copy(raw[9+len(attribute):], value)
	var status windows.IO_STATUS_BLOCK
	set := windows.NewLazySystemDLL("ntdll.dll").NewProc("NtSetEaFile")
	code, _, _ := set.Call(uintptr(h), uintptr(unsafe.Pointer(&status)), uintptr(unsafe.Pointer(&raw[0])), uintptr(len(raw)))
	if int32(code) < 0 {
		t.Fatalf("NtSetEaFile: %#x", uint32(code))
	}
	if got, err := volume.GetXattr(context.Background(), id, attribute); err != nil || !bytes.Equal(got, value) {
		t.Fatal("native EA not visible to Linux core", got, err)
	}
	raw = make([]byte, 4096)
	query := windows.NewLazySystemDLL("ntdll.dll").NewProc("NtQueryEaFile")
	code, _, _ = query.Call(uintptr(h), uintptr(unsafe.Pointer(&status)), uintptr(unsafe.Pointer(&raw[0])), uintptr(len(raw)), 0, 0, 0, 0, 1)
	if int32(code) < 0 {
		t.Fatalf("NtQueryEaFile: %#x", uint32(code))
	}
	found := false
	for offset := 0; offset+8 <= int(status.Information); {
		entry := raw[offset:]
		n, v := int(entry[5]), int(binary.LittleEndian.Uint16(entry[6:8]))
		if offset+9+n+v > int(status.Information) {
			t.Fatal("invalid native EA response")
		}
		if strings.EqualFold(string(entry[8:8+n]), attribute) {
			found = bytes.Equal(entry[9+n:9+n+v], value)
		}
		next := int(binary.LittleEndian.Uint32(entry[:4]))
		if next == 0 {
			break
		}
		offset += next
	}
	if !found {
		t.Fatal("shared user attribute missing from native EA enumeration")
	}
}

func TestWindowsMountedNativeOverlay(t *testing.T) {
	if os.Getenv("STQR_TEST_WINFSP") != "1" {
		t.Skip("set STQR_TEST_WINFSP=1 on disposable Windows with WinFsp")
	}
	ctx, root := context.Background(), client.FilesystemRootID()
	lowerStore, upperStore := &nativeMountStore{client.NewMemoryFilesystemStore()}, &nativeMountStore{client.NewMemoryFilesystemStore()}
	lower, err := client.OpenFilesystem(ctx, lowerStore, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"allowed", "protected"} {
		info, e := lower.Create(ctx, root, name, 0600, 42, 43, false)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = lower.WriteAt(ctx, info.ID, []byte("lower content"), 0); e != nil {
			t.Fatal(e)
		}
	}
	view, err := client.OpenOverlayFilesystem(ctx, lowerStore, upperStore, client.OverlayOptions{Create: true, LowerBucketID: client.BucketID{1}, Whitelist: []string{"allowed", "new"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("writable", func(t *testing.T) {
		drive := mountTestFilesystem(t, New(ctx, view, false), "allowed")
		path := func(name string) string { return filepath.Join(drive+`\`, name) }
		if err := os.WriteFile(path("protected"), []byte("bad"), 0600); err == nil {
			t.Fatal("native open bypassed whitelist")
		}
		if err := os.Remove(path("protected")); err == nil {
			t.Fatal("native delete bypassed whitelist")
		}
		if err := os.WriteFile(path("allowed"), []byte("upper content"), 0600); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(path("allowed")); err != nil || string(got) != "upper content" {
			t.Fatal(string(got), err)
		}
		if err := os.WriteFile(path("new"), []byte("new file"), 0600); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("readonly", func(t *testing.T) {
		ro, err := client.OpenOverlayFilesystem(ctx, lowerStore, upperStore, client.OverlayOptions{ReadOnly: true, LowerBucketID: client.BucketID{1}})
		if err != nil {
			t.Fatal(err)
		}
		drive := mountTestFilesystem(t, New(ctx, ro, true), "allowed")
		path := func(name string) string { return filepath.Join(drive+`\`, name) }
		if got, err := os.ReadFile(path("allowed")); err != nil || string(got) != "upper content" {
			t.Fatal(string(got), err)
		}
		if err := os.WriteFile(path("allowed"), []byte("bad"), 0600); err == nil {
			t.Fatal("native readonly mount accepted a write")
		}
		if err := os.Remove(path("new")); err == nil {
			t.Fatal("native readonly mount accepted unlink")
		}
	})
	info, err := lower.Lookup(ctx, root, "allowed")
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 13)
	if n, err := lower.ReadAt(ctx, info.ID, raw, 0); err != nil || n != len(raw) || string(raw) != "lower content" {
		t.Fatal("native overlay changed lower", string(raw), err)
	}
}
