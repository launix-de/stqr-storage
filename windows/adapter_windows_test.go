//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"github.com/winfsp/cgofuse/fuse"
	"testing"
	"time"
)

func TestWindowsCallbacks(t *testing.T) {
	f, _ := fixture(t)
	a := &Adapter{FS: f}
	code, h := a.Create("/file", fuse.O_CREAT|fuse.O_EXCL|fuse.O_RDWR, 0640)
	if code != 0 {
		t.Fatal(code)
	}
	if n := a.Write("/file", []byte("hello"), 0, h); n != 5 {
		t.Fatal(n)
	}
	if code := a.Rename("/file", "/renamed"); code != 0 {
		t.Fatal(code)
	}
	var info fuse.Stat_t
	if code := a.Getattr("/renamed", &info, h); code != 0 || info.Size != 5 {
		t.Fatal(code, info.Size)
	}
	if code := a.Unlink("/renamed"); code != 0 {
		t.Fatal(code)
	}
	buf := make([]byte, 16)
	if n := a.Read("", buf, 0, h); n != 5 || string(buf[:5]) != "hello" {
		t.Fatal(n, string(buf))
	}
	if code := a.Fsync("", false, h); code != 0 {
		t.Fatal(code)
	}
	if code := a.Release("", h); code != 0 {
		t.Fatal(code)
	}
	if code := a.Link("/x", "/y"); code != -fuse.EOPNOTSUPP {
		t.Fatal("WinFsp must not advertise native hardlinks", code)
	}
	if code := a.Truncate("/missing", -1, 0); code != -fuse.EINVAL {
		t.Fatal(code)
	}
}

func TestWindowsMetadataCallbacks(t *testing.T) {
	f, _ := fixture(t)
	f.UID, f.GID = 1234, 2345
	a := &Adapter{FS: f}
	code, h := a.Create("/metadata", fuse.O_RDWR, 0640)
	if code != 0 {
		t.Fatal(code)
	}
	defer a.Release("", h)
	var info fuse.Stat_t
	if code = a.Getattr("/metadata", &info, h); code != 0 || info.Uid != 1234 || info.Gid != 2345 {
		t.Fatal("creation owner", code, info)
	}
	if code = a.Chown("/metadata", 3456, ^uint32(0)); code != 0 {
		t.Fatal(code)
	}
	if code = a.Chmod("/metadata", 0600); code != 0 {
		t.Fatal(code)
	}
	when := fuse.NewTimespec(time.Unix(1700000000, 123456789))
	if code = a.Utimens("/metadata", []fuse.Timespec{when, when}); code != 0 {
		t.Fatal(code)
	}
	if code = a.Getattr("/metadata", &info, h); code != 0 || info.Uid != 3456 || info.Gid != 2345 || info.Mode&0777 != 0600 || info.Mtim != when {
		t.Fatal("metadata update", code, info)
	}
	if code = a.Symlink("metadata", "/alias"); code != 0 {
		t.Fatal(code)
	}
	if code, target := a.Readlink("/alias"); code != 0 || target != "metadata" {
		t.Fatal(code, target)
	}
	if code = a.Utimens("/metadata", []fuse.Timespec{{Nsec: 1000000000}, when}); code != -fuse.EINVAL {
		t.Fatal("invalid nanoseconds", code)
	}
}
