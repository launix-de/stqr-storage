// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"syscall"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func fixture(t *testing.T) (*Filesystem, *client.MemoryFilesystemStore) {
	t.Helper()
	store := client.NewMemoryFilesystemStore()
	v, e := client.OpenFilesystem(context.Background(), store, client.FSOptions{Create: true})
	if e != nil {
		t.Fatal(e)
	}
	return New(context.Background(), v, false), store
}
func TestHandlesSurviveRenameAndUnlink(t *testing.T) {
	f, _ := fixture(t)
	if e := f.Mkdir("/dir", 0755); e != nil {
		t.Fatal(e)
	}
	h, e := f.Open("/dir/file", OpenOptions{Read: true, Write: true, Create: true, Exclusive: true}, 0640)
	if e != nil {
		t.Fatal(e)
	}
	data := bytes.Repeat([]byte{0x5a}, (1<<20)+512)
	if n, e := f.Write(h, data, 8192); e != nil || n != len(data) {
		t.Fatal(n, e)
	}
	if e = f.Rename("/dir/file", "/dir/renamed", 0); e != nil {
		t.Fatal(e)
	}
	before, e := f.Stat("", h)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Remove("/dir/renamed", false); e != nil {
		t.Fatal(e)
	}
	info, e := f.Stat("/no-such-path", h)
	if e != nil || info.ID != before.ID || info.Nlink != 0 {
		t.Fatal("open-unlink inode", e)
	}
	got := make([]byte, len(data)+8192)
	n, e := f.Read(h, got, 0)
	if e != nil || n != len(got) || !bytes.Equal(got[:8192], make([]byte, 8192)) || !bytes.Equal(got[8192:], data) {
		t.Fatal("sparse/segmented read", n, e)
	}
	if e = f.Sync("", h); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(h); e != nil {
		t.Fatal(e)
	}
	if _, e = f.Stat("", h); !errors.Is(e, syscall.EBADF) {
		t.Fatal(e)
	}
	if len(f.handles) != 0 {
		t.Fatal("closed handle retained")
	}
}
func TestExclusiveAppendAndReadonly(t *testing.T) {
	f, store := fixture(t)
	h, e := f.Open("/log", OpenOptions{Write: true, Create: true, Exclusive: true, Append: true}, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Open("/log", OpenOptions{Write: true, Create: true, Exclusive: true}, 0600); !errors.Is(e, syscall.EEXIST) {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				if _, e := f.Write(h, []byte("line\n"), 0); e != nil {
					t.Error(e)
					return
				}
			}
		}()
	}
	wg.Wait()
	info, e := f.Stat("", h)
	if e != nil || info.Size != 160 {
		t.Fatal("append lost writes", info.Size, e)
	}
	if _, e = f.Read(h, make([]byte, 1), 0); !errors.Is(e, syscall.EBADF) {
		t.Fatal("write-only handle read", e)
	}
	f.Close(h)
	volume, e := client.OpenFilesystem(context.Background(), store, client.FSOptions{ReadOnly: true})
	if e != nil {
		t.Fatal(e)
	}
	ro := New(context.Background(), volume, true)
	for _, operation := range []func() error{func() error { _, e := ro.Open("/log", OpenOptions{Write: true}, 0); return e }, func() error { return ro.Mkdir("/other", 0755) }, func() error { return ro.Remove("/log", false) }, func() error { return ro.Rename("/log", "/other", 0) }, func() error { mode := uint32(0777); return ro.Setattr("/log", 0, client.FSAttributes{Mode: &mode}) }} {
		if e := operation(); !errors.Is(e, syscall.EROFS) {
			t.Fatal("readonly mutation", e)
		}
	}
	read, e := ro.Open("/log", OpenOptions{Read: true}, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer ro.Close(read)
	buffer := make([]byte, 512)
	if n, e := ro.Read(read, buffer, 0); n != 160 || e != nil && !errors.Is(e, io.EOF) {
		t.Fatal(n, e)
	}
}
func TestDirectorySnapshotsAndNames(t *testing.T) {
	f, _ := fixture(t)
	if e := f.Mkdir("/dir", 0755); e != nil {
		t.Fatal(e)
	}
	h, e := f.Open("/dir/file", OpenOptions{Write: true, Create: true}, 0600)
	if e != nil {
		t.Fatal(e)
	}
	f.Close(h)
	d, e := f.Open("/dir", OpenOptions{Read: true, Directory: true}, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close(d)
	entries, e := f.Entries(d)
	if e != nil || len(entries) != 3 || entries[2].Name != "file" {
		t.Fatal(entries, e)
	}
	if e = f.Remove("/dir", true); !errors.Is(e, syscall.ENOTEMPTY) {
		t.Fatal(e)
	}
	for _, p := range []string{"relative", "/bad\x00name", "/bad\\name"} {
		if _, e := f.Lookup(p); !errors.Is(e, syscall.EINVAL) {
			t.Fatal("invalid path", p, e)
		}
	}
	if e = f.Rename("/dir", "/dir/sub", 0); !errors.Is(e, syscall.EINVAL) {
		t.Fatal("rename cycle", e)
	}
	if _, e = f.Open("/dir", OpenOptions{Read: true}, 0); !errors.Is(e, syscall.EISDIR) {
		t.Fatal(e)
	}
}
