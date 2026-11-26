// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"io"
	"syscall"
	"testing"
)

func TestWindowsUserXattrInteroperability(t *testing.T) {
	f, _ := fixture(t)
	h, err := f.Open("/file", OpenOptions{Create: true, Write: true}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close(h)
	if err = f.SetXattr("/file", "user.shared", []byte{0, 255, 42}, 1); err != nil {
		t.Fatal(err)
	}
	info, err := f.Lookup("/file")
	if err != nil {
		t.Fatal(err)
	}
	attrs := f.Volume.(interface {
		GetXattr(context.Context, client.ObjectID, string) ([]byte, error)
		SetXattr(context.Context, client.ObjectID, string, []byte, uint32) error
	})
	if b, err := attrs.GetXattr(f.Context, info.ID, "user.shared"); err != nil || len(b) != 3 || b[1] != 255 {
		t.Fatal(b, err)
	}
	if err = f.SetXattr("/file", "user.shared", nil, 1); !errors.Is(err, syscall.EEXIST) {
		t.Fatal(err)
	}
	if err = f.SetXattr("/file", "user.missing", nil, 2); !errors.Is(err, syscall.ENODATA) {
		t.Fatal(err)
	}
	if err = attrs.SetXattr(f.Context, info.ID, "trusted.private", []byte("protected"), 0); err != nil {
		t.Fatal(err)
	}
	f.UID = 0
	if _, err = f.GetXattr("/file", "trusted.private"); !errors.Is(err, syscall.EACCES) {
		t.Fatal("configured UID gained Unix privilege", err)
	}
	if err = f.SetXattr("/file", "trusted.private", nil, 0); !errors.Is(err, syscall.EACCES) {
		t.Fatal(err)
	}
	names, err := f.ListXattr("/file")
	if err != nil || len(names) != 1 || names[0] != "user.shared" {
		t.Fatal(names, err)
	}
	if err = f.RemoveXattr("/file", "user.shared"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.GetXattr("/file", "user.shared"); !errors.Is(err, syscall.ENODATA) {
		t.Fatal(err)
	}
	f.ReadOnly = true
	if err = f.SetXattr("/file", "user.readonly", nil, 0); !errors.Is(err, syscall.EROFS) {
		t.Fatal(err)
	}
}

func TestWindowsNativeOverlayView(t *testing.T) {
	ctx := context.Background()
	lowerStore, upperStore := client.NewMemoryFilesystemStore(), client.NewMemoryFilesystemStore()
	lower, err := client.OpenFilesystem(ctx, lowerStore, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	root := client.FilesystemRootID()
	for _, name := range []string{"allowed", "protected"} {
		info, err := lower.Create(ctx, root, name, 0600, 42, 43, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lower.WriteAt(ctx, info.ID, []byte("lower content"), 0); err != nil {
			t.Fatal(err)
		}
	}
	view, err := client.OpenOverlayFilesystem(ctx, lowerStore, upperStore, client.OverlayOptions{Create: true, LowerBucketID: client.BucketID{1}, Whitelist: []string{"allowed", "new"}})
	if err != nil {
		t.Fatal(err)
	}
	f := New(ctx, view, false)
	if _, err = f.Open("/protected", OpenOptions{Write: true}, 0); !errors.Is(err, syscall.EROFS) {
		t.Fatal("write open bypassed whitelist", err)
	}
	h, err := f.Open("/allowed", OpenOptions{Read: true, Write: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(h, []byte("UPPER"), 0); err != nil {
		t.Fatal(err)
	}
	if err = f.Rename("/allowed", "/new", 0); err != nil {
		t.Fatal(err)
	}
	if err = f.Remove("/new", false); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 32)
	n, err := f.Read(h, b, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if string(b[:n]) != "UPPER content" {
		t.Fatal(string(b[:n]))
	}
	if err = f.Close(h); err != nil {
		t.Fatal(err)
	}
	info, err := lower.Lookup(ctx, root, "allowed")
	if err != nil {
		t.Fatal(err)
	}
	n, err = lower.ReadAt(ctx, info.ID, b, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if string(b[:n]) != "lower content" {
		t.Fatal("lower modified", string(b[:n]))
	}
	ro, err := client.OpenOverlayFilesystem(ctx, lowerStore, upperStore, client.OverlayOptions{ReadOnly: true, LowerBucketID: client.BucketID{1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ro.Create(ctx, root, "new", 0600, 0, 0, false); !errors.Is(err, syscall.EROFS) {
		t.Fatal("readonly overlay mutated", err)
	}
}
