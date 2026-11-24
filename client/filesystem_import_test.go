// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func importArchive(t *testing.T, headers []*tar.Header, contents map[string][]byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, h := range headers {
		if h.ModTime.IsZero() {
			h.ModTime = time.Unix(1700000000, 0)
		}
		if err := w.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if data := contents[h.Name]; len(data) > 0 {
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func TestImportFilesystemTarPreservesNativeTree(t *testing.T) {
	payload := make([]byte, FSObjectSize+9)
	copy(payload[FSObjectSize:], "last-data")
	archive := importArchive(t, []*tar.Header{
		{Name: "./", Typeflag: tar.TypeDir, Mode: 0750, Uid: 42, Gid: 12},
		{Name: "./alias", Typeflag: tar.TypeLink, Linkname: "./dir/file"},
		{Name: "./dir/", Typeflag: tar.TypeDir, Mode: 02770, Uid: 1000, Gid: 1000},
		{Name: "./dir/file", Typeflag: tar.TypeReg, Mode: 0640, Uid: 1000, Gid: 1000, Size: int64(len(payload)), PAXRecords: map[string]string{"SCHILY.xattr.security.capability": "\x01\x00cap"}},
		{Name: "./link", Typeflag: tar.TypeSymlink, Linkname: "dir/file", Mode: 0777},
		{Name: "./pipe", Typeflag: tar.TypeFifo, Mode: 0600},
	}, map[string][]byte{"./dir/file": payload})
	ctx := context.Background()
	store := NewMemoryFilesystemStore()
	if err := ImportFilesystemTar(ctx, store, bytes.NewReader(archive)); err != nil {
		t.Fatal(err)
	}
	f, err := OpenFilesystem(ctx, store, FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	root, err := f.Stat(ctx, FilesystemRootID())
	if err != nil || root.Mode != 0750 || root.UID != 42 || root.Nlink != 3 {
		t.Fatal(root, err)
	}
	dir, err := f.Lookup(ctx, root.ID, "dir")
	if err != nil || dir.Mode != 02770 {
		t.Fatal(dir, err)
	}
	file, err := f.Lookup(ctx, dir.ID, "file")
	if err != nil || file.Nlink != 2 || file.UID != 1000 {
		t.Fatal(file, err)
	}
	alias, err := f.Lookup(ctx, root.ID, "alias")
	if err != nil || alias.ID != file.ID {
		t.Fatal(alias, err)
	}
	got := make([]byte, len(payload))
	if n, err := f.ReadAt(ctx, file.ID, got, 0); err != nil || n != len(got) || !bytes.Equal(got, payload) {
		t.Fatal(n, err)
	}
	node, _, err := f.load(ctx, file.ID)
	if err != nil || string(node.xattrs["security.capability"]) != "\x01\x00cap" {
		t.Fatal("xattrs", err)
	}
	link, err := f.Lookup(ctx, root.ID, "link")
	if err != nil {
		t.Fatal(err)
	}
	target, err := f.Readlink(ctx, link.ID)
	if err != nil || string(target) != "dir/file" {
		t.Fatal(string(target), err)
	}
	pipe, err := f.Lookup(ctx, root.ID, "pipe")
	if err != nil || pipe.Kind != FSFIFO {
		t.Fatal(pipe, err)
	}
	if err = ImportFilesystemTar(ctx, store, bytes.NewReader(archive)); !errors.Is(err, syscall.EEXIST) {
		t.Fatal("overwrote root", err)
	}
}

type failAtEnd struct{ source *bytes.Reader }

func (r failAtEnd) Read(p []byte) (int, error) {
	n, e := r.source.Read(p)
	if e == io.EOF {
		return n, errors.New("archive checksum mismatch")
	}
	return n, e
}
func TestImportRejectsUnsafeOrUnverifiedArchive(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"../escape", "/absolute", "a/../escape"} {
		raw := importArchive(t, []*tar.Header{{Name: name, Typeflag: tar.TypeReg, Mode: 0600}}, nil)
		store := NewMemoryFilesystemStore()
		if err := ImportFilesystemTar(ctx, store, bytes.NewReader(raw)); err == nil {
			t.Fatal("unsafe path accepted", name)
		}
		if _, _, err := store.Get(ctx, FilesystemRootID()); !errors.Is(err, ErrNotFound) {
			t.Fatal("failed import published root", err)
		}
	}
	raw := importArchive(t, []*tar.Header{{Name: "file", Typeflag: tar.TypeReg, Mode: 0644}}, nil)
	store := NewMemoryFilesystemStore()
	if err := ImportFilesystemTar(ctx, store, failAtEnd{bytes.NewReader(raw)}); err == nil {
		t.Fatal("unverified input published")
	}
	if _, _, err := store.Get(ctx, FilesystemRootID()); !errors.Is(err, ErrNotFound) {
		t.Fatal("unverified root exists", err)
	}
}

type importWriteProbe struct {
	*MemoryFilesystemStore
	current, peak atomic.Int32
	fail          bool
}

func (s *importWriteProbe) CAS(ctx context.Context, id ObjectID, off uint64, old, data []byte, opt CASOptions) (ObjectInfo, error) {
	if len(data) > fsInlineSize {
		active := s.current.Add(1)
		defer s.current.Add(-1)
		for previous := s.peak.Load(); active > previous && !s.peak.CompareAndSwap(previous, active); previous = s.peak.Load() {
		}
		time.Sleep(time.Millisecond)
		if s.fail {
			return ObjectInfo{}, syscall.ENOSPC
		}
	}
	if id == FilesystemRootID() && s.current.Load() != 0 {
		return ObjectInfo{}, errors.New("root published before payload completion")
	}
	return s.MemoryFilesystemStore.CAS(ctx, id, off, old, data, opt)
}
func TestImportWaitsForPayloadsAndRejectsFailedWrites(t *testing.T) {
	data := bytes.Repeat([]byte("p"), 10*FSObjectSize)
	raw := importArchive(t, []*tar.Header{{Name: "large", Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(data))}}, map[string][]byte{"large": data})
	for _, fail := range []bool{false, true} {
		store := &importWriteProbe{MemoryFilesystemStore: NewMemoryFilesystemStore(), fail: fail}
		err := ImportFilesystemTar(context.Background(), store, bytes.NewReader(raw))
		if fail {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatal(err)
			}
			if _, _, err = store.Get(context.Background(), FilesystemRootID()); !errors.Is(err, ErrNotFound) {
				t.Fatal("failed import published root", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if store.current.Load() != 0 || store.peak.Load() > 8 {
			t.Fatal("unfinished/unbounded payload writes", store.current.Load(), store.peak.Load())
		}
	}
}
