// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
)

func TestFilesystemPOSIXLinksAndAttributes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryFilesystemStore()
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	root := FilesystemRootID()
	file, err := f.Create(ctx, root, "file", 0640, 1000, 1000, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, file.ID, []byte("shared"), 0); err != nil {
		t.Fatal(err)
	}
	alias, err := f.Link(ctx, root, "alias", file.ID)
	if err != nil || alias.Nlink != 2 || alias.ID != file.ID {
		t.Fatal(alias, err)
	}
	if err = f.Unlink(ctx, root, "file"); err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat(ctx, file.ID)
	if err != nil || stat.Nlink != 1 {
		t.Fatal(stat, err)
	}
	target, err := f.Create(ctx, root, "target", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Link(ctx, root, "target-other", target.ID); err != nil {
		t.Fatal(err)
	}
	if err = f.RenameWithFlags(ctx, root, "alias", root, "target", 0); err != nil {
		t.Fatal(err)
	}
	stat, err = f.Stat(ctx, target.ID)
	if err != nil || stat.Nlink != 1 {
		t.Fatal(stat, err)
	}
	sym, err := f.Symlink(ctx, root, "relative", "target", 1000, 1000)
	if err != nil || sym.Kind != FSSymlink {
		t.Fatal(sym, err)
	}
	got, err := f.Readlink(ctx, sym.ID)
	if err != nil || string(got) != "target" {
		t.Fatal(string(got), err)
	}
	if _, err = f.Symlink(ctx, root, "bad", "a\x00b", 0, 0); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	for _, kind := range []FSKind{FSFIFO, FSSocket} {
		if _, err = f.Mknod(ctx, root, fmt.Sprint(kind), kind, 0600, 1000, 1000); err != nil {
			t.Fatal(err)
		}
	}
	large := bytes.Repeat([]byte{0, 0xff, 0x42}, 20000)
	if err = f.SetXattr(ctx, file.ID, "user.binary", large, 1); err != nil {
		t.Fatal(err)
	}
	if err = f.SetXattr(ctx, file.ID, "user.binary", nil, 1); !errors.Is(err, syscall.EEXIST) {
		t.Fatal(err)
	}
	if err = f.SetXattr(ctx, file.ID, "user.absent", nil, 2); !errors.Is(err, syscall.ENODATA) {
		t.Fatal(err)
	}
	if err = f.SetXattr(ctx, file.ID, "user.empty", nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, file.ID, []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	got, err = f.GetXattr(ctx, file.ID, "user.binary")
	if err != nil || !bytes.Equal(got, large) {
		t.Fatal(len(got), err)
	}
	if _, err = f.GetXattr(ctx, file.ID, "user.empty"); err != nil {
		t.Fatal(err)
	}
	if err = f.RemoveXattr(ctx, file.ID, "user.empty"); err != nil {
		t.Fatal(err)
	}
	if err = f.SetXattr(ctx, sym.ID, "user.no", nil, 0); !errors.Is(err, syscall.EPERM) {
		t.Fatal(err)
	}
	if err = f.SetXattr(ctx, file.ID, "system.posix_acl_access", nil, 0); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
	if _, err = checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store}); err != nil {
		t.Fatal(err)
	}
	if err = f.Unlink(ctx, root, "target"); err != nil {
		t.Fatal(err)
	}
	stat, err = f.Stat(ctx, file.ID)
	if err != nil || stat.Nlink != 0 {
		t.Fatal(stat, err)
	}
	if _, err = f.Link(ctx, root, "resurrect", file.ID); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	// The open/unlinked object remains usable until an offline collection.
	if _, err = f.WriteAt(ctx, file.ID, []byte("!"), 0); err != nil {
		t.Fatal(err)
	}
}
func TestFilesystemConcurrentHardlinks(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryFilesystemStore()
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	root := FilesystemRootID()
	file, err := f.Create(ctx, root, "original", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			other, e := OpenFilesystem(ctx, store, FSOptions{})
			if e != nil {
				t.Error(e)
				return
			}
			for n := 0; n < 8; n++ {
				name := fmt.Sprintf("link-%d-%d", worker, n)
				if _, e = other.Link(ctx, root, name, file.ID); e != nil {
					t.Error(e)
					return
				}
				if e = other.Unlink(ctx, root, name); e != nil {
					t.Error(e)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	stat, err := f.Stat(ctx, file.ID)
	if err != nil || stat.Nlink != 1 {
		t.Fatal(stat, err)
	}
	if _, err = checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store}); err != nil {
		t.Fatal(err)
	}
}

func TestFilesystemFsckRejectsWrongLinkCountBeforeSweep(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryFilesystemStore()
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	file, err := f.Create(ctx, FilesystemRootID(), "file", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	s, err := f.snapshot(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	setFSLinks(&s.node, 2)
	if _, err = store.CAS(ctx, file.ID, 0, s.raw, encodeFSNode(s.node), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.putImmutable(ctx, []byte("unreachable but must survive failed validation")); err != nil {
		t.Fatal(err)
	}
	before := len(store.objects)
	result, err := checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store})
	if !errors.Is(err, ErrFSFormat) || result.Deleted != 0 || len(store.objects) != before {
		t.Fatal(result, err)
	}
}
func TestFilesystemDirectoryLinkCounts(t *testing.T) {
	ctx := context.Background()
	f, err := OpenFilesystem(ctx, NewMemoryFilesystemStore(), FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	root := FilesystemRootID()
	a, err := f.Create(ctx, root, "a", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.Create(ctx, root, "b", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := f.Stat(ctx, root); err != nil || info.Nlink != 4 {
		t.Fatal(info, err)
	}
	if _, err = f.Create(ctx, a.ID, "child", 0755, 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if err = f.RenameWithFlags(ctx, a.ID, "child", b.ID, "child", 0); err != nil {
		t.Fatal(err)
	}
	if info, err := f.Stat(ctx, a.ID); err != nil || info.Nlink != 2 {
		t.Fatal(info, err)
	}
	if info, err := f.Stat(ctx, b.ID); err != nil || info.Nlink != 3 {
		t.Fatal(info, err)
	}
}

func TestFilesystemHardlinkCrashRecovery(t *testing.T) {
	for _, operation := range []string{"link", "unlink"} {
		for _, after := range []bool{false, true} {
			for at := 1; at <= 12; at++ {
				t.Run(fmt.Sprintf("%s/after-%t/cas-%d", operation, after, at), func(t *testing.T) {
					ctx := context.Background()
					f, store := newTestFS(t)
					root := FilesystemRootID()
					file, err := f.Create(ctx, root, "original", 0600, 0, 0, false)
					if err != nil {
						t.Fatal(err)
					}
					if operation == "unlink" {
						if _, err = f.Link(ctx, root, "alias", file.ID); err != nil {
							t.Fatal(err)
						}
					}
					crash := &fsCrashStore{FilesystemStore: store, at: at, after: after}
					f.store = crash
					if operation == "link" {
						_, err = f.Link(ctx, root, "alias", file.ID)
					} else {
						err = f.Unlink(ctx, root, "alias")
					}
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					recovered, err := OpenFilesystem(ctx, store, FSOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if err = recovered.recoverNamespace(ctx); err != nil {
						t.Fatal(err)
					}
					entries, err := recovered.ReadDir(ctx, root)
					if err != nil {
						t.Fatal(err)
					}
					var refs uint32
					for _, entry := range entries {
						if entry.ID == file.ID {
							refs++
						}
					}
					info, err := recovered.Stat(ctx, file.ID)
					if err != nil || info.Nlink != refs || refs == 0 || refs > 2 {
						t.Fatal(info, refs, err)
					}
					if _, err = checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store}); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestOverlayHardlinksCannotBypassWhitelist(t *testing.T) {
	ctx := context.Background()
	t.Run("existing-lower-alias", func(t *testing.T) {
		lower, o, _, upper, conf, _ := overlayFixture(t, []string{"conf.json"})
		if _, err := lower.Link(ctx, FilesystemRootID(), "protected.php", conf.ID); err != nil {
			t.Fatal(err)
		}
		before := len(upper.objects)
		if _, err := o.WriteAt(ctx, conf.ID, []byte("bad"), 0); err == nil {
			t.Fatal("copied up an aliased lower inode")
		}
		if len(upper.objects) != before {
			t.Fatal("failed copy-up modified the upper")
		}
		if data := readOverlayFile(t, o, conf.ID); string(data) != "base-config" {
			t.Fatal(string(data))
		}
	})
	t.Run("alias-added-after-copy-up", func(t *testing.T) {
		lower, o, _, _, conf, _ := overlayFixture(t, []string{"conf.json"})
		if _, err := o.WriteAt(ctx, conf.ID, []byte("local"), 0); err != nil {
			t.Fatal(err)
		}
		// Even fully independent upper content must not become the contents of
		// a newly introduced, protected lower name that happens to alias its ID.
		if _, err := o.Truncate(ctx, conf.ID, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := o.WriteAt(ctx, conf.ID, []byte("private"), 0); err != nil {
			t.Fatal(err)
		}
		if _, err := lower.Link(ctx, FilesystemRootID(), "protected.php", conf.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := o.Lookup(ctx, FilesystemRootID(), "protected.php"); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal("ambiguous lower alias did not fail closed", err)
		}
		data := make([]byte, len("base-config"))
		if _, err := lower.ReadAt(ctx, conf.ID, data, 0); err != nil || string(data) != "base-config" {
			t.Fatal(string(data), err)
		}
	})
}

func TestFilesystemRejectsLiveDirectoryWithZeroLinks(t *testing.T) {
	body, _ := encodeFSDirectory(nil)
	raw := encodeFSNode(fsNode{kind: fsDirectory, extended: true, links: 0, blob: fsBlob{length: uint64(len(body)), inline: body}})
	if _, err := decodeFSNode(raw); !errors.Is(err, ErrFSFormat) {
		t.Fatal(err)
	}
}

func TestOverlayPOSIXMetadataReads(t *testing.T) {
	ctx := context.Background()
	lower, o, guard, _, _, _ := overlayFixture(t, nil)
	root := FilesystemRootID()
	if err := lower.SetXattr(ctx, root, "user.base", []byte("directory metadata"), 0); err != nil {
		t.Fatal(err)
	}
	if value, err := o.GetXattr(ctx, root, "user.base"); err != nil || string(value) != "directory metadata" {
		t.Fatal(string(value), err)
	}
	if _, err := o.GetXattr(ctx, root, "system.posix_acl_access"); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
	dir, err := o.Create(ctx, root, "private", 0700, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if names, err := o.ListXattr(ctx, dir.ID); err != nil || len(names) != 0 {
		t.Fatal(names, err)
	}
	if _, err := o.GetXattr(ctx, dir.ID, "user.absent"); !errors.Is(err, syscall.ENODATA) {
		t.Fatal(err)
	}
	sym, err := lower.Symlink(ctx, root, "link", "conf.json", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if target, err := o.Readlink(ctx, sym.ID); err != nil || string(target) != "conf.json" {
		t.Fatal(string(target), err)
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.writes != 0 {
		t.Fatal("metadata reads attempted lower writes")
	}
}
