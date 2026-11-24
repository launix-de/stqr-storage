// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"testing"
)

type overlayReadStore struct {
	*MemoryFilesystemStore
	mu     sync.Mutex
	writes int
	reads  map[ObjectID]int
}

func (s *overlayReadStore) Get(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	s.mu.Lock()
	s.reads[id]++
	s.mu.Unlock()
	return s.MemoryFilesystemStore.Get(ctx, id)
}
func (s *overlayReadStore) CAS(context.Context, ObjectID, uint64, []byte, []byte, CASOptions) (ObjectInfo, error) {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return ObjectInfo{}, ErrUnauthorized
}
func overlayFixture(t *testing.T, policy []string) (*Filesystem, *OverlayFilesystem, *overlayReadStore, *MemoryFilesystemStore, FSInfo, FSInfo) {
	t.Helper()
	ctx := context.Background()
	lower, base := newTestFS(t)
	conf := createTestFile(t, lower, "conf.json")
	if _, err := lower.WriteAt(ctx, conf.ID, []byte("base-config"), 0); err != nil {
		t.Fatal(err)
	}
	code := createTestFile(t, lower, "code.php")
	if _, err := lower.WriteAt(ctx, code.ID, []byte("<?php echo 42;"), 0); err != nil {
		t.Fatal(err)
	}
	dir := testDirectory(t, lower, FilesystemRootID(), "fop-files")
	guard := &overlayReadStore{MemoryFilesystemStore: base, reads: map[ObjectID]int{}}
	upper := NewMemoryFilesystemStore()
	o, err := OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{Create: true, LowerBucketID: BucketID{1}, Whitelist: policy})
	if err != nil {
		t.Fatal(err)
	}
	return lower, o, guard, upper, conf, dir
}
func readOverlayFile(t *testing.T, o *OverlayFilesystem, id ObjectID) []byte {
	t.Helper()
	info, err := o.Stat(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, int(info.Size))
	n, err := o.ReadAt(context.Background(), id, data, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatal(n, len(data))
	}
	return data
}
func TestOverlayMetadataAndRangeCopyUp(t *testing.T) {
	ctx := context.Background()
	lower, o, guard, upper, conf, _ := overlayFixture(t, nil)
	want := bytes.Repeat([]byte("large-base"), 350000)
	if _, err := lower.WriteAt(ctx, conf.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	n, _, err := lower.load(ctx, conf.ID)
	if err != nil {
		t.Fatal(err)
	}
	segments, err := lower.segments(ctx, n.blob)
	if err != nil {
		t.Fatal(err)
	}
	mode := uint32(0600)
	if _, err = o.SetAttributes(ctx, conf.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	for _, segment := range segments {
		if guard.reads[segment.id] != 0 {
			t.Fatal("chmod copied/read a payload chunk")
		}
	}
	if _, err = o.WriteAt(ctx, conf.ID, []byte("XX"), 1234567); err != nil {
		t.Fatal(err)
	}
	var stored uint64
	for _, object := range upper.objects {
		stored += uint64(len(object.data))
	}
	if stored > 64<<10 {
		t.Fatal("range write copied whole base", stored)
	}
	copy(want[1234567:], "XX")
	if got := readOverlayFile(t, o, conf.ID); !bytes.Equal(got, want) {
		t.Fatal("merged content differs")
	}
	if original := readTestFile(t, lower, conf.ID); bytes.Equal(original, want) {
		t.Fatal("lower modified")
	}
	if guard.writes != 0 {
		t.Fatal("readonly lower received writes", guard.writes)
	}
	if _, err = OpenFilesystem(ctx, upper, FSOptions{}); !errors.Is(err, ErrOverlayNeedsLower) {
		t.Fatal("upper mounted standalone", err)
	}
	if _, err = OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{2}}); err == nil {
		t.Fatal("wrong lower accepted")
	}
	reopened, err := OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}})
	if err != nil {
		t.Fatal(err)
	}
	if got := readOverlayFile(t, reopened, conf.ID); !bytes.Equal(got, want) {
		t.Fatal("reopen lost range overlay")
	}
	if info, err := reopened.Stat(ctx, conf.ID); err != nil || info.Mode != 0600 {
		t.Fatal(info, err)
	}
}
func TestOverlayWhitelistAndWhiteouts(t *testing.T) {
	ctx := context.Background()
	lower, o, guard, upper, conf, dir := overlayFixture(t, []string{"conf.json", "fop-files/"})
	root := FilesystemRootID()
	code, err := o.Lookup(ctx, root, "code.php")
	if err != nil {
		t.Fatal(err)
	}
	before := len(upper.objects)
	mode := uint32(0777)
	for _, operation := range []func() error{
		func() error { _, e := o.WriteAt(ctx, code.ID, []byte("bad"), 0); return e },
		func() error { _, e := o.Create(ctx, root, "evil.php", 0600, 0, 0, false); return e },
		func() error { return o.Unlink(ctx, root, "code.php") },
		func() error { _, e := o.SetAttributes(ctx, code.ID, FSAttributes{Mode: &mode}); return e },
		func() error { return o.Rename(ctx, root, "conf.json", root, "code.php") },
		func() error { return o.Rename(ctx, root, "code.php", dir.ID, "stolen.php") },
		func() error { _, e := o.Create(ctx, root, "conf.json", 0755, 0, 0, true); return e },
	} {
		if e := operation(); !errors.Is(e, syscall.EROFS) {
			t.Fatal("whitelist bypass", e)
		}
	}
	if len(upper.objects) != before {
		t.Fatal("denied operations stored objects")
	}
	if _, err = o.WriteAt(ctx, conf.ID, []byte("local"), 0); err != nil {
		t.Fatal(err)
	}
	file, err := o.Create(ctx, dir.ID, "upload", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.WriteAt(ctx, file.ID, []byte("uploaded"), 0); err != nil {
		t.Fatal(err)
	}
	if err = o.Unlink(ctx, root, "conf.json"); err != nil {
		t.Fatal(err)
	}
	if _, err = o.Lookup(ctx, root, "conf.json"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	if info, err := lower.Lookup(ctx, root, "conf.json"); err != nil || info.ID != conf.ID {
		t.Fatal("lower deletion", info, err)
	}
	// An already writable, now unlinked inode keeps working.
	if _, err = o.Append(ctx, conf.ID, []byte("tail")); err != nil {
		t.Fatal("open unlinked write", err)
	}
	if _, err = OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}, Whitelist: []string{"code.php"}}); err == nil {
		t.Fatal("policy silently widened")
	}
	reopened, err := OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.Lookup(ctx, root, "conf.json"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal("whiteout not persisted", err)
	}
	if _, err = reopened.WriteAt(ctx, code.ID, []byte("bad"), 0); !errors.Is(err, syscall.EROFS) {
		t.Fatal("policy lost on remount", err)
	}
	if guard.writes != 0 {
		t.Fatal(guard.writes)
	}
}
func TestOverlayDirectoriesAndRename(t *testing.T) {
	ctx := context.Background()
	lower, o, guard, upper, conf, dir := overlayFixture(t, nil)
	root := FilesystemRootID()
	base, err := lower.Create(ctx, dir.ID, "base", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Rmdir(ctx, root, "fop-files"); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal(err)
	}
	if err = o.Unlink(ctx, dir.ID, "base"); err != nil {
		t.Fatal(err)
	}
	if err = o.Rmdir(ctx, root, "fop-files"); err != nil {
		t.Fatal(err)
	}
	if _, err = o.Create(ctx, dir.ID, "stale", 0600, 0, 0, false); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	fresh, err := o.Create(ctx, root, "fop-files", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if entries, err := o.ReadDir(ctx, fresh.ID); err != nil || len(entries) != 0 {
		t.Fatal("recreated directory exposed lower", entries, err)
	}
	if err = o.Rename(ctx, root, "conf.json", fresh.ID, "moved"); err != nil {
		t.Fatal(err)
	}
	if got := readOverlayFile(t, o, conf.ID); string(got) != "base-config" {
		t.Fatal(string(got))
	}
	if err = o.Rename(ctx, root, "fop-files", fresh.ID, "cycle"); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	other, err := o.Create(ctx, root, "other", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.RenameWithFlags(ctx, fresh.ID, "moved", root, "code.php", FSRenameExchange); err != nil {
		t.Fatal(err)
	}
	if err = o.Rename(ctx, root, "fop-files", other.ID, "nested"); err != nil {
		t.Fatal(err)
	}
	if _, err = o.Lookup(ctx, root, "fop-files"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal(err)
	}
	reopened, err := OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.Lookup(ctx, other.ID, "nested"); err != nil {
		t.Fatal(err)
	}
	if _, err = lower.Stat(ctx, base.ID); err != nil {
		t.Fatal(err)
	}
}
func TestOverlayConcurrentNamespacesAndAppend(t *testing.T) {
	ctx := context.Background()
	_, o, guard, upper, _, dir := overlayFixture(t, []string{"fop-files/"})
	other, err := OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}})
	if err != nil {
		t.Fatal(err)
	}
	log, err := o.Create(ctx, dir.ID, "log", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			view := o
			if i%2 == 1 {
				view = other
			}
			for j := 0; j < 12; j++ {
				name := fmt.Sprintf("%d-%d", i, j)
				if _, e := view.Create(ctx, dir.ID, name, 0600, 0, 0, false); e != nil {
					t.Error(e)
					return
				}
				if _, e := view.Append(ctx, log.ID, []byte("x")); e != nil {
					t.Error(e)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	entries, err := o.ReadDir(ctx, dir.ID)
	if err != nil || len(entries) != 97 {
		t.Fatal(len(entries), err)
	}
	if got := readOverlayFile(t, o, log.ID); len(got) != 96 {
		t.Fatal(len(got))
	}
}
func TestOverlaySparseAndChangedBase(t *testing.T) {
	ctx := context.Background()
	lower, o, _, _, conf, _ := overlayFixture(t, nil)
	if _, err := lower.Truncate(ctx, conf.ID, 1<<40); err != nil {
		t.Fatal(err)
	}
	if _, err := o.WriteAt(ctx, conf.ID, []byte("Z"), (1<<40)-1); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := o.ReadAt(ctx, conf.ID, buf, (1<<40)-4); err != nil || !bytes.Equal(buf, []byte{0, 0, 0, 'Z'}) {
		t.Fatal(buf, err)
	}
	if _, err := lower.WriteAt(ctx, conf.ID, []byte("changed"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ReadAt(ctx, conf.ID, buf, 0); !errors.Is(err, syscall.ESTALE) {
		t.Fatal("stale lower silently read", err)
	}
}

func TestOverlayInfersExistingDirectoryWhitelist(t *testing.T) {
	ctx := context.Background()
	_, o, _, _, _, dir := overlayFixture(t, []string{"conf.json", "fop-files"})
	if _, err := o.Create(ctx, dir.ID, "allowed", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"", "/absolute", "../escape", "x/../code.php", "x//y", "x/./y", "x,", "conf.json, fop-files/"} {
		if _, err := ParseFilesystemWhitelist(p); err == nil {
			t.Fatal("invalid policy accepted", p)
		}
	}
}

func TestOverlayNamespaceCrashCutpoints(t *testing.T) {
	for _, operation := range []string{"create", "rmdir", "rename"} {
		for _, after := range []bool{false, true} {
			for at := 1; at <= 8; at++ {
				t.Run(fmt.Sprintf("%s/after-%t/cas-%d", operation, after, at), func(t *testing.T) {
					ctx := context.Background()
					_, o, guard, upper, conf, dir := overlayFixture(t, nil)
					// A segmented sparse manifest exercises payload/index/root cutpoints.
					if err := o.edit(ctx, func(s *overlayState) error {
						for i := 0; i < 150; i++ {
							s.dirs[FilesystemRootID()].entries[fmt.Sprintf("old-%03d", i)] = ObjectID{}
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					o.upper.store = &fsCrashStore{FilesystemStore: upper, at: at, after: after}
					var err error
					switch operation {
					case "create":
						_, err = o.Create(ctx, dir.ID, "new", 0600, 0, 0, false)
					case "rmdir":
						err = o.Rmdir(ctx, FilesystemRootID(), "fop-files")
					case "rename":
						err = o.Rename(ctx, FilesystemRootID(), "conf.json", dir.ID, "moved")
					}
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					reopened, err := OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}})
					if err != nil {
						t.Fatal(err)
					}
					switch operation {
					case "create":
						info, e := reopened.Lookup(ctx, dir.ID, "new")
						if e != nil && !errors.Is(e, syscall.ENOENT) {
							t.Fatal(e)
						}
						if e == nil {
							if _, e = reopened.Stat(ctx, info.ID); e != nil {
								t.Fatal("dangling create", e)
							}
						}
					case "rmdir":
						_, e := reopened.Lookup(ctx, FilesystemRootID(), "fop-files")
						if e != nil && !errors.Is(e, syscall.ENOENT) {
							t.Fatal(e)
						}
						if e == nil {
							if _, e = reopened.Create(ctx, dir.ID, "still-linked", 0600, 0, 0, false); e != nil {
								t.Fatal(e)
							}
						} else if _, e = reopened.Create(ctx, dir.ID, "stale", 0600, 0, 0, false); !errors.Is(e, syscall.ENOENT) {
							t.Fatal("stale removed directory writable", e)
						}
					case "rename":
						_, left := reopened.Lookup(ctx, FilesystemRootID(), "conf.json")
						moved, right := reopened.Lookup(ctx, dir.ID, "moved")
						if (left == nil) == (right == nil) {
							t.Fatal("partial move", left, right)
						}
						if right == nil && moved.ID != conf.ID {
							t.Fatal("inode changed")
						}
					}
					if _, err = checkOverlayFilesystem(ctx, reopened, &fsckMemory{MemoryFilesystemStore: upper}); err != nil {
						t.Fatal("cutpoint left uncollectible graph", err)
					}
					if guard.writes != 0 {
						t.Fatal("lower mutated", guard.writes)
					}
				})
			}
		}
	}
}

func TestOverlayRenameInheritedSymlinkPreservesInlineTarget(t *testing.T) {
	ctx := context.Background()
	lower, o, _, _, _, _ := overlayFixture(t, nil)
	link, err := lower.Symlink(ctx, FilesystemRootID(), "original-link", "conf.json", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Rename(ctx, FilesystemRootID(), "original-link", FilesystemRootID(), "moved-link"); err != nil {
		t.Fatal(err)
	}
	target, err := o.Readlink(ctx, link.ID)
	if err != nil || string(target) != "conf.json" {
		t.Fatal(string(target), err)
	}
}
