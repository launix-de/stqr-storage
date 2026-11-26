// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"fmt"
	"testing"
)

func TestDirectoryCacheDoesNotExposeMutableEntries(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	first := createTestFile(t, f, "first")
	entries, err := f.ReadDir(ctx, FilesystemRootID())
	if err != nil {
		t.Fatal(err)
	}
	entries[0].Name = "damaged"
	entries[0].ID = ObjectID{}
	if info, err := f.Lookup(ctx, FilesystemRootID(), "first"); err != nil || info.ID != first.ID {
		t.Fatal("caller mutated cached directory", info, err)
	}
	other, err := OpenFilesystem(ctx, store, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second := createTestFile(t, other, "second")
	if info, err := f.Lookup(ctx, FilesystemRootID(), "second"); err != nil || info.ID != second.ID {
		t.Fatal("reused an obsolete directory blob", info, err)
	}
	if err = f.Unlink(ctx, FilesystemRootID(), "first"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Lookup(ctx, FilesystemRootID(), "second"); err != nil {
		t.Fatal("directory editing corrupted cached snapshot", err)
	}
}
func TestDirectoryCacheIsBounded(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestFS(t)
	for i := 0; i < directoryCacheEntries*2; i++ {
		raw, err := encodeFSDirectory([]FSEntry{{Name: fmt.Sprintf("name-%d", i), ID: ObjectID{1}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.directoryView(ctx, fsNode{kind: fsDirectory, blob: fsBlob{length: uint64(len(raw)), inline: raw}}); err != nil {
			t.Fatal(err)
		}
	}
	f.directoryCache.mu.Lock()
	defer f.directoryCache.mu.Unlock()
	if f.directoryCache.used > directoryCacheBudget || len(f.directoryCache.entries) > directoryCacheEntries {
		t.Fatal("unbounded directory cache")
	}
}
func TestOverlayNegativeCacheSurvivesCopyUpAndUnlink(t *testing.T) {
	ctx := context.Background()
	_, first, guard, upper, conf, _ := overlayFixture(t, nil)
	second, err := OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}})
	if err != nil {
		t.Fatal(err)
	}
	// Establish that the first reader knows this inode has no upper counterpart.
	if _, err = first.Stat(ctx, conf.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = second.WriteAt(ctx, conf.ID, []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	openRef, err := first.Retain(ctx, conf.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release(ctx, conf.ID, openRef)
	if got := string(readOverlayFile(t, first, conf.ID)); got[:3] != "new" {
		t.Fatal("negative upper cache hid copy-up", got)
	}
	if err = second.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	if got := string(readOverlayFile(t, first, conf.ID)); got[:3] != "new" {
		t.Fatal("open unlinked upper inode fell back to lower", got)
	}
}
