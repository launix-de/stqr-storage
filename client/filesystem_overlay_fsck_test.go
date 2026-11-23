// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
)

func TestOverlayFsckCollectsOnlyUpperObjects(t *testing.T) {
	ctx := context.Background()
	_, o, guard, upper, conf, dir := overlayFixture(t, nil)
	mode := uint32(0600)
	if _, err := o.SetAttributes(ctx, conf.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if _, err := o.WriteAt(ctx, conf.ID, []byte("patch"), 2); err != nil {
		t.Fatal(err)
	}
	file, err := o.Create(ctx, dir.ID, "garbage", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.WriteAt(ctx, file.ID, []byte("payload"), 0); err != nil {
		t.Fatal(err)
	}
	if err = o.Unlink(ctx, dir.ID, "garbage"); err != nil {
		t.Fatal(err)
	}
	if err = o.Rmdir(ctx, FilesystemRootID(), "fop-files"); err != nil {
		t.Fatal(err)
	}
	want := readOverlayFile(t, o, conf.ID)
	baseBefore := len(guard.objects)
	store := &fsckMemory{MemoryFilesystemStore: upper}
	result, err := checkOverlayFilesystem(ctx, o, store)
	if err != nil || result.Deleted == 0 {
		t.Fatal(result, err)
	}
	if string(readOverlayFile(t, o, conf.ID)) != string(want) {
		t.Fatal("live overlay changed")
	}
	if len(guard.objects) != baseBefore || guard.writes != 0 {
		t.Fatal("lower bucket mutated")
	}
	result, err = checkOverlayFilesystem(ctx, o, store)
	if err != nil || result.Deleted != 0 {
		t.Fatal(result, err)
	}
	if _, err = checkFilesystem(ctx, store); !errors.Is(err, ErrOverlayNeedsLower) {
		t.Fatal("standalone fsck accepted overlay", err)
	}
}

func TestOverlayFsckAbortsOnStaleLower(t *testing.T) {
	ctx := context.Background()
	lower, o, _, upper, conf, _ := overlayFixture(t, nil)
	mode := uint32(0600)
	if _, err := o.SetAttributes(ctx, conf.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if _, err := o.upper.putImmutable(ctx, []byte("unreachable")); err != nil {
		t.Fatal(err)
	}
	if _, err := lower.WriteAt(ctx, conf.ID, []byte("changed"), 0); err != nil {
		t.Fatal(err)
	}
	before := len(upper.objects)
	result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if !errors.Is(err, syscall.ESTALE) || result.Deleted != 0 || len(upper.objects) != before {
		t.Fatal(result, err)
	}
}

func TestOverlayMissingUpperNeverFallsBackToOldBase(t *testing.T) {
	ctx := context.Background()
	_, o, _, upper, conf, _ := overlayFixture(t, nil)
	if _, err := o.WriteAt(ctx, conf.ID, []byte("local"), 0); err != nil {
		t.Fatal(err)
	}
	delete(upper.objects, conf.ID)
	if _, err := o.Lookup(ctx, FilesystemRootID(), "conf.json"); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing upper silently fell back", err)
	}
	if _, err := o.ReadAt(ctx, conf.ID, make([]byte, 4), 0); !errors.Is(err, ErrNotFound) {
		t.Fatal("read silently fell back", err)
	}
	before := len(upper.objects)
	result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if err == nil || result.Deleted != 0 || len(upper.objects) != before {
		t.Fatal(result, err)
	}
}

func TestOverlayFsckRemovesRedundantWhiteoutsAfterLowerMaintenance(t *testing.T) {
	ctx := context.Background()
	lower, o, guard, upper, _, dir := overlayFixture(t, nil)
	root := FilesystemRootID()
	if err := o.Unlink(ctx, root, "conf.json"); err != nil {
		t.Fatal(err)
	}
	if err := o.Unlink(ctx, root, "code.php"); err != nil {
		t.Fatal(err)
	}
	if err := lower.Unlink(ctx, root, "conf.json"); err != nil {
		t.Fatal(err)
	}
	local, err := o.Create(ctx, dir.ID, "local", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Unlink(ctx, dir.ID, "local"); err != nil {
		t.Fatal(err)
	}
	beforeBase := len(guard.objects)
	result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if err != nil || result.WhiteoutsRemoved != 2 {
		t.Fatal(result, err)
	}
	s, _, _, err := o.state(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := s.dirs[root].entries["conf.json"]; exists {
		t.Fatal("redundant deletion retained")
	}
	if id, exists := s.dirs[root].entries["code.php"]; !exists || id != (ObjectID{}) {
		t.Fatal("still needed whiteout removed")
	}
	if _, exists := s.dirs[dir.ID].entries["local"]; exists {
		t.Fatal("upper-only deletion marker retained")
	}
	if _, _, err = upper.Get(ctx, local.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("unreferenced upper inode retained", err)
	}
	if len(guard.objects) != beforeBase || guard.writes != 0 {
		t.Fatal("fsck modified lower")
	}
	// Re-creation after the reconciliation is new content and is inherited.
	fresh, err := lower.Create(ctx, root, "conf.json", 0644, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := o.Lookup(ctx, root, "conf.json"); err != nil || info.ID != fresh.ID {
		t.Fatal("new lower entry remained hidden", info, err)
	}
	result, err = checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if err != nil || result.WhiteoutsRemoved != 0 || result.Deleted != 0 {
		t.Fatal(result, err)
	}
}

func TestOverlayFsckKeepsWhiteoutWhenLowerNameAlreadyRecreated(t *testing.T) {
	ctx := context.Background()
	lower, o, _, upper, _, _ := overlayFixture(t, nil)
	root := FilesystemRootID()
	if err := o.Unlink(ctx, root, "conf.json"); err != nil {
		t.Fatal(err)
	}
	if err := lower.Unlink(ctx, root, "conf.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := lower.Create(ctx, root, "conf.json", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if err != nil || result.WhiteoutsRemoved != 0 {
		t.Fatal(result, err)
	}
	if _, err := o.Lookup(ctx, root, "conf.json"); !errors.Is(err, syscall.ENOENT) {
		t.Fatal("existing lower name exposed", err)
	}
}

func TestOverlayFsckDoesNotDeleteLocalDataAfterLowerDirectoryReplacement(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("recreate-%t", recreate), func(t *testing.T) {
			ctx := context.Background()
			lower, o, guard, upper, _, dir := overlayFixture(t, nil)
			root := FilesystemRootID()
			local, err := o.Create(ctx, dir.ID, "local", 0600, 0, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = o.WriteAt(ctx, local.ID, []byte("must survive lower maintenance"), 0); err != nil {
				t.Fatal(err)
			}
			if err = lower.Rmdir(ctx, root, "fop-files"); err != nil {
				t.Fatal(err)
			}
			if recreate {
				if _, err = lower.Create(ctx, root, "fop-files", 0755, 0, 0, true); err != nil {
					t.Fatal(err)
				}
			}
			before := len(upper.objects)
			rootBefore, infoBefore, err := upper.Get(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
			if !errors.Is(err, syscall.ESTALE) || result.Deleted != 0 || len(upper.objects) != before {
				t.Fatalf("local data swept: %+v %v", result, err)
			}
			rootAfter, infoAfter, err := upper.Get(ctx, root)
			if err != nil || infoBefore.Version != infoAfter.Version || !bytes.Equal(rootBefore, rootAfter) {
				t.Fatal("manifest modified on unsafe reconciliation", err)
			}
			if data := readTestFile(t, o.upper, local.ID); string(data) != "must survive lower maintenance" {
				t.Fatal(string(data))
			}
			if guard.writes != 0 {
				t.Fatal("lower writes", guard.writes)
			}
		})
	}
}

func TestOverlayFsckKeepsIndependentUpperFileWhenLowerDeleted(t *testing.T) {
	ctx := context.Background()
	lower, o, _, upper, conf, _ := overlayFixture(t, nil)
	root := FilesystemRootID()
	if _, err := o.Truncate(ctx, conf.ID, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := o.WriteAt(ctx, conf.ID, []byte("local configuration"), 0); err != nil {
		t.Fatal(err)
	}
	if err := lower.Unlink(ctx, root, "conf.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper}); err != nil {
		t.Fatal(err)
	}
	if data := readOverlayFile(t, o, conf.ID); string(data) != "local configuration" {
		t.Fatal(string(data))
	}
}

func TestOverlayFsckPrunesOpaqueDirectoryWhiteouts(t *testing.T) {
	ctx := context.Background()
	_, o, _, upper, _, _ := overlayFixture(t, nil)
	dir, err := o.Create(ctx, FilesystemRootID(), "private", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.Create(ctx, dir.ID, "tmp", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	if err = o.Unlink(ctx, dir.ID, "tmp"); err != nil {
		t.Fatal(err)
	}
	result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if err != nil || result.WhiteoutsRemoved != 1 || result.DirectoriesPruned != 0 {
		t.Fatal(result, err)
	}
	s, _, _, err := o.state(ctx)
	if err != nil || len(s.dirs[dir.ID].entries) != 0 {
		t.Fatal(err)
	}
}

type fsckFailLowerDirectory struct {
	FilesystemStore
	rootReads int
}

func (s *fsckFailLowerDirectory) Get(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	if id == FilesystemRootID() {
		s.rootReads++
		if s.rootReads > 1 {
			return nil, ObjectInfo{}, io.ErrUnexpectedEOF
		}
	}
	return s.FilesystemStore.Get(ctx, id)
}

func TestOverlayFsckDoesNotTreatLowerReadErrorAsAbsence(t *testing.T) {
	ctx := context.Background()
	_, o, guard, upper, _, _ := overlayFixture(t, nil)
	if err := o.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	o.lower.store = &fsckFailLowerDirectory{FilesystemStore: guard}
	before := len(upper.objects)
	body, info, err := upper.Get(ctx, FilesystemRootID())
	if err != nil {
		t.Fatal(err)
	}
	result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if !errors.Is(err, io.ErrUnexpectedEOF) || result.Deleted != 0 || result.WhiteoutsRemoved != 0 || len(upper.objects) != before {
		t.Fatal(result, err)
	}
	after, afterInfo, err := upper.Get(ctx, FilesystemRootID())
	if err != nil || afterInfo.Version != info.Version || !bytes.Equal(body, after) {
		t.Fatal("failed verification changed manifest", err)
	}
}

func TestOverlayFsckPreservesUpperWhenOfflineLowerGcLostSource(t *testing.T) {
	ctx := context.Background()
	lower, o, guard, upper, conf, _ := overlayFixture(t, nil)
	mode := uint32(0600)
	if _, err := o.SetAttributes(ctx, conf.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if err := lower.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	// Updating the persistent link count changes the source inode version.
	// Its payload remains stored, but pinned overlay references are now stale.
	if _, err := o.ReadAt(ctx, conf.ID, make([]byte, 1), 0); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	// A separate lower fsck cannot discover dependencies in an offline upper.

	if _, err := checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: guard.MemoryFilesystemStore}); err != nil {
		t.Fatal(err)
	}
	before := len(upper.objects)
	result, err := checkOverlayFilesystem(ctx, o, &fsckMemory{MemoryFilesystemStore: upper})
	if !errors.Is(err, ErrNotFound) || result.Deleted != 0 || result.WhiteoutsRemoved != 0 || len(upper.objects) != before {
		t.Fatal("missing lower source caused upper deletion", result, err)
	}
}
