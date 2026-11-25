// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func ticket32LiveObjects(store *MemoryFilesystemStore) (int, uint64) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	var size uint64
	for _, object := range store.objects {
		size += uint64(len(object.data))
	}
	return len(store.objects), size
}

func TestTicket32OnlineReclamationWithoutRetryHistory(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	for cycle := 0; cycle < 100; cycle++ {
		file := createTestFile(t, f, "online")
		for write := 0; write < 10; write++ {
			if _, err := f.WriteAt(ctx, file.ID, bytes.Repeat([]byte{byte(write)}, 8192), 0); err != nil {
				t.Fatal(err)
			}
			count, _ := ticket32LiveObjects(store)
			if count > 5 {
				t.Fatalf("write retained a descriptor or replacement allocation: %d", count)
			}
		}
		first, err := f.Retain(ctx, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		other, err := OpenFilesystem(ctx, store, FSOptions{})
		if err != nil {
			t.Fatal(err)
		}
		last, err := other.Retain(ctx, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.Unlink(ctx, FilesystemRootID(), "online"); err != nil {
			t.Fatal(err)
		}
		if err = f.Release(ctx, file.ID, first); err != nil {
			t.Fatal(err)
		}
		if _, _, err = store.Get(ctx, file.ID); err != nil {
			t.Fatal("nonfinal close reclaimed inode", err)
		}
		if err = other.Release(ctx, file.ID, last); err != nil {
			t.Fatal(err)
		}
		if _, _, err = store.Get(ctx, file.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal("last close retained inode", err)
		}
		count, _ := ticket32LiveObjects(store)
		if count != 2 {
			t.Fatalf("cycle %d retained objects beyond filesystem and orphan roots: %d", cycle, count)
		}
		store.mu.RLock()
		refs, claims := len(store.references), len(store.claims)
		store.mu.RUnlock()
		if refs != 0 || claims != 0 {
			t.Fatalf("cycle %d retained reference metadata: %d/%d", cycle, refs, claims)
		}
	}
}

func TestTicket32DecisionPinDrainsDelayedHelper(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f, store := newTestFS(t)
	id, _ := pendingTestTransaction(t, f)
	delayed := NewRequestID()
	if err := store.PinObject(ctx, id, delayed); err != nil {
		t.Fatal(err)
	}
	defer store.UnpinObject(context.Background(), id, delayed)
	done := make(chan error, 1)
	go func() { _, err := f.Stat(ctx, FilesystemRootID()); done <- err }()
	for {
		raw, _, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > 6 && raw[6] != fsTxPending {
			break
		}
		select {
		case err := <-done:
			t.Fatal("helper completed while descriptor pinned", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	head, _, err := store.Get(ctx, FilesystemRootID())
	if err != nil || !bytes.Equal(head, fsLock(id)) {
		t.Fatal("published before helper pin drained", err)
	}
	if err = store.UnpinObject(ctx, id, delayed); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, _, err = store.Get(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatal("terminal decision retained", err)
	}
	mode := uint32(0600)
	if _, err = f.SetAttributes(ctx, FilesystemRootID(), FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if info, err := f.Stat(ctx, FilesystemRootID()); err != nil || info.Mode != mode {
		t.Fatal(info, err)
	}
}

type ticket32CrashStore struct {
	*MemoryFilesystemStore
	at, calls int
	after     bool
}

func (s *ticket32CrashStore) CAS(ctx context.Context, id ObjectID, offset uint64, before, after []byte, opt CASOptions) (ObjectInfo, error) {
	s.calls++
	if s.calls == s.at && !s.after {
		return ObjectInfo{}, context.Canceled
	}
	info, err := s.MemoryFilesystemStore.CAS(ctx, id, offset, before, after, opt)
	if s.calls == s.at && s.after {
		return ObjectInfo{}, context.Canceled
	}
	return info, err
}
func (s *ticket32CrashStore) Delete(ctx context.Context, id ObjectID, opt DeleteOptions) error {
	s.calls++
	if s.calls == s.at && !s.after {
		return context.Canceled
	}
	err := s.MemoryFilesystemStore.Delete(ctx, id, opt)
	if s.calls == s.at && s.after {
		return context.Canceled
	}
	return err
}

func TestTicket32ManagedCrashRecoveryWithoutReceipts(t *testing.T) {
	for _, after := range []bool{false, true} {
		for at := 1; at <= 16; at++ {
			t.Run(fmt.Sprintf("after-%v/%d", after, at), func(t *testing.T) {
				ctx := context.Background()
				f, store := newTestFS(t)
				file := createTestFile(t, f, "crash")
				before := bytes.Repeat([]byte("a"), 8192)
				next := bytes.Repeat([]byte("b"), 8192)
				if _, err := f.WriteAt(ctx, file.ID, before, 0); err != nil {
					t.Fatal(err)
				}
				f.store = &ticket32CrashStore{MemoryFilesystemStore: store, at: at, after: after}
				_, writeErr := f.WriteAt(ctx, file.ID, next, 0)
				if writeErr != nil && !errors.Is(writeErr, context.Canceled) {
					t.Fatal(writeErr)
				}
				reopened, err := OpenFilesystem(ctx, store, FSOptions{})
				if err != nil {
					t.Fatal(err)
				}
				got := readTestFile(t, reopened, file.ID)
				if !bytes.Equal(got, before) && !bytes.Equal(got, next) {
					t.Fatal("torn recovered contents")
				}
				if writeErr == nil && !bytes.Equal(got, next) {
					t.Fatal("successful overwrite lost")
				}
			})
		}
	}
}

type ticket32LostAckStore struct {
	*MemoryFilesystemStore
	phase string
	fired bool
}

func (s *ticket32LostAckStore) CAS(ctx context.Context, id ObjectID, offset uint64, before, after []byte, opt CASOptions) (ObjectInfo, error) {
	match := false
	if !s.fired && len(after) >= 7 {
		switch s.phase {
		case "create":
			match = string(after[:4]) == "STFT" && opt.Absent
		case "decision":
			match = string(after[:4]) == "STFT" && after[6] == fsTxCommitted
		case "marker":
			match = string(after[:4]) == "STFL"
		case "publish":
			match = len(before) >= 4 && string(before[:4]) == "STFL" && string(after[:4]) == "STFS"
		}
	}
	info, err := s.MemoryFilesystemStore.CAS(ctx, id, offset, before, after, opt)
	if match && err == nil {
		s.fired = true
		// The server committed, the ACK was lost, and the native replay sees
		// the current bytes rather than a retained historical CAS result.
		_, replayErr := s.MemoryFilesystemStore.CAS(ctx, id, offset, before, after, opt)
		return ObjectInfo{}, replayErr
	}
	return info, err
}

func TestTicket32ManagedAppendLostAckNeverDuplicates(t *testing.T) {
	for _, phase := range []string{"create", "marker", "decision", "publish"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			f, store := newTestFS(t)
			file := createTestFile(t, f, "append")
			if _, err := f.WriteAt(ctx, file.ID, []byte("initial"), 0); err != nil {
				t.Fatal(err)
			}
			fault := &ticket32LostAckStore{MemoryFilesystemStore: store, phase: phase}
			f.store = fault
			_, appendErr := f.Append(ctx, file.ID, []byte("-once"))
			if !fault.fired {
				t.Fatal("lost ACK fixture did not hit intended phase")
			}
			reopened, err := OpenFilesystem(ctx, store, FSOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got := readTestFile(t, reopened, file.ID)
			if appendErr == nil {
				if string(got) != "initial-once" {
					t.Fatalf("successful append duplicated or lost: %q", got)
				}
			} else if string(got) != "initial" && string(got) != "initial-once" {
				t.Fatalf("uncertain append duplicated: %q %v", got, appendErr)
			}
		})
	}
}

func TestTicket32LegacySharedBlocksWaitForGraphCollection(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	data := bytes.Repeat([]byte("shared"), 2048)
	dataID, err := f.putImmutable(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	indexID, err := f.putImmutable(ctx, encodeFSIndex([]fsSegment{{id: dataID, length: uint64(len(data))}}))
	if err != nil {
		t.Fatal(err)
	}
	files := make([]FSInfo, 3)
	for i := range files {
		files[i] = createTestFile(t, f, fmt.Sprint("legacy-", i))
		raw, _, err := store.Get(ctx, files[i].ID)
		if err != nil {
			t.Fatal(err)
		}
		n, err := decodeFSNode(raw)
		if err != nil {
			t.Fatal(err)
		}
		n.blob = fsBlob{root: indexID, depth: 1, length: uint64(len(data))}
		if _, err = store.CAS(ctx, files[i].ID, 0, raw, encodeFSNode(n), CASOptions{Scope: CASObject}); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.Unlink(ctx, FilesystemRootID(), "legacy-0"); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(ctx, files[1].ID, []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	if got := readTestFile(t, f, files[2].ID); !bytes.Equal(got, data) {
		t.Fatal("online retirement reclaimed shared legacy blocks")
	}
	if _, _, err = store.Get(ctx, indexID); err != nil {
		t.Fatal(err)
	}
}

func TestTicket32ReadOnlyLastCloseDefersToWriter(t *testing.T) {
	ctx := context.Background()
	writer, store := newTestFS(t)
	file := createTestFile(t, writer, "reader")
	if _, err := writer.WriteAt(ctx, file.ID, []byte("retained"), 0); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenFilesystem(ctx, store, FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	token, err := reader.Retain(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Unlink(ctx, FilesystemRootID(), "reader"); err != nil {
		t.Fatal(err)
	}
	if string(readTestFile(t, reader, file.ID)) != "retained" {
		t.Fatal("readonly open lost unlinked data")
	}
	if err = reader.Release(ctx, file.ID, token); err != nil {
		t.Fatal(err)
	}
	if err = writer.CollectOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.Get(ctx, file.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("writer did not collect released orphan", err)
	}
}

func TestTicket32OverlayDependencySurvivesOfflineLowerCollection(t *testing.T) {
	ctx := context.Background()
	lower, overlay, guard, upper, file, _ := overlayFixture(t, nil)
	want := bytes.Repeat([]byte("lower"), 2048)
	if _, err := lower.WriteAt(ctx, file.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	mode := uint32(0600)
	if _, err := overlay.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	deps, err := guard.MemoryFilesystemStore.ReadObjectDependencies(ctx)
	if err != nil || len(deps) != 1 || deps[0] != file.ID {
		t.Fatalf("missing persistent lower dependency: %v %v", deps, err)
	}
	if err := lower.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: guard.MemoryFilesystemStore}); err != nil {
		t.Fatal(err)
	}
	if got := readOverlayFile(t, overlay, file.ID); !bytes.Equal(got, want) {
		t.Fatal("lower collection reclaimed a still referenced upper source")
	}
	if _, err := checkOverlayFilesystem(ctx, overlay, &fsckMemory{MemoryFilesystemStore: upper}); err != nil {
		t.Fatal(err)
	}
}

func TestTicket32OfflineRecoveryClearsCrashedHoldersButKeepsDependencies(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	file := createTestFile(t, f, "crashed")
	if _, err := f.WriteAt(ctx, file.ID, bytes.Repeat([]byte("x"), 8192), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Retain(ctx, file.ID); err != nil {
		t.Fatal(err)
	} // holder crashes without Release
	if err := f.Unlink(ctx, FilesystemRootID(), "crashed"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(ctx, file.ID); err != nil {
		t.Fatal("crashed durable holder not retained", err)
	}
	if _, err := checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(ctx, file.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("offline recovery retained crashed open inode", err)
	}
	id, _ := pendingTestTransaction(t, f)
	if err := store.ClaimObjectRetirement(ctx, id, NewRequestID()); err != nil {
		t.Fatal(err)
	} // finalizer crashes
	if _, err := checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store}); err != nil {
		t.Fatal(err)
	}
	if info, err := f.Stat(ctx, FilesystemRootID()); err != nil || info.Mode != 0711 {
		t.Fatal("abandoned finalizer not recovered", info, err)
	}
}

type ticket32AnchorProofStore struct {
	*fsckMemory
	allowed bool
}

func (s *ticket32AnchorProofStore) CanReclaimDependencies(context.Context) (bool, error) {
	return s.allowed, nil
}

func TestTicket32OfflineAnchorLedgerEndsDeletedOwnerLifetime(t *testing.T) {
	ctx := context.Background()
	lower, overlay, guard, upper, file, _ := overlayFixture(t, nil)
	if _, err := lower.WriteAt(ctx, file.ID, bytes.Repeat([]byte("lower"), 2048), 0); err != nil {
		t.Fatal(err)
	}
	mode := uint32(0600)
	if _, err := overlay.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if err := overlay.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := upper.Get(ctx, file.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("unopened upper owner not retired", err)
	}
	deps, err := guard.ReadObjectDependencies(ctx)
	if err != nil || len(deps) != 1 {
		t.Fatal(deps, err)
	}
	proof := &ticket32AnchorProofStore{fsckMemory: &fsckMemory{MemoryFilesystemStore: upper}, allowed: false}
	if _, err := checkOverlayFilesystem(ctx, overlay, proof); err != nil {
		t.Fatal(err)
	}
	deps, err = guard.ReadObjectDependencies(ctx)
	if err != nil || len(deps) != 1 {
		t.Fatal("unproven family lost dependency", deps, err)
	}
	proof.allowed = true
	if _, err := checkOverlayFilesystem(ctx, overlay, proof); err != nil {
		t.Fatal(err)
	}
	deps, err = guard.ReadObjectDependencies(ctx)
	if err != nil || len(deps) != 0 {
		t.Fatal("proof-authorized collection retained ended dependency", deps, err)
	}
	_, entries, err := overlay.upper.lowerAnchorsSnapshot(ctx, false)
	if err != nil || len(entries) != 0 {
		t.Fatal("anchor token lost without compacting ledger", entries, err)
	}
	if err := lower.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := guard.Get(ctx, file.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("lower inode still held by ended upper lifetime", err)
	}
}

func TestTicket32OfflineAnchorLedgerReleasesFullyMaterializedOwner(t *testing.T) {
	ctx := context.Background()
	lower, overlay, guard, upper, file, _ := overlayFixture(t, nil)
	original := bytes.Repeat([]byte("a"), 8192)
	latest := bytes.Repeat([]byte("b"), 8192)
	if _, err := lower.WriteAt(ctx, file.ID, original, 0); err != nil {
		t.Fatal(err)
	}
	mode := uint32(0600)
	if _, err := overlay.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if _, err := overlay.WriteAt(ctx, file.ID, latest, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := checkOverlayFilesystem(ctx, overlay, &fsckMemory{MemoryFilesystemStore: upper}); err != nil {
		t.Fatal(err)
	}
	deps, err := guard.ReadObjectDependencies(ctx)
	if err != nil || len(deps) != 0 {
		t.Fatal("materialized owner retained lower dependency", deps, err)
	}
	if err := lower.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readOverlayFile(t, overlay, file.ID), latest) {
		t.Fatal("materialized data depended on freed lower")
	}
}

func TestTicket32InheritedAnchorWaitsForCloneFamilyProof(t *testing.T) {
	ctx := context.Background()
	lower, source, guard, sourceStore, file, _ := overlayFixture(t, nil)
	want := bytes.Repeat([]byte("snapshot"), 2048)
	if _, err := lower.WriteAt(ctx, file.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	mode := uint32(0600)
	if _, err := source.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	// Model the relevant ordinary object bytes copied by bucket COW. Generic
	// reference state belongs to the lower bucket and is not duplicated here.
	cloneStore := NewMemoryFilesystemStore()
	sourceStore.mu.RLock()
	for id, obj := range sourceStore.objects {
		cloneStore.objects[id] = fsMemoryObject{data: bytes.Clone(obj.data)}
	}
	sourceStore.mu.RUnlock()
	clone, err := OpenOverlayFilesystem(ctx, guard, cloneStore, OverlayOptions{LowerBucketID: BucketID{1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	proof := &ticket32AnchorProofStore{fsckMemory: &fsckMemory{MemoryFilesystemStore: sourceStore}, allowed: false}
	if _, err := checkOverlayFilesystem(ctx, source, proof); err != nil {
		t.Fatal(err)
	}
	if err := lower.Unlink(ctx, FilesystemRootID(), "conf.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: guard.MemoryFilesystemStore}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readOverlayFile(t, clone, file.ID), want) {
		t.Fatal("source cleanup reclaimed inherited clone data")
	}
	// Only the server's verified retirement of the last other clone permits
	// releasing the shared external anchor. Until then the ledger is retained.
	clone = nil
	cloneStore.objects = nil
	proof.allowed = true
	if _, err := checkOverlayFilesystem(ctx, source, proof); err != nil {
		t.Fatal(err)
	}
	if err := lower.CollectOrphans(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := guard.Get(ctx, file.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("retired clone family retained lower inode", err)
	}
}

func TestTicket32SegmentedOrphanQueueShrinksAfterCooperativeCloses(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	type held struct {
		id    ObjectID
		token ID
	}
	var files []held
	for i := 0; i < 160; i++ {
		name := fmt.Sprintf("held-%03d", i)
		file := createTestFile(t, f, name)
		if _, err := f.WriteAt(ctx, file.ID, bytes.Repeat([]byte("x"), 8192), 0); err != nil {
			t.Fatal(err)
		}
		token, err := f.Retain(ctx, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.Unlink(ctx, FilesystemRootID(), name); err != nil {
			t.Fatal(err)
		}
		files = append(files, held{id: file.ID, token: token})
	}
	s, ids, err := f.orphanSnapshot(ctx, false)
	if err != nil || len(ids) != len(files) || s.node.blob.root == (ObjectID{}) {
		t.Fatal("fixture did not create segmented orphan list", len(ids), err)
	}
	for _, file := range files {
		if err := f.Release(ctx, file.id, file.token); err != nil {
			t.Fatal(err)
		}
	}
	count, _ := ticket32LiveObjects(store)
	if count != 2 {
		t.Fatalf("segmented orphan queue retained backing pages: %d", count)
	}
}

func TestTicket32LargeDirectoryBackingPagesRetireOnline(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	root := FilesystemRootID()
	dir := testDirectory(t, f, root, "large-dir")
	var names []string
	for i := 0; i < 160; i++ {
		name := fmt.Sprintf("long-directory-entry-%03d", i)
		names = append(names, name)
		if _, err := f.Create(ctx, dir.ID, name, 0600, 0, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	n, _, err := f.load(ctx, dir.ID)
	if err != nil || n.blob.length <= 8192 {
		t.Fatal("fixture lacks multiple 4K directory pages", n.blob.length, err)
	}
	for _, name := range names {
		if err := f.Unlink(ctx, dir.ID, name); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Rmdir(ctx, root, "large-dir"); err != nil {
		t.Fatal(err)
	}
	count, _ := ticket32LiveObjects(store)
	if count != 2 {
		t.Fatalf("directory data/index pages survived last unlink: %d", count)
	}
}

func TestTicket32VirtualDirectoryDependencyPreservesLowerMetadata(t *testing.T) {
	ctx := context.Background()
	lower, overlay, guard, upper, _, dir := overlayFixture(t, nil)
	local, err := overlay.Create(ctx, dir.ID, "local", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = overlay.WriteAt(ctx, local.ID, []byte("keep"), 0); err != nil {
		t.Fatal(err)
	}
	if err = lower.Rmdir(ctx, FilesystemRootID(), "fop-files"); err != nil {
		t.Fatal(err)
	}
	if _, err = checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: guard.MemoryFilesystemStore}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = guard.Get(ctx, dir.ID); err != nil {
		t.Fatal("lower collection removed referenced directory header", err)
	}
	before, _ := ticket32LiveObjects(upper)
	result, err := checkOverlayFilesystem(ctx, overlay, &fsckMemory{MemoryFilesystemStore: upper})
	if err == nil || result.Deleted != 0 {
		t.Fatal("changed lower ancestry was silently swept", result, err)
	}
	after, _ := ticket32LiveObjects(upper)
	if after != before {
		t.Fatal("failed reconciliation removed upper data")
	}
	if string(readOverlayFile(t, overlay, local.ID)) != "keep" {
		t.Fatal("upper local data lost")
	}
}

// The reclamation point is exclusive offline fsck, with all filesystem handles
// discarded. Delayed helpers and open/unlinked inodes must remain valid until
// that point; measuring only root-reachable objects would hide retained data.
func TestTicket32FilesystemOverwriteAndTruncateBoundedAfterCollection(t *testing.T) {
	ctx := context.Background()
	store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	file := createTestFile(t, f, "overwrite")
	want := bytes.Repeat([]byte("a"), 8192)
	if _, err = f.WriteAt(ctx, file.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	first, _, err := f.load(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := f.segmentsReadOnly(ctx, first.blob)
	if err != nil || len(parts) != 1 {
		t.Fatal(parts, err)
	}
	if _, err = checkFilesystem(ctx, store); err != nil {
		t.Fatal(err)
	}
	count, size := ticket32LiveObjects(store.MemoryFilesystemStore)
	for i := 0; i < 100; i++ {
		want = bytes.Repeat([]byte{byte(i)}, 8192)
		if _, err = f.WriteAt(ctx, file.ID, want, 0); err != nil {
			t.Fatal(i, err)
		}
		current, _, err := f.load(ctx, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		currentParts, err := f.segmentsReadOnly(ctx, current.blob)
		if err != nil || len(currentParts) != 1 || currentParts[0].id != parts[0].id || current.blob.root != first.blob.root {
			t.Fatalf("overwrite %d replaced live block identities: %v", i, err)
		}
		if !bytes.Equal(readTestFile(t, f, file.ID), want) {
			t.Fatal("overwrite contents differ", i)
		}
		if i%10 == 9 {
			// Simulate closing all mounts before the explicit collection step.
			f = nil
			result, err := checkFilesystem(ctx, store)
			if err != nil {
				t.Fatal(result, err)
			}
			gotCount, gotSize := ticket32LiveObjects(store.MemoryFilesystemStore)
			if gotCount != count || gotSize != size {
				t.Fatalf("collection after %d writes: live objects %d -> %d, bytes %d -> %d", i+1, count, gotCount, size, gotSize)
			}
			f, err = OpenFilesystem(ctx, store, FSOptions{})
			if err != nil || !bytes.Equal(readTestFile(t, f, file.ID), want) {
				t.Fatal("reopen after collection", err)
			}
		}
	}
	for i := 0; i < 100; i++ {
		if _, err = f.Truncate(ctx, file.ID, 6144); err != nil {
			t.Fatal(err)
		}
		if _, err = f.Truncate(ctx, file.ID, 8192); err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteAt(ctx, file.ID, want, 0); err != nil {
			t.Fatal(err)
		}
		if i%10 == 9 {
			f = nil
			if _, err = checkFilesystem(ctx, store); err != nil {
				t.Fatal(err)
			}
			gotCount, gotSize := ticket32LiveObjects(store.MemoryFilesystemStore)
			if gotCount != count || gotSize != size {
				t.Fatalf("truncate cycle %d retained live objects/payload: %d/%d, %d/%d", i+1, gotCount, count, gotSize, size)
			}
			f, err = OpenFilesystem(ctx, store, FSOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestTicket32FilesystemUnlinkLifetimeAndDeleteLoop(t *testing.T) {
	ctx := context.Background()
	store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	root := FilesystemRootID()
	for i := 0; i < 100; i++ {
		file := createTestFile(t, f, "remove")
		want := bytes.Repeat([]byte{byte(i)}, 8192)
		if _, err = f.WriteAt(ctx, file.ID, want, 0); err != nil {
			t.Fatal(err)
		}
		if _, err = f.Link(ctx, root, "retained-link", file.ID); err != nil {
			t.Fatal(err)
		}
		if err = f.Unlink(ctx, root, "remove"); err != nil {
			t.Fatal(err)
		}
		f = nil
		if _, err = checkFilesystem(ctx, store); err != nil {
			t.Fatal("collection must retain the hardlinked inode", err)
		}
		f, err = OpenFilesystem(ctx, store, FSOptions{})
		if err != nil {
			t.Fatal(err)
		}
		alias, err := f.Lookup(ctx, root, "retained-link")
		if err != nil || alias.ID != file.ID || !bytes.Equal(readTestFile(t, f, file.ID), want) {
			t.Fatal("collection damaged the surviving link", err)
		}
		reader, err := OpenFilesystem(ctx, store, FSOptions{})
		if err != nil {
			t.Fatal(err)
		}
		reference, err := reader.Retain(ctx, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.Unlink(ctx, root, "retained-link"); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(readTestFile(t, reader, file.ID), want) {
			t.Fatal("unlink reclaimed an inode still held by another mount")
		}
		if err := reader.Release(ctx, file.ID, reference); err != nil {
			t.Fatal(err)
		}
		// No namespace reference remains, and the last inode holder is closed.
		f, reader = nil, nil
		if _, err = checkFilesystem(ctx, store); err != nil {
			t.Fatal(err)
		}
		if _, _, err = store.Get(ctx, file.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal("unreferenced inode survived offline collection", err)
		}
		count, _ := ticket32LiveObjects(store.MemoryFilesystemStore)
		if count != 1 {
			t.Fatal("delete loop left live objects beyond the root", i, count)
		}
		f, err = OpenFilesystem(ctx, store, FSOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTicket32OverlayOverwriteCollectionPreservesLower(t *testing.T) {
	ctx := context.Background()
	lower, overlay, guard, upper, file, _ := overlayFixture(t, nil)
	wantLower := bytes.Repeat([]byte("lower"), 2048)
	if _, err := lower.WriteAt(ctx, file.ID, wantLower, 0); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("u"), len(wantLower))
	if _, err := overlay.WriteAt(ctx, file.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	store := &fsckMemory{MemoryFilesystemStore: upper}
	if _, err := checkOverlayFilesystem(ctx, overlay, store); err != nil {
		t.Fatal(err)
	}
	count, size := ticket32LiveObjects(upper)
	for i := 0; i < 100; i++ {
		want = bytes.Repeat([]byte{byte(i)}, len(wantLower))
		if _, err := overlay.WriteAt(ctx, file.ID, want, 0); err != nil {
			t.Fatal(err)
		}
		if i%10 == 9 {
			if _, err := checkOverlayFilesystem(ctx, overlay, store); err != nil {
				t.Fatal(err)
			}
			gotCount, gotSize := ticket32LiveObjects(upper)
			if gotCount != count || gotSize != size {
				t.Fatalf("overlay collection after %d writes: %d/%d objects, %d/%d bytes", i+1, gotCount, count, gotSize, size)
			}
			var err error
			overlay, err = OpenOverlayFilesystem(ctx, guard, upper, OverlayOptions{LowerBucketID: BucketID{1}})
			if err != nil || !bytes.Equal(readOverlayFile(t, overlay, file.ID), want) {
				t.Fatal("reopen upper", err)
			}
		}
	}
	if !bytes.Equal(readTestFile(t, lower, file.ID), wantLower) || guard.writes != 0 {
		t.Fatal("upper overwrite/collection modified the read-only lower")
	}
}
