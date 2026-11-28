// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// The queue is not a namespace entry or a COW dependency. It is nevertheless
// operational metadata: every rename-over-existing/unlink may append to it.
func TestFilesystemFsckInterruptedSweepPreservesOrphanQueueGraph(t *testing.T) {
	ctx := context.Background()
	store := &fsckMemory{MemoryFilesystemStore: NewMemoryFilesystemStore(), failAfter: 1}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]ObjectID, 160)
	for i := range ids {
		ids[i][0] = 0x80
		ids[i][30] = byte(i >> 8)
		ids[i][31] = byte(i + 1)
	}
	body := encodeFSOrphans(ids)
	indexID, dataID := ObjectID{1}, ObjectID{2}
	node := fsNode{kind: fsFile, extended: true, mode: 0600, links: 1, blob: fsBlob{length: uint64(len(body)), depth: 1, root: indexID, mutable: true}}
	index := encodeFSIndex([]fsSegment{{id: dataID, length: uint64(len(body))}})
	for id, raw := range map[ObjectID][]byte{indexID: index, dataID: body, filesystemOrphansID: encodeFSNode(node), ObjectID{3}: []byte("garbage one"), ObjectID{4}: []byte("garbage two")} {
		if _, err = store.CAS(ctx, id, 0, nil, raw, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, actual, err := f.orphanSnapshot(ctx, false); err != nil || len(actual) != len(ids) {
		t.Fatal("fixture queue invalid", err, len(actual))
	}
	result, err := checkFilesystem(ctx, store)
	if !errors.Is(err, context.Canceled) || result.Deleted != 1 {
		t.Fatal(result, err)
	}
	// Before the fix sorted sweep deleted index 01 before inode b5..., leaving
	// the queue head present but unusable when the offline sweep was cancelled.
	if _, actual, err := f.orphanSnapshot(ctx, false); err != nil || len(actual) != len(ids) {
		t.Fatal("partial sweep damaged operational orphan queue", err, len(actual))
	}
	for id, want := range map[ObjectID][]byte{indexID: index, dataID: body} {
		got, _, err := store.Get(ctx, id)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("orphan queue page changed", id, err)
		}
	}
	store.failAfter = 0
	if _, err = checkFilesystem(ctx, store); err != nil {
		t.Fatal("resumed sweep", err)
	}
	if _, actual, err := f.orphanSnapshot(ctx, false); err != nil || len(actual) != len(ids) {
		t.Fatal("finished sweep damaged queue", err, len(actual))
	}
}
