// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"testing"
)

func TestFencePersistsOnlyItsPublishedImageAcrossCrash(t *testing.T) {
	disk := newCrash(32 << 20)
	gate := &checkpointGateIO{VolumeIO: disk}
	v, err := Format(gate, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if _, err := testPut(v, testKey(1), []byte("confirmed"), Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := testPut(v, testKey(1), []byte("first visible"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	target := v.Header().Generation
	entered, release := gate.arm(nil)
	defer release()
	done := make(chan error, 1)
	go func() { done <- v.CheckpointThrough(target) }()
	<-entered
	if _, err := testPut(v, testKey(1), []byte("later visible"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := testPut(v, testKey(2), []byte("later new object"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	later := v.Header().Generation
	mustGet(t, v, testKey(1), []byte("later visible"))
	// No physical write of the later overlay is permitted while this fence
	// checkpoints the earlier image, even when the allocator reuses a page.
	old, err := Open(disk.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, old, testKey(1), []byte("confirmed"))
	old.Close()
	release()
	awaitCheckpointTest(t, done)
	if !v.IsDurableThrough(target) || v.IsDurableThrough(later) {
		t.Fatal("fence confirmed the wrong publication")
	}
	first, err := Open(disk.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, first, testKey(1), []byte("first visible"))
	if _, _, err := testGet(first, testKey(2)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("future write leaked into the checkpoint: %v", err)
	}
	first.Close()
	if err := v.CheckpointThrough(later); err != nil {
		t.Fatal(err)
	}
	final, err := Open(disk.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()
	mustGet(t, final, testKey(1), []byte("later visible"))
	mustGet(t, final, testKey(2), []byte("later new object"))
}

func TestBufferedReclaimBatchesReceiptsAndPreservesPayload(t *testing.T) {
	v, gate := newCheckpointGate(t)
	key := testKey(1)
	data := bytes.Repeat([]byte("alive"), 30)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	var ids []ID
	for i := uint64(1); i <= 128; i++ {
		id := testID(i)
		ids = append(ids, id)
		if _, err := h.Put(key, data, Options{RequestID: id, Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	if err := v.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats().SyncOps
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := h.ForgetRequestsAccepted(ids)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != before {
		t.Fatal("publishing cleanup synchronously flushed")
	}
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, ok, err := h.OperationStatus(id); err != nil || ok {
			t.Fatalf("cleanup not visible: %v %v", ok, err)
		}
	}
	h.Close()
	mustGet(t, v, key, data)
	entered, release := gate.arm(nil)
	defer release()
	done := make(chan error, 1)
	go func() { done <- v.CheckpointThrough(generation) }()
	<-entered
	if _, err := testPut(v, testKey(2), []byte("concurrent"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	mustGet(t, v, testKey(2), []byte("concurrent"))
	release()
	awaitCheckpointTest(t, done)
	if n := v.IOStats().SyncOps - before; n != 4 {
		t.Fatalf("128 metadata records required %d barriers, want one checkpoint", n)
	}
}

func TestMetadataFencePublishesBeforeDurableACK(t *testing.T) {
	v, gate := newCheckpointGate(t)
	id := testID(9)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("payload"), Options{RequestID: id}); err != nil {
		t.Fatal(err)
	}
	entered, release := gate.arm(nil)
	defer release()
	done := make(chan error, 1)
	go func() {
		h, err := v.Open(true)
		if err == nil {
			err = h.ForgetRequest(id)
			h.Close()
		}
		done <- err
	}()
	<-entered
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := h.OperationStatus(id)
	h.Close()
	if err != nil || found {
		t.Fatalf("metadata not published: %v %v", found, err)
	}
	target := v.Header().Generation
	if _, err := testPut(v, testKey(2), []byte("later"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	mustGet(t, v, key, []byte("payload"))
	mustGet(t, v, testKey(2), []byte("later"))
	release()
	awaitCheckpointTest(t, done)
	if !v.IsDurableThrough(target) || v.IsDurableThrough(v.Header().Generation) {
		t.Fatal("metadata fence confirmed a later overlay")
	}
}

func TestCleanCloseRejectsNewPublicationsDuringFinalFence(t *testing.T) {
	disk := newCrash(16 << 20)
	gate := &checkpointGateIO{VolumeIO: disk}
	v, err := Format(gate, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testPut(v, testKey(1), []byte("accepted before close"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	entered, release := gate.arm(nil)
	defer release()
	done := make(chan error, 1)
	go func() { done <- v.Close() }()
	<-entered
	h, err := v.Open(true)
	if h != nil {
		h.Close()
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("close admitted a publication: %v", err)
	}
	release()
	awaitCheckpointTest(t, done)
	reopened, err := Open(disk.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	mustGet(t, reopened, testKey(1), []byte("accepted before close"))
}
