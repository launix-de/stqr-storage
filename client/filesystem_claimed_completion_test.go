// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type delayedCancelledRedoStore struct {
	*MemoryFilesystemStore
	gateMu   sync.Mutex
	target   ObjectID
	armed    bool
	admitted chan struct{}
	late     chan struct{}
	finished chan struct{}
}

func (s *delayedCancelledRedoStore) Put(ctx context.Context, id ObjectID, data []byte, opt PutOptions) (ObjectInfo, error) {
	before, _, err := s.MemoryFilesystemStore.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		before = nil
	} else if err != nil {
		return ObjectInfo{}, err
	}
	return s.MemoryFilesystemStore.CAS(ctx, id, 0, before, data, CASOptions{Scope: CASObject, MutationOptions: opt})
}
func (s *delayedCancelledRedoStore) PutRange(ctx context.Context, id ObjectID, off uint64, data []byte, opt PutOptions) (ObjectInfo, error) {
	s.gateMu.Lock()
	hold := s.armed && id == s.target
	if hold {
		s.armed = false
	}
	s.gateMu.Unlock()
	if !hold {
		return s.MemoryFilesystemStore.PutRange(ctx, id, off, data, opt)
	}
	accepted := bytes.Clone(data)
	close(s.admitted)
	select {
	case <-s.late:
		info, err := s.MemoryFilesystemStore.PutRange(ctx, id, off, accepted, opt)
		close(s.finished)
		return info, err
	case <-ctx.Done():
		go func() {
			<-s.late
			_, err := s.MemoryFilesystemStore.PutRange(context.Background(), id, off, accepted, opt)
			if err != nil {
				panic(err)
			}
			close(s.finished)
		}()
		return ObjectInfo{}, ctx.Err()
	}
}
func TestCancelledClaimedRedoCompletesBeforeLaterWriter(t *testing.T) {
	ctx := context.Background()
	s := &delayedCancelledRedoStore{MemoryFilesystemStore: NewMemoryFilesystemStore(), admitted: make(chan struct{}), late: make(chan struct{}), finished: make(chan struct{})}
	f, err := OpenFilesystem(ctx, s, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Create(ctx, FilesystemRootID(), "state", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	initial := bytes.Repeat([]byte{'A'}, 8192)
	if _, err = f.WriteAt(ctx, info.ID, initial, 0); err != nil {
		t.Fatal(err)
	}
	s.target = fsStableID(info.ID, 0, 0, 0)
	s.armed = true
	cancelled, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	first := make(chan error, 1)
	go func() { _, e := f.WriteAt(cancelled, info.ID, bytes.Repeat([]byte{'B'}, 8192), 0); first <- e }()
	select {
	case <-s.admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("old REDO was not admitted")
	}
	cancel()
	second := make(chan error, 1)
	newer := bytes.Repeat([]byte{'C'}, 8192)
	go func() { _, e := f.WriteAt(ctx, info.ID, newer, 0); second <- e }()
	select {
	case e := <-first:
		close(s.late)
		t.Fatalf("claimed completion returned before admitted REDO settled: %v", e)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case e := <-second:
		close(s.late)
		t.Fatalf("later writer escaped the outstanding exclusive claim: %v", e)
	default:
	}
	close(s.late)
	for index, result := range []<-chan error{first, second} {
		select {
		case e := <-result:
			if e != nil && !(index == 0 && errors.Is(e, context.Canceled)) {
				t.Fatal(e)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("claimed completion/later writer did not finish")
		}
	}
	before, _, err := s.MemoryFilesystemStore.Get(ctx, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.finished:
	case <-time.After(5 * time.Second):
		t.Fatal("late admitted REDO did not finish")
	}
	after, _, err := s.MemoryFilesystemStore.Get(ctx, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("test requires unchanged newer inode head")
	}
	got := make([]byte, len(newer))
	if _, err = f.ReadAt(ctx, info.ID, got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newer) {
		t.Fatalf("late cancelled REDO overwrote newer data under unchanged inode head: firstByte=%q want=%q", got[0], newer[0])
	}
}
