// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type parallelReadStore struct {
	*MemoryFilesystemStore
	before func(context.Context, ObjectID) error
	active atomic.Int32
	peak   atomic.Int32
	syncs  atomic.Int32
}

func (*parallelReadStore) ConcurrentObjectRequests() bool { return true }
func (s *parallelReadStore) enter(ctx context.Context, id ObjectID) error {
	n := s.active.Add(1)
	for old := s.peak.Load(); n > old && !s.peak.CompareAndSwap(old, n); old = s.peak.Load() {
	}
	if s.before != nil {
		return s.before(ctx, id)
	}
	return nil
}
func (s *parallelReadStore) Get(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	defer s.active.Add(-1)
	if e := s.enter(ctx, id); e != nil {
		return nil, ObjectInfo{}, e
	}
	return s.MemoryFilesystemStore.Get(ctx, id)
}
func (s *parallelReadStore) GetRange(ctx context.Context, id ObjectID, off, n uint64) ([]byte, ObjectInfo, error) {
	defer s.active.Add(-1)
	if e := s.enter(ctx, id); e != nil {
		return nil, ObjectInfo{}, e
	}
	return s.MemoryFilesystemStore.GetRange(ctx, id, off, n)
}
func (s *parallelReadStore) Sync(context.Context) error { s.syncs.Add(1); return nil }
func scatterFixture(t *testing.T, n int) (*Filesystem, *parallelReadStore, fsSegment, []byte) {
	t.Helper()
	s := &parallelReadStore{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f := &Filesystem{store: s}
	segments := make([]fsSegment, n)
	var want []byte
	for i := range segments {
		data := bytes.Repeat([]byte{byte(i + 1)}, 32)
		id, e := f.putImmutable(context.Background(), data)
		if e != nil {
			t.Fatal(e)
		}
		segments[i] = fsSegment{id: id, length: 32}
		want = append(want, data...)
	}
	root, e := f.putImmutable(context.Background(), encodeFSIndex(segments))
	if e != nil {
		t.Fatal(e)
	}
	return f, s, fsSegment{id: root, length: uint64(len(want))}, want
}
func TestFilesystemParallelReadOverlapAndSelection(t *testing.T) {
	f, s, root, want := scatterFixture(t, 3)
	children, e := f.index(context.Background(), root, 1)
	if e != nil {
		t.Fatal(e)
	}
	first, second := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	s.before = func(ctx context.Context, id ObjectID) error {
		if id == children[0].id {
			close(first)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if id == children[1].id {
			close(second)
		}
		if id == children[2].id {
			t.Error("unselected leaf read")
		}
		return nil
	}
	dst := make([]byte, 40)
	done := make(chan error, 1)
	go func() { done <- f.readSegment(context.Background(), root, 1, dst, 8) }()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first leaf not started")
	}
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("independent leaf blocked behind first")
	}
	// Release via cancellation-free gate, then join before inspecting destination.
	release <- struct{}{}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(dst, want[8:48]) {
		t.Fatal("offset assembly")
	}
	if s.active.Load() != 0 || s.syncs.Load() != 0 {
		t.Fatal("unfinished read or implicit sync")
	}
}
func TestFilesystemReadScatterBoundAndCancellation(t *testing.T) {
	f, s, root, _ := scatterFixture(t, 40)
	started := make(chan struct{}, 40)
	s.before = func(ctx context.Context, id ObjectID) error {
		if id == root.id {
			return nil
		}
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.readSegment(ctx, root, 1, make([]byte, 1280), 0) }()
	for i := 0; i < maxParallelObjectReads; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers not launched")
		}
	}
	if s.peak.Load() > maxParallelObjectReads {
		t.Fatal("unbounded", s.peak.Load())
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if s.active.Load() != 0 {
		t.Fatal("read returned before tasks joined")
	}
}
func TestFilesystemReadScatterErrorCancelsSiblings(t *testing.T) {
	f, s, root, _ := scatterFixture(t, 2)
	children, _ := f.index(context.Background(), root, 1)
	sentinel := errors.New("leaf failure")
	s.before = func(ctx context.Context, id ObjectID) error {
		if id == children[0].id {
			<-ctx.Done()
			return ctx.Err()
		}
		if id == children[1].id {
			return sentinel
		}
		return nil
	}
	if e := f.readSegment(context.Background(), root, 1, make([]byte, 64), 0); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	if s.active.Load() != 0 {
		t.Fatal("unjoined sibling")
	}
}

func TestFilesystemParallelReadDescendsSelectedSubtrees(t *testing.T) {
	f, s, left, want := scatterFixture(t, 2)
	rightData := bytes.Repeat([]byte{9}, 64)
	rid, e := f.putImmutable(context.Background(), rightData)
	if e != nil {
		t.Fatal(e)
	}
	right, e := f.putImmutable(context.Background(), encodeFSIndex([]fsSegment{{id: rid, length: 64}}))
	if e != nil {
		t.Fatal(e)
	}
	root, e := f.putImmutable(context.Background(), encodeFSIndex([]fsSegment{left, {id: right, length: 64}}))
	if e != nil {
		t.Fatal(e)
	}
	blocked, independent, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	s.before = func(ctx context.Context, id ObjectID) error {
		if id == left.id {
			close(blocked)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if id == rid {
			close(independent)
		}
		return nil
	}
	done := make(chan error, 1)
	dst := make([]byte, 128)
	go func() { done <- f.readSegment(context.Background(), fsSegment{id: root, length: 128}, 2, dst, 0) }()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("first index not read")
	}
	select {
	case <-independent:
	case <-time.After(time.Second):
		t.Fatal("second subtree blocked behind first index")
	}
	release <- struct{}{}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(dst, append(want, rightData...)) {
		t.Fatal("subtree ordering")
	}
	s.before = func(_ context.Context, id ObjectID) error {
		if id == left.id {
			t.Error("tiny read fetched unrelated index subtree")
		}
		return nil
	}
	one := make([]byte, 1)
	if e := f.readSegment(context.Background(), fsSegment{id: root, length: 128}, 2, one, 100); e != nil || one[0] != 9 {
		t.Fatal("tiny read", one, e)
	}
}

func TestFilesystemParallelReadRechecksInode(t *testing.T) {
	ctx := context.Background()
	f, s, root, want := scatterFixture(t, 2)
	// Build a real inode with stable indexed data, then replace its head during
	// one selected leaf read. ReadAt must retry after joining that entire view.
	inode := fsNode{kind: fsFile, blob: fsBlob{root: root.id, depth: 1, length: 64}}
	id, e := f.putImmutable(ctx, encodeFSNode(inode))
	if e != nil {
		t.Fatal(e)
	}
	children, _ := f.index(ctx, root, 1)
	var changed atomic.Bool
	replacement := bytes.Repeat([]byte{99}, 64)
	s.before = func(ctx context.Context, obj ObjectID) error {
		if obj == children[0].id && changed.CompareAndSwap(false, true) {
			raw, _, e := s.MemoryFilesystemStore.Get(ctx, id)
			if e != nil {
				return e
			}
			next := inode
			next.blob = fsBlob{inline: replacement, length: 64}
			_, e = s.MemoryFilesystemStore.CAS(ctx, id, 0, raw, encodeFSNode(next), CASOptions{Scope: CASObject})
			return e
		}
		return nil
	}
	dst := make([]byte, len(want))
	n, e := f.ReadAt(ctx, id, dst, 0)
	if e != nil || n != 64 || !bytes.Equal(dst, replacement) {
		t.Fatalf("stale view escaped: n=%d err=%v data=%x", n, e, dst)
	}
}

func TestFilesystemParallelReadHolesAndSparsePayload(t *testing.T) {
	ctx := context.Background()
	s := &parallelReadStore{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f := &Filesystem{store: s}
	id, e := f.putImmutable(ctx, []byte("ab"))
	if e != nil {
		t.Fatal(e)
	}
	root, e := f.putImmutable(ctx, encodeFSIndex([]fsSegment{{length: 4}, {id: id, length: 4}}))
	if e != nil {
		t.Fatal(e)
	}
	dst := bytes.Repeat([]byte{99}, 8)
	ctx = context.WithValue(ctx, fsSparsePageKey{}, true)
	if e = f.readSegment(ctx, fsSegment{id: root, length: 8}, 1, dst, 0); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(dst, []byte{0, 0, 0, 0, 'a', 'b', 0, 0}) {
		t.Fatal(dst)
	}
}
