// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
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
	backing := bytes.Repeat([]byte{0xa5}, 56)
	dst := backing[8:48]
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
	if !bytes.Equal(backing[:8], bytes.Repeat([]byte{0xa5}, 8)) || !bytes.Equal(backing[48:], bytes.Repeat([]byte{0xa5}, 8)) {
		t.Fatal("read modified bytes outside destination window")
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
	for _, size := range []int{32, 64, 96} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			ctx := context.Background()
			f, s, root, _ := scatterFixture(t, 2)
			inode := fsNode{kind: fsFile, blob: fsBlob{root: root.id, depth: 1, length: 64}}
			id, err := f.putImmutable(ctx, encodeFSNode(inode))
			if err != nil {
				t.Fatal(err)
			}
			children, err := f.index(ctx, root, 1)
			if err != nil {
				t.Fatal(err)
			}
			var changed atomic.Bool
			replacement := bytes.Repeat([]byte{99}, size)
			s.before = func(ctx context.Context, obj ObjectID) error {
				if obj == children[0].id && changed.CompareAndSwap(false, true) {
					raw, _, err := s.MemoryFilesystemStore.Get(ctx, id)
					if err != nil {
						return err
					}
					next := inode
					next.blob = fsBlob{inline: replacement, length: uint64(size)}
					_, err = s.MemoryFilesystemStore.CAS(ctx, id, 0, raw, encodeFSNode(next), CASOptions{Scope: CASObject})
					return err
				}
				return nil
			}
			backing := bytes.Repeat([]byte{0xa5}, 66)
			dst := backing[8:58]
			n, err := f.ReadAt(ctx, id, dst, 7)
			want := min(50, size-7)
			if n != want || !bytes.Equal(dst[:n], replacement[7:7+want]) || (want == 50 && err != nil) || (want < 50 && !errors.Is(err, io.EOF)) {
				t.Fatalf("stale/incorrect retry view: n=%d err=%v data=%x", n, err, dst)
			}
			if !changed.Load() || s.active.Load() != 0 || !bytes.Equal(backing[:8], bytes.Repeat([]byte{0xa5}, 8)) || !bytes.Equal(backing[58:], bytes.Repeat([]byte{0xa5}, 8)) {
				t.Fatal("retry did not join or damaged bytes outside destination")
			}
		})
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

// A backend may acknowledge cancellation before it can stop touching dst. The
// reader must join it before validating the inode or returning to its caller.
func TestFilesystemReadErrorDrainsBeforeInodeValidation(t *testing.T) {
	f, s, root, _ := scatterFixture(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inode := fsNode{kind: fsFile, blob: fsBlob{root: root.id, depth: 1, length: 64}}
	id, err := f.putImmutable(ctx, encodeFSNode(inode))
	if err != nil {
		t.Fatal(err)
	}
	children, err := f.index(ctx, root, 1)
	if err != nil {
		t.Fatal(err)
	}
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	validated := make(chan struct{}, 1)
	var inodeReads atomic.Int32
	sentinel := errors.New("selected leaf failed")
	s.before = func(ctx context.Context, obj ObjectID) error {
		switch obj {
		case id:
			if inodeReads.Add(1) > 1 {
				validated <- struct{}{}
			}
		case children[0].id:
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release // Deliberately ignore cancellation until explicitly released.
			return ctx.Err()
		case children[1].id:
			select {
			case <-started:
			case <-ctx.Done():
				return ctx.Err()
			}
			return sentinel
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { _, err := f.ReadAt(ctx, id, make([]byte, 64), 0); done <- err }()
	select {
	case <-canceled:
	case err := <-done:
		t.Fatal("read returned before selected callbacks drained", err)
	case <-ctx.Done():
		t.Fatal("sibling did not observe cancellation")
	}
	select {
	case <-validated:
		t.Fatal("inode validated while a selected callback was still active")
	case err := <-done:
		t.Fatal("read returned before selected callbacks drained", err)
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("read did not join after release")
	}
	if s.active.Load() != 0 || s.syncs.Load() != 0 {
		t.Fatal("callback leak or implicit sync")
	}
	select {
	case <-validated:
	default:
		t.Fatal("inode was not validated after join")
	}
}

type serialFilesystemReadStore struct{ FilesystemStore }

func TestFilesystemCustomStoreReadsRemainSerial(t *testing.T) {
	f, s, root, want := scatterFixture(t, 3)
	f.store = serialFilesystemReadStore{s}
	children, err := f.index(context.Background(), root, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, second, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	s.before = func(ctx context.Context, id ObjectID) error {
		if id == children[0].id {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if id == children[1].id {
			close(second)
		}
		return nil
	}
	dst := make([]byte, len(want))
	done := make(chan error, 1)
	go func() { done <- f.readSegment(ctx, root, 1, dst, 0) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first serial callback did not start")
	}
	select {
	case <-second:
		t.Fatal("custom store was called concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("serial read did not complete")
	}
	select {
	case <-second:
	default:
		t.Fatal("positive control: second callback never ran")
	}
	if !bytes.Equal(dst, want) || s.peak.Load() != 1 {
		t.Fatal("serial assembly or concurrency", s.peak.Load())
	}
}

func TestFilesystemNestedLowerReadSharesObjectBudget(t *testing.T) {
	f, s, lowerRoot, want := scatterFixture(t, 32)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inode := fsNode{kind: fsFile, blob: fsBlob{root: lowerRoot.id, depth: 1, length: uint64(len(want))}}
	id, err := f.putImmutable(ctx, encodeFSNode(inode))
	if err != nil {
		t.Fatal(err)
	}
	// The supported STIX v2 representation references a lower inode. Reading
	// each selected segment recursively starts a second indexed scatter.
	e := Encoder{B: []byte("STIX")}
	e.U16(2)
	e.U32(2)
	for i := 0; i < 2; i++ {
		e.U64(uint64(len(want)))
		e.B = append(e.B, id[:]...)
		e.U64(0)
		e.U64(1)
	}
	upperRoot, err := f.putImmutable(ctx, e.B)
	if err != nil {
		t.Fatal(err)
	}
	upper := &Filesystem{store: s, lower: f}
	children, err := f.index(ctx, lowerRoot, 1)
	if err != nil {
		t.Fatal(err)
	}
	leaves := map[ObjectID]bool{}
	for _, child := range children {
		leaves[child.id] = true
	}
	started := make(chan struct{}, 64)
	release := make(chan struct{})
	var released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	defer unblock()
	s.before = func(ctx context.Context, obj ObjectID) error {
		if leaves[obj] {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	dst := make([]byte, 2*len(want))
	done := make(chan error, 1)
	go func() { done <- upper.readSegment(ctx, fsSegment{id: upperRoot, length: uint64(len(dst))}, 1, dst, 0) }()
	// At least two actual object calls must overlap; recursive jobs may reserve
	// execution slots while their own children finish, but may not add budgets.
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("nested reads did not overlap")
		}
	}
	time.Sleep(25 * time.Millisecond)
	if s.peak.Load() > maxParallelObjectReads {
		t.Fatal("lower recursion multiplied active object calls", s.peak.Load())
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("nested scatter deadlocked")
	}
	if !bytes.Equal(dst, append(want, want...)) || s.active.Load() != 0 {
		t.Fatal("nested assembly or callback lifetime")
	}
}
