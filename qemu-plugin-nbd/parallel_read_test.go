// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"sync/atomic"
	"testing"
	"time"
)

type parallelStripeStore struct {
	*testStore
	before func(context.Context, client.ObjectID) error
	active atomic.Int32
	peak   atomic.Int32
}

func (*parallelStripeStore) ConcurrentObjectRequests() bool { return true }
func (s *parallelStripeStore) enter(ctx context.Context, id client.ObjectID) error {
	n := s.active.Add(1)
	for old := s.peak.Load(); n > old && !s.peak.CompareAndSwap(old, n); old = s.peak.Load() {
	}
	if s.before != nil {
		return s.before(ctx, id)
	}
	return nil
}
func (s *parallelStripeStore) Get(ctx context.Context, id client.ObjectID) ([]byte, client.ObjectInfo, error) {
	defer s.active.Add(-1)
	if e := s.enter(ctx, id); e != nil {
		return nil, client.ObjectInfo{}, e
	}
	return s.testStore.Get(ctx, id)
}
func (s *parallelStripeStore) GetRange(ctx context.Context, id client.ObjectID, o, n uint64) ([]byte, client.ObjectInfo, error) {
	defer s.active.Add(-1)
	if e := s.enter(ctx, id); e != nil {
		return nil, client.ObjectInfo{}, e
	}
	return s.testStore.GetRange(ctx, id, o, n)
}
func TestNBDParallelReadOverlapAndSparseEdges(t *testing.T) {
	s := &parallelStripeStore{testStore: newTestStore()}
	c := NewCatalog(s)
	m := Metadata{ID: client.ID{1}, Size: 3 * StripeSize}
	s.objects[StripeID(m.ID, 0)] = testObject{bytes.Repeat([]byte{7}, int(StripeSize))}
	s.objects[StripeID(m.ID, 1)] = testObject{[]byte{8, 9}}
	first, second, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	s.before = func(ctx context.Context, id client.ObjectID) error {
		if id == StripeID(m.ID, 0) {
			close(first)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if id == StripeID(m.ID, 1) {
			close(second)
		}
		return nil
	}
	backing := bytes.Repeat([]byte{0xa5}, int(StripeSize)+24)
	dst := backing[8 : len(backing)-8]
	done := make(chan error, 1)
	go func() { done <- c.ReadAt(context.Background(), m, dst, StripeSize-4) }()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first stripe not started")
	}
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("second stripe serialized")
	}
	release <- struct{}{}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	want := make([]byte, len(dst))
	copy(want, []byte{7, 7, 7, 7, 8, 9})
	if !bytes.Equal(backing[:8], bytes.Repeat([]byte{0xa5}, 8)) || !bytes.Equal(backing[len(backing)-8:], bytes.Repeat([]byte{0xa5}, 8)) {
		t.Fatal("read modified bytes outside destination window")
	}
	if !bytes.Equal(dst, want) {
		t.Fatal("sparse stripe assembly")
	}
	if s.active.Load() != 0 || s.syncs != 0 {
		t.Fatal("pending read or implicit sync")
	}
	if e := c.ReadAt(context.Background(), m, make([]byte, 2), m.Size-1); !errors.Is(e, client.ErrRange) {
		t.Fatal(e)
	}
}
func TestNBDReadScatterBoundAndCancellation(t *testing.T) {
	s := &parallelStripeStore{testStore: newTestStore()}
	c := NewCatalog(s)
	m := Metadata{ID: client.ID{1}, Size: 32 * StripeSize}
	started := make(chan struct{}, 32)
	s.before = func(ctx context.Context, _ client.ObjectID) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.ReadAt(ctx, m, make([]byte, m.Size), 0) }()
	for i := 0; i < maxParallelStripeReads; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers not started")
		}
	}
	if s.peak.Load() > maxParallelStripeReads {
		t.Fatal("unbounded reads")
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if s.active.Load() != 0 {
		t.Fatal("unjoined read")
	}
}

func TestNBDReadScatterErrorCancelsSiblings(t *testing.T) {
	s := &parallelStripeStore{testStore: newTestStore()}
	c := NewCatalog(s)
	m := Metadata{ID: client.ID{1}, Size: 2 * StripeSize}
	sentinel := errors.New("stripe failed")
	s.before = func(ctx context.Context, id client.ObjectID) error {
		if id == StripeID(m.ID, 0) {
			<-ctx.Done()
			return ctx.Err()
		}
		return sentinel
	}
	if e := c.ReadAt(context.Background(), m, make([]byte, m.Size), 0); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	if s.active.Load() != 0 || s.syncs != 0 {
		t.Fatal("unfinished sibling or implicit sync")
	}
}

// An admitted overlapping write cannot outlive a canceled read's callbacks.
// Hold a callback after cancellation to prove the operation guard drains too.
func TestNBDReadErrorDrainsBeforeReleasingGuard(t *testing.T) {
	s := &parallelStripeStore{testStore: newTestStore()}
	c := NewCatalog(s)
	m := Metadata{ID: client.ID{1}, Size: 2 * StripeSize}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	sentinel := errors.New("selected stripe failed")
	s.before = func(ctx context.Context, id client.ObjectID) error {
		if id == StripeID(m.ID, 0) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return ctx.Err()
		}
		select {
		case <-started:
		case <-ctx.Done():
			return ctx.Err()
		}
		return sentinel
	}
	done := make(chan error, 1)
	go func() { done <- c.ReadAt(ctx, m, make([]byte, m.Size), 0) }()
	select {
	case <-canceled:
	case err := <-done:
		t.Fatal("read returned before callback release", err)
	case <-ctx.Done():
		t.Fatal("sibling did not observe cancellation")
	}
	select {
	case err := <-done:
		t.Fatal("read returned before callback release", err)
	default:
	}
	writeDone := make(chan error, 1)
	go func() { writeDone <- c.WriteAt(ctx, m, []byte("next"), 0) }()
	waitBlockOperations(t, c, 2)
	select {
	case err := <-done:
		t.Fatal("read returned before callback release", err)
	case err := <-writeDone:
		t.Fatal("write passed undrained read guard", err)
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("read did not join")
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("operation guard not released")
	}
	if s.active.Load() != 0 || s.syncs != 0 {
		t.Fatal("callback leak or implicit sync")
	}
}

type serialStripeReadStore struct{ Store }

func TestNBDCustomStoreReadsRemainSerial(t *testing.T) {
	s := &parallelStripeStore{testStore: newTestStore()}
	c := NewCatalog(serialStripeReadStore{s})
	m := Metadata{ID: client.ID{1}, Size: 2 * StripeSize}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, second, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	s.before = func(ctx context.Context, id client.ObjectID) error {
		if id == StripeID(m.ID, 0) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			close(second)
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- c.ReadAt(ctx, m, make([]byte, m.Size), 0) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first serial callback did not start")
	}
	select {
	case <-second:
		t.Fatal("custom store called concurrently")
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
	if s.peak.Load() != 1 {
		t.Fatal("custom store concurrency", s.peak.Load())
	}
}
