// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyMembershipUnknownAndLockFreeAbsent(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	defer v.Close()
	key := testKey(1)
	if !v.MayContainObject(key) || !v.MayContainRequest(testID(1)) || !v.MayContainReplica(testID(2)) || !v.MayContainPending(key) {
		t.Fatal("fresh unknown claimed absence")
	}
	if err := v.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v.MayContainObject(key) || v.MayContainRequest(testID(1)) || v.MayContainReplica(testID(2)) || v.MayContainPending(key) {
		t.Fatal("empty index contains key")
	}
	f := v.keyMembership.Load()
	if len(f.words)*8 != keyMembershipBytes {
		t.Fatal("unbounded filter")
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() { done <- v.MayContainObject(key) }()
	select {
	case maybe := <-done:
		if maybe {
			t.Fatal("absent key changed")
		}
	case <-time.After(time.Second):
		t.Fatal("hint took volume lock")
	}
	h.Close()
	for i := range f.words {
		f.words[i].Store(^uint64(0))
	}
	if !v.MayContainObject(key) {
		t.Fatal("saturated filter unsafe")
	}
}
func TestKeyMembershipWritersReceiptsAndRollback(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	defer v.Close()
	if err := v.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	request := testID(7)
	if _, err := testPut(v, key, []byte("payload"), Options{Accepted: true, RequestID: request}); err != nil {
		t.Fatal(err)
	}
	if !v.MayContainObject(key) || !v.MayContainRequest(request) {
		t.Fatal("published key false negative")
	}
	if err := testDelete(v, key, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if !v.MayContainObject(key) {
		t.Fatal("tombstone missing")
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	tx := v.transaction()
	for _, k := range []indexKey{objectKey(testKey(99)), requestKey(testID(99)), replicaKey(testID(98)), pendingKey(testKey(98))} {
		if err = tx.set(&tx.sb.ObjectRoot, k, 123); err != nil {
			t.Fatal(err)
		}
	}
	tx.close()
	h.Close()
	if !v.MayContainObject(testKey(99)) || !v.MayContainRequest(testID(99)) || !v.MayContainReplica(testID(98)) || !v.MayContainPending(testKey(98)) {
		t.Fatal("aborted insert must only add false positives")
	}
	if _, _, err := testGet(v, testKey(99)); !errors.Is(err, ErrNotFound) {
		t.Fatal("rolled back key leaked", err)
	}
}
func TestKeyMembershipConcurrentCursorAndCancel(t *testing.T) {
	v, _ := memoryVolume(t, 16<<20)
	defer v.Close()
	for i := 1; i <= keyMembershipBatch+30; i++ {
		if _, err := testPut(v, testKey(uint64(i+100)), []byte("x"), Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	var calls int
	err := v.warmKeyMembership(context.Background(), func() {
		calls++
		if calls != 1 {
			return
		}
		for _, k := range []Key{testKey(1), testKey(99999)} {
			if _, err := testPut(v, k, []byte("late"), Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []Key{testKey(1), testKey(99999)} {
		if !v.MayContainObject(k) {
			t.Fatal("cursor missed late insert")
		}
	}
	v.invalidateKeyMembership()
	ctx, cancel := context.WithCancel(context.Background())
	err = v.warmKeyMembership(ctx, func() { cancel() })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !v.MayContainObject(testKey(999999)) {
		t.Fatal("cancel completed partial filter")
	}
}
func TestKeyMembershipImportInvalidatesEmptyProof(t *testing.T) {
	src, _ := memoryVolume(t, 4<<20)
	defer src.Close()
	key := testKey(123)
	id := testID(8)
	if _, err := testPut(src, key, []byte("imported"), Options{RequestID: id}); err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := src.Export(&snapshot); err != nil {
		t.Fatal(err)
	}
	dst, _ := memoryVolume(t, 4<<20)
	defer dst.Close()
	if err := dst.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dst.MayContainObject(key) {
		t.Fatal("empty destination not absent")
	}
	if err := dst.Import(&snapshot, nil); err != nil {
		t.Fatal(err)
	}
	if dst.keyMembership.Load() != nil || !dst.MayContainObject(key) || !dst.MayContainRequest(id) {
		t.Fatal("import reused old empty proof")
	}
	if err := dst.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !dst.MayContainObject(key) || !dst.MayContainRequest(id) {
		t.Fatal("imported key false negative")
	}
}
func TestKeyMembershipCloseAndReopenUnknown(t *testing.T) {
	disk := newCrash(4 << 20)
	v, err := Format(disk, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(4)
	if _, err = testPut(v, key, []byte("durable"), Options{}); err != nil {
		t.Fatal(err)
	}
	if err = v.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	copy := disk.crash()
	fresh, err := Open(copy, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if fresh.keyMembership.Load() != nil || !fresh.MayContainObject(testKey(999)) {
		t.Fatal("reopen trusted old proof")
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	if !v.MayContainObject(testKey(999)) {
		t.Fatal("closed claimed absence")
	}
}
func TestKeyMembershipConcurrentQueriesAndWrites(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	defer v.Close()
	if err := v.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var published atomic.Uint64
	go func() {
		defer close(done)
		for i := uint64(1); i <= 100; i++ {
			if _, err := testPut(v, testKey(i), []byte("x"), Options{Accepted: true}); err != nil {
				return
			}
			published.Store(i)
		}
	}()
	for {
		for i := uint64(1); i <= published.Load(); i++ {
			if !v.MayContainObject(testKey(i)) {
				t.Fatal("published key absent", i)
			}
		}
		select {
		case <-done:
			if published.Load() != 100 {
				t.Fatal("writer failed")
			}
			return
		default:
		}
	}
}

type membershipInspectIO struct {
	VolumeIO
	armed    atomic.Bool
	reads    atomic.Uint64
	failSync atomic.Bool
}

func (d *membershipInspectIO) ReadAt(p []byte, off int64) (int, error) {
	n, err := d.VolumeIO.ReadAt(p, off)
	if d.armed.Load() {
		d.reads.Add(1)
		if n != PageSize || string(p[:4]) != "BPT2" {
			return n, errors.New("membership read non-index page")
		}
	}
	return n, err
}
func (d *membershipInspectIO) Sync() error {
	if d.failSync.Load() {
		return errors.New("injected sync failure")
	}
	return d.VolumeIO.Sync()
}
func TestKeyMembershipKeyOnlyBuildAndFailedVolume(t *testing.T) {
	mem, err := NewMemory(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	disk := &membershipInspectIO{VolumeIO: mem}
	v, err := Format(disk, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	for i := uint64(1); i <= 700; i++ {
		if _, err = testPut(v, testKey(i), bytes.Repeat([]byte("x"), 7), Options{}); err != nil {
			t.Fatal(err)
		}
	}
	v.nodes = newNodeCache(-1, 1)
	before := v.IOStats()
	disk.armed.Store(true)
	if err = v.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	disk.armed.Store(false)
	after := v.IOStats()
	if disk.reads.Load() == 0 || after.WriteBytes != before.WriteBytes || after.SyncOps != before.SyncOps {
		t.Fatal("builder did not only read index", disk.reads.Load(), before, after)
	}
	disk.failSync.Store(true)
	if err = v.Checkpoint(); err == nil {
		t.Fatal("sync did not fail")
	}
	if v.keyMembership.Load() != nil || !v.MayContainObject(testKey(9999)) {
		t.Fatal("failed volume retained negative proof")
	}
}
func TestKeyMembershipCancellationAtFinalBarrier(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	defer v.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- v.warmKeyMembership(ctx, func() { close(ready); <-resume }) }()
	<-ready
	v.mu.Lock()
	close(resume)
	cancel()
	v.mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !v.MayContainObject(testKey(999)) {
		t.Fatal("cancelled final publication became complete")
	}
}
func TestKeyMembershipImportInvalidatesCollector(t *testing.T) {
	src, _ := memoryVolume(t, 4<<20)
	defer src.Close()
	key := testKey(5)
	if _, err := testPut(src, key, []byte("x"), Options{}); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if err := src.Export(&stream); err != nil {
		t.Fatal(err)
	}
	dst, _ := memoryVolume(t, 4<<20)
	defer dst.Close()
	err := dst.warmKeyMembership(context.Background(), func() {
		if err := dst.Import(&stream, nil); err != nil {
			t.Fatal(err)
		}
	})
	if !errors.Is(err, ErrBusy) {
		t.Fatal("obsolete collector published", err)
	}
	if dst.keyMembership.Load() != nil || !dst.MayContainObject(key) {
		t.Fatal("import key hidden")
	}
}
func TestKeyMembershipReplicaLifecycleAndResize(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	defer v.Close()
	key, id := testKey(1), testID(7)
	if _, err := testPut(v, key, []byte("old"), Options{}); err != nil {
		t.Fatal(err)
	}
	if err := v.WarmKeyMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	binding := ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.PrepareMutation(opAppend, key, 0, 1, bytes.NewReader([]byte("!")), nil, Options{RequestID: id}, binding)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !v.MayContainReplica(id) || !v.MayContainPending(key) {
		t.Fatal("prepared records absent")
	}
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.DecideReplica(id, binding)
	if err == nil {
		_, err = h.PublishReplica(id, binding)
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !v.MayContainObject(key) || !v.MayContainRequest(id) || !v.MayContainReplica(id) {
		t.Fatal("published replica records absent")
	}
	if err = v.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = v.Shrink(4 << 20); err != nil {
		t.Fatal(err)
	}
	if err = v.Grow(8 << 20); err != nil {
		t.Fatal(err)
	}
	if !v.MayContainObject(key) || !v.MayContainRequest(id) {
		t.Fatal("maintenance lost membership")
	}
	mustGet(t, v, key, []byte("old!"))
}

func TestKeyMembershipExtremeKeysAndBuildContention(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	defer v.Close()
	var high Key
	for i := range high.Bucket {
		high.Bucket[i] = 255
	}
	for i := range high.Object {
		high.Object[i] = 255
	}
	for _, key := range []Key{{}, high} {
		if _, err := testPut(v, key, nil, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	ready, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- v.warmKeyMembership(context.Background(), func() { close(ready); <-resume }) }()
	<-ready
	if !v.MayContainObject(testKey(9090)) {
		t.Fatal("collecting filter proved absence")
	}
	if err := v.WarmKeyMembership(context.Background()); !errors.Is(err, ErrBusy) {
		t.Fatal("second collector did not backpressure", err)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !v.MayContainObject(Key{}) || !v.MayContainObject(high) {
		t.Fatal("extreme key omitted")
	}
	if n := testing.AllocsPerRun(100, func() { v.MayContainObject(testKey(9090)) }); n != 0 {
		t.Fatal("hint allocates", n)
	}
}

func TestKeyMembershipCloseDuringBuild(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	ready, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- v.warmKeyMembership(context.Background(), func() { close(ready); <-resume }) }()
	<-ready
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	close(resume)
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatal("closed collector published", err)
	}
	if v.keyMembership.Load() != nil || !v.MayContainObject(testKey(999)) {
		t.Fatal("closed collector left negative proof")
	}
}
func BenchmarkKeyMembershipMiss(b *testing.B) {
	d, err := NewMemory(4 << 20)
	if err != nil {
		b.Fatal(err)
	}
	v, err := Format(d, FormatOptions{Class: RAM})
	if err != nil {
		b.Fatal(err)
	}
	defer v.Close()
	if err = v.WarmKeyMembership(context.Background()); err != nil {
		b.Fatal(err)
	}
	key := testKey(999)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v.MayContainObject(key) {
			b.Fatal("empty proof changed")
		}
	}
	b.ReportMetric(keyMembershipBytes, "filter_bytes/volume")
}
