// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/launix-de/stqr-storage/volume"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cleanupProbeIO struct {
	volume.VolumeIO
	reads            atomic.Uint64
	count            atomic.Bool
	mu               sync.Mutex
	entered, release chan struct{}
	fail             error
}

func (p *cleanupProbeIO) ReadAt(b []byte, off int64) (int, error) {
	n, err := p.VolumeIO.ReadAt(b, off)
	if p.count.Load() {
		var pcs [20]uintptr
		frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, ".loadDedupe") {
				p.reads.Add(1)
				break
			}
			if !more {
				break
			}
		}
	}
	return n, err
}
func (p *cleanupProbeIO) Sync() error {
	p.mu.Lock()
	entered, release, failure := p.entered, p.release, p.fail
	if entered != nil {
		p.entered = nil
		close(entered)
	}
	p.mu.Unlock()
	if release != nil {
		<-release
	}
	if failure != nil {
		return failure
	}
	return p.VolumeIO.Sync()
}
func cleanupFixture(t *testing.T, count int) (*Server, *volume.Volume, *cleanupProbeIO) {
	t.Helper()
	dev, err := volume.NewMemory(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	probe := &cleanupProbeIO{VolumeIO: dev}
	v, err := volume.Format(probe, volume.FormatOptions{Class: volume.HDD, VolumeID: volume.ID{1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	s := &Server{ctx: context.Background(), volumes: map[string]*managedVolume{v.Identity().VolumeID.String(): {store: v}}, bucketsByID: map[volume.ID]*Bucket{}}
	view := &systemView{}
	for i := 1; i <= count; i++ {
		id := volume.ID{byte(i + 10)}
		d := BucketDescription{ID: id.String(), State: BucketDeleting, Revision: 1}
		s.bucketsByID[id] = &Bucket{server: s, description: d}
		view.descriptions = append(view.descriptions, d)
	}
	s.currentSystem.Store(view)
	return s, v, probe
}
func cleanupID(i int) volume.ID {
	var id volume.ID
	binary.BigEndian.PutUint64(id[8:], uint64(i+1))
	return id
}
func seedCleanup(t *testing.T, s *Server, v *volume.Volume, receipts int) {
	t.Helper()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	var objects []volume.ObjectRecord
	for i := 0; i < receipts; i++ {
		bucket := volume.ID{byte(11 + i%len(s.bucketsByID))}
		key := volume.Key{Bucket: bucket, Object: volume.ObjectID{1}}
		if _, err = h.Put(key, []byte("delete"), volume.Options{Accepted: true, RequestID: cleanupID(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := receipts; i < receipts+512; i++ {
		key := volume.Key{Bucket: volume.ID{250}, Object: volume.ObjectID{1}}
		if _, err = h.Put(key, []byte("keep"), volume.Options{Accepted: true, RequestID: cleanupID(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for id := range s.bucketsByID {
		objects = append(objects, volume.ObjectRecord{Key: volume.Key{Bucket: id, Object: volume.ObjectID{1}}})
	}
	if err = h.PurgeBatch(objects); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if err = v.SetAdmission(nil); err != nil {
		t.Fatal(err)
	}
}
func TestCleanupSharedReceiptSweep(t *testing.T) {
	s, v, probe := cleanupFixture(t, 8)
	seedCleanup(t, s, v, 256)
	// Positive control: separate traversals, identical data/cache/instrumentation.
	probe.count.Store(true)
	for i := 0; i < 8; i++ {
		var after *volume.ID
		for {
			h, _ := v.Open(false)
			rows, err := h.ScanRequestRecords(after, 128)
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) == 0 {
				break
			}
			next := rows[len(rows)-1].ID
			after = &next
		}
	}
	control := probe.reads.Swap(0)
	for i := 0; i < 200; i++ {
		s.reclaimBucketStep()
		done := len(s.cleanupJobs) == 8
		for _, j := range s.cleanupJobs {
			done = done && j.done
		}
		if done {
			break
		}
		if i == 199 {
			t.Fatal("shared sweep did not finish")
		}
	}
	actual := probe.reads.Load()
	if actual >= control/2 {
		t.Fatalf("receipt work still scales with buckets: shared=%d separate=%d", actual, control)
	}
	h, _ := v.Open(false)
	defer h.Close()
	for i := 0; i < 256; i++ {
		_, found, err := h.OperationStatus(cleanupID(i))
		if err != nil || found {
			t.Fatal("deleted receipt", i, found, err)
		}
	}
	for i := 256; i < 768; i++ {
		_, found, err := h.OperationStatus(cleanupID(i))
		if err != nil || !found {
			t.Fatal("unrelated receipt", i, found, err)
		}
	}
	data, _, err := h.Get(volume.Key{Bucket: volume.ID{250}, Object: volume.ObjectID{1}})
	if err != nil || string(data) != "keep" {
		t.Fatal("unrelated data", string(data), err)
	}
	t.Logf("receipt reads: shared %d, per-bucket control %d", actual, control)
}
func TestCleanupProgressUnderForegroundLoad(t *testing.T) {
	s, v, _ := cleanupFixture(t, 1)
	h, _ := v.Open(true)
	key := volume.Key{Bucket: volume.ID{11}, Object: volume.ObjectID{1}}
	_, err := h.Put(key, []byte("delete"), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	s.ioPriority.users.Store(1)
	if s.reclaimBucketStep() {
		t.Fatal("fresh low-priority work bypassed foreground")
	}
	s.cleanupLast = time.Now().Add(-2 * cleanupMaxWait)
	if !s.reclaimBucketStep() {
		t.Fatal("continuous foreground starved overdue cleanup")
	}
	h, _ = v.Open(false)
	rows, err := h.ScanBucketObjects(key.Bucket, nil, 32)
	h.Close()
	if err != nil || len(rows) != 0 {
		t.Fatal("no object progress", len(rows), err)
	}
	s.ioPriority.rebuilds.Store(1)
	s.cleanupLast = time.Now().Add(-2 * cleanupMaxWait)
	if s.reclaimBucketStep() {
		t.Fatal("mandatory migration priority bypassed")
	}
}
func TestCleanupSkipsBusyVolume(t *testing.T) {
	s, v, _ := cleanupFixture(t, 1)
	dev, _ := volume.NewMemory(16 << 20)
	other, err := volume.Format(dev, volume.FormatOptions{Class: volume.HDD, VolumeID: volume.ID{2}})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	s.volumes[other.Identity().VolumeID.String()] = &managedVolume{store: other}
	for _, store := range []*volume.Volume{v, other} {
		h, _ := store.Open(true)
		_, err = h.Put(volume.Key{Bucket: volume.ID{11}, Object: volume.ObjectID{1}}, []byte("delete"), volume.Options{})
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	busy, _ := v.Open(false)
	defer busy.Close()
	if !s.reclaimBucketStep() {
		t.Fatal("busy first volume suppressed the runnable second volume")
	}
	h, _ := other.Open(false)
	rows, err := h.ScanBucketObjects(volume.ID{11}, nil, 32)
	h.Close()
	if err != nil || len(rows) != 0 {
		t.Fatal("second volume made no progress", err)
	}
}
func TestCleanupFenceReleasesNodeAndVolumeLocks(t *testing.T) {
	s, v, p := cleanupFixture(t, 1)
	h, _ := v.Open(true)
	_, err := h.Put(volume.Key{Bucket: volume.ID{11}, Object: volume.ObjectID{1}}, []byte("delete"), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	p.mu.Lock()
	p.entered = entered
	p.release = release
	p.mu.Unlock()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan bool, 1)
	go func() { done <- s.reclaimBucketStep() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup never reached physical fence")
	}
	if !s.opMu.TryLock() {
		t.Fatal("physical fence holds node admission")
	}
	s.opMu.Unlock()
	read := make(chan error, 1)
	go func() {
		h, err := v.Open(false)
		if err == nil {
			_, err = h.StorageUsage()
			h.Close()
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("physical fence holds volume metadata lock")
	}
	select {
	case <-done:
		t.Fatal("reported durable progress before fence completed")
	default:
	}
	unblock()
	if !<-done {
		t.Fatal("cleanup failed after fence")
	}
	// Discard advisory jobs/cursors; reconstruct from the durable intent.
	s.cleanupJobs = nil
	s.cleanupReceipts = nil
	s.cleanupView = nil
	for i := 0; i < 10; i++ {
		s.reclaimBucketStep()
	}
	if !s.cleanupJobs[volume.ID{11}].done {
		t.Fatal("reconstructed cleanup did not finish")
	}
}

func TestCleanupQueuesOverdueWriterWithoutNodeLock(t *testing.T) {
	s, v, _ := cleanupFixture(t, 1)
	h, _ := v.Open(true)
	_, err := h.Put(volume.Key{Bucket: volume.ID{11}, Object: volume.ObjectID{1}}, []byte("delete"), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	busy, _ := v.Open(false)
	defer busy.Close()
	s.cleanupLast = time.Now().Add(-2 * cleanupMaxWait)
	done := make(chan bool, 1)
	go func() { done <- s.reclaimBucketStep() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		probe, acquired, err := v.TryOpen(false)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			break
		}
		probe.Close()
		if time.Now().After(deadline) {
			t.Fatal("overdue writer never queued behind background reader")
		}
		runtime.Gosched()
	}
	if !s.opMu.TryLock() {
		t.Fatal("queued volume writer holds node admission")
	}
	s.opMu.Unlock()
	busy.Close()
	select {
	case worked := <-done:
		if !worked {
			t.Fatal("queued writer did not progress")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued writer did not finish")
	}
}

func TestCleanupFailureCannotCompleteJob(t *testing.T) {
	s, v, p := cleanupFixture(t, 1)
	h, _ := v.Open(true)
	_, err := h.Put(volume.Key{Bucket: volume.ID{11}, Object: volume.ObjectID{1}}, []byte("delete"), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.fail = errors.New("injected physical fence failure")
	p.mu.Unlock()
	if s.reclaimBucketStep() {
		t.Fatal("failed fence reported successful progress")
	}
	job := s.cleanupJobs[volume.ID{11}]
	if job.done || job.volumes[0].checkpoint == 0 {
		t.Fatal("lost pending durability obligation")
	}
	for i := 0; i < 3; i++ {
		s.reclaimBucketStep()
	}
	if job.done {
		t.Fatal("volatile empty bucket confirmed after failed fence")
	}
}

func TestCleanupNewParticipantStartsBeforePassedReceipts(t *testing.T) {
	s, v, _ := cleanupFixture(t, 2)
	seedCleanup(t, s, v, 256)
	second := s.bucketsByID[volume.ID{12}]
	second.description.State = BucketActive
	firstView := *s.currentSystem.Load()
	firstView.descriptions = append([]BucketDescription(nil), firstView.descriptions...)
	firstView.descriptions[1] = second.Describe()
	s.currentSystem.Store(&firstView)
	s.cleanupMu.Lock()
	s.refreshCleanupJobs(&firstView)
	if !s.cleanupReceiptPage(false) {
		t.Fatal("first shared page did not run")
	}
	s.cleanupMu.Unlock()
	// The first page passed receipts belonging to the not-yet-deleting bucket.
	second.description.State = BucketDeleting
	second.description.Revision++
	nextView := firstView
	nextView.descriptions = append([]BucketDescription(nil), firstView.descriptions...)
	nextView.descriptions[1] = second.Describe()
	s.currentSystem.Store(&nextView)
	for i := 0; i < 100; i++ {
		s.reclaimBucketStep()
		done := len(s.cleanupJobs) == 2
		for _, j := range s.cleanupJobs {
			done = done && j.done
		}
		if done {
			break
		}
		if i == 99 {
			t.Fatal("late participant never completed")
		}
	}
	h, _ := v.Open(false)
	defer h.Close()
	for i := 0; i < 256; i++ {
		_, found, err := h.OperationStatus(cleanupID(i))
		if err != nil || found {
			t.Fatal("late participant skipped a receipt before the old cursor", i, err)
		}
	}
}
