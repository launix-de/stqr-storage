// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// Real reads/writes remain independent while a test holds the physical flush.
// This exercises the public Handle API, not an implementation-only lock probe.
type checkpointGateIO struct {
	VolumeIO
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	fail    error
}

func (d *checkpointGateIO) Sync() error {
	d.mu.Lock()
	entered, release, fail := d.entered, d.release, d.fail
	if entered != nil {
		d.entered = nil
		close(entered)
	}
	d.mu.Unlock()
	if release != nil {
		<-release
	}
	if fail != nil {
		return fail
	}
	return d.VolumeIO.Sync()
}
func newCheckpointGate(t *testing.T) (*Volume, *checkpointGateIO) {
	t.Helper()
	memory, err := NewMemory(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	d := &checkpointGateIO{VolumeIO: memory}
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	return v, d
}
func (d *checkpointGateIO) arm(fail error) (<-chan struct{}, func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entered = make(chan struct{})
	d.release = make(chan struct{})
	d.fail = fail
	entered, release := d.entered, d.release
	var once sync.Once
	return entered, func() { once.Do(func() { close(release) }) }
}
func awaitCheckpointTest(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("operation waited behind an unrelated physical flush")
	}
}
func TestCheckpointAllowsReadsAndCoalescesBarriers(t *testing.T) {
	v, d := newCheckpointGate(t)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("old"), Options{}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("visible"), 1700)
	if _, err := testPut(v, key, payload, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	target := v.Header().Generation
	before := v.IOStats().SyncOps
	entered, release := d.arm(nil)
	defer release()
	first := make(chan error, 1)
	go func() { first <- v.CheckpointThrough(target) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint did not start")
	}
	second := make(chan error, 1)
	go func() { second <- v.CheckpointThrough(target) }()
	read := make(chan error, 1)
	go func() {
		for n := 0; n < 25; n++ {
			got, _, err := testGet(v, key)
			if err != nil {
				read <- err
				return
			}
			if !bytes.Equal(got, payload) {
				read <- errors.New("read saw partial checkpoint bytes")
				return
			}
		}
		read <- nil
	}()
	awaitCheckpointTest(t, read)
	if h, ok, err := v.TryOpen(true); err != nil || ok {
		if h != nil {
			h.Close()
		}
		t.Fatalf("idle writer entered checkpoint: %v %v", ok, err)
	}
	release()
	awaitCheckpointTest(t, first)
	awaitCheckpointTest(t, second)
	if delta := v.IOStats().SyncOps - before; delta != 4 {
		t.Fatalf("duplicate barrier flushed again: %d syncs", delta)
	}
	if !v.IsDurableThrough(target) {
		t.Fatal("completed fence not recorded")
	}
}
func TestCheckpointOrdersLaterInPlaceWrites(t *testing.T) {
	v, d := newCheckpointGate(t)
	if _, err := testPut(v, testKey(1), []byte("first"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	target := v.Header().Generation
	entered, release := d.arm(nil)
	defer release()
	fence := make(chan error, 1)
	go func() { fence <- v.CheckpointThrough(target) }()
	<-entered
	writer := make(chan error, 1)
	go func() { _, err := testPut(v, testKey(1), []byte("later"), Options{Accepted: true}); writer <- err }()
	awaitCheckpointTest(t, writer)
	mustGet(t, v, testKey(1), []byte("later"))
	if v.Header().Generation <= target {
		t.Fatal("later write was not independently published")
	}
	release()
	awaitCheckpointTest(t, fence)
	mustGet(t, v, testKey(1), []byte("later"))
	if v.IsDurableThrough(v.Header().Generation) {
		t.Fatal("old barrier incorrectly confirmed the later write")
	}
}
func TestCheckpointFailureFencesWaitingWriter(t *testing.T) {
	v, d := newCheckpointGate(t)
	if _, err := testPut(v, testKey(1), []byte("accepted"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	entered, release := d.arm(io.ErrClosedPipe)
	defer release()
	fence := make(chan error, 1)
	go func() { fence <- v.FlushAccepted() }()
	<-entered
	read := make(chan error, 1)
	go func() { _, _, err := testGet(v, testKey(1)); read <- err }()
	awaitCheckpointTest(t, read)
	writer := make(chan error, 1)
	go func() {
		_, err := testPut(v, testKey(2), []byte("accepted until failure"), Options{Accepted: true})
		writer <- err
	}()
	awaitCheckpointTest(t, writer)
	release()
	if err := <-fence; !errors.Is(err, ErrRecovery) {
		t.Fatalf("not fenced: %v", err)
	}
	if _, err := testPut(v, testKey(3), []byte("must fail"), Options{}); !errors.Is(err, ErrRecovery) {
		t.Fatalf("write survived a failed durability fence: %v", err)
	}

}

func TestExplicitSyncAllowsReaders(t *testing.T) {
	v, d := newCheckpointGate(t)
	if _, err := testPut(v, testKey(1), []byte("durable"), Options{}); err != nil {
		t.Fatal(err)
	}
	entered, release := d.arm(nil)
	defer release()
	flushed := make(chan error, 1)
	go func() { flushed <- v.Sync() }()
	<-entered
	read := make(chan error, 1)
	go func() { _, _, err := testGet(v, testKey(1)); read <- err }()
	awaitCheckpointTest(t, read)
	if _, pending := v.PendingCheckpoint(true, 1); pending {
		t.Fatal("physical flush blocked buffered publication")
	}
	writer := make(chan error, 1)
	go func() { _, err := testPut(v, testKey(2), []byte("new"), Options{Accepted: true}); writer <- err }()
	awaitCheckpointTest(t, writer)
	mustGet(t, v, testKey(2), []byte("new"))
	generation := uint64(0)
	waited := make(chan error, 1)
	go func() { waited <- v.CheckpointThrough(generation) }()
	select {
	case err := <-waited:
		t.Fatalf("admission wait returned before explicit flush: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	awaitCheckpointTest(t, flushed)
	awaitCheckpointTest(t, waited)
}
