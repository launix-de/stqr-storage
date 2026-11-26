// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// Observe Cond.Wait's atomic registration/unlock, without a scheduling sleep or
// a production hook. Ordinary volume lock releases do not signal this channel.
type maintenanceWaitLocker struct {
	mu      *sync.RWMutex
	waiting chan struct{}
}

func (l *maintenanceWaitLocker) Lock() { l.mu.Lock() }
func (l *maintenanceWaitLocker) Unlock() {
	l.mu.Unlock()
	l.waiting <- struct{}{}
}

func TestMaintenanceReleaseWakesOpenWithoutCheckpoint(t *testing.T) {
	v, _ := newCheckpointGate(t)
	v.mu.Lock()
	v.beginMaintenance()
	waiting := make(chan struct{}, 1)
	v.prepareCond = sync.NewCond(&maintenanceWaitLocker{mu: &v.mu, waiting: waiting})
	v.mu.Unlock()

	opened := make(chan error, 1)
	go func() {
		h, err := v.Open(true)
		if h != nil {
			h.Close()
		}
		opened <- err
	}()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("Open did not wait for maintenance")
	}
	// No physical checkpoint follows this release. Only the predicate change
	// itself can wake the registered public Open waiter.
	v.mu.Lock()
	v.endMaintenance()
	v.mu.Unlock()
	awaitCheckpointTest(t, opened)
}

func TestOlderDrainPreservesLaterMaintenanceReservation(t *testing.T) {
	v, disk := newCheckpointGate(t)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("accepted"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	entered, release := disk.arm(nil)
	defer release()
	drained := make(chan error, 1)
	go func() {
		h, err := v.Open(true)
		if err == nil {
			err = h.ForgetRequest(testID(99))
			h.Close()
		}
		drained <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("generic metadata drain did not reach physical I/O")
	}
	// Direct maintenance callers can reserve while an older drain releases mu
	// for I/O. Its eventual exit must leave this later owner's admission closed.
	v.mu.Lock()
	v.beginMaintenance()
	v.mu.Unlock()
	release()
	awaitCheckpointTest(t, drained)
	h, acquired, err := v.TryOpenBuffered()
	if h != nil {
		h.Close()
	}
	v.mu.Lock()
	v.endMaintenance()
	v.mu.Unlock()
	if acquired || !errors.Is(err, ErrBusy) {
		t.Fatalf("older drain erased later maintenance: acquired=%v err=%v", acquired, err)
	}
	if _, err := testPut(v, testKey(2), []byte("after"), Options{Accepted: true}); err != nil {
		t.Fatal("admission did not resume after final maintenance owner", err)
	}
	mustGet(t, v, key, []byte("accepted"))
	mustGet(t, v, testKey(2), []byte("after"))
}

type maintenancePanicIO struct {
	VolumeIO
	panicSync bool
}

func (d *maintenancePanicIO) Sync() error {
	if d.panicSync {
		panic("maintenance checkpoint panic")
	}
	return d.VolumeIO.Sync()
}

func TestRecoveredDrainPanicDoesNotStrandAdmission(t *testing.T) {
	memory, err := NewMemory(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	disk := &maintenancePanicIO{VolumeIO: memory}
	v, err := Format(disk, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	if _, err := testPut(v, testKey(1), []byte("pending"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	disk.panicSync = true
	func() {
		defer h.Close()
		defer func() {
			if got := recover(); got != "maintenance checkpoint panic" {
				t.Errorf("unexpected recovered panic: %v", got)
			}
		}()
		if err := h.ForgetRequest(testID(100)); err != nil {
			t.Errorf("drain returned instead of panicking: %v", err)
		}
	}()
	admission := make(chan error, 1)
	go func() {
		h, err := v.Open(true)
		if h != nil {
			h.Close()
		}
		if !errors.Is(err, ErrRecovery) {
			admission <- errors.New("recovered panic did not retain recovery fencing")
			return
		}
		admission <- nil
	}()
	awaitCheckpointTest(t, admission)
	h, acquired, err := v.TryOpenBuffered()
	if h != nil {
		h.Close()
	}
	if acquired || !errors.Is(err, ErrRecovery) {
		t.Fatalf("buffered admission masked recovery: acquired=%v err=%v", acquired, err)
	}
}
