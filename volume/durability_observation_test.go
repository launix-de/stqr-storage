// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"testing"
	"time"
)

func TestTryIsDurableThroughTracksConfirmedProgress(t *testing.T) {
	v, disk := newCheckpointGate(t)
	baseline := v.Header().Generation
	if !v.TryIsDurableThrough(baseline) {
		t.Fatal("formatted generation is not confirmed")
	}
	if _, err := testPut(v, testKey(1), []byte("accepted"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	target := v.Header().Generation
	if target <= baseline || v.TryIsDurableThrough(target) {
		t.Fatal("accepted generation was falsely confirmed")
	}
	entered, release := disk.arm(nil)
	defer release()
	flushed := make(chan error, 1)
	go func() { flushed <- v.CheckpointThrough(target) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint did not start")
	}
	if !v.TryIsDurableThrough(baseline) || v.TryIsDurableThrough(target) {
		t.Fatal("in-flight checkpoint changed confirmed progress")
	}
	release()
	awaitCheckpointTest(t, flushed)
	if !v.TryIsDurableThrough(target) || v.TryIsDurableThrough(target+1) {
		t.Fatal("completed checkpoint confirmation is incorrect")
	}
}

func TestTryIsDurableThroughDoesNotWaitForWriter(t *testing.T) {
	v, _ := newCheckpointGate(t)
	confirmed := v.Header().Generation
	func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		result := make(chan bool, 1)
		go func() { result <- v.TryIsDurableThrough(confirmed) }()
		select {
		case durable := <-result:
			if durable {
				t.Fatal("busy metadata was treated as positive confirmation")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("optional confirmation waited for an unrelated writer")
		}
	}()
	if !v.TryIsDurableThrough(confirmed) {
		t.Fatal("confirmation did not resume after writer released metadata")
	}
}
