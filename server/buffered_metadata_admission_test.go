// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A foreground accepted write can lose the metadata TryLock while an explicit
// checkpoint has released that lock for physical I/O. Releasing the reader must
// let the write publish in the next overlay, without releasing the I/O fence.
func TestAcceptedMutationMetadataContentionDoesNotWaitForCheckpoint(t *testing.T) {
	s := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false).node
	addTestVolume(t, s, "buffered-contention", "hdd", 32<<20)
	var gate *serverFlushGate
	v := replaceTestVolumeIO(t, s, "buffered-contention", func(dev volume.VolumeIO) volume.VolumeIO { gate = &serverFlushGate{VolumeIO: dev}; return gate })
	b, _ := createProtocolBucket(t, s, "buffered-contention")
	p := quotaPrincipal(t, s)
	ctx := context.Background()
	id := volume.ObjectID{17}
	if _, err := b.Put(ctx, p, id, []byte("initial"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(ctx, p, id, []byte("before"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); err != nil {
		t.Fatal(err)
	}
	generation := connectionTestPut(t, v, volume.Key{Bucket: b.ID(), Object: volume.ObjectID{18}}, "cut")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	gate.mu.Lock()
	gate.entered = entered
	gate.release = release
	gate.mu.Unlock()
	checkpoint := make(chan error, 1)
	go func() { checkpoint <- v.CheckpointThrough(generation) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("checkpoint did not enter controlled I/O")
	}
	reader, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	done := make(chan error, 1)
	go func() {
		_, err := b.Put(ctx, p, id, []byte("after"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true})
		done <- err
	}()
	// Read handles may coexist with pre-admission inspection, but prevent the
	// final exclusive TryOpenBuffered. Leave enough time to encounter contention.
	select {
	case err := <-done:
		t.Fatalf("mutation unexpectedly completed while reader owns metadata: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	reader.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("accepted mutation waited for physical checkpoint after metadata reader closed")
	}
	select {
	case err := <-checkpoint:
		t.Fatalf("checkpoint escaped its explicit I/O gate: %v", err)
	default:
	}
	data, _, err := b.Get(ctx, p, id)
	if err != nil || string(data) != "after" {
		t.Fatalf("new accepted bytes not visible: %q %v", data, err)
	}
	unblock()
	select {
	case err := <-checkpoint:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("explicit checkpoint did not finish")
	}
	if err := v.CheckpointThrough(generation); err != nil {
		t.Fatal(err)
	}
}
