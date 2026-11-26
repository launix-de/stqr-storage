// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"sync"
	"testing"
	"time"
)

type serverFlushGate struct {
	volume.VolumeIO
	mu               sync.Mutex
	entered, release chan struct{}
}

func (d *serverFlushGate) Sync() error {
	d.mu.Lock()
	entered, release := d.entered, d.release
	if entered != nil {
		d.entered = nil
		close(entered)
	}
	d.mu.Unlock()
	if release != nil {
		<-release
	}
	return d.VolumeIO.Sync()
}
func TestAcceptedVolumeSwitchWaitsOnlyForExplicitSync(t *testing.T) {
	s := testServer(t)
	for _, name := range []string{"a", "b", "other"} {
		addTestVolume(t, s, name, "hdd", 32<<20)
	}
	var a, b, other *volume.Volume
	var managedA *managedVolume
	s.mu.RLock()
	for _, m := range s.volumes {
		switch m.config.Name {
		case "a":
			a, managedA = m.store, m
		case "b":
			b = m.store
		case "other":
			other = m.store
		}
	}
	s.mu.RUnlock()
	if a == nil || b == nil || other == nil {
		t.Fatal("missing fixture volumes")
	}
	identity := a.Identity()
	a.Close()
	memory, err := volume.NewMemory(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	gate := &serverFlushGate{VolumeIO: memory}
	a, err = volume.Format(gate, volume.FormatOptions{Class: volume.HDD, ClusterID: identity.ClusterID, NodeID: identity.NodeID, VolumeID: identity.VolumeID})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	managedA.store = a
	s.mu.Unlock()
	bucket, _ := createProtocolBucket(t, s, "switching")
	independent, _ := createProtocolBucket(t, s, "independent")
	bucket.mu.Lock()
	bucket.description.LocalVolumes = []string{a.Identity().VolumeID.String(), b.Identity().VolumeID.String()}
	bucket.mu.Unlock()
	independent.mu.Lock()
	independent.description.LocalVolumes = []string{other.Identity().VolumeID.String()}
	independent.mu.Unlock()
	key := volume.Key{Bucket: bucket.ID(), Object: volume.ObjectID{1}}
	h, err := b.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(key, []byte("old"), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	h, err = a.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(volume.Key{Bucket: bucket.ID(), Object: volume.ObjectID{2}}, []byte("dependency"), volume.Options{Accepted: true})
	generation, _ := h.Generation()
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	s.recordAccepted(bucket.ID(), a, generation)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	gate.mu.Lock()
	gate.entered, gate.release = entered, release
	gate.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := quotaPrincipal(t, s)
	finished := make(chan error, 1)
	go func() {
		_, err := bucket.Put(ctx, p, key.Object, []byte("next"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true})
		finished <- err
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-entered:
		t.Fatal("changing destination started an unrequested flush")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	mark, pending := s.acceptedMark(bucket.ID())
	if !pending {
		t.Fatal("missing accepted mark")
	}
	fence := client.Encoder{}
	fence.ID(client.BucketID(bucket.ID()))
	fence.ID(mark.Boot)
	fence.U64(mark.Sequence)
	go func() {
		_, _, err := s.executeSync(ctx, client.Message{Opcode: client.BucketSync, Body: fence.B}, nil, p)
		finished <- err
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("explicit fence did not reach storage: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	separate := make(chan error, 1)
	go func() {
		_, err := independent.Put(ctx, p, volume.ObjectID{3}, []byte("independent"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true})
		separate <- err
	}()
	select {
	case err := <-separate:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("one volume's flush held the global node gate")
	}
	select {
	case err := <-finished:
		t.Fatalf("explicit Sync passed an unfinished checkpoint: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	h, err = b.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	_, err = h.CopyTo(key, 0, 4, &got)
	h.Close()
	if err != nil || got.String() != "next" {
		t.Fatalf("retry consumed or changed the payload: %q %v", got.String(), err)
	}
	if !a.IsDurableThrough(generation) {
		t.Fatal("dependency was not durable")
	}
}
