// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"sync"
	"testing"
	"time"
)

func TestDurableACKDoesNotBlockPublishedReadsOrLaterWrites(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "fenced", "hdd", 32<<20)
	var managed *managedVolume
	s.mu.RLock()
	for _, m := range s.volumes {
		if m.config.Name == "fenced" {
			managed = m
		}
	}
	s.mu.RUnlock()
	if managed == nil {
		t.Fatal("missing volume")
	}
	identity := managed.store.Identity()
	managed.store.Close()
	memory, err := volume.NewMemory(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	gate := &serverFlushGate{VolumeIO: memory}
	v, err := volume.Format(gate, volume.FormatOptions{Class: volume.HDD, ClusterID: identity.ClusterID, NodeID: identity.NodeID, VolumeID: identity.VolumeID})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	managed.store = v
	s.mu.Unlock()
	bucket, _ := createProtocolBucket(t, s, "fence-client")
	bucket.mu.Lock()
	bucket.description.LocalVolumes = []string{identity.VolumeID.String()}
	bucket.mu.Unlock()
	p := quotaPrincipal(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := volume.ObjectID{1}
	if _, err := bucket.Put(ctx, p, key, []byte("old"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	gate.mu.Lock()
	gate.entered, gate.release = entered, release
	gate.mu.Unlock()
	durable := make(chan error, 1)
	go func() {
		_, err := bucket.Put(ctx, p, key, []byte("published"), volume.Options{RequestID: volume.ID(client.NewRequestID())})
		durable <- err
	}()
	select {
	case <-entered:
	case err := <-durable:
		t.Fatalf("write did not reach its fence: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	progress := make(chan error, 1)
	go func() {
		got, _, err := bucket.Get(ctx, p, key)
		if err != nil {
			progress <- err
			return
		}
		if string(got) != "published" {
			progress <- fmt.Errorf("read saw %q before ACK", got)
			return
		}
		_, err = bucket.Put(ctx, p, volume.ObjectID{2}, []byte("later"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())})
		if err != nil {
			progress <- err
			return
		}
		got, _, err = bucket.Get(ctx, p, volume.ObjectID{2})
		if err == nil && string(got) != "later" {
			err = fmt.Errorf("later write saw %q", got)
		}
		progress <- err
	}()
	select {
	case err := <-progress:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("physical sync held node admission or volume access")
	}
	select {
	case err := <-durable:
		t.Fatalf("durable ACK passed an unfinished fence: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-durable:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
