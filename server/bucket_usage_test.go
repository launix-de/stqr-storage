// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestBucketUsageCountsCommitsWithoutRepeatedIndexScans(t *testing.T) {
	s := testServer(t)
	op := addTestVolume(t, s, "usage", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "usage")
	p := userPrincipal(t, s)
	ctx := context.Background()
	options := func() volume.Options { return volume.Options{RequestID: volume.ID(client.NewRequestID())} }
	var total uint64
	for i := 0; i < 128; i++ {
		data := make([]byte, i)
		if _, err := b.Put(ctx, p, volume.ObjectID{byte(i)}, data, options()); err != nil {
			t.Fatal(err)
		}
		total += uint64(i)
	}
	v := s.volumes[op.VolumeID].store
	v.SetCacheLimits(-1, 0)
	before := v.IOStats()
	if used, err := b.logicalUsage(); err != nil || used != total {
		t.Fatal("initial count", used, err)
	}
	warm := v.IOStats()
	if warm.ReadOps <= before.ReadOps {
		t.Fatal("test did not exercise a cold volume index")
	}
	for i := 0; i < 20; i++ {
		if used, err := b.logicalUsage(); err != nil || used != total {
			t.Fatal("cached count", used, err)
		}
	}
	if v.IOStats().ReadOps != warm.ReadOps {
		t.Fatal("warm usage read traversed the object index")
	}
	id := volume.ObjectID{1}
	appendOptions := options()
	first, err := b.Append(ctx, p, id, []byte("1234567890"), appendOptions)
	if err != nil {
		t.Fatal(err)
	}
	total += 10
	// Replaying an old result after a newer write cannot roll the counter back
	// or charge its original ten-byte append again.
	if _, err := b.PutRange(ctx, p, id, 2047, []byte{1}, options()); err != nil {
		t.Fatal(err)
	}
	total += 2048 - 11
	if retry, err := b.Append(ctx, p, id, []byte("1234567890"), appendOptions); err != nil || retry != first {
		t.Fatal("retry", retry, err)
	}
	if _, err := b.Trunc(ctx, p, id, 3, options()); err != nil {
		t.Fatal(err)
	}
	total -= 2048 - 3
	if err := b.Delete(ctx, p, id, options()); err != nil {
		t.Fatal(err)
	}
	total -= 3
	before = v.IOStats()
	if used, err := b.logicalUsage(); err != nil || used != total {
		t.Fatal("incremental count", used, total, err)
	}
	if v.IOStats().ReadOps != before.ReadOps {
		t.Fatal("successful mutations invalidated the incremental counter")
	}
}

func TestBucketUsageConcurrentMeasurementsAndAppends(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "usage", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "usage-concurrent")
	p := userPrincipal(t, s)
	ctx := context.Background()
	if _, err := b.logicalUsage(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := b.Append(ctx, p, volume.ObjectID{1}, []byte{1}, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				errors <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := b.logicalUsage(); err != nil {
				errors <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	used, err := b.logicalUsage()
	if err != nil || used != 100 {
		t.Fatal("measurement raced a commit", used, err)
	}
}
