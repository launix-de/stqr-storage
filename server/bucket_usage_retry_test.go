// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestRejectedCreateDoesNotRescanWholeBucketForQuota(t *testing.T) {
	s := testServer(t)
	v := addReplicaDataVolume(t, s, "quota-retry-index", "hdd", 16<<20)
	draft := newTestBucket(t, "quota-retry-index")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 1
	if _, status, _ := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	b, err := s.Bucket(draft.ID())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	actor := userPrincipal(t, s)
	for i := uint32(0); i < 2048; i++ {
		var id volume.ObjectID
		binary.LittleEndian.PutUint32(id[:], i)
		if _, err = b.Put(ctx, actor, id, nil, volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err = v.CheckpointThrough(v.Stats().Generation); err != nil {
		t.Fatal(err)
	}
	v.SetCacheLimits(1, 4096)
	before := v.IOStats().ReadBytes
	for i := 0; i < 32; i++ {
		_, err = b.Put(ctx, actor, volume.ObjectID{}, []byte("x"), volume.Options{RequestID: volume.ID(client.NewRequestID()), CreateOnly: true, Accepted: true})
		if !errors.Is(err, volume.ErrPrecondition) {
			t.Fatal("immutable object already exists", err)
		}
	}
	reads := v.IOStats().ReadBytes - before
	t.Logf("32 rejected creates: %d physical index-read bytes", reads)
	// A one-page cache still permits bounded B-tree path lookups. It must
	// not reread thousands of unrelated object/receipt records per attempt.
	if reads > 4<<20 {
		t.Errorf("rejected creates reread the bucket index: %d bytes", reads)
	}
	if _, err = b.Put(ctx, actor, volume.ObjectID{}, []byte("x"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Put(ctx, actor, volume.ObjectID{0xff, 0xff}, []byte("y"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); !errors.Is(err, volume.ErrFull) {
		t.Fatal("hard quota was not preserved", err)
	}
}

func TestPhysicalFullRetryDoesNotRescanWholeBucketForQuota(t *testing.T) {
	s := testServer(t)
	v := addReplicaDataVolume(t, s, "physical-retry-index", "hdd", 16<<20)
	draft := newTestBucket(t, "physical-retry-index")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 32 << 20
	if _, status, _ := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	b, err := s.Bucket(draft.ID())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	actor := userPrincipal(t, s)
	for i := uint32(0); i < 256; i++ {
		var id volume.ObjectID
		binary.LittleEndian.PutUint32(id[:], i)
		if _, err = b.Put(ctx, actor, id, nil, volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err = v.CheckpointThrough(v.Stats().Generation); err != nil {
		t.Fatal(err)
	}
	v.SetCacheLimits(1, 4096)
	before := v.IOStats().ReadBytes
	payload := make([]byte, 20<<20)
	for i := 0; i < 8; i++ {
		_, err = b.Put(ctx, actor, volume.ObjectID{}, payload, volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true})
		if !errors.Is(err, volume.ErrFull) {
			t.Fatal("physical capacity rejection", err)
		}
	}
	reads := v.IOStats().ReadBytes - before
	t.Logf("8 physically rejected writes: %d physical index-read bytes", reads)
	if reads > 1<<20 {
		t.Errorf("capacity retries reread the bucket index: %d bytes", reads)
	}
	if _, err = b.Trunc(ctx, actor, volume.ObjectID{}, 32<<20, volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Put(ctx, actor, volume.ObjectID{0xff, 0xff}, []byte("x"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); !errors.Is(err, volume.ErrFull) {
		t.Fatal("hard quota was not preserved", err)
	}
}
