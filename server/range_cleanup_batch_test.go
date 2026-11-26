// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestRangeCleanupBatchesReceiptsAndPreservesOtherBuckets(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cleanup", "hdd", 32<<20)
	var v *volume.Volume
	s.mu.RLock()
	for _, m := range s.volumes {
		if m.config.Name == "cleanup" {
			v = m.store
		}
	}
	s.mu.RUnlock()
	if v == nil {
		t.Fatal("missing volume")
	}
	bucket, _ := createProtocolBucket(t, s, "retired")
	bucket.mu.Lock()
	bucket.description.LocalVolumes = []string{v.Identity().VolumeID.String()}
	bucket.mu.Unlock()
	node, err := parseID(s.cfg.Node.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := client.RangeEntry{PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{{Node: client.ID(node), Volume: client.ID(v.Identity().VolumeID), Class: uint8(volume.HDD)}}}
	before := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID(bucket.ID()), ToEnd: true, Entries: []client.RangeEntry{entry}, Addresses: []client.NodeAddress{
		{Node: client.ID(node), Addresses: []string{"http://127.0.0.1:7419"}},
		{Node: client.ID{2}, Addresses: []string{"http://127.0.0.1:7420"}},
	}}
	plan, err := planRangeMove(before, entry, client.ID{2}, volume.ID{3}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := &rangeManager{server: s, node: client.ID(node), txns: map[[32]byte]*rangeTxn{plan.id(): {plan: plan, decision: &rangeCommit{}}}}
	key := volume.Key{Bucket: bucket.ID(), Object: volume.ObjectID{1}}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	var ids []volume.ID
	for i := 1; i <= 128; i++ {
		id := volume.ID{byte(i)}
		ids = append(ids, id)
		if _, err := h.Put(key, []byte("obsolete"), volume.Options{Accepted: true, RequestID: id}); err != nil {
			t.Fatal(err)
		}
	}
	other := volume.Key{Bucket: volume.ID{99}, Object: volume.ObjectID{1}}
	if _, err := h.Put(other, []byte("keep"), volume.Options{Accepted: true, RequestID: volume.ID{255}}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if err := v.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	beforeIO := v.IOStats()
	if err := r.cleanup(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	afterIO := v.IOStats()
	if n := afterIO.SyncOps - beforeIO.SyncOps; n > 16 {
		t.Fatalf("128 request IDs still flushed individually: %d barriers", n)
	}
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.RecordInfo(key); !errors.Is(err, volume.ErrNotFound) {
		t.Fatalf("old object retained: %v", err)
	}
	for _, id := range ids {
		if _, ok, err := h.OperationStatus(id); err != nil || ok {
			t.Fatalf("old retry retained: %v %v", ok, err)
		}
	}
	if _, ok, err := h.OperationStatus(volume.ID{255}); err != nil || !ok {
		t.Fatalf("other bucket receipt removed: %v %v", ok, err)
	}
	if got, _, err := h.Get(other); err != nil || string(got) != "keep" {
		t.Fatalf("other bucket changed: %q %v", got, err)
	}
}
