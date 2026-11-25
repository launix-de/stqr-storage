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

func TestRangeCleanupBatchesObjectsAndReceipts(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cleanup", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, s, "cleanup")
	v := b.allVolumes()[0]
	before := rangeWireFixture(t).Before
	before.Bucket = client.ID(b.ID())
	before.Entries[0].Locations[0].Volume = client.ID(v.Identity().VolumeID)
	_, right, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	p, err := planRangeMove(before, right, client.ID{2}, volume.ID{5}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := &rangeManager{server: s, txns: map[[32]byte]*rangeTxn{}}
	r.txns[p.id()] = &rangeTxn{plan: p, decision: &rangeCommit{}}
	type saved struct {
		key     volume.Key
		receipt volume.ID
		purge   bool
	}
	var savedRecords []saved
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 192; i++ {
		k := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{0x80, byte(i)}}
		purge := i < 128
		if i >= 128 {
			k.Object[0] = 1
		}
		var req volume.ID
		binary.LittleEndian.PutUint64(req[:], uint64(i+1))
		if _, err = h.Put(k, []byte("small"), volume.Options{RequestID: req}); err != nil {
			t.Fatal(err)
		}
		savedRecords = append(savedRecords, saved{k, req, purge})
	}
	h.Close()
	old := v.IOStats()
	if err = r.cleanup(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	delta := v.IOStats().SyncOps - old.SyncOps
	if delta > 40 {
		t.Fatalf("cleanup committed per object/receipt: %d flushes", delta)
	}
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	for _, item := range savedRecords {
		_, err := h.Stat(item.key)
		if item.purge && !errors.Is(err, volume.ErrNotFound) || !item.purge && err != nil {
			t.Fatalf("incorrect cleanup boundary: %+v %v", item.key, err)
		}
		_, found, err := h.OperationStatus(item.receipt)
		if err != nil || found == item.purge {
			t.Fatalf("incorrect receipt cleanup: found=%v purge=%v err=%v", found, item.purge, err)
		}
	}
	t.Logf("128 objects and 128 retry records cleaned in %d flushes", delta)
}
