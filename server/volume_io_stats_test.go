// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestVolumeAccessIOCounterFamilies(t *testing.T) {
	in := volume.IOStats{ReadOps: 3, ReadBytes: 11, ViewReadOps: 5, ViewReadBytes: 17, WriteOps: 7, WriteBytes: 23, Errors: 2, SyncOps: 13, SyncNanoseconds: 19}
	want := client.IOStats{PhysicalReadBytes: 28, PhysicalWriteBytes: 23, Errors: 2, Flushes: 13, FlushNS: 19}
	if got := volumeAccessIO(in); !reflect.DeepEqual(got, want) {
		t.Fatal("backend families lost, doubled or mistaken for object ops", got, want)
	}
	for _, in := range []volume.IOStats{{ReadBytes: 31}, {ViewReadBytes: 31}, {ReadBytes: 13, ViewReadBytes: 18}} {
		if got := volumeAccessIO(in); got.PhysicalReadBytes != 31 {
			t.Fatal(in, got)
		}
	}
	if got := volumeAccessIO(volume.IOStats{}); got != (client.IOStats{}) {
		t.Fatal("idle storage access invented traffic", got)
	}
}

func TestCollectStatsIncludesMappedAccessWithoutInventingDiskIO(t *testing.T) {
	s := testServer(t)
	op := addTestVolume(t, s, "mapped-access-telemetry", "hdd", 4<<20)
	v := s.volumes[op.VolumeID].store
	b, _ := createProtocolBucket(t, s, "mapped-access-stats")
	key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}
	payload := bytes.Repeat([]byte("mapped"), 1024)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(key, payload, volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	baseline := s.CollectStats(client.StatsFilter{})
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	_, err = h.CopyTo(key, 0, uint64(len(payload)), &out)
	h.Close()
	if err != nil || !bytes.Equal(out.Bytes(), payload) {
		t.Fatal(err)
	}
	after := v.IOStats()
	if after.ViewReadBytes <= before.ViewReadBytes || after.ReadBytes != before.ReadBytes || after.ReadOps != before.ReadOps {
		t.Fatal("mapped source was misclassified as positioned/device reads", before, after)
	}
	snap := s.CollectStats(client.StatsFilter{})
	id := client.ID(v.Identity().VolumeID)
	find := func(snapshot client.StatsSnapshot) client.VolumeStats {
		for _, row := range snapshot.Volumes {
			if row.ID == id {
				return row
			}
		}
		t.Fatal("volume missing")
		return client.VolumeStats{}
	}
	old, current := find(baseline), find(snap)
	delta := after.ReadBytes - before.ReadBytes + after.ViewReadBytes - before.ViewReadBytes
	if got := current.IO.PhysicalReadBytes - old.IO.PhysicalReadBytes; got != delta || got < uint64(len(payload)) {
		t.Fatal("mapped bytes omitted/double-counted in volume wire", got, delta)
	}
	if got := snap.IO.PhysicalReadBytes - baseline.IO.PhysicalReadBytes; got < delta {
		t.Fatal("node total omitted mapped volume traffic", got, delta)
	}
	// Backend reads must not manufacture delivered client traffic. Collecting or
	// repeating a cached stats snapshot must itself cause no storage access.
	for _, snapshot := range []client.StatsSnapshot{baseline, snap} {
		for _, row := range snapshot.Buckets {
			if row.ID == client.ID(b.ID()) && row.IO.LogicalReadBytes != 0 {
				t.Fatal("backend counters leaked into logical payload", row.IO)
			}
		}
	}
	beforeCollect := v.IOStats()
	again := s.CollectStats(client.StatsFilter{})
	afterCollect := v.IOStats()
	if beforeCollect != afterCollect || find(again).IO != current.IO {
		t.Fatal("stat collection itself generated or double-counted volume reads", beforeCollect, afterCollect)
	}
	// Existing binary codec must retain this exact storage-access total.
	decoded, err := client.DecodeStats(client.EncodeStats(snap))
	if err != nil || find(decoded).IO != current.IO {
		t.Fatal("wire counter roundtrip changed", err)
	}
}
