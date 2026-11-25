// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/binary"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type placementReceiptProbe struct {
	volume.VolumeIO
	armed   atomic.Bool
	reads   atomic.Uint64
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (p *placementReceiptProbe) ReadAt(data []byte, offset int64) (int, error) {
	n, err := p.VolumeIO.ReadAt(data, offset)
	if p.armed.Load() {
		var pcs [16]uintptr
		frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, ".loadDedupe") {
				p.reads.Add(1)
				p.once.Do(func() { close(p.entered); <-p.resume })
				break
			}
			if !more {
				break
			}
		}
	}
	return n, err
}

func placementInventoryFixture(t *testing.T) (*rangeManager, *volume.Volume, *placementReceiptProbe, client.MapPatch, BucketDescription) {
	t.Helper()
	dev, err := volume.NewMemory(32 << 20)
	if err != nil {
		t.Fatal(err)
	}
	probe := &placementReceiptProbe{VolumeIO: dev, entered: make(chan struct{}), resume: make(chan struct{})}
	vid, bid, node := volume.ID{3}, volume.ID{2}, client.ID{1}
	v, err := volume.Format(probe, volume.FormatOptions{Class: volume.HDD, VolumeID: vid})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	h, _ := v.Open(true)
	if _, err = h.Put(volume.Key{Bucket: bid, Object: volume.ObjectID{1}}, []byte("selected"), volume.Options{}); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 2048; i++ {
		var request volume.ID
		binary.BigEndian.PutUint64(request[8:], i)
		if _, err = h.Put(volume.Key{Bucket: volume.ID{4}, Object: volume.ObjectID{1}}, []byte("other bucket"), volume.Options{RequestID: request, Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	if err = v.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	d := BucketDescription{Schema: 1, ID: bid.String(), Name: "measurement", State: BucketActive, Revision: 1, Policy: BucketPolicy{Version: 1, StorageStrategy: "hdd"}}
	s := &Server{ctx: context.Background(), volumes: map[string]*managedVolume{vid.String(): {config: VolumeConfig{ID: vid.String(), State: "active"}, store: v}}, bucketsByID: make(map[volume.ID]*Bucket)}
	s.bucketsByID[bid] = &Bucket{server: s, description: d}
	p := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID(bid), ToEnd: true, Entries: []client.RangeEntry{{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: node, Volume: client.ID(vid)}}}}, Addresses: []client.NodeAddress{{Node: node, Addresses: []string{"http://node:7419"}}}}
	image, err := buildPlacement([]client.MapPatch{p})
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	s.currentSystem.Store(&systemView{manifest: systemManifest{Membership: client.Membership{Epoch: 1, Members: []client.Member{{ID: node, Addresses: []string{"http://node:7419"}}}}, Generation: 1, Revision: 1}})
	if err = v.SetAdmission(nil, func(snapshot volume.StorageSnapshot) { s.publishQuotaUsage(vid, snapshot.Owned, snapshot.All) }); err != nil {
		t.Fatal(err)
	}
	return &rangeManager{server: s, node: node}, v, probe, p, d
}

func TestPlacementInventoryYieldsVolumeToQueuedWriter(t *testing.T) {
	r, v, probe, p, _ := placementInventoryFixture(t)
	var resume sync.Once
	defer resume.Do(func() { close(probe.resume) })
	probe.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, _, err := r.policyPlacementCost(p, p.Entries[0], client.ID{}, volume.ID{}, volume.Raw)
		done <- err
	}()
	select {
	case <-probe.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request inventory did not start")
	}
	writer := make(chan uint64, 1)
	go func() {
		h, err := v.Open(true)
		if err != nil {
			writer <- ^uint64(0)
			return
		}
		reads := probe.reads.Load()
		h.Close()
		writer <- reads
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h, acquired, err := v.TryOpen(false)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			break // RWMutex has queued the writer behind the paused reader.
		}
		h.Close()
		if time.Now().After(deadline) {
			resume.Do(func() { close(probe.resume) })
			t.Fatal("writer was not queued")
		}
		time.Sleep(time.Millisecond)
	}
	resume.Do(func() { close(probe.resume) })
	select {
	case reads := <-writer:
		if reads > 512 {
			t.Errorf("receipt census blocked writer through %d reads", reads)
		}
	case <-time.After(5 * time.Second):
		t.Error("writer did not acquire between inventory pages")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("inventory did not complete")
	}
}

func TestPlacementWholeRangeUsesCachedUsageWithoutIO(t *testing.T) {
	r, v, _, p, d := placementInventoryFixture(t)
	shape := placementShape{bucket: p.Bucket, copies: 1}
	allowance := 1
	before := v.IOStats()
	m, ok := r.measuredPlacement(shape, p.Entries[0], d, false, &allowance)
	after := v.IOStats()
	if !ok || m.cost == 0 || after.ReadOps != before.ReadOps || after.WriteOps != before.WriteOps {
		t.Fatal("whole-range optimization read storage", ok, m.cost, before, after)
	}
	// An optimization estimate must not be reused as a mandatory measurement.
	before = after
	m, ok = r.measuredPlacement(shape, p.Entries[0], d, true, &allowance)
	if !ok || m.estimateOnly || v.IOStats().ReadOps == before.ReadOps {
		t.Fatal("required measurement reused an optimization estimate")
	}
}
