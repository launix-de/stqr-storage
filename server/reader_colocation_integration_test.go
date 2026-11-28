// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDaemonReaderColocation(t *testing.T) {
	source, peer := startAutomaticDaemonPair(t, false)
	s := source.node
	src := addReplicaDataVolume(t, s, "reader-source", "hdd", 32<<20)
	dst := addReplicaDataVolume(t, peer.node, "reader-near", "hdd", 32<<20)
	reviewWaitDest(t, s, 2)
	b, _ := createProtocolBucket(t, s, "reader-colocation")
	object := volume.ObjectID{3}
	before, _ := s.routingPatch(b.ID(), object)
	plan, err := planRangeMove(before, before.Entries[0], s.scopes.node, src.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = s.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x67}, 1<<20)
	request := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	original, err := b.Put(ctx, userPrincipal(t, s), object, payload, request)
	if err != nil {
		t.Fatal(err)
	}
	// Equal-capacity healthy volumes alone do not justify moving this range.
	targets := s.scopes.buildPlacementTargets("", s.bucketSnapshots())
	if p, ok := s.scopes.nextPlacementTarget(targets); ok {
		t.Fatal("fixture moved without reader information", p)
	}
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, "http://"+source.listener.Addr().String(), admin.Username, admin.Password, client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetPlacementMeasurements(false)
	if err = c.ReportPlacementHints(ctx, []client.PlacementPath{
		{Node: s.scopes.node, RTTNS: uint64(500 * time.Millisecond), BytesPerSecond: 100 << 20},
		{Node: peer.node.scopes.node, RTTNS: uint64(time.Millisecond), BytesPerSecond: 100 << 20},
	}); err != nil {
		t.Fatal("report", err)
	}
	// Actual successful operations supply demand. Reports cannot invent traffic.
	for i := 0; i < 8; i++ {
		got, _, err := c.Get(ctx, client.ObjectID(object))
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("source read", len(got), err)
		}
	}
	policy := b.Describe().Policy
	s.scopes.start()
	peer.node.scopes.start()
	for {
		entry, _ := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(object))
		if len(entry.Locations) == 1 && entry.Locations[0].Node == peer.node.scopes.node && entry.Locations[0].Volume == client.ID(dst.Identity().VolumeID) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("unchanged-policy worker did not colocate reader", entry)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if b.Describe().Policy != policy || b.Describe().DesiredPolicy != nil {
		t.Fatal("locality changed hard policy")
	}
	got, _, err := c.Get(ctx, client.ObjectID(object))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("reader after relocation", len(got), err)
	}
	// The preexisting mutation receipt must travel with the data.
	moved, err := peer.node.Bucket(b.ID())
	if err != nil {
		t.Fatal(err)
	}
	again, err := moved.Put(ctx, userPrincipal(t, s), object, payload, request)
	if err != nil || again != original {
		t.Fatal("relocation lost retry result", again, original, err)
	}
}
