// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestBlockMaintenanceWaitsForCapacityHandoff(t *testing.T) {
	// Hold back the planner until a real client mutation encounters local
	// exhaustion. This proves the pressure path, rather than lucky balancing.
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "small", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, source.node, "block-capacity")
	remote := addTestVolume(t, target.node, "large", "ssd", 16<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	for {
		target.node.publishStats()
		destinations, _ := source.node.scopes.destinations(b.Describe())
		ready := false
		for _, d := range destinations {
			ready = ready || d.volume.ID.String() == remote.VolumeID
		}
		if ready {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("remote capacity missing", ctx.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
	a := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), "bd")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.BeginBlockMaintenance(ctx, false); err != nil {
		t.Fatal(err)
	}
	lease := source.node.leaseRecord(b)
	if lease == nil || !lease.AllowHandoff || b.blocksPlacementChange() {
		t.Fatal("block lease pinned placement")
	}
	payload := bytes.Repeat([]byte{0x5a}, 1<<20)
	for i := byte(1); i <= 3; i++ {
		if _, err = c.PutRange(ctx, client.ObjectID{i}, 0, payload, client.PutOptions{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	request := client.NewRequestID()
	go func() {
		_, e := c.PutRange(ctx, client.ObjectID{4}, 0, payload, client.PutOptions{Accepted: true, RequestID: request})
		done <- e
	}()
	for {
		r := source.node.scopes
		r.mu.Lock()
		pending := len(r.pressures) > 0
		r.mu.Unlock()
		if pending {
			break
		}
		select {
		case e := <-done:
			t.Fatal("full-volume request did not wait for migration", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(source.node.scopes.capacityWake) != 1 {
		t.Fatal("capacity pressure did not wake planner immediately")
	}
	source.node.scopes.start()
	select {
	case err = <-done:
		if err != nil {
			logDaemonRangeState(t, source, target)
			t.Fatal("blocked write did not resume", err)
		}
	case <-ctx.Done():
		logDaemonRangeState(t, source, target)
		t.Fatal(ctx.Err())
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for i := byte(1); i <= 4; i++ {
		got, _, e := c.GetRange(ctx, client.ObjectID{i}, 0, uint64(len(payload)))
		if e != nil || !bytes.Equal(got, payload) {
			t.Fatal("handoff changed bytes", i, e)
		}
	}
	current := source.node.leaseRecord(b)
	if current.Token != lease.Token || current.Released {
		t.Fatal("handoff ended block lease")
	}
	if other, e := client.Dial(ctx, target.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), "bd"); e == nil {
		other.Close()
		t.Fatal("handoff admitted competing client")
	}
	// Genuine cluster exhaustion still terminates, rather than waiting forever.
	_, err = c.PutRange(ctx, client.ObjectID{5}, 0, bytes.Repeat([]byte{3}, 17<<20), client.PutOptions{})
	if !errors.Is(err, client.ErrFull) {
		t.Fatal("no suitable volume must return capacity error", err)
	}
}

func TestOfflineInventoriesKeepPlacementFence(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "pinned-inventory")
	remote := addTestVolume(t, target.node, "data", "hdd", 8<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	vid, _ := parseID(remote.VolumeID)
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID{})
	check := func() {
		t.Helper()
		if !b.blocksPlacementChange() {
			t.Fatal("offline inventory lost placement fence")
		}
		plan, e := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 0)
		if e != nil {
			t.Fatal(e)
		}
		if e = source.node.scopes.move(ctx, plan); statusOf(e) != 409 {
			t.Fatal("handoff crossed inventory fence", e)
		}
	}
	actor := userPrincipal(t, source.node)
	actor.frontendPermission = BucketRead
	copyClient, err := source.node.openFrontend(ctx, b.ID(), actor)
	if err != nil {
		t.Fatal(err)
	}
	if err = copyClient.BeginBlockMaintenance(ctx, true); err != nil {
		copyClient.Close()
		t.Fatal(err)
	}
	check()
	copyClient.Close()
	// END is synchronous; once its websocket is gone, fsck can bind its own
	// distinct lease and must retain the same placement exclusion.
	deadline := time.Now().Add(3 * time.Second)
	for {
		source.node.wsMu.Lock()
		live := 0
		for ws := range source.node.sockets {
			if ws.authorization != nil && ws.authorization.bucketID == b.ID() && !ws.authorization.invalid.Load() {
				live++
			}
		}
		source.node.wsMu.Unlock()
		if live == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("copy socket retained")
		}
		time.Sleep(10 * time.Millisecond)
	}
	a := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), "bd")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.BeginMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestBlockLeaseWaitsForExistingHandoff(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "lease-after-handoff")
	remote := addTestVolume(t, target.node, "data", "hdd", 8<<20)
	vid, _ := parseID(remote.VolumeID)
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID{})
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.reserve(plan); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), "bd")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan error, 1)
	go func() { done <- c.BeginBlockMaintenance(ctx, false) }()
	select {
	case err := <-done:
		t.Fatal("handoff should delay lease acquisition", err)
	case <-time.After(250 * time.Millisecond):
	}
	_ = source.node.scopes.cancelBeforeFence(ctx, plan, errors.New("release test handoff"))
	select {
	case err = <-done:
		if err != nil {
			t.Fatal("lease did not resume", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
