// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDaemonInventoryMergesTombstonesAndWaitsOnlyForOwners(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "inventory")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ids := []client.ObjectID{{}, {1}, {2}, {3}, {0x80}, {0x81}, {0x82}, {0xff}}
	for _, id := range ids {
		if _, err := c.Put(ctx, id, []byte{8, 9}, client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	deleted := map[client.ObjectID]bool{ids[0]: true, ids[1]: true, ids[4]: true}
	for id := range deleted {
		if err := c.Delete(ctx, id, client.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// A known member without any range of this bucket is not a dependency.
	target.close()
	page, next, err := c.ScanObjects(ctx, nil, 1000)
	if err != nil || next != nil || len(page) != len(ids)-len(deleted) {
		t.Fatal("inventory waited for an unrelated offline node", err)
	}
	target = restartDaemonTestNode(t, target)
	until := time.Now().Add(10 * time.Second)
	for {
		if _, err := source.node.controller.info(ctx, target.node.control.ledger.node); err == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal("inventory peer failed to reconnect")
		}
		time.Sleep(25 * time.Millisecond)
	}
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID{0x81})
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	var targetVolume volume.ID
	for _, v := range target.node.volumes {
		if !v.config.System {
			targetVolume = v.store.Identity().VolumeID
		}
	}
	plan, err := planRangeMove(before, moving, target.node.control.ledger.node, targetVolume, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		found := map[client.ObjectID]bool{}
		var after *client.ObjectID
		for pages := 0; ; pages++ {
			if pages > 20 {
				t.Fatal("inventory cursor failed to progress")
			}
			page, next, err := c.ScanObjects(ctx, after, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, object := range page {
				if deleted[object.ID] || found[object.ID] {
					t.Fatal("tombstone/duplicate leaked", object.ID)
				}
				if object.Info.Length != 2 {
					t.Fatal("incorrect inventory metadata")
				}
				found[object.ID] = true
			}
			if next == nil {
				break
			}
			after = next
		}
		if len(found) != len(ids)-len(deleted) {
			t.Fatal("remote live objects omitted", len(found))
		}
	}
	check()
	if err := target.node.prepareShortShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	target.close()
	deadline, stop := context.WithTimeout(ctx, 350*time.Millisecond)
	page, next, err = c.ScanObjects(deadline, nil, 1)
	stop()
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || page != nil || next != nil {
		t.Fatal("offline inventory owner produced a partial successful list", page, next, err)
	}
	if _, _, err := c.Get(ctx, ids[2]); err != nil {
		t.Fatal("independent object read waited for inventory peer", err)
	}
	target = restartDaemonTestNode(t, target)
	check()
}

func TestInventoryPeerDisconnectKeepsCallerCancellationDistinct(t *testing.T) {
	target := testServer(t)
	source := peerTestNode(t, target.cfg)
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	node, _ := parseID(target.cfg.Node.ID)
	peer, err := source.dialPeer(ctx, host.URL, client.ID(node))
	if err != nil {
		t.Fatal(err)
	}
	verifyPeerPing(t, peer)
	peer.close()
	_, transportErr := peer.request(ctx, peerBucketInventory, nil)
	if !errors.Is(transportErr, context.Canceled) || ctx.Err() != nil {
		t.Fatal("expected owner transport cancellation with a live caller", transportErr, ctx.Err())
	}
	if err := inventoryCallError(ctx, transportErr); objectStatus(err) != 503 {
		t.Fatal("lost inventory peer must request retry", objectStatus(err), err)
	}
	cancel()
	if err := inventoryCallError(ctx, transportErr); !errors.Is(err, context.Canceled) || objectStatus(err) != 499 {
		t.Fatal("caller cancellation lost its identity", err)
	}
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if err := inventoryCallError(deadline, transportErr); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("caller deadline lost its identity", err)
	}
	other := errors.New("independent storage failure")
	if inventoryCallError(context.Background(), other) != other {
		t.Fatal("inventory masked an unrelated failure")
	}
}
