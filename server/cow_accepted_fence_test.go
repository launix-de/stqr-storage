// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// This fixture intentionally places the inherited bytes on the other node.
// Copying them into the writing bucket must await visibility, never fdatasync.
// Preservation into a DIFFERENT linked bucket retains its stronger old barrier.
func TestCOWAcceptedPreservationSingleNode(t *testing.T)   { testCOWAcceptedPreservation(t, false) }
func TestCOWAcceptedPreservationRemoteSource(t *testing.T) { testCOWAcceptedPreservation(t, true) }

func testCOWAcceptedPreservation(t *testing.T, remote bool) {
	var coordinator, sourceNode, targetNode *Server
	var source, target *Bucket
	var dataVolumes []*volume.Volume
	var sourceURL, targetURL string
	var peerVolume *volume.Volume
	if remote {
		first, second := startAutomaticDaemonPair(t, false)
		coordinator, targetNode, sourceNode = first.node, first.node, second.node
		local := addReplicaDataVolume(t, targetNode, "cow-accepted", "hdd", 16<<20)
		peer := addReplicaDataVolume(t, sourceNode, "cow-accepted", "hdd", 16<<20)
		dataVolumes = []*volume.Volume{local, peer}
		reviewWaitDest(t, coordinator, 2)
		source, _ = createProtocolBucket(t, coordinator, "cow-accepted-source")
		target, _ = createProtocolBucket(t, coordinator, "cow-accepted-target")
		peerVolume = peer
		sourceURL, targetURL = targetNode.cfg.Node.Advertise, targetNode.cfg.Node.Advertise
	} else {
		coordinator = testServer(t)
		sourceNode, targetNode = coordinator, coordinator
		dataVolumes = []*volume.Volume{addReplicaDataVolume(t, coordinator, "cow-accepted", "hdd", 16<<20)}
		source, _ = createProtocolBucket(t, coordinator, "cow-accepted-source")
		target, _ = createProtocolBucket(t, coordinator, "cow-accepted-target")
		http := httptest.NewServer(coordinator.Handler())
		t.Cleanup(http.Close)
		sourceURL, targetURL = http.URL, http.URL
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := coordinator.cfg.Auth.BootstrapAdmin
	dial := func(url string, b *Bucket) *client.Client {
		c, err := client.Dial(ctx, url, admin.Username, admin.Password, client.ID(b.ID()), "fs")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	src := dial(sourceURL, source)
	object := client.ObjectID{0x81}
	siblingObject := client.ObjectID{0x82}
	original, changed := bytes.Repeat([]byte{'a'}, 210), bytes.Repeat([]byte{'b'}, 210)
	if _, err := src.Put(ctx, object, original, client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Put(ctx, siblingObject, original, client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if remote {
		before, ok := coordinator.routingPatch(source.ID(), volume.ObjectID(object))
		if !ok {
			t.Fatal("source placement missing")
		}
		if before.Entries[0].Locations[0].Node != sourceNode.control.ledger.node {
			plan, err := planRangeMove(before, before.Entries[0], sourceNode.control.ledger.node, peerVolume.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if err = coordinator.scopes.move(ctx, plan); err != nil {
				t.Fatal(err)
			}
		}
	}

	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(target.ID()))
	if _, err := coordinator.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, coordinator)); err != nil {
		t.Fatal(err)
	}
	dst := dial(targetURL, target)
	if remote {
		owner, err := targetNode.cowOwner(target.ID(), volume.ObjectID(object))
		if err != nil || !targetNode.cowLocal(owner) {
			t.Fatal("target owner must be local", err)
		}
		owner, err = targetNode.cowOwner(source.ID(), volume.ObjectID(object))
		if err != nil || targetNode.cowLocal(owner) {
			t.Fatal("source must be a remote read", err)
		}
	}
	before := make(map[*volume.Volume]uint64)
	for _, v := range dataVolumes {
		before[v] = v.IOStats().SyncOps
	}
	if _, err := dst.CAS(ctx, object, 0, original, changed, client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true}}); err != nil {
		t.Fatal(err)
	}
	assertBytes := func(c *client.Client, want []byte) {
		t.Helper()
		got, _, err := c.Get(ctx, object)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("got %q want %q: %v", got, want, err)
		}
	}
	assertBytes(src, original)
	assertBytes(dst, changed)
	for _, v := range dataVolumes {
		if got := v.IOStats().SyncOps; got != before[v] {
			t.Fatalf("accepted inherited CAS checkpointed before explicit Sync: old=%d new=%d", before[v], got)
		}
	}
	localTarget, err := targetNode.Bucket(target.ID())
	if err != nil {
		t.Fatal(err)
	}
	key := volume.Key{Bucket: target.ID(), Object: volume.ObjectID(object)}
	selected, _, found, err := localTarget.selectVolume(localTarget.Describe(), key, volume.ID{}, false)
	if err != nil || !found {
		t.Fatal("materialized target missing", found, err)
	}
	generation := selected.Stats().Generation
	if selected.IsDurableThrough(generation) {
		t.Fatal("accepted generation already durable without Sync")
	}
	if err := dst.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if !selected.IsDurableThrough(generation) {
		t.Fatal("explicit Sync omitted inherited materialization")
	}
	if selected.IOStats().SyncOps <= before[selected] {
		t.Fatal("positive Sync counter control did not checkpoint")
	}
	// Reopen a copy of the actual on-disk image, not the still-live RAM overlay.
	// Copying it keeps the node's exclusive ownership of the original volume.
	var filename string
	targetNode.mu.RLock()
	for _, m := range targetNode.volumes {
		if m.store == selected {
			filename = m.config.Path
		}
	}
	targetNode.mu.RUnlock()
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "durable-volume")
	if err = os.WriteFile(copyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := volume.New(copyPath, 0, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	h, err := reopened.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	actual, _, err := h.Get(key)
	h.Close()
	if err != nil || !bytes.Equal(actual, changed) {
		t.Fatal("explicit Sync did not survive volume reopen", err)
	}
	// An accepted mutation of the source must still preserve another linked
	// namespace before overwriting its sole preimage. This is deliberately a
	// positive barrier control: local connection touches cannot certify a
	// preservation stored on another owner/connection.
	preservedBefore := uint64(0)
	for _, v := range dataVolumes {
		preservedBefore += v.IOStats().SyncOps
	}
	if _, err = src.CAS(ctx, siblingObject, 0, original, changed, client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true}}); err != nil {
		t.Fatal(err)
	}
	sibling, _, err := dst.Get(ctx, siblingObject)
	if err != nil || !bytes.Equal(sibling, original) {
		t.Fatal("source write lost linked bucket's preimage", err)
	}
	preservedAfter := uint64(0)
	for _, v := range dataVolumes {
		preservedAfter += v.IOStats().SyncOps
	}
	if preservedAfter <= preservedBefore {
		t.Fatal("cross-bucket preservation barrier was removed")
	}
	t.Logf("accepted inherited CAS: implicit syncs=0 remote-source=%v explicit fence persisted generation=%d; cross-bucket barrier retained", remote, generation)
}
