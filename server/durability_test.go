// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"

	"testing"
	"time"
)

func TestAcceptedProtocolBarrier(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{0xa1}
	info, err := writer.Put(ctx, id, []byte("accepted"), client.PutOptions{RequestID: client.NewRequestID(), Accepted: true})
	if err != nil {
		t.Fatal(err)
	}
	s.durability.mu.Lock()
	usedAccepted := false
	for _, state := range s.durability.buckets {
		usedAccepted = usedAccepted || state.sequence > 0
	}
	s.durability.mu.Unlock()
	if !usedAccepted {
		t.Fatal("mutation did not use accepted path")
	}
	s.mu.RLock()
	var active *volume.Volume
	for _, v := range s.volumes {
		active = v.store
		break
	}
	s.mu.RUnlock()
	if active == nil {
		t.Fatal("fixture missing volume")
	}
	generation := active.Header().Generation
	data, got, err := reader.Get(ctx, id)
	if err != nil || string(data) != "accepted" || got.Length != info.Length {
		t.Fatal(string(data), got, err)
	}
	if err = reader.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	before := active.IOStats().SyncOps
	if err = active.CheckpointThrough(generation); err != nil {
		t.Fatal(err)
	}
	if active.IOStats().SyncOps != before {
		t.Fatal("reader barrier did not checkpoint observed version")
	}
	if err = writer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(ctx, id, []byte("!"), client.AppendOptions{RequestID: client.NewRequestID(), Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	data, _, err = reader.Get(ctx, id)
	if err != nil || string(data) != "accepted!" {
		t.Fatal(string(data), err)
	}
}
func TestAcceptedSubscriptionReceipt(t *testing.T) {
	_, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{0xa2}
	if _, err := writer.Put(ctx, id, []byte("old"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	sub, err := reader.Subscribe(ctx, id, client.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)
	if _, err = writer.Put(ctx, id, []byte("new"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.Changes():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = reader.Sync(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestAcceptedInvalidIncarnation(t *testing.T) {
	s := testServer(t)
	e := client.Encoder{}
	e.ID(client.NewRequestID())
	e.ID(client.NewRequestID())
	e.U64(1)
	_, _, err := s.executeSync(context.Background(), client.Message{Opcode: client.BucketSync, Body: e.B}, nil, principal{})
	if statusOf(err) != 410 {
		t.Fatal(err)
	}
}

func TestAcceptedVolumeSwitchOrdering(t *testing.T) {
	s := testServer(t)
	a, err := volume.NewRAM(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := volume.NewRAM(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	bucket := volume.ID(client.NewRequestID())
	key := volume.Key{Bucket: bucket, Object: volume.ObjectID{1}}
	switchTo := func(target *volume.Volume) error {
		for {
			s.opMu.Lock()
			err := s.switchAcceptedVolume(bucket, target, true)
			s.opMu.Unlock()
			var wait *acceptedCheckpoint
			if !errors.As(err, &wait) {
				return err
			}
			if err = wait.volume.CheckpointThrough(wait.generation); err != nil {
				return err
			}
		}
	}
	if err = switchTo(a); err != nil {
		t.Fatal(err)
	}
	h, err := a.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Put(key, []byte("dependency"), volume.Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	gen, err := h.Generation()
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	s.recordAccepted(bucket, a, gen)
	if err = switchTo(b); err != nil {
		t.Fatal(err)
	}
	syncs := a.IOStats().SyncOps
	if err = a.CheckpointThrough(gen); err != nil {
		t.Fatal(err)
	}
	if a.IOStats().SyncOps != syncs {
		t.Fatal("volume switch did not checkpoint dependency")
	}
	// A confirmed detached store must not block the next destination.
	if err = switchTo(a); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if err = switchTo(b); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedCrossOwnerSubscriptionDependency(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 32<<20)
	addTestVolume(t, target.node, "data", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, source.node, "accepted-dependencies")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, second := client.ObjectID{0x10}, client.ObjectID{0x90}
	for _, id := range []client.ObjectID{first, second} {
		if _, err = c.Put(ctx, id, []byte("independent"), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID(second))
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	var destination *volume.Volume
	target.node.mu.RLock()
	for _, v := range target.node.volumes {
		if !v.config.System {
			destination = v.store
		}
	}
	target.node.mu.RUnlock()
	plan, err := planRangeMove(before, moving, target.node.control.ledger.node, destination.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	for _, id := range []client.ObjectID{first, second} {
		if _, _, err = c.Get(ctx, id); err != nil {
			t.Fatal(err)
		}
	}

	observer, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	sub, err := observer.Subscribe(ctx, first, client.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)
	_, err = c.Put(ctx, first, []byte("accepted dependency"), client.PutOptions{Accepted: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = waitSubscription(t, ctx, sub, "accepted dependency"); err != nil {
		t.Fatal(err)
	}
	var origin *volume.Volume
	source.node.mu.RLock()
	for _, v := range source.node.volumes {
		if !v.config.System {
			origin = v.store
		}
	}
	source.node.mu.RUnlock()
	generation := origin.Header().Generation
	// Explicit dependency publication: a connection/owner change itself must
	// no longer force a checkpoint on the format-agnostic transport.
	if err = observer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = observer.Put(ctx, second, []byte("reference to accepted dependency"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	beforeSync := origin.IOStats().SyncOps
	if err = origin.CheckpointThrough(generation); err != nil {
		t.Fatal(err)
	}
	if origin.IOStats().SyncOps != beforeSync {
		t.Fatal("cross-owner publication preceded dependency checkpoint")
	}
	if err = observer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
}
