// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
	"time"
)

func TestNewFiniteBucketLargerThanEveryVolumeUsesAvailableHeadroom(t *testing.T) {
	for _, class := range []string{"hdd", "ssd"} {
		t.Run(class, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			addReplicaDataVolume(t, source.node, "tiny-coordinator", "hdd", 4<<20)
			dst := addReplicaDataVolume(t, target.node, "larger-peer", class, 32<<20)
			reviewWaitDest(t, source.node, 2)
			existing, _ := createProtocolBucket(t, source.node, "keep-existing-owner")
			before, _ := source.node.routingPatch(existing.ID(), volume.ObjectID{})
			draft := newTestBucket(t, "oversized-soft-limit")
			draft.description.Policy.Limits.Unlimited = false
			draft.description.Policy.Limits.Absolute = 128 << 20
			if _, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
				t.Fatal("create", status)
			}
			patch, ok := source.node.routingPatch(draft.ID(), volume.ObjectID{})
			if !ok || len(patch.Entries) != 1 || len(patch.Entries[0].Locations) != 1 {
				t.Fatal("missing initial placement")
			}
			loc := patch.Entries[0].Locations[0]
			if loc.Node != target.node.control.ledger.node || loc.Volume != client.ID(dst.Identity().VolumeID) {
				t.Fatal("oversized soft limit stranded bucket on tiny coordinator", loc)
			}
			after, _ := source.node.routingPatch(existing.ID(), volume.ObjectID{})
			if after.Entries[0].Locations[0] != before.Entries[0].Locations[0] {
				t.Fatal("new bucket moved an existing bucket")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			admin := source.node.cfg.Auth.BootstrapAdmin
			c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			data := bytes.Repeat([]byte{0x73}, 1<<20)
			// More than the entire coordinator volume must work immediately,
			// without making boot writes wait for a capacity handoff first.
			for i := byte(1); i <= 5; i++ {
				id := client.ObjectID{i}
				if _, err = c.Put(ctx, id, data, client.PutOptions{}); err != nil {
					t.Fatal("write", i, err)
				}
				got, _, err := c.Get(ctx, id)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatal("readback", i, err)
				}
			}
		})
	}
}

func TestNewFiniteBucketPrefersFittingVolumeWithoutMovingExistingBuckets(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addReplicaDataVolume(t, source.node, "finite-small", "hdd", 8<<20)
	dst := addReplicaDataVolume(t, target.node, "finite-large", "hdd", 64<<20)
	reviewWaitDest(t, source.node, 2)
	old, _ := createProtocolBucket(t, source.node, "existing-placement")
	before, ok := source.node.routingPatch(old.ID(), volume.ObjectID{})
	if !ok {
		t.Fatal("old map missing")
	}
	draft := newTestBucket(t, "finite-preferred")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 32 << 20
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	patch, ok := source.node.routingPatch(draft.ID(), volume.ObjectID{})
	if !ok || len(patch.Entries) != 1 || len(patch.Entries[0].Locations) != 1 {
		t.Fatal("new map missing")
	}
	loc := patch.Entries[0].Locations[0]
	if loc.Node != target.node.control.ledger.node || loc.Volume != client.ID(dst.Identity().VolumeID) {
		t.Fatal("finite bucket starts on undersized volume", loc)
	}
	after, ok := source.node.routingPatch(old.ID(), volume.ObjectID{})
	if !ok || after.Entries[0].Locations[0] != before.Entries[0].Locations[0] {
		t.Fatal("new bucket moved existing bucket")
	}
	source.node.scopes.mu.Lock()
	defer source.node.scopes.mu.Unlock()
	for _, tx := range source.node.scopes.txns {
		if tx.plan.Before.Bucket == client.ID(draft.ID()) {
			t.Fatal("empty placement required migration")
		}
	}
}
