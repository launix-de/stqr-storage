// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

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
