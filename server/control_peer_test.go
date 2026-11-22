// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Actual authenticated WebSocket RPCs and actual system-object publication,
// with explicit test installers. This is not default daemon admission or a
// test of multinode user-object routing/migration.
func TestPeerControlSystemCommit(t *testing.T) {
	source := testServer(t)
	target := peerTestNode(t, source.running)
	sourceID, _ := parseID(source.running.Node.ID)
	targetID, _ := parseID(target.running.Node.ID)
	cluster, _ := parseID(source.running.ClusterID)
	base := controlBase{Membership: client.Membership{Cluster: client.ID(cluster), Epoch: 1, Members: []client.Member{{ID: client.ID(sourceID)}}}, Generation: 1, MapRevision: 1}
	initial := controlBase{Membership: client.Membership{Cluster: client.ID(cluster)}}
	left, err := openControlLedger(filepath.Join(t.TempDir(), "source.stqr"), cluster, sourceID, &base)
	if err != nil {
		t.Fatal(err)
	}
	defer left.journal.close()
	right, err := openControlLedger(filepath.Join(t.TempDir(), "target.stqr"), cluster, targetID, &initial)
	if err != nil {
		t.Fatal(err)
	}
	defer right.journal.close()
	var installedSource, installedTarget atomic.Pointer[systemView]
	source.control = &controlRuntime{server: source, ledger: left, install: func(v *systemView) error { installedSource.Store(v); return nil }}
	target.control = &controlRuntime{server: target, ledger: right, install: func(v *systemView) error { installedTarget.Store(v); return nil }}
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	peer, err := source.dialPeer(ctx, host.URL, client.ID(targetID))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.close()
	if err := source.startPeerObservation(peer); err != nil {
		t.Fatal(err)
	}
	participants := peerControlParticipants{runtime: source.control}
	coordinator := controlCoordinator{ledger: left, participants: participants}
	required := []client.ID{client.ID(sourceID), client.ID(targetID)}
	sort.Slice(required, func(i, j int) bool { return bytes.Compare(required[i][:], required[j][:]) < 0 })
	claim, err := coordinator.begin(ctx, controlMembership, required)
	if err != nil {
		t.Fatal(err)
	}
	next := base.Membership
	next.Epoch = 2
	next.Members = []client.Member{{ID: required[0]}, {ID: required[1]}}
	bundle, err := source.captureSystemImage(next, 1, 1, 1, map[client.ID]volume.ID{client.ID(sourceID): source.system.Identity().VolumeID, client.ID(targetID): target.system.Identity().VolumeID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.stageSystemBundle(bundle); err != nil {
		t.Fatal(err)
	}
	if err := target.putSystemContent(bundle.Root, bundle.Objects[bundle.Root]); err != nil {
		t.Fatal(err)
	}
	proposal := controlProposal{Claim: claim, Kind: controlMembership, Image: [32]byte(bundle.Root), NextMembership: &next}
	if err := coordinator.prepared(ctx, proposal, []byte("committed")); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.resume(ctx); err == nil {
		t.Fatal("remote READY without referenced children")
	}
	right.mu.Lock()
	remoteSequence := right.current.Sequence
	right.mu.Unlock()
	if installedSource.Load() != nil || installedTarget.Load() != nil || left.current.Sequence != 0 || remoteSequence != 0 {
		t.Fatal("incomplete graph published")
	}
	// Supplying missing immutable children allows the original request to resume.
	if err := target.stageSystemBundle(bundle); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.resume(ctx)
	if err != nil || string(result) != "committed" {
		t.Fatal("native global commit failed", err)
	}
	for i, view := range []*systemView{installedSource.Load(), installedTarget.Load()} {
		if view == nil || view.root != bundle.Root || view.manifest.Membership.Epoch != 2 {
			t.Fatal("installer not called", i)
		}
	}
	right.mu.Lock()
	remoteReleased := right.slot == nil
	right.mu.Unlock()
	if left.slot != nil || !remoteReleased || left.request != nil {
		t.Fatal("global round not released")
	}
	// A delayed duplicate remains idempotent after release over the same socket.
	if _, err := participants.Prepare(ctx, client.ID(targetID), proposal); err != nil {
		t.Fatal("late native PREPARE", err)
	}
	right.journal.mu.Lock()
	before := right.journal.sequence
	right.journal.mu.Unlock()
	forged := claim
	forged.Round.Initiator = client.ID(targetID)
	forged.Round.Attempt++
	wire, _ := marshalControlClaim(forged)
	if _, err := peer.request(ctx, client.PeerControlReserve, wire); statusOf(err) != 403 {
		t.Fatal("identity substitution accepted", err)
	}
	right.journal.mu.Lock()
	defer right.journal.mu.Unlock()
	if right.journal.sequence != before {
		t.Fatal("invalid actor wrote a journal record")
	}
}
