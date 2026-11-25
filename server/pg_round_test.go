// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestPGRoundRejectsWrongPredecessorWithoutPoisoningJournal(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 8<<20)
	to := addTestVolume(t, target.node, "target", "hdd", 8<<20)
	group := auditSplitPG(t, source.node, "round-validation", []*daemonTestNode{source, target})
	bucket := centralBucket(t, source.node, group, "round-validation", 1<<20)
	before, ok := source.node.routingPatch(bucket.ID(), volume.ObjectID{})
	if !ok {
		t.Fatal("missing placement")
	}
	vid, _ := parseID(to.VolumeID)
	p, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, members, mask, err := source.node.pgForPlan(p)
	if err != nil {
		t.Fatal(err)
	}
	a := pgRebuildRound{Group: client.ID(group), ID: p.id(), Generation: 2, Membership: before.Membership, Mask: mask, Members: members, Plan: mustRangeWire(p)}
	r := source.node.scopes
	r.mu.Lock()
	prior := r.journal.sequence
	r.mu.Unlock()
	body, _ := json.Marshal(pgRebuildStateRequest{Action: 0, Round: a})
	if _, err = source.node.pgRebuildPeer(context.Background(), client.Message{Kind: client.Request, Opcode: peerPGRebuildState, Body: body}, members[0]); err == nil {
		t.Fatal("accepted skipped generation")
	}
	r.mu.Lock()
	after := r.journal.sequence
	r.mu.Unlock()
	if prior != after {
		t.Fatal("invalid proposal was appended before validation")
	}
	// A correct proposal still works after the rejection and survives replay.
	a.Generation = 1
	body, _ = json.Marshal(pgRebuildStateRequest{Action: 0, Round: a})
	if _, err = source.node.pgRebuildPeer(context.Background(), client.Message{Kind: client.Request, Opcode: peerPGRebuildState, Body: body}, members[0]); err != nil {
		t.Fatal(err)
	}
	source = restartDaemonTestNode(t, source)
	info := source.node.scopes.pgInfo(group.String())
	if info.Generation != "0" || info.State != "preparing" {
		t.Fatalf("prepared round lost after restart: %+v", info)
	}
}

func TestPGArchiveRejectsEqualCounterWithDifferentHead(t *testing.T) {
	group := client.ID{1}
	id := [32]byte{2}
	r := &rangeManager{pgOrder: [][32]byte{id}, pgRounds: map[[32]byte]pgRebuildRound{id: {Group: group, ID: id, Generation: 1}}}
	wrong, _ := json.Marshal(pgArchiveRequest{Group: group, After: 1, Head: [32]byte{3}})
	if _, err := r.pgArchive(wrong); err == nil {
		t.Fatal("equal PG counters concealed conflicting round heads")
	}
	right, _ := json.Marshal(pgArchiveRequest{Group: group, After: 1, Head: id})
	body, err := r.pgArchive(right)
	if err != nil || string(body) != "[]" {
		t.Fatalf("matching head could not finish catchup: %s %v", body, err)
	}
}
