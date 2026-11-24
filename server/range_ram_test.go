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

func TestDaemonRAMHandoffCrashRecovery(t *testing.T) {
	for _, phase := range []string{"unsealed-source", "sealed-source", "sealed-target", "source-commit-target", "source-commit-both", "target-commit-target"} {
		t.Run(phase, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			for _, n := range []*daemonTestNode{source, target} {
				if _, err := n.node.AddVolume(newID(), AddVolumeRequest{Name: "ram", Source: 2, Class: "ram", SizeBytes: 4 << 20}); err != nil {
					t.Fatal(err)
				}
			}
			draft := newTestBucket(t, "ram-crash")
			draft.description.Policy.StorageStrategy = "ram"
			_, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft))
			if status != 201 {
				t.Fatal(status)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			admin := source.node.cfg.Auth.BootstrapAdmin
			c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			oid := client.ObjectID{0x90}
			payload := bytes.Repeat([]byte{0x67}, 1<<20)
			initial, err := c.Put(ctx, oid, payload, client.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			before, known := source.node.routingPatch(draft.ID(), volume.ObjectID(oid))
			if !known || before.Entries[0].Locations[0].Node != source.node.control.ledger.node {
				t.Fatal("unexpected initial owner")
			}
			var destination *volume.Volume
			for _, v := range target.node.volumes {
				if !v.config.System {
					destination = v.store
				}
			}
			p, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, destination.Identity().VolumeID, uint8(volume.RAM), volume.Raw, 2<<20)
			if err != nil {
				t.Fatal(err)
			}
			r := source.node.scopes
			if err := r.reserve(p); err != nil {
				t.Fatal(err)
			}
			if _, err := r.call(ctx, p.Target, peerRangeReserve, mustRangeWire(p)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := r.sendSnapshot(ctx, p, false); err != nil {
				t.Fatal(err)
			}
			var decision rangeCommit
			if phase != "unsealed-source" {
				if err := r.fence(p.id()); err != nil {
					t.Fatal(err)
				}
				digest, proof, err := r.sendSnapshot(ctx, p, true)
				if err != nil {
					t.Fatal(err)
				}
				if phase == "source-commit-target" || phase == "source-commit-both" || phase == "target-commit-target" {
					sourceProof, err := r.ready(p.id(), digest, true)
					if err != nil {
						t.Fatal(err)
					}
					decision = rangeCommit{ID: p.id(), Digest: digest, LogicalUsed: r.txns[p.id()].logicalUsed, Source: sourceProof, Target: proof}
					if _, err := r.commit(decision); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "target-commit-target" {
					wire, err := decision.marshal()
					if err != nil {
						t.Fatal(err)
					}
					if _, err := r.call(ctx, p.Target, peerRangeCommit, wire); err != nil {
						t.Fatal(err)
					}
				}
			}
			if phase == "unsealed-source" || phase == "sealed-source" || phase == "source-commit-both" {
				source = restartDaemonTestNode(t, source)
				r = source.node.scopes
			}
			if phase == "sealed-target" || phase == "source-commit-target" || phase == "source-commit-both" || phase == "target-commit-target" {
				target = restartDaemonTestNode(t, target)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				_, err = source.node.controller.info(ctx, p.Target)
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("peer reconnect", err)
				}
				time.Sleep(25 * time.Millisecond)
			}
			err = r.move(ctx, p)
			if phase == "unsealed-source" {
				if err == nil {
					t.Fatal("unsealed RAM shadow promoted after source loss")
				}
				for _, n := range []*daemonTestNode{source, target} {
					n.node.scopes.mu.Lock()
					txn := n.node.scopes.txns[p.id()]
					closed := txn != nil && txn.aborted && !n.node.scopes.active(txn)
					n.node.scopes.mu.Unlock()
					if !closed {
						t.Fatal("lost unsealed offer was not closed")
					}
				}
				return
			}
			if err != nil {
				t.Fatal("resume handoff", err)
			}
			if phase == "source-commit-both" || phase == "target-commit-target" {
				entry, known := target.node.routingPatch(draft.ID(), volume.ObjectID(oid))
				if !known || target.node.scopes.ramAvailable(draft.ID(), volume.ObjectID(oid), entry.Entries[0]) {
					t.Fatal("lost committed RAM era falsely available")
				}
				return
			}
			got, info, err := c.Get(ctx, oid)
			if err != nil || !bytes.Equal(got, payload) || info.Length != initial.Length || info.RuntimeID != initial.RuntimeID {
				t.Fatal("RAM recovery changed data/version/runtime", initial, info, err)
			}
		})
	}
}
