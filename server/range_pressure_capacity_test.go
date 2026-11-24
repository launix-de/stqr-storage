// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestPressureSkipsFailedUnpinnedVolume(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "pressure-local", "hdd", 16<<20)
	addReplicaDataVolume(t, target.node, "pressure-peer", "hdd", 8<<20)
	reviewWaitDest(t, source.node, 2)
	b, _ := createProtocolBucket(t, source.node, "pressure-unpinned")
	id := volume.ObjectID{0x40}
	plan, ok := source.node.scopes.pressurePlan(rangePressure{bucket: b.ID(), object: id, needed: 64 << 10, failed: src.Identity().VolumeID, requested: time.Now()})
	if !ok {
		t.Fatal("no alternative capacity target")
	}
	if plan.Target != target.node.control.ledger.node || plan.Volume == src.Identity().VolumeID {
		t.Fatal("pressure returned to failed unpinned volume", plan.Target, plan.Volume)
	}
}
func TestPressureNewObjectMovesParentWhenCapacityAllows(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addReplicaDataVolume(t, source.node, "pressure-parent", "hdd", 4<<20)
	addReplicaDataVolume(t, target.node, "pressure-large", "hdd", 16<<20)
	reviewWaitDest(t, source.node, 2)
	b, _ := createProtocolBucket(t, source.node, "pressure-parent")
	if _, err := b.Put(context.Background(), userPrincipal(t, source.node), volume.ObjectID{0xf0}, bytes.Repeat([]byte("p"), 1<<20), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	id := volume.ObjectID{0x10}
	plan, ok := source.node.scopes.pressurePlan(rangePressure{bucket: b.ID(), object: id, needed: 1 << 20, requested: time.Now()})
	if !ok || plan.Target != target.node.control.ledger.node {
		t.Fatal("no suitable peer target")
	}
	if plan.Moving.PrefixBits != plan.Before.Entries[0].PrefixBits {
		t.Fatal("new object unnecessarily split out an empty child", plan.Moving.PrefixBits)
	}
	if !plan.contains(volume.ObjectID{0xf0}) {
		t.Fatal("plan does not release existing source data")
	}
}

func TestCapacityOverflowManyNewObjectsDoesNotChurnEmptyRanges(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addReplicaDataVolume(t, source.node, "overflow-small", "hdd", 2<<20)
	addReplicaDataVolume(t, target.node, "overflow-large", "hdd", 32<<20)
	reviewWaitDest(t, source.node, 2)
	b, _ := createProtocolBucket(t, source.node, "overflow-import")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte("bulk"), 64<<10)
	for i := byte(0); i < 64; i++ {
		id := client.ObjectID(sha256.Sum256([]byte{i}))
		if _, err = c.CAS(ctx, id, 0, nil, payload, client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{RequestID: client.NewRequestID(), Absent: true, Accepted: true}}); err != nil {
			logDaemonRangeState(t, source, target)
			t.Fatal("overflow object", i, err)
		}
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for i := byte(0); i < 64; i++ {
		id := client.ObjectID(sha256.Sum256([]byte{i}))
		got, _, err := c.Get(ctx, id)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("overflow readback", i, err)
		}
	}
	source.node.scopes.mu.Lock()
	moves := 0
	for _, tx := range source.node.scopes.txns {
		if tx.plan.Before.Bucket == client.ID(b.ID()) && tx.plan.Source == source.node.control.ledger.node {
			moves++
			t.Logf("capacity attempt prefix=%d physical=%d logical=%d aborted=%v committed=%v done=%v", tx.plan.Moving.PrefixBits, tx.plan.Physical, tx.logicalUsed, tx.aborted, tx.decision != nil, tx.sourceDone.RecordSequence != 0)
		}
	}
	source.node.scopes.mu.Unlock()
	if moves > 4 {
		t.Fatal("new IDs created repeated tiny capacity handoffs", moves)
	}
}
