// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDaemonClusterShutdownAndReverseRestart(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "cluster-shutdown")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{0x71}
	payload := []byte("persistent data across complete cluster stop")
	before, err := c.Put(ctx, oid, payload, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(source.node.control.ledger.node)
	e.U8(2)
	_, status, _ := adminBucketMessage(t, source.node, client.NodeShutdown, e.B)
	if status != 202 {
		t.Fatal("cluster shutdown command", status)
	}
	for _, n := range []*daemonTestNode{source, target} {
		select {
		case <-n.node.stopRequest:
		case <-ctx.Done():
			n.node.control.ledger.mu.Lock()
			slot, request := n.node.control.ledger.slot, n.node.control.ledger.request
			n.node.control.ledger.mu.Unlock()
			t.Fatalf("cluster did not stop: node=%s paused=%v slot=%+v request=%+v error=%v", n.node.cfg.Node.Name, n.node.clusterPaused(), slot, request, ctx.Err())
		}
	}
	view := source.node.currentSystem.Load()
	stop := view.manifest.Shutdown
	if stop == nil || stop.ID == (client.ID{}) {
		t.Fatal("shutdown decision missing")
	}
	source.close()
	target.close()
	target = restartDaemonTestNode(t, target)
	if target.node.clusterPaused() {
		t.Fatal("completed shutdown did not resume")
	}
	select {
	case <-target.node.stopRequest:
		t.Fatal("old shutdown stopped restarted process")
	default:
	}
	source = restartDaemonTestNode(t, source)
	got, info, err := c.Get(ctx, oid)
	if err != nil || !bytes.Equal(got, payload) || info.Length != before.Length {
		t.Fatal("cluster restart lost data/version", err)
	}
	if source.node.currentSystem.Load().manifest.Membership.Epoch != view.manifest.Membership.Epoch || len(source.node.currentSystem.Load().manifest.Membership.Members) != 2 {
		t.Fatal("shutdown changed membership")
	}
	if _, err := c.Put(ctx, oid, []byte("resumed writes"), client.PutOptions{}); err != nil {
		t.Fatal("restarted cluster remained read only", err)
	}
	// The old certificate may be replayed during peer reconnect. It must never
	// shut down a process that already fulfilled this shutdown through restart.
	decision, cert, ok := source.node.stopCertificate(stop)
	if !ok {
		t.Fatal("STOP certificate lost after restart")
	}
	wire, err := marshalControlCertificate(decision, cert)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.node.receiveClusterStop(wire, source.node.control.ledger.node); err != nil {
		t.Fatal("idempotent old STOP", err)
	}
	if target.node.clusterPaused() {
		t.Fatal("old STOP paused resumed member")
	}
}

type gatedShutdownPrepare struct {
	controlParticipants
	target  client.ID
	blocked atomic.Bool
}

func (g *gatedShutdownPrepare) Prepare(ctx context.Context, node client.ID, p controlProposal) (client.Proof, error) {
	if p.Kind == controlShutdown && node == g.target && g.blocked.Load() {
		return client.Proof{}, fail(503, "test withholds required READY")
	}
	return g.controlParticipants.Prepare(ctx, node, p)
}

func TestDaemonClusterShutdownWaitsForEveryReadyAfterCrash(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "pending-shutdown")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	oid := volume.ObjectID{8}
	if _, err := bucket.Put(ctx, userPrincipal(t, source.node), oid, []byte("before READY"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	source.node.controller.mu.Lock()
	gate := &gatedShutdownPrepare{controlParticipants: source.node.controller.coordinator.participants, target: target.node.control.ledger.node}
	gate.blocked.Store(true)
	source.node.controller.coordinator.participants = gate
	source.node.controller.mu.Unlock()
	e := client.Encoder{}
	e.ID(source.node.control.ledger.node)
	e.U8(2)
	_, status, _ := adminBucketMessage(t, source.node, client.NodeShutdown, e.B)
	if status != 202 {
		t.Fatal(status)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		l := source.node.control.ledger
		l.mu.Lock()
		ready := l.slot != nil && l.slot.Proposal != nil && l.slot.Proposal.Kind == controlShutdown && l.slot.Ready.RecordSequence != 0
		l.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("local READY missing")
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, n := range []*daemonTestNode{source, target} {
		select {
		case <-n.node.stopRequest:
			t.Fatal("stopped without all READYs")
		default:
		}
	}
	target.close()
	source = restartDaemonTestNode(t, source)
	b, err := source.node.Bucket(bucket.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !source.node.bucketFrozen(bucket.ID()) {
		t.Fatal("restart forgot PREPARE mutation fence")
	}
	if _, err := b.Put(ctx, userPrincipal(t, source.node), oid, []byte("must fail"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); statusOf(err) != 503 {
		t.Fatal("PREPARE permitted mutation", err)
	}
	got, _, err := b.Get(ctx, userPrincipal(t, source.node), oid)
	if err != nil || string(got) != "before READY" {
		t.Fatal("paused reads unavailable or write slipped through", err)
	}
	select {
	case <-source.node.stopRequest:
		t.Fatal("missing peer was treated as READY")
	default:
	}
	target = restartDaemonTestNode(t, target)
	for _, n := range []*daemonTestNode{source, target} {
		select {
		case <-n.node.stopRequest:
		case <-ctx.Done():
			t.Fatal("prepared shutdown did not resume", ctx.Err())
		}
	}
}

func TestDaemonClusterShutdownMissedStopWaitsForReconnect(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "missed-stop")
	// Drop only STOP delivery after the real ALL PREPARE/COMMIT/APPLY/RELEASE.
	// The receiver must stay paused, not infer STOP from a missing coordinator.
	source.node.controller.mu.Lock()
	source.node.controller.stopSender = func(context.Context, client.ID, []byte) error { return errors.New("test drops STOP") }
	source.node.controller.mu.Unlock()
	e := client.Encoder{}
	e.U8(2)
	_, status, _ := adminBucketMessage(t, source.node, client.ClusterShutdown, e.B)
	if status != 202 {
		t.Fatal(status)
	}
	select {
	case <-source.node.stopRequest:
	case <-time.After(15 * time.Second):
		t.Fatal("requester did not stop")
	}
	source.close()
	if !target.node.clusterPaused() {
		t.Fatal("member that missed STOP resumed autonomously")
	}
	select {
	case <-target.node.stopRequest:
		t.Fatal("missing coordinator was treated as STOP")
	default:
	}
	b, err := target.node.Bucket(bucket.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !target.node.bucketFrozen(b.ID()) {
		t.Fatal("paused member lost its mutation fence")
	}
	target = restartDaemonTestNode(t, target)
	if !target.node.clusterPaused() {
		t.Fatal("restart forgot pending cluster shutdown")
	}
	source = restartDaemonTestNode(t, source)
	select {
	case <-target.node.stopRequest:
	case <-time.After(15 * time.Second):
		t.Fatal("STOP was not redelivered after reconnect")
	}
	if source.node.clusterPaused() {
		t.Fatal("returning sender was stopped twice")
	}
	select {
	case <-source.node.stopRequest:
		t.Fatal("returning sender repeated old shutdown")
	default:
	}
	target = restartDaemonTestNode(t, target)
	if target.node.clusterPaused() {
		t.Fatal("received STOP was not fulfilled by restart")
	}
}
