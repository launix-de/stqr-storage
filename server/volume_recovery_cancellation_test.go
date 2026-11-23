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

func TestRecoveryClearsOnlyDurablyCancelledUnsealedSourceFence(t *testing.T) {
	checkRecoveryCancelledSourceFence(t, false)
}

func TestRecoveryRebuildsCancellationEvidenceAfterCoordinatorRestart(t *testing.T) {
	checkRecoveryCancelledSourceFence(t, true)
}

type interruptedCancellationPrepare struct{ controlParticipants }

func (p interruptedCancellationPrepare) Prepare(ctx context.Context, node client.ID, proposal controlProposal) (client.Proof, error) {
	if proposal.Claim.Exclusion != nil && proposal.Claim.Exclusion.RecoverOnly {
		return client.Proof{}, fail(503, "injected interruption before recovery READY")
	}
	return p.controlParticipants.Prepare(ctx, node, proposal)
}

func checkRecoveryCancelledSourceFence(t *testing.T, interrupt bool) {
	source, target := startAutomaticDaemonPair(t, false)
	old := addTestVolume(t, source.node, "source", "hdd", 4<<20)
	destination := addTestVolume(t, target.node, "target", "hdd", 4<<20)
	path := source.node.volumes[old.VolumeID].config.Path
	bucket, _ := createProtocolBucket(t, source.node, "cancelled-migration-recovery")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	object := client.ObjectID{0x81}
	payload := bytes.Repeat([]byte("x"), 128<<10)
	if _, err := c.Put(ctx, object, payload, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID(object))
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(destination.VolumeID)
	plan, err := planRangeMove(before, moving, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 512<<10)
	if err != nil {
		t.Fatal(err)
	}
	r := source.node.scopes
	if err := r.reserve(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := r.call(ctx, plan.Target, peerRangeReserve, mustRangeWire(plan)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.sendSnapshot(ctx, plan, false); err != nil {
		t.Fatal(err)
	}
	request := client.NewRequestID()
	info, err := c.Append(ctx, object, []byte("last acknowledged tail"), client.AppendOptions{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, []byte("last acknowledged tail")...)
	c.Close()
	if err := r.fence(plan.id()); err != nil {
		t.Fatal(err)
	}
	if interrupt {
		target.node.controller.mu.Lock()
		target.node.controller.coordinator.participants = interruptedCancellationPrepare{target.node.controller.coordinator.participants}
		target.node.controller.mu.Unlock()
		source.close()
		e := client.Encoder{}
		e.V(1)
		e.ID(source.node.control.ledger.node)
		e.Bool(false)
		e.Bool(true)
		e.U64(target.node.currentSystem.Load().manifest.Membership.Epoch)
		message := client.Message{Kind: client.Request, Opcode: client.NodeRecoveryBegin, ID: client.NewRequestID(), Body: e.B}
		if status, _ := bucketHTTPReply(t, target.node, message, admin.Username, admin.Password); status != 202 {
			t.Fatal("injected recovery interruption", status)
		}
		target = restartDaemonTestNode(t, target)
		for len(target.node.currentSystem.Load().manifest.VolumeRecoveries) == 0 {
			if ctx.Err() != nil {
				t.Fatal("recovery reconstruction", ctx.Err())
			}
			time.Sleep(25 * time.Millisecond)
		}
	} else {
		beginTestNodeRecovery(t, source, target)
	}
	origins := target.node.currentSystem.Load().manifest.VolumeRecoveries
	if len(origins) != 1 || len(origins[0].CancelledMigrations) != 1 {
		t.Fatal("missing durable fence clearance", len(origins))
	}
	clearance := origins[0].CancelledMigrations[0]
	if clearance.Proof.Node != target.node.control.ledger.node {
		t.Fatal("wrong cancellation witness")
	}
	target.node.scopes.mu.Lock()
	transaction := target.node.scopes.txns[plan.id()]
	if !transaction.aborted || transaction.cancellationProof != clearance.Proof || transaction.targetReady.RecordSequence != 0 {
		t.Fatal("unsealed cancellation not durable")
	}
	target.node.scopes.mu.Unlock()
	// The recovered candidate is part of the immutable control graph, not
	// volatile coordinator state or the old source's private range journal.
	target = restartDaemonTestNode(t, target)
	id, err := target.node.StageRecoveryVolume("rescued", path)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := commitTestRecoveredVolumes(t, target, admin, id); status != 200 {
		t.Fatal("restore cancelled source", status)
	}
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, current, err := c.Get(ctx, object)
	if err != nil || !bytes.Equal(got, payload) || current.Version != info.Version {
		t.Fatal("last acknowledged source version lost", err)
	}
	if retry, err := c.Append(ctx, object, []byte("last acknowledged tail"), client.AppendOptions{RequestID: request}); err != nil || retry.Version != info.Version {
		t.Fatal("source retry receipt lost", err)
	}
	// A high numeric version on an unsealed shadow cannot become authority or
	// prevent collection once the imported source cohort is committed.
	shadow := target.node.volumes[destination.VolumeID].store
	h, err := shadow.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := h.Put(volume.Key{Bucket: bucket.ID(), Object: volume.ObjectID(object)}, []byte("obsolete shadow"), volume.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	for i := 0; i < 6; i++ {
		if err := target.node.scopes.collectAbortedImports(); err != nil {
			t.Fatal("cancelled shadow cleanup", err)
		}
	}
	if stats := shadow.Stats(); stats.Objects != 0 || stats.DataPhysical != 0 {
		t.Fatal("obsolete migration payload was retained", stats.Objects, stats.DataPhysical)
	}
}

func TestRecoveryOriginsRequireCapabilityBeforeJoin(t *testing.T) {
	view := &systemView{}
	if !supportsRecoveryOrigins(view, client.CapabilityVolumeRecovery) {
		t.Fatal("legacy recovery without clearances unnecessarily blocks join")
	}
	view.manifest.VolumeRecoveries = []volumeRecoveryOrigin{{CancelledMigrations: []volumeRecoveryCancellation{{}}}}
	if supportsRecoveryOrigins(view, client.CapabilityVolumeRecovery) {
		t.Fatal("old peer can enter an unsupported recovery graph")
	}
	if !supportsRecoveryOrigins(view, client.CapabilityRecoveryFenceClearance) {
		t.Fatal("capable peer rejected")
	}
}

func TestRecoveryRejectsChangedHistoricalCancellation(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 4<<20)
	createProtocolBucket(t, source.node, "immutable-recovery")
	beginTestNodeRecovery(t, source, target)
	view := target.node.currentSystem.Load()
	candidate := *view
	candidate.manifest = view.manifest
	candidate.manifest.VolumeRecoveries = cloneRecoveryOrigins(view.manifest.VolumeRecoveries)
	candidate.manifest.VolumeRecoveries[0].CancelledMigrations = []volumeRecoveryCancellation{{Proof: client.Proof{Node: target.node.control.ledger.node, RecordSequence: 1}}}
	target.node.opMu.Lock()
	err := target.node.prepareRecoveryCancellations(&candidate, controlClaim{})
	target.node.opMu.Unlock()
	if err == nil {
		t.Fatal("new clearance introduced into an already committed origin")
	}
}
