// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// This exercises native peer transfer and cutover using an explicitly chosen
// plan. The automatic placement planner is exercised separately, not assumed.
func checkDaemonRangeHandoff(t *testing.T, source, target *daemonTestNode, bucket volume.ID, objects *client.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	object := client.ObjectID{0x90}
	payload := bytes.Repeat([]byte{5}, 1<<20)
	if _, err := objects.Put(ctx, object, payload, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before, ok := source.node.routingPatch(bucket, volume.ObjectID(object))
	if !ok {
		t.Fatal("missing source map")
	}
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	target.node.mu.RLock()
	var destination *volume.Volume
	for _, v := range target.node.volumes {
		if !v.config.System {
			destination = v.store
			break
		}
	}
	target.node.mu.RUnlock()
	if destination == nil {
		t.Fatal("missing destination")
	}
	p, err := planRangeMove(before, moving, target.node.control.ledger.node, destination.Identity().VolumeID, uint8(destination.Identity().Class), volume.Raw, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := source.node.scopes
	// A declined target must close the offer on both sides, including an old
	// delayed RESERVE. A fresh attempt on the same unchanged parent is valid.
	declined, err := planRangeMove(before, moving, p.Target, p.Volume, uint8(destination.Identity().Class), volume.Raw, destination.Stats().AdmissionFreePhysical+1)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.move(ctx, declined); err == nil {
		t.Fatal("over-capacity offer was accepted")
	}
	for _, node := range []*Server{source.node, target.node} {
		node.scopes.mu.Lock()
		txn := node.scopes.txns[declined.id()]
		closed := txn != nil && txn.aborted && !node.scopes.active(txn)
		node.scopes.mu.Unlock()
		if !closed {
			t.Fatal("declined offer retained an active claim")
		}
	}
	if _, err := r.call(ctx, p.Target, peerRangeReserve, mustRangeWire(declined)); err == nil {
		t.Fatal("delayed cancelled RESERVE revived")
	}
	// Also abort an interrupted bulk transfer; its unowned staged bytes and
	// retry receipts must be reclaimed without touching the authoritative source.
	if err := r.reserve(p); err != nil {
		t.Fatal(err)
	}
	if _, err := r.call(ctx, p.Target, peerRangeReserve, mustRangeWire(p)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.sendSnapshot(ctx, p, false); err != nil {
		t.Fatal(err)
	}
	if err := r.cancelBeforeFence(ctx, p, errors.New("test interrupts bulk offer")); err == nil {
		t.Fatal("expected interrupted offer")
	}
	for i := 0; i < 8; i++ {
		if err := target.node.scopes.collectAbortedImports(); err != nil {
			t.Fatal(err)
		}
	}
	h, err := destination.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	_, recordErr := h.RecordInfo(volume.Key{Bucket: bucket, Object: volume.ObjectID(object)})
	h.Close()
	if !errors.Is(recordErr, volume.ErrNotFound) || destination.Stats().InFlightReservedPhysical != 0 {
		t.Fatal("aborted import retained bytes/reservation", recordErr)
	}
	target = restartDaemonTestNode(t, target)
	target.node.mu.RLock()
	destination = target.node.volumes[p.Volume.String()].store
	target.node.mu.RUnlock()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := source.node.controller.info(ctx, p.Target); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("target peer did not reconnect after abort replay")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := r.call(ctx, p.Target, peerRangeReserve, mustRangeWire(declined)); err == nil {
		t.Fatal("restart forgot cancelled offer")
	}
	p, err = planRangeMove(before, moving, p.Target, p.Volume, uint8(destination.Identity().Class), volume.Raw, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.reserve(p); err != nil {
		t.Fatal(err)
	}
	if _, err := r.call(ctx, p.Target, peerRangeReserve, mustRangeWire(p)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.sendSnapshot(ctx, p, false); err != nil {
		t.Fatal("bulk snapshot", err)
	}
	appendID := client.NewRequestID()
	length := uint64(len(payload))
	delta := []byte("final delta")
	first, err := objects.Append(ctx, object, delta, client.AppendOptions{RequestID: appendID, ExpectedLength: &length})
	if err != nil {
		t.Fatal("write during bulk copy", err)
	}
	// Reopen the target with a live staged transfer. Canonical bytes, indexed
	// copy memos and its own remaining physical reservation must reconstruct
	// before catch-up resumes; the primary stayed writable during bulk copy.
	target = restartDaemonTestNode(t, target)
	target.node.mu.RLock()
	destination = target.node.volumes[p.Volume.String()].store
	target.node.mu.RUnlock()
	if destination.Stats().InFlightReservedPhysical == 0 {
		t.Fatal("restart forgot active import reservation")
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, err := source.node.controller.info(ctx, p.Target); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bulk target did not reconnect")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err := r.move(ctx, p); err != nil {
		t.Fatal("range catch-up/cutover", err)
	}
	got, info, err := objects.Get(ctx, object)
	if err != nil || !bytes.Equal(got, append(append([]byte{}, payload...), delta...)) || info.Version != first.Version {
		t.Fatal("handoff lost committed tail", err)
	}
	retry, err := objects.Append(ctx, object, delta, client.AppendOptions{RequestID: appendID, ExpectedLength: &length})
	if err != nil || retry.Version != first.Version || retry.StartOffset != first.StartOffset {
		t.Fatal("handoff lost retry identity", err)
	}
	for _, v := range r.sourceVolumes(p) {
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		_, recordErr := h.RecordInfo(volume.Key{Bucket: bucket, Object: volume.ObjectID(object)})
		_, receipt, receiptErr := h.OperationStatus(volume.ID(appendID))
		h.Close()
		if !errors.Is(recordErr, volume.ErrNotFound) || receiptErr != nil || receipt {
			t.Fatal("obsolete physical record/receipt was retained", recordErr, receiptErr)
		}
	}
	for _, id := range []client.ObjectID{{1, 1}, {2, 2}, {0x91}, {0x92}} {
		if _, err := objects.Put(ctx, id, payload, client.PutOptions{}); err != nil {
			t.Fatal("one client could not extend across nodes", id, err)
		}
	}
	source.node.mu.RLock()
	var sourceBytes uint64
	for _, v := range source.node.volumes {
		if !v.config.System {
			sourceBytes += v.store.Stats().LogicalBytes
		}
	}
	source.node.mu.RUnlock()
	targetBytes := destination.Stats().LogicalBytes
	if sourceBytes+targetBytes <= 4<<20 || sourceBytes == 0 || targetBytes == 0 {
		t.Fatal("test did not exceed one node's data capacity", sourceBytes, targetBytes)
	}
	// Restart the owner of the moved half first. That half remains usable while
	// the owner of the unaffected half is still deliberately offline.
	source.close()
	target = restartDaemonTestNode(t, target)
	admin := source.node.cfg.Auth.BootstrapAdmin
	reader, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, _, err = reader.Get(ctx, object)
	if err != nil || len(got) != len(payload)+len(delta) {
		t.Fatal("independent migrated range waited for old initiator", err)
	}
	source = restartDaemonTestNode(t, source)
	if _, _, err := reader.Get(ctx, client.ObjectID{1, 1}); err != nil {
		t.Fatal("retained half did not resume", err)
	}
}

// Gate only RELEASE after all durable APPLIED proofs, to exercise a retiring
// leaver restarting with a system PG that correctly excludes its old anchor.
type gatedLeaveRelease struct {
	controlParticipants
	leaving client.ID
	blocked atomic.Bool
}

func (p *gatedLeaveRelease) Release(ctx context.Context, node client.ID, decision client.Decision, certificate client.Certificate) (client.Proof, error) {
	if node == p.leaving && p.blocked.Load() {
		return client.Proof{}, fail(503, "test gates ALL release")
	}
	return p.controlParticipants.Release(ctx, node, decision, certificate)
}

func startAutomaticDaemonPair(t *testing.T, automatic ...bool) (*daemonTestNode, *daemonTestNode) {
	t.Helper()
	auto := len(automatic) == 0 || automatic[0]
	source := startDaemonTestNode(t, filepath.Join(t.TempDir(), "source.yaml"), nil, auto)
	cfg := copyConfig(source.node.cfg)
	cfg.Node.ID = newID()
	cfg.Node.Name = "extension-peer"
	cfg.Node.DataDir = "."
	cfg.Volumes = nil
	cfg.Peers = []PeerConfig{{NodeID: source.node.cfg.Node.ID, URL: source.node.cfg.Node.Advertise}}
	cfg.ControlVersion = 0
	cfg.RangeVersion = 0
	cfg.QuotaVersion = 0
	cfg.PeerDirectoryVersion = 0
	cfg.BucketCatalogVersion = 0
	cfg.PlacementGroupsVersion = 0
	cfg.JoinPending = true
	cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
	target := startDaemonTestNode(t, filepath.Join(t.TempDir(), "target.yaml"), &cfg, auto)
	deadline := time.Now().Add(20 * time.Second)
	for {
		a, b := source.node.currentSystem.Load(), target.node.currentSystem.Load()
		if a != nil && b != nil && a.root == b.root && len(a.manifest.Membership.Members) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("automatic admission timed out")
		}
		time.Sleep(25 * time.Millisecond)
	}
	return source, target
}

// Production automatic planning, not a test-selected handoff, must make more
// than one node's physical data capacity available through one bucket client.
func TestDaemonAutomaticExtension(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "automatic-extension")
	// An empty bucket stays where it was created. Extension is triggered by
	// actual pressure from the writes below, not cosmetic hash-space balancing.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	objects, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	payload := bytes.Repeat([]byte{0x6e}, 1<<20)
	ids := []client.ObjectID{{1}, {2}, {3}, {0x81}, {0x82}, {0x83}}
	for _, id := range ids {
		if _, err := objects.Put(ctx, id, payload, client.PutOptions{}); err != nil {
			t.Fatal("automatic extension PUT", id, err)
		}
	}
	for _, id := range ids {
		got, _, err := objects.Get(ctx, id)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("automatic extension GET", id, err)
		}
	}
	var logical uint64
	for _, node := range []*Server{source.node, target.node} {
		node.mu.RLock()
		var local uint64
		for _, v := range node.volumes {
			if !v.config.System {
				local += v.store.Stats().LogicalBytes
			}
		}
		node.mu.RUnlock()
		if local == 0 {
			t.Fatal("automatic extension left a node unused")
		}
		logical += local
	}
	if logical != 6<<20 {
		t.Fatal("incorrect distributed physical object totals", logical)
	}
	// Explicit volume withdrawal must move its authoritative groups, purge
	// physical records/receipts, then release the device claim without unlinking.
	var cursor *client.ObjectID
	inventory := map[client.ObjectID]bool{}
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("cluster inventory cursor did not finish")
		}
		page, next, err := objects.ScanObjects(ctx, cursor, 1)
		if err != nil {
			t.Fatal("cluster inventory", err)
		}
		for _, object := range page {
			if inventory[object.ID] {
				t.Fatal("duplicate cluster inventory entry")
			}
			inventory[object.ID] = true
		}
		if next == nil {
			break
		}
		cursor = next
	}
	if len(inventory) != len(ids) {
		t.Fatal("cluster inventory omitted remote objects", len(inventory))
	}
	source.node.mu.RLock()
	var withdrawn string
	for id, v := range source.node.volumes {
		if !v.config.System {
			withdrawn = id
			break
		}
	}
	source.node.mu.RUnlock()
	addTestVolume(t, source.node, "replacement", "hdd", 8<<20)
	if _, err := source.node.RemoveVolume(newID(), withdrawn); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(25 * time.Second)
	for {
		source.node.mu.RLock()
		_, attached := source.node.volumes[withdrawn]
		source.node.mu.RUnlock()
		if !attached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("drained volume claim was not released")
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, id := range ids {
		got, _, err := objects.Get(ctx, id)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("volume withdrawal lost data", id, err)
		}
	}
	// A voluntary node departure retains its system anchor until ALL global
	// release, while all ordinary ranges evacuate to the surviving member.
	addTestVolume(t, target.node, "drain-capacity", "hdd", 8<<20)
	target.node.controller.mu.Lock()
	gate := &gatedLeaveRelease{controlParticipants: target.node.controller.coordinator.participants, leaving: source.node.control.ledger.node}
	gate.blocked.Store(true)
	target.node.controller.coordinator.participants = gate
	target.node.controller.mu.Unlock()
	e := client.Encoder{}
	e.ID(source.node.control.ledger.node)
	_, status, _ := adminBucketMessage(t, source.node, client.NodeDrain, e.B)
	if status != 202 {
		t.Fatal("node drain command", status)
	}
	deadline = time.Now().Add(25 * time.Second)
	for !source.node.retiring() {
		if time.Now().After(deadline) {
			t.Fatal("node did not reach retiring/APPLIED state")
		}
		time.Sleep(25 * time.Millisecond)
	}
	source = restartDaemonTestNode(t, source)
	if !source.node.retiring() {
		t.Fatal("retiring state was not restored")
	}
	if source.node.placementGroups.Load().allows(systemPlacementGroup.String(), source.node.cfg.Node.ID, source.node.system.Identity().VolumeID.String()) {
		t.Fatal("leaver retained an active metadata placement")
	}
	gate.blocked.Store(false)
	select {
	case <-source.node.stopRequest:
	case <-time.After(35 * time.Second):
		source.node.control.ledger.mu.Lock()
		slot, req := source.node.control.ledger.slot != nil, source.node.control.ledger.request != nil
		source.node.control.ledger.mu.Unlock()
		t.Fatalf("node departure did not finish: retiring=%v slot=%v request=%v", source.node.retiring(), slot, req)
	}
	source.close()
	view := target.node.currentSystem.Load()
	if len(view.manifest.Membership.Members) != 1 {
		t.Fatal("departure left membership unchanged")
	}
	reader, err := client.Dial(context.Background(), target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()
	for _, id := range ids {
		got, _, err := objects.Get(readCtx, id)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("existing client did not recover from seed departure", id, err)
		}
		got, _, err = reader.Get(readCtx, id)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("node departure lost data", id, err)
		}
	}
	if reopened, err := Open(Options{ConfigPath: source.path}); !errors.Is(err, ErrNodeDrained) {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatal("departed node automatically rejoined after restart", err)
	}
}

func TestDaemonRAMRuntimeResetFencesOldRetries(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, true)
	if _, err := node.node.AddVolume(newID(), AddVolumeRequest{Name: "ram", Source: 2, Class: "ram", SizeBytes: 4 << 20}); err != nil {
		t.Fatal(err)
	}
	draft := newTestBucket(t, "ram-runtime")
	draft.description.Policy.StorageStrategy = "ram"
	_, status, _ := adminBucketMessage(t, node.node, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 201 {
		t.Fatal("create RAM bucket", status)
	}
	admin := node.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, node.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{9}
	request := client.NewRequestID()
	first, err := c.Put(ctx, oid, []byte("before process restart"), client.PutOptions{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	if first.RuntimeID == (client.ID{}) {
		t.Fatal("RAM reply omitted runtime")
	}
	node = restartDaemonTestNode(t, node)
	deadline := time.Now().Add(10 * time.Second)
	for {
		patch, known := node.node.routingPatch(draft.ID(), volume.ObjectID(oid))
		if known && patch.Entries[0].Runtime == node.node.boot {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("RAM runtime reset did not commit")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := c.Put(ctx, oid, []byte("before process restart"), client.PutOptions{RequestID: request}); !errors.Is(err, client.ErrGone) {
		var status *client.StatusError
		if !errors.As(err, &status) || status.Status != 410 {
			t.Fatal("old runtime retry was accepted", err)
		}
	}
	fresh, err := client.Dial(ctx, node.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, _, err := fresh.Get(ctx, oid); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("single-copy RAM unexpectedly survived process death", err)
	}
	info, err := fresh.Put(ctx, oid, []byte("new runtime"), client.PutOptions{})
	if err != nil || info.RuntimeID == first.RuntimeID {
		t.Fatal("fresh RAM runtime unavailable", err)
	}
}

func TestDaemonRAMNodeDrainPreservesLiveRuntime(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	for _, pair := range []struct {
		node *daemonTestNode
		size int64
	}{{source, 4 << 20}, {target, 8 << 20}} {
		if _, err := pair.node.node.AddVolume(newID(), AddVolumeRequest{Name: "ram", Source: 2, Class: "ram", SizeBytes: pair.size}); err != nil {
			t.Fatal(err)
		}
	}
	draft := newTestBucket(t, "ram-drain")
	draft.description.Policy.StorageStrategy = "ram"
	_, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 201 {
		t.Fatal("create RAM bucket", status)
	}
	var oid client.ObjectID
	deadline := time.Now().Add(20 * time.Second)
	for {
		found := false
		for i := 0; i < 256; i++ {
			candidate := client.ObjectID{byte(i)}
			patch, known := source.node.routingPatch(draft.ID(), volume.ObjectID(candidate))
			if known && patch.Entries[0].Locations[0].Node == source.node.control.ledger.node {
				oid, found = candidate, true
				break
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no source-owned RAM range")
		}
		time.Sleep(25 * time.Millisecond)
	}
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte{0x5a}, 1<<20)
	before, err := c.Put(ctx, oid, payload, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(source.node.control.ledger.node)
	_, status, _ = adminBucketMessage(t, source.node, client.NodeDrain, e.B)
	if status != 202 {
		t.Fatal("RAM source drain", status)
	}
	select {
	case <-source.node.stopRequest:
	case <-ctx.Done():
		logDaemonRangeState(t, source, target)
		t.Fatal("RAM node drain did not complete", ctx.Err())
	}
	source.close()
	got, after, err := c.Get(ctx, oid)
	if err != nil || !bytes.Equal(got, payload) || before.RuntimeID != after.RuntimeID || before.Version != after.Version {
		t.Fatal("live RAM handoff changed content, version or runtime", before, after, err)
	}
	// Once the sole remaining RAM process restarts, its latest data is lost.
	// An obsolete source copy must never be restored as if it were a replica.
	if updated, err := c.Put(ctx, oid, bytes.Repeat([]byte{0x68}, (1<<20)+17), client.PutOptions{}); err != nil || updated.RuntimeID != before.RuntimeID {
		t.Fatal("streaming write changed inherited RAM runtime", updated, err)
	}
	target = restartDaemonTestNode(t, target)
	if _, _, err := c.Get(ctx, oid); !errors.Is(err, client.ErrGone) {
		t.Fatal("old RAM runtime remained readable after target restart", err)
	}
	fresh, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, _, err := fresh.Get(ctx, oid); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("obsolete RAM data resurrected", err)
	}
}

func TestDaemonExtensionWithConcentratedObjectHashes(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	// Six MiB still exceeds either node's capacity. Leave room for the
	// transaction's temporary redo image as well as committed payloads: two
	// four-MiB volumes cannot admit the sixth one-MiB PUT with that workspace.
	addTestVolume(t, source.node, "data", "hdd", 5<<20)
	addTestVolume(t, target.node, "data", "hdd", 5<<20)
	bucket, _ := createProtocolBucket(t, source.node, "concentrated-hashes")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte{0x73}, 1<<20)
	// All hashes are in the same initial half; extension must react to capacity
	// pressure instead of relying on a lucky distribution among static halves.
	for i := byte(1); i <= 6; i++ {
		id := client.ObjectID{i}
		if _, err := c.Put(ctx, id, payload, client.PutOptions{}); err != nil {
			logDaemonRangeState(t, source, target)
			t.Fatal("capacity handoff PUT", id, err)
		}
	}
	for i := byte(1); i <= 6; i++ {
		got, _, err := c.Get(ctx, client.ObjectID{i})
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("capacity handoff GET", i, err)
		}
	}
	var used uint64
	for _, s := range []*Server{source.node, target.node} {
		b, err := s.Bucket(bucket.ID())
		if err != nil {
			t.Fatal(err)
		}
		s.opMu.Lock()
		local, err := b.logicalUsage()
		s.opMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		used += local
	}
	if used != 6<<20 {
		t.Fatal("authoritative usage counted staging/obsolete copies", used)
	}
	// Either initiating member sees at most 4 MiB locally. A global shrink to
	// 5 MiB must still reject the cluster's 6 MiB, then release only this bucket's
	// durable fence when the unprepared administration request is cancelled.
	quota := client.Encoder{}
	quota.ID(client.ID(bucket.ID()))
	quota.U8(0)
	quota.U64(0)
	quota.Bool(false)
	quota.U64(5 << 20)
	quota.U32(0)
	quota.U64(bucket.Describe().Policy.Version)
	quotaMessage := client.Message{Kind: client.Request, Opcode: client.BucketSetQuota, ID: client.NewRequestID(), Body: quota.B}
	deadline := time.Now().Add(15 * time.Second)
	status := 503
	var lastReply client.Message
	for status == 503 && time.Now().Before(deadline) {
		status, lastReply = bucketHTTPReply(t, target.node, quotaMessage, admin.Username, admin.Password)
		if status == 503 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if status != 507 {
		for _, node := range []*Server{source.node, target.node} {
			r := node.scopes
			r.mu.Lock()
			t.Log("range worker", r.node, r.lastError)
			for _, txn := range r.txns {
				if r.active(txn) {
					t.Log("active", txn.plan.id(), "source", txn.plan.Source, "target", txn.plan.Target, "fenced", txn.fenced, "committed", txn.decision != nil, "remaining", txn.remaining, "sourceDone", txn.sourceDone.RecordSequence, "targetDone", txn.targetDone.RecordSequence)
				}
			}
			r.mu.Unlock()
			node.mu.RLock()
			for _, v := range node.volumes {
				if !v.config.System {
					t.Log("volume", v.store.Identity().VolumeID, v.store.Stats())
				}
			}
			node.mu.RUnlock()
		}
		t.Fatal("global quota shrink ignored remote usage", status, string(lastReply.Body))
	}
	for _, node := range []*Server{source.node, target.node} {
		b, err := node.Bucket(bucket.ID())
		if err != nil || !b.Describe().Policy.Limits.Unlimited || node.bucketFrozen(bucket.ID()) {
			t.Fatal("failed shrink leaked policy or mutation fence", err)
		}
	}
}

func TestDaemonNativeShortShutdownCommand(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, true)
	addTestVolume(t, node.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, node.node, "short-restart")
	admin := node.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, node.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{6}
	if _, err := c.Put(ctx, id, []byte("keep across SHORT"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before, ok := node.node.routingPatch(bucket.ID(), volume.ObjectID(id))
	if !ok {
		t.Fatal("missing placement")
	}
	e := client.Encoder{}
	e.ID(node.node.control.ledger.node)
	e.U8(1)
	command, status, _ := adminBucketMessage(t, node.node, client.NodeShutdown, e.B)
	if status != 202 {
		t.Fatal("SHORT command", status)
	}
	select {
	case <-node.node.stopRequest:
	case <-ctx.Done():
		t.Fatal("SHORT did not finish", ctx.Err())
	}
	node.node.mu.RLock()
	paused, draining := node.node.cfg.Node.Paused, node.node.cfg.Node.Draining
	node.node.mu.RUnlock()
	if !paused || draining {
		t.Fatal("SHORT changed drain intent or omitted pause")
	}
	node = restartDaemonTestNode(t, node)
	after, ok := node.node.routingPatch(bucket.ID(), volume.ObjectID(id))
	if !ok || !reflect.DeepEqual(before.Entries, after.Entries) {
		t.Fatal("SHORT changed ownership")
	}
	got, _, err := c.Get(ctx, id)
	if err != nil || string(got) != "keep across SHORT" {
		t.Fatal("SHORT lost committed data", err)
	}
	node.node.opMu.Lock()
	operation, err := node.node.operation(command.ID.String())
	node.node.opMu.Unlock()
	if err != nil || operation.State != "fulfilled" {
		t.Fatal("SHORT operation status", operation, err)
	}
}

func logDaemonRangeState(t *testing.T, nodes ...*daemonTestNode) {
	t.Helper()
	for _, node := range nodes {
		r := node.node.scopes
		r.mu.Lock()
		t.Log("range worker", r.node, r.lastError, "hints", len(r.pressures))
		for _, txn := range r.txns {
			if r.active(txn) {
				t.Log("active source", txn.plan.Source, "target", txn.plan.Target, "prefix", txn.plan.Moving.Start, txn.plan.Moving.PrefixBits, "fenced", txn.fenced, "committed", txn.decision != nil, "remaining", txn.remaining, "sourceDone", txn.sourceDone.RecordSequence, "targetDone", txn.targetDone.RecordSequence)
			}
		}
		r.mu.Unlock()
		node.node.mu.RLock()
		for _, v := range node.node.volumes {
			if !v.config.System {
				stats := v.store.Stats()
				t.Log("volume", v.config.ID, "logical", stats.LogicalBytes, "free", stats.AdmissionFreePhysical, "inflight", stats.InFlightReservedPhysical)
			}
		}
		node.node.mu.RUnlock()
	}
}

func TestDaemonBucketDeletionReclaimsEveryNode(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "distributed-delete")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Populate beyond one volume so the fixture exercises real extension.
	payload := bytes.Repeat([]byte{0x39}, 1<<20)
	for _, id := range []client.ObjectID{{1}, {2}, {0x81}, {0x82}} {
		if _, err := c.Put(ctx, id, payload, client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		populated := 0
		for _, node := range []*Server{source.node, target.node} {
			node.mu.RLock()
			var logical uint64
			for _, v := range node.volumes {
				if !v.config.System {
					logical += v.store.Stats().LogicalBytes
				}
			}
			node.mu.RUnlock()
			if logical > 0 {
				populated++
			}
		}
		if populated == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delete fixture did not populate both nodes")
		}
		time.Sleep(25 * time.Millisecond)
	}
	body := client.Encoder{}
	body.ID(client.ID(bucket.ID()))
	message := client.Message{Kind: client.Request, Opcode: client.BucketDelete, ID: client.NewRequestID(), Body: body.B}
	deadline = time.Now().Add(10 * time.Second)
	status := 503
	for status == 503 && time.Now().Before(deadline) {
		status, _ = bucketHTTPReply(t, source.node, message, admin.Username, admin.Password)
		if status == 503 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if status != 202 && status != 200 {
		t.Fatal("DELETE intent failed", status)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		complete := true
		for _, node := range []*Server{source.node, target.node} {
			deleted := false
			for _, description := range node.currentSystem.Load().descriptions {
				if description.ID == bucket.ID().String() && description.DeletionComplete {
					deleted = true
				}
			}
			complete = complete && deleted
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cluster DELETE did not finish")
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, node := range []*Server{source.node, target.node} {
		if entries := node.placement.Load().bucketEntries(client.ID(bucket.ID())); len(entries) != 0 {
			t.Fatal("deleted bucket retained authoritative map entries")
		}
		node.mu.RLock()
		var stores []*volume.Volume
		for _, v := range node.volumes {
			if !v.config.System {
				stores = append(stores, v.store)
			}
		}
		node.mu.RUnlock()
		for _, store := range stores {
			h, err := store.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			records, err := h.ScanBucketObjects(bucket.ID(), nil, 1)
			var receipts bool
			if err == nil {
				err = h.ScanRequests(func(r volume.RequestRecord) error {
					if r.Key.Bucket == bucket.ID() {
						receipts = true
					}
					return nil
				})
			}
			h.Close()
			if err != nil || len(records) != 0 || receipts || store.Stats().LogicalBytes != 0 {
				t.Fatal("deleted bucket retained physical data/retry state", err)
			}
		}
	}
	source = restartDaemonTestNode(t, source)
	if _, err := source.node.Bucket(bucket.ID()); !errors.Is(err, ErrBucketNotFound) {
		t.Fatal("restart exposed deleted bucket", err)
	}
	deleted := false
	for _, description := range source.node.currentSystem.Load().descriptions {
		if description.ID == bucket.ID().String() && description.DeletionComplete {
			deleted = true
		}
	}
	if !deleted {
		t.Fatal("restart lost global deletion completion")
	}
}

func TestDaemonCodecPolicyConvertsEveryOwner(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "distributed-codec")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte{0x21}, 1<<20)
	ids := []client.ObjectID{{1}, {0x81}}
	versions := map[client.ObjectID]uint64{}
	for _, id := range ids {
		info, err := c.Put(ctx, id, payload, client.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		versions[id] = info.Version
	}
	description := bucket.Describe()
	oldVersion := description.Policy.Version
	description.Policy.Version++
	description.Policy.Compression = "gzip"
	draft := &Bucket{description: description}
	body := client.Encoder{B: bucketPolicyBody(t, draft)}
	body.U64(oldVersion)
	message := client.Message{Kind: client.Request, Opcode: client.BucketSetPolicy, ID: client.NewRequestID(), Body: body.B}
	deadline := time.Now().Add(10 * time.Second)
	status := 503
	for status == 503 && time.Now().Before(deadline) {
		status, _ = bucketHTTPReply(t, target.node, message, admin.Username, admin.Password)
		if status == 503 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if status != 202 && status != 200 {
		t.Fatal("codec intent", status)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		complete := true
		for _, node := range []*Server{source.node, target.node} {
			b, err := node.Bucket(bucket.ID())
			if err != nil {
				t.Fatal(err)
			}
			description := b.Describe()
			complete = complete && description.DesiredPolicy == nil && description.Policy.Compression == "gzip"
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("global codec conversion did not finish")
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, id := range ids {
		got, info, err := c.Get(ctx, id)
		if err != nil || !bytes.Equal(got, payload) || info.Version != versions[id] || info.Compression != uint8(volume.Gzip) {
			t.Fatal("codec conversion changed content/version or missed owner", id, info, err)
		}
	}
	for _, node := range []*Server{source.node, target.node} {
		for _, entry := range node.placement.Load().bucketEntries(client.ID(bucket.ID())) {
			if entry.PolicyVersion != oldVersion+1 {
				t.Fatal("map omitted policy completion")
			}
		}
	}
	target = restartDaemonTestNode(t, target)
	if b, err := target.node.Bucket(bucket.ID()); err != nil || b.Describe().Policy.Compression != "gzip" || b.Describe().DesiredPolicy != nil {
		t.Fatal("restart lost converted policy", err)
	}
}

func TestDaemonStorageClassPolicyMovesToEligibleOwner(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, source.node, "hdd-data", "hdd", 4<<20)
	addTestVolume(t, target.node, "ssd-data", "ssd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "class-transition")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte{0x42}, 1<<20)
	ids := []client.ObjectID{{1}, {0x81}}
	versions := map[client.ObjectID]uint64{}
	for _, id := range ids {
		info, err := c.Put(ctx, id, payload, client.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		versions[id] = info.Version
	}
	old := bucket.Describe()
	desired := cloneBucketDescription(old)
	desired.Policy.Version++
	desired.Policy.StorageStrategy = "ssd"
	// A combined codec/class change must re-encode during the range handoff.
	desired.Policy.Compression = "lz4"
	body := client.Encoder{B: bucketPolicyBody(t, &Bucket{description: desired})}
	body.U64(old.Policy.Version)
	message := client.Message{Kind: client.Request, Opcode: client.BucketSetPolicy, ID: client.NewRequestID(), Body: body.B}
	deadline := time.Now().Add(10 * time.Second)
	status := 503
	for status == 503 && time.Now().Before(deadline) {
		status, _ = bucketHTTPReply(t, source.node, message, admin.Username, admin.Password)
		if status == 503 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if status != 202 && status != 200 {
		t.Fatal("storage-class intent", status)
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		complete := true
		for _, node := range []*Server{source.node, target.node} {
			local, err := node.Bucket(bucket.ID())
			if err != nil {
				t.Fatal(err)
			}
			d := local.Describe()
			complete = complete && d.DesiredPolicy == nil && d.Policy.StorageStrategy == "ssd" && d.Policy.Compression == "lz4"
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			logDaemonRangeState(t, source, target)
			t.Fatal("storage class did not converge")
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, id := range ids {
		got, info, err := c.Get(ctx, id)
		if err != nil || !bytes.Equal(got, payload) || info.Version != versions[id] || info.Compression != uint8(volume.LZ4) {
			t.Fatal("class migration changed content/version/codec", id, info, err)
		}
	}
	for _, node := range []*Server{source.node, target.node} {
		for _, entry := range node.placement.Load().bucketEntries(client.ID(bucket.ID())) {
			if entry.PolicyVersion != desired.Policy.Version {
				t.Fatal("class policy missing from map")
			}
			for _, location := range entry.Locations {
				if location.Node != target.node.control.ledger.node || location.Class != uint8(volume.SSD) {
					t.Fatal("SSD object remains assigned to ineligible HDD", location)
				}
			}
		}
	}
	source.node.mu.RLock()
	for _, v := range source.node.volumes {
		if !v.config.System && v.store.Stats().LogicalBytes != 0 {
			source.node.mu.RUnlock()
			t.Fatal("old HDD copy was not freed")
		}
	}
	source.node.mu.RUnlock()
	target = restartDaemonTestNode(t, target)
	reader, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got, _, err := reader.Get(ctx, ids[0]); err != nil || !bytes.Equal(got, payload) {
		t.Fatal("new class placement lost across restart", err)
	}
}
