// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestRecoveryOnNewNodeThroughDashboardPeer(t *testing.T) {
	source, survivor := startAutomaticDaemonPair(t, false)
	rescued := addTestVolume(t, source.node, "rescued", "hdd", 4<<20)
	path := source.node.volumes[rescued.VolumeID].config.Path
	bucket, _ := createProtocolBucket(t, source.node, "new-hardware")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	object := client.ObjectID{9}
	if _, err := c.Put(ctx, object, []byte("new hardware, original disks"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	beginTestNodeRecovery(t, source, survivor)
	survivor.node.mu.RLock()
	cfg := copyConfig(survivor.node.cfg)
	survivor.node.mu.RUnlock()
	cfg.Node.ID, cfg.Node.Name, cfg.Node.DataDir = newID(), "replacement", "."
	cfg.Volumes, cfg.RecoveryVolumes = nil, nil
	cfg.Peers = []PeerConfig{{NodeID: survivor.node.cfg.Node.ID, URL: survivor.node.cfg.Node.Advertise}}
	cfg.ControlVersion, cfg.RangeVersion, cfg.QuotaVersion, cfg.PeerDirectoryVersion, cfg.BucketCatalogVersion, cfg.PlacementGroupsVersion = 0, 0, 0, 0, 0, 0
	cfg.JoinPending = true
	cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
	target := startDaemonTestNode(t, filepath.Join(t.TempDir(), "replacement.yaml"), &cfg)
	for {
		a, b := survivor.node.currentSystem.Load(), target.node.currentSystem.Load()
		if a != nil && b != nil && a.root == b.root && len(a.manifest.Membership.Members) == 2 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("replacement join", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	proxy := func(op uint16, body []byte, id client.ID) int {
		t.Helper()
		wire, err := (client.NodeAdmin{Target: target.node.control.ledger.node, Opcode: op, Body: body}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		status, _ := bucketHTTPReply(t, survivor.node, client.Message{Kind: client.Request, Opcode: client.NodeAdminRequest, ID: id, Body: wire}, admin.Username, admin.Password)
		return status
	}
	e := client.Encoder{}
	e.Text("saved")
	e.Text(path)
	stageID := client.NewRequestID()
	if status := proxy(client.VolumeRecoveryStage, e.B, stageID); status != 201 {
		t.Fatal("proxy stage", status)
	}
	if status := proxy(client.VolumeRecoveryStage, e.B, stageID); status != 201 {
		t.Fatal("proxy stage retry", status)
	}
	changed := client.Encoder{}
	changed.Text("different")
	changed.Text(path)
	if status := proxy(client.VolumeRecoveryStage, changed.B, stageID); status != 400 {
		t.Fatal("stage ID reuse", status)
	}
	e = client.Encoder{}
	e.U64(target.node.currentSystem.Load().manifest.Membership.Epoch)
	e.V(1)
	volumeID, err := parseID(rescued.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	e.ID(client.ID(volumeID))
	if status := proxy(client.VolumeRecoveryCommit, e.B, client.NewRequestID()); status != 200 {
		t.Fatal("proxy takeover", status)
	}
	c, err = client.Dial(ctx, survivor.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, _, err := c.Get(ctx, object)
	if err != nil || string(got) != "new hardware, original disks" {
		t.Fatal("rescued object", string(got), err)
	}
}

func TestRecoveryQuotaRejectionDoesNotFreezeCluster(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	v := addTestVolume(t, source.node, "data", "hdd", 4<<20)
	path := source.node.volumes[v.VolumeID].config.Path
	bucket, _ := createProtocolBucket(t, source.node, "quota-recovery")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(ctx, client.ObjectID{1}, bytes.Repeat([]byte{7}, 128<<10), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	beginTestNodeRecovery(t, source, target)
	id, err := target.node.StageRecoveryVolume("saved", path)
	if err != nil {
		t.Fatal(err)
	}
	setLimit := func(limit uint64) {
		t.Helper()
		b, _ := target.node.Bucket(bucket.ID())
		e := client.Encoder{}
		e.ID(client.ID(bucket.ID()))
		e.U8(0)
		e.U64(0)
		e.Bool(false)
		e.U64(limit)
		e.U32(0)
		e.U64(b.Describe().Policy.Version)
		if status, reply := bucketHTTPReply(t, target.node, client.Message{Kind: client.Request, Opcode: client.BucketSetQuota, ID: client.NewRequestID(), Body: e.B}, admin.Username, admin.Password); status != 200 {
			t.Fatal("quota change", status, string(reply.Body))
		}
	}
	setLimit(64 << 10)
	if status, _ := commitTestRecoveredVolumes(t, target, admin, id); status != 507 {
		t.Fatal("overfull rescued bucket", status)
	}
	if target.node.forceFrozen() {
		t.Fatal("unprepared takeover left cluster frozen")
	}
	if target.node.recoveryVolumes[id].store.Identity().NodeID != volume.ID(source.node.control.ledger.node) {
		t.Fatal("failed takeover rewrote header")
	}
	setLimit(256 << 10)
	if status, _ := commitTestRecoveredVolumes(t, target, admin, id); status != 200 {
		t.Fatal("retry after quota increase", status)
	}
	// A policy revision alone must not make the recovered cohort invisible.
	setLimit(512 << 10)
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	data, _, err := c.Get(ctx, client.ObjectID{1})
	if err != nil || len(data) != 128<<10 {
		t.Fatal("quota revision hid rescued object", err)
	}
}

func TestRecoveryRejectsUnresolvedSourceFence(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	v := addTestVolume(t, source.node, "data", "hdd", 4<<20)
	store := source.node.volumes[v.VolumeID].store
	path := source.node.volumes[v.VolumeID].config.Path
	createProtocolBucket(t, source.node, "fenced-recovery")
	if err := source.node.ensureVolumeAuthority(); err != nil {
		t.Fatal(err)
	}
	source.node.opMu.Lock()
	record, err := readVolumeAuthority(store)
	if err != nil || len(record.Entries) != 1 {
		t.Fatal("fixture provenance", err)
	}
	record.Entries[0].Active = false
	record.Entries[0].Fence = [32]byte{1}
	if err := writeVolumeAuthority(store, record); err != nil {
		t.Fatal(err)
	}
	source.node.opMu.Unlock()
	admin := source.node.cfg.Auth.BootstrapAdmin
	beginTestNodeRecovery(t, source, target)
	id, err := target.node.StageRecoveryVolume("saved", path)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := commitTestRecoveredVolumes(t, target, admin, id); status != 409 {
		t.Fatal("unresolved fence accepted", status)
	}
	if target.node.forceFrozen() {
		t.Fatal("invalid media froze cluster")
	}
}

func TestRecoveryBeginPreservesUnavailableRanges(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	rescued := addTestVolume(t, source.node, "rescued", "hdd", 4<<20)
	path := source.node.volumes[rescued.VolumeID].config.Path
	bucket, _ := createProtocolBucket(t, source.node, "recover-bucket")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	object := client.ObjectID{3}
	payload := bytes.Repeat([]byte("x"), 2<<20)
	if _, err = c.Put(ctx, object, payload, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	appendID := client.NewRequestID()
	appended, err := c.Append(ctx, object, []byte("tail"), client.AppendOptions{RequestID: appendID})
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, []byte("tail")...)
	c.Close()
	sourceID := source.node.control.ledger.node
	epoch := target.node.currentSystem.Load().manifest.Membership.Epoch
	source.close()
	body := client.Encoder{}
	body.V(1)
	body.ID(sourceID)
	body.Bool(false)
	body.Bool(true)
	body.U64(epoch)
	message := client.Message{Kind: client.Request, Opcode: client.NodeRecoveryBegin, ID: client.NewRequestID(), Body: body.B}
	if status, reply := bucketHTTPReply(t, target.node, message, admin.Username, admin.Password); status != 200 {
		target.node.controller.mu.Lock()
		_, resumeErr := target.node.controller.resume(ctx)
		target.node.controller.mu.Unlock()
		t.Fatal("recovery begin", status, resumeErr, string(reply.Body))
	}
	view := target.node.currentSystem.Load()
	if len(view.manifest.VolumeRecoveries) != 1 || view.manifest.VolumeRecoveries[0].Complete || view.manifest.VolumeRecoveries[0].After.State != 4 {
		t.Fatal("missing recovery hold", len(view.manifest.VolumeRecoveries))
	}
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	if data, _, err := c.Get(short, object); err == nil || bytes.Equal(data, payload) {
		t.Fatal("unrecovered object was exposed")
	}
	target = restartDaemonTestNode(t, target)
	if len(target.node.currentSystem.Load().manifest.VolumeRecoveries) != 1 {
		t.Fatal("recovery origin lost on restart")
	}
	if status, _ := bucketHTTPReply(t, target.node, message, admin.Username, admin.Password); status != 200 {
		t.Fatal("recovery receipt not repeatable", status)
	}
	id, err := target.node.StageRecoveryVolume("saved", path)
	if err != nil {
		t.Fatal("stage rescued disk", err)
	}
	store := target.node.recoveryVolumes[id].store
	before := store.Stats()
	body = client.Encoder{}
	body.U64(target.node.currentSystem.Load().manifest.Membership.Epoch)
	body.V(1)
	body.ID(client.ID(id))
	restore := client.Message{Kind: client.Request, Opcode: client.VolumeRecoveryCommit, ID: client.NewRequestID(), Body: body.B}
	if status, reply := bucketHTTPReply(t, target.node, restore, admin.Username, admin.Password); status != 200 {
		target.node.controller.mu.Lock()
		_, resumeErr := target.node.controller.resume(ctx)
		target.node.controller.mu.Unlock()
		t.Fatal("restore", status, resumeErr, string(reply.Body))
	}
	if store.Identity().NodeID != volume.ID(target.node.control.ledger.node) || store.Stats().DataPhysical != before.DataPhysical {
		t.Fatal("payload layout changed during takeover")
	}
	if store.IOStats().WriteBytes-before.IO.WriteBytes >= 512<<10 {
		t.Fatal("takeover copied the payload")
	}
	c2, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	got, info, err := c2.Get(ctx, object)
	if err != nil || !bytes.Equal(got, payload) || info.Length != appended.Length {
		t.Fatal("recovered bytes/version", err)
	}
	if result, err := c2.Append(ctx, object, []byte("tail"), client.AppendOptions{RequestID: appendID}); err != nil || result.Length != appended.Length {
		t.Fatal("append retry was not retained", err)
	}
	c2.Close()
	target = restartDaemonTestNode(t, target)
	if status, _ := bucketHTTPReply(t, target.node, restore, admin.Username, admin.Password); status != 200 {
		t.Fatal("takeover receipt lost", status)
	}
}

func beginTestNodeRecovery(t *testing.T, old, survivor *daemonTestNode) {
	t.Helper()
	id := old.node.control.ledger.node
	epoch := survivor.node.currentSystem.Load().manifest.Membership.Epoch
	admin := old.node.cfg.Auth.BootstrapAdmin
	old.close()
	e := client.Encoder{}
	e.V(1)
	e.ID(id)
	e.Bool(false)
	e.Bool(true)
	e.U64(epoch)
	if status, reply := bucketHTTPReply(t, survivor.node, client.Message{Kind: client.Request, Opcode: client.NodeRecoveryBegin, ID: client.NewRequestID(), Body: e.B}, admin.Username, admin.Password); status != 200 {
		t.Fatal("recovery begin", status, string(reply.Body))
	}
}
func commitTestRecoveredVolumes(t *testing.T, node *daemonTestNode, admin BootstrapAdmin, ids ...volume.ID) (int, client.Message) {
	t.Helper()
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	e := client.Encoder{}
	e.U64(node.node.currentSystem.Load().manifest.Membership.Epoch)
	e.V(uint64(len(ids)))
	for _, id := range ids {
		e.ID(client.ID(id))
	}
	m := client.Message{Kind: client.Request, Opcode: client.VolumeRecoveryCommit, ID: client.NewRequestID(), Body: e.B}
	status, _ := bucketHTTPReply(t, node.node, m, admin.Username, admin.Password)
	return status, m
}
func TestRecoveryRequiresCompleteCohort(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	first := addTestVolume(t, source.node, "one", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "cohort")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{7}, 2<<20)
	if _, err = c.Put(ctx, client.ObjectID{1}, payload, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	second := addTestVolume(t, source.node, "two", "hdd", 4<<20)
	if _, err = c.Put(ctx, client.ObjectID{2}, payload, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	paths := []string{source.node.volumes[first.VolumeID].config.Path, source.node.volumes[second.VolumeID].config.Path}
	beginTestNodeRecovery(t, source, target)
	one, err := target.node.StageRecoveryVolume("one", paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := commitTestRecoveredVolumes(t, target, admin, one); status != 409 {
		t.Fatal("partial cohort became authoritative", status)
	}
	if target.node.forceFrozen() {
		t.Fatal("failed validation stranded control")
	}
	target = restartDaemonTestNode(t, target)
	two, err := target.node.StageRecoveryVolume("two", paths[1])
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := commitTestRecoveredVolumes(t, target, admin, one, two); status != 200 {
		t.Fatal("complete cohort", status)
	}
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, id := range []client.ObjectID{{1}, {2}} {
		got, _, err := c.Get(ctx, id)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("cohort payload", id, err)
		}
	}
}

func TestRecoveredStaleCopiesCannotOverrideOtherRanges(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	old := addTestVolume(t, source.node, "rescued", "hdd", 8<<20)
	addTestVolume(t, target.node, "current", "hdd", 8<<20)
	rescued, _ := createProtocolBucket(t, source.node, "rescued-bucket")
	admin := source.node.cfg.Auth.BootstrapAdmin
	draft := newTestBucket(t, "other-bucket")
	request := client.Message{Kind: client.Request, Opcode: client.BucketCreate, ID: client.NewRequestID(), Body: bucketPolicyBody(t, draft)}
	if status, _ := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 201 {
		t.Fatal(status)
	}
	other, err := target.node.Bucket(draft.ID())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(rescued.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Put(ctx, client.ObjectID{1}, []byte("rescued"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(other.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	oid := client.ObjectID{5}
	current, err := c.Put(ctx, oid, []byte("current"), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	store := source.node.volumes[old.VolumeID].store
	path := source.node.volumes[old.VolumeID].config.Path
	h, err := store.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err = h.Put(volume.Key{Bucket: other.ID(), Object: volume.ObjectID(oid)}, []byte("obsolete copy"), volume.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	h.Close()
	beginTestNodeRecovery(t, source, target)
	id, err := target.node.StageRecoveryVolume("rescued", path)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := commitTestRecoveredVolumes(t, target, admin, id); status != 200 {
		t.Fatal(status)
	}
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(other.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, info, err := c.Get(ctx, oid)
	if err != nil || string(got) != "current" || info.Length != current.Length {
		t.Fatal("stale copy overrode current data", err)
	}
	c.Close()
	operation := client.NewRequestID().String()
	if _, err := target.node.RemoveVolume(operation, id.String()); err != nil {
		t.Fatal("drain recovered drive", err)
	}
	plan, ok := target.node.scopes.planNext()
	if !ok {
		t.Fatal("recovered drain was not planned")
	}
	if err := target.node.scopes.move(ctx, plan); err != nil {
		t.Fatal("migrate recovered data", err)
	}
	for i := 0; i < 4; i++ {
		if err := target.node.scopes.finishEmptyDrains(); err != nil {
			t.Fatal("release recovered drive", err)
		}
	}
	target.node.mu.RLock()
	attached := target.node.volumes[id.String()] != nil
	target.node.mu.RUnlock()
	if attached {
		t.Fatal("obsolete unrelated copies kept recovered drive attached")
	}
	target = restartDaemonTestNode(t, target)
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(rescued.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	data, _, err := c.Get(ctx, client.ObjectID{1})
	if err != nil || string(data) != "rescued" {
		t.Fatal("drained recovered data", err)
	}
}

type interruptedRecoveryApply struct {
	controlParticipants
}

func (p *interruptedRecoveryApply) Apply(ctx context.Context, node client.ID, decision client.Decision) (client.Proof, error) {
	return client.Proof{}, fail(503, "test interrupted before APPLIED")
}
func TestRecoveryResumesCommittedTakeoverAfterRestart(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	v := addTestVolume(t, source.node, "data", "hdd", 4<<20)
	path := source.node.volumes[v.VolumeID].config.Path
	bucket, _ := createProtocolBucket(t, source.node, "resume-recovery")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Put(ctx, client.ObjectID{1}, []byte("retained"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	beginTestNodeRecovery(t, source, target)
	id, err := target.node.StageRecoveryVolume("saved", path)
	if err != nil {
		t.Fatal(err)
	}
	target.node.controller.mu.Lock()
	target.node.controller.coordinator.participants = &interruptedRecoveryApply{controlParticipants: target.node.controller.coordinator.participants}
	target.node.controller.mu.Unlock()
	if status, _ := commitTestRecoveredVolumes(t, target, admin, id); status != 202 {
		t.Fatal("commit was not interrupted", status)
	}
	staged := target.node.recoveryVolumes[id]
	if staged == nil || staged.store.Identity().NodeID != volume.ID(source.node.control.ledger.node) {
		t.Fatal("disk rebound before APPLY")
	}
	target = restartDaemonTestNode(t, target)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if !target.node.forceFrozen() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("takeover did not resume")
		}
		time.Sleep(10 * time.Millisecond)
	}
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, _, err := c.Get(ctx, client.ObjectID{1})
	if err != nil || string(got) != "retained" {
		t.Fatal("committed takeover lost data", err)
	}
}
