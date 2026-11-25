// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDaemonExplicitExclusionMarksLostAndPurgesStaleCopies(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	local := addTestVolume(t, source.node, "data", "hdd", 4<<20)
	remote := addTestVolume(t, target.node, "data", "hdd", 4<<20)
	draft := newTestBucket(t, "force-loss")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 2 << 20
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	left, right := client.ObjectID{1}, client.ObjectID{0x81}
	leftData, rightData := bytes.Repeat([]byte{1}, 10000), bytes.Repeat([]byte{2}, 10000)
	leftInfo, err := c.Put(ctx, left, leftData, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requestID := client.NewRequestID()
	// APPEND has a durable retry receipt; ordinary PUT intentionally does not.
	if _, err := c.Append(ctx, right, rightData, client.AppendOptions{RequestID: requestID}); err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(draft.ID(), volume.ObjectID(right))
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(remote.VolumeID)
	plan, err := planRangeMove(before, moving, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	// Reintroduce an obsolete, unowned copy plus its old dedupe receipt on the
	// survivor. Force PREPARE must remove both before allowing complete repair.
	h, err := target.node.volumes[remote.VolumeID].store.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := h.ScanRequestRecords(nil, 128)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	var receipt volume.RequestRecord
	for _, r := range receipts {
		if r.ID == volume.ID(requestID) {
			receipt = r
		}
	}
	if receipt.ID == (volume.ID{}) {
		t.Fatal("original receipt missing")
	}
	h, err = source.node.volumes[local.VolumeID].store.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	stale := []byte("obsolete unowned bytes")
	err = h.Restore(volume.Key{Bucket: draft.ID(), Object: volume.ObjectID(right)}, volume.Info{Length: uint64(len(stale))}, func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader(stale), uint64(len(stale)))
	}, volume.Options{})
	if err == nil {
		err = h.RestoreRequest(receipt)
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	victim := target.node.control.ledger.node
	epoch := source.node.currentSystem.Load().manifest.Membership.Epoch
	body := func(loss, fenced bool) []byte {
		e := client.Encoder{}
		e.V(1)
		e.ID(victim)
		e.Bool(loss)
		e.Bool(fenced)
		e.U64(epoch)
		return e.B
	}
	for _, flags := range [][2]bool{{false, true}, {true, false}} {
		m := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: body(flags[0], flags[1])}
		if status, _ := bucketHTTPReply(t, source.node, m, admin.Username, admin.Password); status != 400 {
			t.Fatal("missing explicit loss/fencing acceptance", status)
		}
	}
	// Only this disposable test node is fenced. Never force-disconnect either
	// real deployment merely to obtain an acceptance-test result.
	target.close()
	// A DEBIT whose recipient was fenced before RECEIVE is deliberately left
	// unsettled. Exclusion must measure fresh usage, without refunding that old
	// credit or waiting forever for the removed recipient's acknowledgement.
	source.node.opMu.Lock()
	allocation := findQuota(source.node.currentSystem.Load(), client.ID(draft.ID()))
	allowance, err := source.node.quotaAllowance(client.ID(draft.ID()))
	var stranded quotaGrant
	if err == nil {
		stranded, err = source.node.quotas.ledger.debit(quotaGrant{ID: client.NewRequestID(), Bucket: client.ID(draft.ID()), Epoch: allocation.Epoch, Source: source.node.control.ledger.node, Target: victim, Bytes: 4 << 10}, allowance)
	}
	source.node.opMu.Unlock()
	if err != nil || len(source.node.pendingQuotaGrants()) != 1 {
		t.Fatal("stranded quota fixture", err)
	}
	m := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: body(true, true)}
	if status, reply := bucketHTTPReply(t, source.node, m, admin.Username, admin.Password); status != 200 {
		t.Fatal("explicit exclusion", status, string(reply.Body))
	}
	view := source.node.currentSystem.Load()
	if len(source.node.pendingQuotaGrants()) != 0 || findQuota(view, client.ID(draft.ID())).Epoch == stranded.Epoch {
		t.Fatal("excluded quota grant retained current authority")
	}
	source.node.controller.mu.Lock()
	_, _, checkpointErr := source.node.controller.change(ctx, nil, nil)
	source.node.controller.mu.Unlock()
	if checkpointErr != nil {
		t.Fatal("obsolete quota grant blocked later administration", checkpointErr)
	}
	if len(view.manifest.Membership.Members) != 1 || !hasControlNode(view.manifest.Membership.Excluded, victim) {
		t.Fatal("membership did not exclude the fenced node")
	}
	if data, info, err := c.Get(ctx, left); err != nil || !bytes.Equal(data, leftData) || info.Length != leftInfo.Length {
		t.Fatal("survivor data changed", err)
	}
	if _, _, err := c.Get(ctx, right); !errors.Is(err, client.ErrGone) {
		t.Fatal("lost range served stale bytes or false absence", err)
	}
	if _, err := c.Append(ctx, right, []byte("fragment"), client.AppendOptions{}); !errors.Is(err, client.ErrGone) {
		t.Fatal("partial lost-object repair", err)
	}
	if _, err := c.Put(ctx, right, rightData, client.PutOptions{RequestID: requestID}); err != nil {
		t.Fatal("complete repair", err)
	}
	if data, _, err := c.Get(ctx, right); err != nil || !bytes.Equal(data, rightData) {
		t.Fatal("stale receipt prevented full repair", err)
	}
	if status, _ := bucketHTTPReply(t, source.node, m, admin.Username, admin.Password); status != 200 {
		t.Fatal("force request replay", status)
	}
	source = restartDaemonTestNode(t, source)
	if status, _ := bucketHTTPReply(t, source.node, m, admin.Username, admin.Password); status != 200 {
		t.Fatal("force replay after restart", status)
	}
	// A late historical PREPARE must return its durable READY without sweeping
	// objects restored after ALL release, including after a process restart.
	var proposal controlProposal
	source.node.control.ledger.mu.Lock()
	for _, done := range source.node.control.ledger.completed {
		if p := done.Slot.Proposal; p != nil && p.Claim.Exclusion != nil && p.Claim.Exclusion.Operation == m.ID {
			proposal = *p
		}
	}
	source.node.control.ledger.mu.Unlock()
	if proposal.Claim.Exclusion == nil {
		t.Fatal("exclusion proof missing")
	}
	if _, err := source.node.prepareSystemImage(source.node.control.ledger, proposal); err != nil {
		t.Fatal("historical READY", err)
	}
	if data, _, err := c.Get(ctx, right); err != nil || !bytes.Equal(data, rightData) {
		t.Fatal("late PREPARE destroyed repaired data", err)
	}
}

func TestDaemonExclusionDelegatesAndSurvivesOriginalInitiatorLoss(t *testing.T) {
	checkExclusionWithSurvivingHandoff(t, false)
}
func TestDaemonExclusionRecoversRetainedRAMSource(t *testing.T) {
	checkExclusionWithSurvivingHandoff(t, true)
}
func checkExclusionWithSurvivingHandoff(t *testing.T, restartRAMSource bool) {
	original, b := startAutomaticDaemonPair(t, false)
	original.node.mu.RLock()
	cfg := copyConfig(original.node.cfg)
	original.node.mu.RUnlock()
	cfg.Node.ID = newID()
	cfg.Node.Name = "third-survivor"
	cfg.Node.DataDir = "."
	cfg.Volumes = nil
	cfg.Peers = []PeerConfig{{NodeID: original.node.cfg.Node.ID, URL: original.node.cfg.Node.Advertise}}
	cfg.ControlVersion, cfg.RangeVersion, cfg.QuotaVersion, cfg.PeerDirectoryVersion, cfg.BucketCatalogVersion, cfg.PlacementGroupsVersion = 0, 0, 0, 0, 0, 0
	cfg.JoinPending = true
	cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
	c := startDaemonTestNode(t, filepath.Join(t.TempDir(), "third.yaml"), &cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	for {
		ready := true
		for _, n := range []*daemonTestNode{original, b, c} {
			view := n.node.currentSystem.Load()
			n.node.control.ledger.mu.Lock()
			busy := n.node.control.ledger.slot != nil || n.node.control.ledger.request != nil
			n.node.control.ledger.mu.Unlock()
			ready = ready && view != nil && len(view.manifest.Membership.Members) == 3 && !busy
		}
		if ready {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("three-node admission", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	storageClass := volume.HDD
	add := func(n *daemonTestNode) Operation {
		if !restartRAMSource {
			return addTestVolume(t, n.node, "data", "hdd", 4<<20)
		}
		storageClass = volume.RAM
		op, err := n.node.AddVolume(newID(), AddVolumeRequest{Name: "ram", Source: 2, Class: "ram", SizeBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	add(original)
	v := add(b)
	thirdVolume := add(c)
	draft := newTestBucket(t, "original-initiator-loss")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 1 << 20
	if restartRAMSource {
		draft.description.Policy.StorageStrategy = "ram"
	}
	if _, status, _ := adminBucketMessage(t, original.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	bucket, err := original.node.Bucket(draft.ID())
	if err != nil {
		t.Fatal(err)
	}
	admin := original.node.cfg.Auth.BootstrapAdmin
	objects, err := client.Dial(ctx, b.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	kept, lost := client.ObjectID{1}, client.ObjectID{0x81}
	if _, err := objects.Put(ctx, kept, []byte("kept"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Put(ctx, lost, []byte("lost"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before, _ := original.node.routingPatch(bucket.ID(), volume.ObjectID(kept))
	left, _, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(v.VolumeID)
	plan, err := planRangeMove(before, left, b.node.control.ledger.node, vid, uint8(storageClass), volume.Raw, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := original.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	// A second handoff between the survivors has committed at its source,
	// but its target has not received COMMIT when the unrelated node is lost.
	// The exclusion must finish that exact decision before its checkpoint.
	survivingBefore, _ := b.node.routingPatch(bucket.ID(), volume.ObjectID(kept))
	quarter, _, err := client.SplitRange(survivingBefore.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	thirdID, _ := parseID(thirdVolume.VolumeID)
	survivingPlan, err := planRangeMove(survivingBefore, quarter, c.node.control.ledger.node, thirdID, uint8(storageClass), volume.Raw, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	r := b.node.scopes
	if err := r.reserve(survivingPlan); err != nil {
		t.Fatal(err)
	}
	// Drive the same pre-reservation history transfer as rangeManager.move.
	// The third node need not have received the first handoff's asynchronous
	// announcement yet. Reserving against that stale map must be rejected;
	// this fixture must not rely on gossip winning a timing race.
	if err := r.sendHistory(ctx, survivingPlan.Target, survivingPlan.id()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.call(ctx, survivingPlan.Target, peerRangeReserve, mustRangeWire(survivingPlan)); err != nil {
		t.Fatal(err)
	}
	if err := r.fence(survivingPlan.id()); err != nil {
		t.Fatal(err)
	}
	digest, targetProof, err := r.sendSnapshot(ctx, survivingPlan, true)
	if err != nil {
		t.Fatal(err)
	}
	sourceProof, err := r.ready(survivingPlan.id(), digest, true)
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	used := r.txns[survivingPlan.id()].logicalUsed
	r.mu.Unlock()
	if restartRAMSource {
		// The durable source fence remains but its RAM payload is gone. Only the
		// target's sealed live snapshot can finish this uncommitted offer.
		b = restartDaemonTestNode(t, b)
		for {
			_, reconnectErr := b.node.controller.info(ctx, c.node.control.ledger.node)
			c.node.peerStatsMu.RLock()
			observation := c.node.peerStats[b.node.control.ledger.node]
			connected := observation != nil && observation.online
			c.node.peerStatsMu.RUnlock()
			if reconnectErr == nil && connected {
				break
			}
			if ctx.Err() != nil {
				t.Fatal("RAM source reconnect", ctx.Err())
			}
			time.Sleep(25 * time.Millisecond)
		}
	} else if _, err := r.commit(rangeCommit{ID: survivingPlan.id(), Digest: digest, LogicalUsed: used, Source: sourceProof, Target: targetProof}); err != nil {
		t.Fatal(err)
	}
	victim := original.node.control.ledger.node
	epoch := b.node.currentSystem.Load().manifest.Membership.Epoch
	original.close() // External fencing of this disposable process, before the command.
	caller, coordinator := b, c
	if caller.node.control.ledger.node.String() < coordinator.node.control.ledger.node.String() {
		caller, coordinator = coordinator, caller
	}
	e := client.Encoder{}
	e.V(1)
	e.ID(victim)
	e.Bool(true)
	e.Bool(true)
	e.U64(epoch)
	m := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
	if status, reply := bucketHTTPReply(t, caller.node, m, admin.Username, admin.Password); status != 200 {
		t.Fatal("delegated exclusion", status, string(reply.Body))
	}
	for _, n := range []*daemonTestNode{b, c} {
		view := n.node.currentSystem.Load()
		if len(view.manifest.Membership.Members) != 2 || !hasControlNode(view.manifest.Membership.Excluded, victim) {
			t.Fatal("survivor did not publish exclusion")
		}
	}
	var totalUsed, totalCredits uint64
	for _, n := range []*daemonTestNode{b, c} {
		n.node.opMu.Lock()
		localBucket, _ := n.node.Bucket(bucket.ID())
		used, usageErr := localBucket.logicalUsage()
		credits, creditErr := n.node.quotaAllowance(client.ID(bucket.ID()))
		n.node.opMu.Unlock()
		if usageErr != nil || creditErr != nil || used > credits {
			t.Fatal("recovered quota invariant", used, credits, usageErr, creditErr)
		}
		totalUsed += used
		totalCredits += credits
	}
	if totalUsed != 4 || totalCredits != 1<<20 {
		t.Fatal("sealed bytes missing from recovered quota", totalUsed, totalCredits)
	}
	if data, _, err := objects.Get(ctx, kept); err != nil || string(data) != "kept" {
		t.Fatal("survivor lost its authority", err)
	}
	if _, _, err := objects.Get(ctx, lost); !errors.Is(err, client.ErrGone) {
		t.Fatal("original initiator data not LOST", err)
	}
	if status, _ := bucketHTTPReply(t, caller.node, m, admin.Username, admin.Password); status != 200 {
		t.Fatal("delegated retry after epoch advance", status)
	}
	op, err := caller.node.operation(m.ID.String())
	if err != nil || op.State != "fulfilled" {
		t.Fatal("survivor could not inspect global exclusion outcome", err)
	}
	// The old initiator remains stopped. A survivor's restart uses its system
	// graph/directory and retains membership without that original seed.
	if restartRAMSource {
		caller = b
	} // Keep the sole live RAM target running.
	caller = restartDaemonTestNode(t, caller)
	if status, _ := bucketHTTPReply(t, caller.node, m, admin.Username, admin.Password); status != 200 {
		t.Fatal("witness proof replay after restart", status)
	}
	if data, _, err := objects.Get(ctx, kept); err != nil || string(data) != "kept" {
		t.Fatal("read after survivor restart", err)
	}
}

func TestDaemonExclusionCancelsUncommittedHandoffs(t *testing.T) {
	for _, tc := range []struct {
		name                                           string
		sourceLost, sealed, committed, targetCommitted bool
	}{
		{"target-lost-during-bulk", false, false, false, false},
		{"target-lost-after-ready", false, true, false, false},
		{"source-lost-before-seal", true, false, false, false},
		{"source-lost-after-ready", true, true, false, false},
		{"target-lost-after-source-commit", false, true, true, false},
		{"source-lost-after-target-commit", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			addTestVolume(t, source.node, "data", "hdd", 4<<20)
			dst := addTestVolume(t, target.node, "data", "hdd", 4<<20)
			bucket, _ := createProtocolBucket(t, source.node, "interrupted-force")
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			admin := source.node.cfg.Auth.BootstrapAdmin
			objects, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer objects.Close()
			object := client.ObjectID{0x81}
			data := bytes.Repeat([]byte("payload"), 1000)
			original, err := objects.Put(ctx, object, data, client.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID(object))
			_, moving, err := client.SplitRange(before.Entries[0])
			if err != nil {
				t.Fatal(err)
			}
			vid, _ := parseID(dst.VolumeID)
			p, err := planRangeMove(before, moving, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 128<<10)
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
			if tc.sealed {
				if err := r.fence(p.id()); err != nil {
					t.Fatal(err)
				}
			}
			digest, _, err := r.sendSnapshot(ctx, p, tc.sealed)
			if err != nil {
				t.Fatal(err)
			}
			if tc.sealed {
				sourceProof, err := r.ready(p.id(), digest, true)
				if err != nil {
					t.Fatal(err)
				}
				if tc.committed {
					r.mu.Lock()
					used := r.txns[p.id()].logicalUsed
					r.mu.Unlock()
					target.node.scopes.mu.Lock()
					targetProof := target.node.scopes.txns[p.id()].targetReady
					target.node.scopes.mu.Unlock()
					decision := rangeCommit{ID: p.id(), Digest: digest, LogicalUsed: used, Source: sourceProof, Target: targetProof}
					if _, err := r.commit(decision); err != nil {
						t.Fatal(err)
					}
					if tc.targetCommitted {
						wire, err := decision.marshal()
						if err != nil {
							t.Fatal(err)
						}
						if _, err := r.call(ctx, p.Target, peerRangeCommit, wire); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			survivor, victim := source, target
			if tc.sourceLost {
				survivor, victim = target, source
			}
			victimID := victim.node.control.ledger.node
			epoch := survivor.node.currentSystem.Load().manifest.Membership.Epoch
			victim.close()
			e := client.Encoder{}
			e.V(1)
			e.ID(victimID)
			e.Bool(true)
			e.Bool(true)
			e.U64(epoch)
			m := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
			if status, reply := bucketHTTPReply(t, survivor.node, m, admin.Username, admin.Password); status != 200 {
				t.Fatal("force interrupted handoff", status, string(reply.Body))
			}
			survivor.node.scopes.mu.Lock()
			tx := survivor.node.scopes.txns[p.id()]
			cancelled := tx != nil && (tx.aborted || tx.forceRetired) && !survivor.node.scopes.active(tx)
			survivor.node.scopes.mu.Unlock()
			if !cancelled {
				t.Fatal("old handoff remained active")
			}
			if _, err := survivor.node.scopes.commit(rangeCommit{ID: p.id()}); err == nil {
				t.Fatal("delayed COMMIT revived cancelled handoff")
			}
			survivor = restartDaemonTestNode(t, survivor)
			if err := survivor.node.scopes.reserve(p); err == nil {
				t.Fatal("restart forgot cancelled handoff")
			}
			reader, err := client.Dial(ctx, survivor.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			got, info, err := reader.Get(ctx, object)
			expectLost := tc.sourceLost && !tc.sealed || !tc.sourceLost && tc.committed
			if expectLost {
				if !errors.Is(err, client.ErrGone) {
					t.Fatal("unsealed shadow became authoritative", err)
				}
				if _, err := reader.Put(ctx, object, data, client.PutOptions{}); err != nil {
					t.Fatal(err)
				}
			} else if err != nil || !bytes.Equal(got, data) || info.Length != original.Length {
				t.Fatal("surviving source lost acknowledged data", err)
			}
			if err := survivor.node.scopes.cleanup(ctx, p); !expectLost && !tc.sourceLost && err == nil {
				t.Fatal("delayed cleanup accepted cancelled handoff")
			}
			if !expectLost {
				if got, _, err := reader.Get(ctx, object); err != nil || !bytes.Equal(got, data) {
					t.Fatal("delayed cleanup destroyed source", err)
				}
			}
		})
	}
}

type blockedControlCommit struct {
	controlParticipants
	mu       sync.Mutex
	decision client.Decision
}

func (p *blockedControlCommit) Commit(ctx context.Context, node client.ID, decision client.Decision, certificate client.Certificate) (client.Proof, error) {
	p.mu.Lock()
	if p.decision.Sequence == 0 {
		p.decision = decision
	}
	blocked := p.decision == decision
	p.mu.Unlock()
	if blocked {
		return client.Proof{}, errControlLostReply
	}
	return p.controlParticipants.Commit(ctx, node, decision, certificate)
}

func TestDaemonForceReplacesUncommittedGlobalRound(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	dst := addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "blocked-global")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	objects, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	object := client.ObjectID{1}
	if _, err := objects.Put(ctx, object, []byte("kept"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID(object))
	left, _, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(dst.VolumeID)
	plan, err := planRangeMove(before, left, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	controller := source.node.controller
	controller.mu.Lock()
	controller.coordinator.participants = &blockedControlCommit{controlParticipants: controller.coordinator.participants}
	_, _, buildErr := controller.change(ctx, nil, nil)
	controller.mu.Unlock()
	if !errors.Is(buildErr, errControlLostReply) {
		t.Fatal("global READY fixture", buildErr)
	}
	target.node.control.ledger.mu.Lock()
	old := target.node.control.ledger.slot
	waiting := old != nil && old.Ready.RecordSequence != 0 && old.Committed.RecordSequence == 0
	var oldRound client.RoundID
	if old != nil {
		oldRound = old.Claim.Round
	}
	target.node.control.ledger.mu.Unlock()
	if !waiting {
		t.Fatal("survivor did not retain pending global READY")
	}
	victim := source.node.control.ledger.node
	epoch := target.node.currentSystem.Load().manifest.Membership.Epoch
	source.close()
	e := client.Encoder{}
	e.V(1)
	e.ID(victim)
	e.Bool(true)
	e.Bool(true)
	e.U64(epoch)
	request := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
	if status, reply := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("force stalled global READY", status, string(reply.Body))
	}
	target = restartDaemonTestNode(t, target)
	l := target.node.control.ledger
	l.mu.Lock()
	retired := l.slot == nil && l.closed[oldRound] && l.request == nil
	l.mu.Unlock()
	if !retired {
		t.Fatal("old global authority survived exclusion")
	}
	if data, _, err := objects.Get(ctx, object); err != nil || string(data) != "kept" {
		t.Fatal("survivor data lost", err)
	}
}

type witnessedControlCommit struct {
	controlParticipants
	witness client.ID
}

func (p *witnessedControlCommit) Commit(ctx context.Context, node client.ID, decision client.Decision, certificate client.Certificate) (client.Proof, error) {
	if node != p.witness {
		return client.Proof{}, errControlLostReply
	}
	return p.controlParticipants.Commit(ctx, node, decision, certificate)
}

func TestDaemonForcePreservesCommittedAdminParent(t *testing.T) { checkCommittedAdminParent(t, false) }
func TestDaemonForceHonorsAdminRevocationInCommittedParent(t *testing.T) {
	checkCommittedAdminParent(t, true)
}
func checkCommittedAdminParent(t *testing.T, revoked bool) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "committed-parent")
	user := client.ID{0xc1}
	if revoked {
		create := client.Encoder{}
		create.ID(user)
		create.Text("committed-user")
		create.U8(1)
		create.Bool(false)
		create.Text("test-password")
		if _, status, _ := adminBucketMessage(t, source.node, client.UserCreate, create.B); status != 201 {
			t.Fatal(status)
		}
	}
	controller := source.node.controller
	controller.mu.Lock()
	controller.coordinator.participants = &witnessedControlCommit{controlParticipants: controller.coordinator.participants, witness: target.node.control.ledger.node}
	controller.mu.Unlock()
	e := client.Encoder{}
	e.ID(user)
	e.Text("committed-user")
	e.U8(0)
	e.Bool(false)
	opcode := uint16(client.UserUpdate)
	if !revoked {
		opcode = client.UserCreate
		e.Text("test-password")
	}
	if _, status, _ := adminBucketMessage(t, source.node, opcode, e.B); status < 400 {
		t.Fatal("partial COMMIT fixture unexpectedly fulfilled", status)
	}
	l := target.node.control.ledger
	l.mu.Lock()
	pending := l.slot
	witnessed := pending != nil && pending.Committed.RecordSequence != 0 && pending.Applied.RecordSequence == 0
	var parent client.Decision
	if pending != nil {
		parent = pending.Decision
	}
	l.mu.Unlock()
	if !witnessed {
		t.Fatal("survivor has no durable parent COMMIT")
	}
	before, beforeErr := target.node.findAccount("committed-user")
	if !revoked && beforeErr == nil || revoked && (beforeErr != nil || before.Role != "ADMIN") {
		t.Fatal("unapplied parent already visible", beforeErr)
	}
	victim := source.node.control.ledger.node
	epoch := target.node.currentSystem.Load().manifest.Membership.Epoch
	admin := source.node.cfg.Auth.BootstrapAdmin
	source.close()
	e = client.Encoder{}
	e.V(1)
	e.ID(victim)
	e.Bool(true)
	e.Bool(true)
	e.U64(epoch)
	request := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
	if revoked {
		if status, _ := bucketHTTPReply(t, target.node, request, "committed-user", "test-password"); status != 403 {
			t.Fatal("revoked administrator completed exclusion", status)
		}
		view := target.node.currentSystem.Load()
		if view.manifest.Membership.Epoch != epoch || len(view.manifest.Membership.Members) != 2 {
			t.Fatal("revoked administrator changed membership")
		}
		request.ID = client.NewRequestID()
	}
	if status, reply := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("force committed admin parent", status, string(reply.Body))
	}
	account, err := target.node.findAccount("committed-user")
	if err != nil || account.ID != user.String() || account.Role != "USER" || account.PasswordHash == "" {
		t.Fatal("committed account was rolled back", err)
	}
	l = target.node.control.ledger
	l.mu.Lock()
	var previous [32]byte
	for _, completed := range l.completed {
		if completed.Slot.Proposal != nil && completed.Slot.Proposal.Claim.Exclusion != nil {
			previous = completed.Slot.Decision.Previous
		}
	}
	l.mu.Unlock()
	if previous != parent.Digest {
		t.Fatal("force decision does not extend committed parent")
	}
	target = restartDaemonTestNode(t, target)
	account, err = target.node.findAccount("committed-user")
	if err != nil || account.ID != user.String() {
		t.Fatal("committed account lost on force restart", err)
	}
	if _, err := target.node.Bucket(bucket.ID()); err != nil {
		t.Fatal("parent catalog lost", err)
	}
}

func TestDaemonForceCompletesCommittedJoinAndDelegatesToNewMember(t *testing.T) {
	source, survivor := startAutomaticDaemonPair(t, false)
	addTestVolume(t, survivor.node, "data", "hdd", 4<<20)
	createProtocolBucket(t, source.node, "join-parent")
	source.node.mu.RLock()
	cfg := copyConfig(source.node.cfg)
	source.node.mu.RUnlock()
	var candidateID client.ID
	candidateID[15] = 1 // deterministically below both randomly generated old IDs
	cfg.Node.ID, cfg.Node.Name, cfg.Node.DataDir = candidateID.String(), "join-witness", "."
	cfg.Volumes = nil
	cfg.Peers = []PeerConfig{{NodeID: source.node.cfg.Node.ID, URL: source.node.cfg.Node.Advertise}}
	cfg.ControlVersion, cfg.RangeVersion, cfg.QuotaVersion, cfg.PeerDirectoryVersion, cfg.BucketCatalogVersion, cfg.PlacementGroupsVersion = 0, 0, 0, 0, 0, 0
	cfg.JoinPending = true
	cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
	// Either admitted member may coordinate the discovered candidate's join.
	// Inject the partial COMMIT at both requesters before discovery starts.
	for _, node := range []*daemonTestNode{source, survivor} {
		node.node.controller.mu.Lock()
		node.node.controller.coordinator.participants = &witnessedControlCommit{controlParticipants: node.node.controller.coordinator.participants, witness: candidateID}
		node.node.controller.mu.Unlock()
	}
	candidate := startDaemonTestNode(t, filepath.Join(t.TempDir(), "join-witness.yaml"), &cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	var parent client.Decision
	for {
		candidate.node.control.ledger.mu.Lock()
		slot := candidate.node.control.ledger.slot
		witnessed := slot != nil && slot.Proposal != nil && slot.Proposal.Kind == controlMembership && slot.Committed.RecordSequence != 0 && slot.Applied.RecordSequence == 0
		if witnessed {
			parent = slot.Decision
		}
		candidate.node.control.ledger.mu.Unlock()
		survivor.node.peerStatsMu.RLock()
		observation := survivor.node.peerStats[candidateID]
		connected := observation != nil && observation.online
		survivor.node.peerStatsMu.RUnlock()
		if witnessed && connected {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("bound join COMMIT fixture", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	epoch := survivor.node.currentSystem.Load().manifest.Membership.Epoch
	admin := source.node.cfg.Auth.BootstrapAdmin
	victim := source.node.control.ledger.node
	source.close()
	e := client.Encoder{}
	e.V(1)
	e.ID(victim)
	e.Bool(true)
	e.Bool(true)
	e.U64(epoch)
	request := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
	if status, reply := bucketHTTPReply(t, survivor.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("committed join force recovery", status, string(reply.Body))
	}
	for _, n := range []*daemonTestNode{survivor, candidate} {
		view := n.node.currentSystem.Load()
		if view == nil || len(view.manifest.Membership.Members) != 2 || view.manifest.Membership.Epoch != epoch+2 || !hasControlNode(view.manifest.Membership.Excluded, victim) || !hasControlNode(membershipIDs(view.manifest.Membership), candidateID) || n.node.joinPending() {
			t.Fatal("committed joining member was discarded")
		}
		completion := n.node.completedExclusionProposal(request.ID)
		if completion == nil || completion.Claim.Round.Initiator != candidateID || exclusionObservedEpoch(completion.Claim) != epoch || completion.Claim.Base.Membership.Epoch != epoch+1 {
			t.Fatal("original force intent was not retained across join/delegation")
		}
		if completion.Claim.Base.Head != parent.Digest {
			t.Fatal("force rolled back the join COMMIT")
		}
		if status, _ := bucketHTTPReply(t, n.node, request, admin.Username, admin.Password); status != 200 {
			t.Fatal("original-epoch retry rejected", status)
		}
	}
	candidate = restartDaemonTestNode(t, candidate)
	if status, _ := bucketHTTPReply(t, candidate.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("original intent lost on restart", status)
	}
}

func TestDaemonForceCompletesCommittedDepartureWithoutLosingData(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	dst := addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "departure-parent")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	objects, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	object := client.ObjectID{1}
	written, err := objects.Put(ctx, object, []byte("kept"), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID(object))
	vid, _ := parseID(dst.VolumeID)
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	victim := source.node.control.ledger.node
	next := source.node.currentSystem.Load().manifest.Membership
	epoch := next.Epoch
	next.Epoch++
	next.Members = []client.Member{}
	for _, member := range source.node.currentSystem.Load().manifest.Membership.Members {
		if member.ID != victim {
			next.Members = append(next.Members, member)
		}
	}
	controller := source.node.controller
	controller.mu.Lock()
	controller.coordinator.participants = &witnessedControlCommit{controlParticipants: controller.coordinator.participants, witness: target.node.control.ledger.node}
	_, _, buildErr := controller.change(ctx, &next, nil)
	controller.mu.Unlock()
	if !errors.Is(buildErr, errControlLostReply) {
		t.Fatal("departure COMMIT fixture", buildErr)
	}
	source.close()
	e := client.Encoder{}
	e.V(1)
	e.ID(victim)
	e.Bool(true)
	e.Bool(true)
	e.U64(epoch)
	request := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
	if status, reply := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("force committed departure", status, string(reply.Body))
	}
	view := target.node.currentSystem.Load()
	if view.manifest.Membership.Epoch != epoch+2 || len(view.manifest.Membership.Members) != 1 || !hasControlNode(view.manifest.Membership.Excluded, victim) {
		t.Fatal("departure was rolled back")
	}
	proof := target.node.completedExclusionProposal(request.ID)
	if proof == nil || len(proof.Claim.Exclusion.AlreadyRetired) != 1 || proof.Claim.Exclusion.AlreadyRetired[0] != victim {
		t.Fatal("historical retirement proof missing")
	}
	target = restartDaemonTestNode(t, target)
	if data, info, err := objects.Get(ctx, object); err != nil || string(data) != "kept" || info.Length != written.Length {
		t.Fatal("recovered departure lost authoritative data", err)
	}
	if status, _ := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("old-epoch departure retry rejected", status)
	}
}

type blockedControlApply struct {
	controlParticipants
	mu       sync.Mutex
	decision client.Decision
}

func (p *blockedControlApply) Apply(ctx context.Context, node client.ID, decision client.Decision) (client.Proof, error) {
	p.mu.Lock()
	if p.decision.Sequence == 0 {
		p.decision = decision
	}
	blocked := p.decision == decision
	p.mu.Unlock()
	if blocked {
		return client.Proof{}, errControlLostReply
	}
	return p.controlParticipants.Apply(ctx, node, decision)
}

func TestDaemonForcePreservesEarlierForceCommit(t *testing.T) {
	for _, coordinatorLost := range []bool{true, false} {
		name := "coordinator-survives"
		if coordinatorLost {
			name = "coordinator-lost"
		}
		t.Run(name, func(t *testing.T) {
			original, b := startAutomaticDaemonPair(t, false)
			addTestVolume(t, original.node, "data", "hdd", 4<<20)
			dst := addTestVolume(t, b.node, "data", "hdd", 4<<20)
			original.node.mu.RLock()
			cfg := copyConfig(original.node.cfg)
			original.node.mu.RUnlock()
			var thirdID client.ID
			if coordinatorLost {
				thirdID[15] = 1
			} else {
				for i := range thirdID {
					thirdID[i] = 255
				}
			}
			cfg.Node.ID, cfg.Node.Name, cfg.Node.DataDir = thirdID.String(), "force-parent-witness", "."
			cfg.Volumes = nil
			cfg.Peers = []PeerConfig{{NodeID: original.node.cfg.Node.ID, URL: original.node.cfg.Node.Advertise}}
			cfg.ControlVersion, cfg.RangeVersion, cfg.QuotaVersion, cfg.PeerDirectoryVersion, cfg.BucketCatalogVersion, cfg.PlacementGroupsVersion = 0, 0, 0, 0, 0, 0
			cfg.JoinPending = true
			cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
			third := startDaemonTestNode(t, filepath.Join(t.TempDir(), "force-witness.yaml"), &cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
			defer cancel()
			for {
				ready := true
				for _, n := range []*daemonTestNode{original, b, third} {
					view := n.node.currentSystem.Load()
					n.node.control.ledger.mu.Lock()
					busy := n.node.control.ledger.slot != nil || n.node.control.ledger.request != nil
					n.node.control.ledger.mu.Unlock()
					ready = ready && view != nil && len(view.manifest.Membership.Members) == 3 && !busy
				}
				if ready {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(25 * time.Millisecond)
			}
			bucket, _ := createProtocolBucket(t, original.node, "force-parent-data")
			admin := original.node.cfg.Auth.BootstrapAdmin
			objects, err := client.Dial(ctx, b.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer objects.Close()
			object := client.ObjectID{1}
			written, err := objects.Put(ctx, object, []byte("kept"), client.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			before, _ := original.node.routingPatch(bucket.ID(), volume.ObjectID(object))
			left, _, err := client.SplitRange(before.Entries[0])
			if err != nil {
				t.Fatal(err)
			}
			vid, _ := parseID(dst.VolumeID)
			plan, err := planRangeMove(before, left, b.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 128<<10)
			if err != nil {
				t.Fatal(err)
			}
			if err := original.node.scopes.move(ctx, plan); err != nil {
				t.Fatal(err)
			}
			coordinator := b
			if coordinatorLost {
				coordinator = third
			}
			coordinator.node.controller.mu.Lock()
			coordinator.node.controller.coordinator.participants = &blockedControlApply{controlParticipants: coordinator.node.controller.coordinator.participants}
			coordinator.node.controller.mu.Unlock()
			victim := original.node.control.ledger.node
			epoch := b.node.currentSystem.Load().manifest.Membership.Epoch
			original.close()
			makeRequest := func(victim client.ID) client.Message {
				e := client.Encoder{}
				e.V(1)
				e.ID(victim)
				e.Bool(true)
				e.Bool(true)
				e.U64(epoch)
				return client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
			}
			first := makeRequest(victim)
			if status, _ := bucketHTTPReply(t, coordinator.node, first, admin.Username, admin.Password); status != 202 {
				t.Fatal("first force did not remain pending", status)
			}
			b.node.control.ledger.mu.Lock()
			slot := b.node.control.ledger.slot
			pending := slot != nil && slot.Committed.RecordSequence != 0 && slot.Applied.RecordSequence == 0
			var parent client.Decision
			if slot != nil {
				parent = slot.Decision
			}
			b.node.control.ledger.mu.Unlock()
			if !pending {
				t.Fatal("first force COMMIT fixture missing")
			}
			third.close()
			second := makeRequest(thirdID)
			if status, reply := bucketHTTPReply(t, b.node, second, admin.Username, admin.Password); status != 200 {
				t.Fatal("second force failed", status, string(reply.Body))
			}
			view := b.node.currentSystem.Load()
			if view.manifest.Membership.Epoch != epoch+2 || len(view.manifest.Membership.Members) != 1 || !hasControlNode(view.manifest.Membership.Excluded, victim) || !hasControlNode(view.manifest.Membership.Excluded, thirdID) {
				t.Fatal("earlier exclusion was lost")
			}
			proof := b.node.completedExclusionProposal(second.ID)
			if proof == nil || proof.Claim.Base.Head != parent.Digest || b.node.completedExclusionProposal(first.ID) == nil {
				t.Fatal("earlier force COMMIT was rolled back")
			}
			if status, _ := bucketHTTPReply(t, b.node, first, admin.Username, admin.Password); status != 200 {
				t.Fatal("ancestor operation replay failed", status)
			}
			b = restartDaemonTestNode(t, b)
			if data, info, err := objects.Get(ctx, object); err != nil || string(data) != "kept" || info.Length != written.Length {
				t.Fatal("survivor data lost", err)
			}
			for _, request := range []client.Message{first, second} {
				if status, _ := bucketHTTPReply(t, b.node, request, admin.Username, admin.Password); status != 200 {
					t.Fatal("force lineage lost on restart", status)
				}
			}
		})
	}
}

type gatedControlRelease struct {
	controlParticipants
	blocked atomic.Bool
}

func (p *gatedControlRelease) Release(ctx context.Context, node client.ID, decision client.Decision, certificate client.Certificate) (client.Proof, error) {
	if p.blocked.Load() {
		return client.Proof{}, errControlLostReply
	}
	return p.controlParticipants.Release(ctx, node, decision, certificate)
}

func TestDaemonForceRetryAfterApplyResumesOriginalIntent(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	controller := target.node.controller
	controller.mu.Lock()
	gate := &gatedControlRelease{controlParticipants: controller.coordinator.participants}
	gate.blocked.Store(true)
	controller.coordinator.participants = gate
	controller.mu.Unlock()
	victim := source.node.control.ledger.node
	epoch := target.node.currentSystem.Load().manifest.Membership.Epoch
	admin := source.node.cfg.Auth.BootstrapAdmin
	source.close()
	e := client.Encoder{}
	e.V(1)
	e.ID(victim)
	e.Bool(true)
	e.Bool(true)
	e.U64(epoch)
	request := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
	for retry := 0; retry < 2; retry++ {
		if status, reply := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 202 {
			t.Fatal("pending force retry", retry, status, string(reply.Body))
		}
		l := target.node.control.ledger
		l.mu.Lock()
		pending := l.slot != nil && l.slot.Applied.RecordSequence != 0 && l.request != nil && l.request.Claim.Exclusion.Operation == request.ID
		l.mu.Unlock()
		if !pending || target.node.currentSystem.Load().manifest.Membership.Epoch != epoch+1 {
			t.Fatal("retry started another exclusion")
		}
	}
	gate.blocked.Store(false)
	if status, reply := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("resumed force", status, string(reply.Body))
	}
	target = restartDaemonTestNode(t, target)
	if status, _ := bucketHTTPReply(t, target.node, request, admin.Username, admin.Password); status != 200 {
		t.Fatal("completed retry after restart", status)
	}
}
