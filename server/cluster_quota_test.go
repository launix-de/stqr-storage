// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDaemonFiniteQuotaAcrossOwnersAndRestart(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	v := addTestVolume(t, target.node, "data", "hdd", 8<<20)
	draft := newTestBucket(t, "distributed-quota")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 1 << 20
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal("create", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	before, _ := source.node.routingPatch(draft.ID(), volume.ObjectID{})
	_, right, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(v.VolumeID)
	plan, err := planRangeMove(before, right, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	// Concurrent writers on both owners must share one hard quota. Neither
	// owner may treat the complete bucket limit as its personal allowance.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successful []client.ObjectID
	var failures []error
	parallel := make(chan struct{}, 4)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			parallel <- struct{}{}
			defer func() { <-parallel }()
			id := client.ObjectID{byte((i%2)*128 + 1), byte(i)}
			_, err := c.Put(ctx, id, bytes.Repeat([]byte{byte(i)}, 64<<10), client.PutOptions{})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successful = append(successful, id)
			} else if !errors.Is(err, client.ErrFull) {
				failures = append(failures, err)
			}
		}(i)
	}
	wg.Wait()
	if len(failures) != 0 || len(successful) != 16 {
		t.Fatalf("success=%d unexpected errors=%v", len(successful), failures)
	}
	check := func() {
		t.Helper()
		var used, credits uint64
		for _, n := range []*daemonTestNode{source, target} {
			n.node.opMu.Lock()
			b, _ := n.node.Bucket(draft.ID())
			u, e := b.logicalUsage()
			a, ae := n.node.quotaAllowance(client.ID(draft.ID()))
			n.node.opMu.Unlock()
			if e != nil || ae != nil || u > a {
				t.Fatalf("quota invariant used=%d allowance=%d errors=%v/%v", u, a, e, ae)
			}
			used += u
			credits += a
		}
		if used != 1<<20 || credits != 1<<20 {
			t.Fatal("global quota", used, credits)
		}
	}
	check()
	// A completed global checkpoint folds both transfer journals without
	// changing their total. Reopening then must not replay credits twice.
	deadline := time.Now().Add(8 * time.Second)
	for {
		source.node.controller.mu.Lock()
		_, _, err = source.node.controller.change(ctx, nil, nil)
		source.node.controller.mu.Unlock()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("quota checkpoint", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	target = restartDaemonTestNode(t, target)
	source = restartDaemonTestNode(t, source)
	check()
	// Deleting on either owner makes that capacity lendable again. A new
	// object on the opposite half exercises reuse through a different owner.
	removed := successful[0]
	if err := c.Delete(ctx, removed, client.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement := client.ObjectID{removed[0] ^ 128, 0xff}
	if _, err := c.Put(ctx, replacement, bytes.Repeat([]byte{9}, 64<<10), client.PutOptions{}); err != nil {
		t.Fatal("reused remote credits", err)
	}
	check()
	// A live owner spends its existing credits without asking an unrelated
	// SHORT-stopped peer. No quota lease or membership timeout is involved.
	var localID client.ObjectID
	for _, id := range successful {
		if id[0] < 128 && id != removed {
			localID = id
			break
		}
	}
	if localID == (client.ObjectID{}) {
		t.Fatal("no surviving source object")
	}
	if err := c.Delete(ctx, localID, client.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	target.close()
	localContext, stopLocal := context.WithTimeout(ctx, 2*time.Second)
	_, err = c.Put(localContext, client.ObjectID{2, 0xfe}, bytes.Repeat([]byte{7}, 64<<10), client.PutOptions{})
	stopLocal()
	if err != nil {
		t.Fatal("existing quota credits depended on an offline peer", err)
	}
	target = restartDaemonTestNode(t, target)
	check()
}

func TestDaemonQuotaMovesLiveBytesAndChangesLimits(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	v := addTestVolume(t, target.node, "data", "hdd", 8<<20)
	draft := newTestBucket(t, "quota-handoff")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 2 << 20
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal("create", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, id := range []client.ObjectID{{1}, {0x81}} {
		if _, err := c.Put(ctx, id, bytes.Repeat([]byte{4}, 512<<10), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := source.node.routingPatch(draft.ID(), volume.ObjectID{0x81})
	_, right, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(v.VolumeID)
	plan, err := planRangeMove(before, right, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*daemonTestNode{source, target} {
		n.node.opMu.Lock()
		b, _ := n.node.Bucket(draft.ID())
		used, e := b.logicalUsage()
		limit, le := n.node.quotaAllowance(client.ID(draft.ID()))
		n.node.opMu.Unlock()
		if e != nil || le != nil || used != 512<<10 || limit < used {
			t.Fatalf("handoff quota: used=%d cap=%d errors=%v/%v", used, limit, e, le)
		}
	}
	setLimit := func(limit uint64, want int) {
		t.Helper()
		b, _ := source.node.Bucket(draft.ID())
		e := client.Encoder{}
		e.ID(client.ID(draft.ID()))
		e.U8(0)
		e.U64(0)
		e.Bool(false)
		e.U64(limit)
		e.U32(0)
		e.U64(b.Describe().Policy.Version)
		if _, status, _ := adminBucketMessage(t, source.node, client.BucketSetQuota, e.B); status != want {
			t.Fatal("quota change", limit, status, "want", want)
		}
	}
	// Shrinking checks both owners under the durable bucket fence. Raising
	// and subsequently restoring a limit creates a new, non-ABA quota epoch.
	original := findQuota(source.node.currentSystem.Load(), client.ID(draft.ID())).Epoch
	setLimit(900<<10, 507)
	setLimit(3<<20, 200)
	setLimit(2<<20, 200)
	current := findQuota(source.node.currentSystem.Load(), client.ID(draft.ID()))
	if current.Epoch == original {
		t.Fatal("quota limit ABA reused an old grant epoch")
	}
	if _, err := c.Put(ctx, client.ObjectID{0x82}, bytes.Repeat([]byte{5}, 1<<20), client.PutOptions{}); err != nil {
		t.Fatal("new headroom was unavailable", err)
	}
	if _, err := c.Append(ctx, client.ObjectID{0x82}, []byte{1}, client.AppendOptions{}); !errors.Is(err, client.ErrFull) {
		t.Fatal("quota overflow after migration", err)
	}
}

func TestDaemonQuotaNewOwnerReceivesReusableBlockBudget(t *testing.T) {
	testReusableQuotaBudget(t, 16<<20, 1<<20)
}
func TestDaemonLargeQuotaOwnerReceivesReusableBurstBudget(t *testing.T) {
	testReusableQuotaBudget(t, 4<<30, 64<<20)
}
func testReusableQuotaBudget(t *testing.T, limit, minimum uint64) {
	t.Helper()
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "credit-source", "hdd", 16<<20)
	v := addTestVolume(t, target.node, "credit-target", "hdd", 16<<20)
	draft := newTestBucket(t, "reusable-block-credits")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = limit
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	before, _ := source.node.routingPatch(draft.ID(), volume.ObjectID{})
	_, right, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(v.VolumeID)
	plan, err := planRangeMove(before, right, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	// Model the post-handoff case: only transferred live bytes were credited,
	// and this empty new range has no unused local allowance yet.
	target.node.opMu.Lock()
	initial, err := target.node.quotaAllowance(client.ID(draft.ID()))
	allocation := findQuota(target.node.currentSystem.Load(), client.ID(draft.ID()))
	if err != nil {
		target.node.opMu.Unlock()
		t.Fatal(err)
	}
	if initial != 0 {
		grant, err := target.node.quotas.ledger.debit(quotaGrant{ID: client.NewRequestID(), Bucket: client.ID(draft.ID()), Epoch: allocation.Epoch, Source: target.node.control.ledger.node, Target: source.node.control.ledger.node, Bytes: initial}, initial)
		target.node.opMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		wire, err := encodeQuotaGrant(grant)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := source.node.quotaPeerMessage(client.Message{Kind: client.Request, Opcode: peerQuotaGrant, Body: wire}, grant.Source)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := client.DecodeProof(ack)
		if err != nil {
			t.Fatal(err)
		}
		if err = target.node.quotas.ledger.settle(grant.ID, proof, grant.Target); err != nil {
			t.Fatal(err)
		}
	} else {
		target.node.opMu.Unlock()
	}
	target.node.fulfillQuotaDemand(quotaDemand{bucket: draft.ID(), object: volume.ObjectID{0x81}, growth: 210})
	target.node.opMu.Lock()
	allowance, err := target.node.quotaAllowance(client.ID(draft.ID()))
	target.node.opMu.Unlock()
	if err != nil || allowance < minimum || allowance > draft.description.Policy.Limits.Absolute {
		t.Fatal("new owner lacks reusable block credits", allowance, err)
	}
	source.node.opMu.Lock()
	remaining, err := source.node.quotaAllowance(client.ID(draft.ID()))
	source.node.opMu.Unlock()
	if err != nil || remaining+allowance != draft.description.Policy.Limits.Absolute {
		t.Fatal("grant changed total quota", remaining, allowance, err)
	}
}
