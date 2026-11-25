// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"path/filepath"
	"testing"
	"time"
)

func TestClusterBlockMaintenanceRoutesAndFences(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "blocks", "hdd", 16<<20)
	addTestVolume(t, target.node, "blocks", "hdd", 16<<20)
	bucket, _ := createProtocolBucket(t, source.node, "cluster-blocks")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	dial := func(url string) *client.Client {
		t.Helper()
		c, e := client.Dial(ctx, url, admin.Username, admin.Password, client.ID(bucket.ID()))
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	c := dial(source.node.cfg.Node.Advertise)
	for _, id := range []client.ObjectID{{1}, {0x81}} {
		if _, e := c.Put(ctx, id, []byte("old"), client.PutOptions{}); e != nil {
			t.Fatal(e)
		}
	}
	c.Close()
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID{0x81})
	_, moving, e := client.SplitRange(before.Entries[0])
	if e != nil {
		t.Fatal(e)
	}
	var targetVolume volume.ID
	for _, v := range target.node.volumes {
		if !v.config.System {
			targetVolume = v.store.Identity().VolumeID
		}
	}
	plan, e := planRangeMove(before, moving, target.node.control.ledger.node, targetVolume, uint8(volume.HDD), volume.Raw, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	if e = source.node.scopes.move(ctx, plan); e != nil {
		t.Fatal(e)
	}
	waitSockets := func() {
		t.Helper()
		for _, s := range []*Server{source.node, target.node} {
			deadline := time.Now().Add(5 * time.Second)
			for {
				n := 0
				s.wsMu.Lock()
				for ws := range s.sockets {
					if ws.authorization != nil && ws.authorization.bucket != nil {
						n++
					}
				}
				s.wsMu.Unlock()
				if n == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("bucket sockets retained")
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	waitSockets()
	c = dial(target.node.cfg.Node.Advertise)
	defer c.Close()
	if e = c.BeginBlockMaintenance(ctx, false); e != nil {
		t.Fatal("cluster begin", e)
	}
	for _, s := range []*Server{source.node, target.node} {
		if other, e := client.Dial(ctx, s.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(bucket.ID())); e == nil {
			other.Close()
			t.Fatal("another client entered cluster lease")
		}
	}
	payload := bytes.Repeat([]byte{0x5a}, 512)
	for _, id := range []client.ObjectID{{1}, {0x81}} {
		if _, e = c.PutRange(ctx, id, 512, payload, client.PutOptions{Accepted: true, RequestID: client.NewRequestID()}); e != nil {
			t.Fatal("ranged write", id, e)
		}
		got, _, e := c.GetRange(ctx, id, 512, 512)
		if e != nil || !bytes.Equal(got, payload) {
			t.Fatal("ranged read", id, e)
		}
	}
	if e = c.Sync(ctx); e != nil {
		t.Fatal("multi-owner flush", e)
	}
	page, _, e := c.ScanObjects(ctx, nil, 100)
	if e != nil || len(page) != 2 {
		t.Fatal("cluster inventory", len(page), e)
	}
	c.Close()
	waitSockets()
	c = dial(source.node.cfg.Node.Advertise)
	defer c.Close()
	if e = c.BeginBlockMaintenance(ctx, true); e != nil {
		t.Fatal("readonly begin", e)
	}
	if _, e = c.PutRange(ctx, client.ObjectID{0x81}, 0, payload, client.PutOptions{RequestID: client.NewRequestID()}); e == nil {
		t.Fatal("readonly remote write succeeded")
	}
	c.Close()
	waitSockets()
	for _, s := range []*Server{source.node, target.node} {
		b, _ := s.Bucket(bucket.ID())
		if b.underMaintenance() {
			t.Fatal("lease retained after close")
		}
		record := s.leaseRecord(b)
		if record == nil || !record.Released {
			t.Fatal("durable release missing")
		}
		if e = s.loadBucketLeases(); e != nil {
			t.Fatal(e)
		}
		if b.underMaintenance() {
			t.Fatal("restart loading resurrected released lease")
		}
	}
	clone, _ := createProtocolBucket(t, source.node, "cluster-copy")
	account, err := target.node.findAccount(admin.Username)
	if err != nil {
		t.Fatal(err)
	}
	actor := principal{User: account}
	enc := client.Encoder{}
	enc.ID(client.ID(bucket.ID()))
	enc.ID(client.ID(clone.ID()))
	copyMessage := client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: enc.B}
	if _, e := target.node.executeBucketCopy(ctx, copyMessage, actor); e != nil {
		t.Fatalf("cluster clone status %d: %v", statusOf(e), e)
	}
	cloneClient, e := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(clone.ID()))
	if e != nil {
		t.Fatal(e)
	}
	got, _, e := cloneClient.GetRange(ctx, client.ObjectID{0x81}, 512, 512)
	if e != nil || !bytes.Equal(got, payload) {
		t.Fatal("cloned remote bytes", e)
	}
	cloneClient.Close()
	waitSockets()
	enc = client.Encoder{}
	enc.ID(client.ID(clone.ID()))
	clearMessage := client.Message{Kind: client.Request, Opcode: client.BucketClear, ID: client.NewRequestID(), Body: enc.B}
	if _, e := target.node.executeBucketCopy(ctx, clearMessage, actor); e != nil {
		t.Fatal("cluster clear", e)
	}
	cloneClient, e = client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(clone.ID()))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = cloneClient.Put(ctx, client.ObjectID{7}, []byte("later"), client.PutOptions{RequestID: client.NewRequestID()}); e != nil {
		t.Fatal(e)
	}
	cloneClient.Close()
	waitSockets()
	if _, e := source.node.executeBucketCopy(ctx, clearMessage, userPrincipal(t, source.node)); e != nil {
		t.Fatal("clear replay from other node", e)
	}
	cloneClient, e = client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(clone.ID()))
	if e != nil {
		t.Fatal(e)
	}
	defer cloneClient.Close()
	if got, _, e := cloneClient.Get(ctx, client.ObjectID{7}); e != nil || string(got) != "later" {
		t.Fatal("replayed clear removed later writes", e)
	}

}

func TestClusterMaintenanceRetainsFenceAndReleasesOrphan(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "lease-data", "hdd", 8<<20)
	addTestVolume(t, target.node, "lease-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "lease-recovery")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	a := source.node.cfg.Auth.BootstrapAdmin
	c, e := client.Dial(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Put(ctx, client.ObjectID{0x81}, []byte("persistent"), client.PutOptions{RequestID: client.NewRequestID()}); e != nil {
		t.Fatal(e)
	}
	c.Close()
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID{0x81})
	_, moving, e := client.SplitRange(before.Entries[0])
	if e != nil {
		t.Fatal(e)
	}
	var volumeID volume.ID
	for _, v := range target.node.volumes {
		if !v.config.System {
			volumeID = v.store.Identity().VolumeID
		}
	}
	plan, e := planRangeMove(before, moving, target.node.control.ledger.node, volumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	if e = source.node.scopes.move(ctx, plan); e != nil {
		t.Fatal(e)
	}
	c, e = client.Dial(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if e = c.BeginBlockMaintenance(ctx, false); e != nil {
		t.Fatal(e)
	}
	if data, _, e := c.Get(ctx, client.ObjectID{0x81}); e != nil || string(data) != "persistent" {
		t.Fatal("remote lease read", e)
	}
	old := source.node.leaseRecord(b)
	if old == nil {
		t.Fatal("lease absent")
	}
	// A participant reboot must restore its durable fence, not silently unlock.
	target = restartDaemonTestNode(t, target)
	remote, _ := target.node.Bucket(b.ID())
	if !remote.underMaintenance() {
		t.Fatal("participant restart forgot active lease")
	}
	if data, _, e := c.Get(ctx, client.ObjectID{0x81}); e != nil || string(data) != "persistent" {
		t.Fatal("auxiliary socket did not reauthenticate after passive restart", e)
	}
	if other, e := client.Dial(ctx, target.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID())); e == nil {
		other.Close()
		t.Fatal("participant restart admitted another client")
	}
	// Abruptly lose the original socket, without sending MAINTENANCE_END.
	source.node.wsMu.Lock()
	for ws := range source.node.sockets {
		if ws.authorization != nil && ws.authorization.leaseOwner.Load() {
			claim := ws.authorization.lease.Load()
			if claim != nil && claim.Token == old.Token {
				ws.close()
			}
		}
	}
	source.node.wsMu.Unlock()
	deadline := time.Now().Add(12 * time.Second)
	for {
		if !b.underMaintenance() && !remote.underMaintenance() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("orphan lease was not released")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, _, e := c.Get(ctx, client.ObjectID{0x81}); !errors.Is(e, client.ErrClosed) {
		t.Fatal("lost owner socket silently reconnected", e)
	}
	c.Close()
	// Released high-water marks reject delayed participant prepares.
	ids, _, _ := source.node.leaseMembers()
	raw, _ := json.Marshal(leaseRequest{Action: 1, Lease: *old})
	for _, s := range []*Server{source.node, target.node} {
		if _, e = s.receiveBucketLease(ctx, raw, ids[0]); e == nil {
			t.Fatal("delayed reserve resurrected released lease")
		}
	}
	fresh, e := client.Dial(ctx, target.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()))
	if e != nil {
		t.Fatal(e)
	}
	defer fresh.Close()
	if e = fresh.BeginBlockMaintenance(ctx, false); e != nil {
		t.Fatal("reacquire", e)
	}
	current := target.node.leaseRecord(remote)
	if current.Token == old.Token {
		t.Fatal("grant reused bearer token")
	}
	raw, _ = json.Marshal(leaseRequest{Action: 3, Lease: *old})
	for _, s := range []*Server{source.node, target.node} {
		if _, e = s.receiveBucketLease(ctx, raw, ids[0]); e != nil {
			t.Fatal(e)
		}
		bucket, _ := s.Bucket(b.ID())
		if !bucket.underMaintenance() {
			t.Fatal("delayed release cleared newer lease")
		}
	}
}

func TestDaemonBucketCopyReceiptSurvivesRestart(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "single.yaml"), nil, false)
	addTestVolume(t, node.node, "copy-data", "hdd", 8<<20)
	src, _ := createProtocolBucket(t, node.node, "source")
	dst, _ := createProtocolBucket(t, node.node, "destination")
	a := node.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	put := func(bucket volume.ID, data string) {
		t.Helper()
		c, e := client.Dial(ctx, node.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(bucket))
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		if _, e = c.Put(ctx, client.ObjectID{1}, []byte(data), client.PutOptions{RequestID: client.NewRequestID()}); e != nil {
			t.Fatal(e)
		}
	}
	put(src.ID(), "before")
	enc := client.Encoder{}
	enc.ID(client.ID(src.ID()))
	enc.ID(client.ID(dst.ID()))
	clone := client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: enc.B}
	if _, e := node.node.executeBucketCopy(ctx, clone, userPrincipal(t, node.node)); e != nil {
		t.Fatal("clone", e)
	}
	enc = client.Encoder{}
	enc.ID(client.ID(dst.ID()))
	clear := client.Message{Kind: client.Request, Opcode: client.BucketClear, ID: client.NewRequestID(), Body: enc.B}
	if _, e := node.node.executeBucketCopy(ctx, clear, userPrincipal(t, node.node)); e != nil {
		t.Fatal("clear", e)
	}
	put(dst.ID(), "after")
	node = restartDaemonTestNode(t, node)
	if _, e := node.node.executeBucketCopy(ctx, clear, userPrincipal(t, node.node)); e != nil {
		t.Fatal("replay", e)
	}
	c, e := client.Dial(ctx, node.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(dst.ID()))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if data, _, e := c.Get(ctx, client.ObjectID{1}); e != nil || string(data) != "after" {
		t.Fatal("clear replay after restart removed new bytes", e)
	}
}

// Installer preflight must work before its first image bucket is created.
func TestClusterMaintenanceCapabilityProbeBeforeBucketCreation(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	// This public probe fans out to both members, including the joined node
	// whose bootstrap credentials are intentionally absent from its config.
	_, status, reply := adminBucketMessage(t, source.node, client.BucketMaintenanceStatus, nil)
	if status != 200 {
		t.Fatalf("maintenance capability before bucket creation: %d %v", status, reply)
	}
	s := source.node
	l := bucketLease{Epoch: s.currentSystem.Load().manifest.Membership.Epoch, Token: client.NewRequestID()}
	wire, err := json.Marshal(leaseRequest{Action: 4, Lease: l})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.receiveBucketLease(context.Background(), wire, client.ID{0xff}); err == nil {
		t.Fatal("capability probe accepted a nonmember")
	}
	l.Epoch++
	wire, _ = json.Marshal(leaseRequest{Action: 4, Lease: l})
	if _, err = s.receiveBucketLease(context.Background(), wire, target.node.control.ledger.node); err == nil {
		t.Fatal("capability probe accepted a stale membership binding")
	}
}
