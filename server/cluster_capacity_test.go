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

func TestClusterQuotaDisplayUsesAllMembersWithoutBlockingLocalWrites(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	targetVolume := addTestVolume(t, target.node, "data", "hdd", 8<<20)
	draft := newTestBucket(t, "remote-quota-display")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 20 << 20
	_, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 201 {
		t.Fatal(status)
	}
	bucket, _ := source.node.Bucket(draft.ID())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{8}
	if _, err := c.Put(ctx, oid, bytes.Repeat([]byte{3}, 1<<20), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID(oid))
	vid, _ := parseID(targetVolume.VolumeID)
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	other, _ := createProtocolBucket(t, source.node, "independent-local-write")
	localClient, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(other.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer localClient.Close()
	target.node.publishStats()
	for {
		ready := false
		source.node.peerStatsMu.RLock()
		if observation := source.node.peerStats[target.node.control.ledger.node]; observation != nil && observation.snapshot != nil && observation.snapshot.NodeState == 0 {
			for _, v := range observation.snapshot.Volumes {
				if v.ID == client.ID(vid) && v.PayloadPhysical >= 1<<20 && v.InFlightReservedPhysical == 0 {
					ready = true
				}
			}
		}
		source.node.peerStatsMu.RUnlock()
		if ready {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("fresh remote volume statistics missing", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	// Discovery/stale rows outside committed membership do not add capacity.
	source.node.peerStatsMu.Lock()
	source.node.peerStats[client.ID{0xfa}] = &peerObservation{snapshot: &client.StatsSnapshot{Node: client.ID{0xfa}, Volumes: []client.VolumeStats{{ID: client.ID{0xfb}, State: 1, CapacityPhysical: 1 << 30, AdmissionFreePhysical: 1 << 30}}}}
	source.node.peerStatsMu.Unlock()

	handle, err := target.node.volumes[vid.String()].store.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	e := client.Encoder{}
	e.ID(client.ID(bucket.ID()))
	e.U8(0)
	e.U8(5)
	e.U32(0)
	query := client.Message{Kind: client.Request, Opcode: client.BucketQuotaCapacity, ID: client.NewRequestID(), Body: e.B}
	type result struct {
		body   []byte
		status int
	}
	done := make(chan result, 1)
	actor := userPrincipal(t, source.node)
	go func() {
		body, status := source.node.dispatchAs(query, nil, actor)
		done <- result{body, status}
	}()
	for {
		pending := false
		source.node.peerStatsMu.RLock()
		peer := source.node.peerStats[target.node.control.ledger.node].connection
		source.node.peerStatsMu.RUnlock()
		peer.mu.Lock()
		for _, request := range peer.pending {
			pending = pending || request.opcode == peerBucketUsage
		}
		peer.mu.Unlock()
		if pending {
			break
		}
		select {
		case result := <-done:
			t.Fatal("quota query did not wait for its owner", result.status)
		default:
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(5 * time.Millisecond)
	}
	writeContext, stopWrite := context.WithTimeout(ctx, 2*time.Second)
	_, err = localClient.Put(writeContext, client.ObjectID{1}, []byte("independent"), client.PutOptions{})
	stopWrite()
	if err != nil {
		t.Fatal("remote quota display held the local mutation lock", err)
	}
	handle.Close()
	var response result
	select {
	case response = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if response.status != 200 {
		t.Fatal("cluster capacity", response.status)
	}
	d := client.Decoder{B: bucketReplyResult(t, client.Message{Body: response.body}, 200)}
	used, available, _, maximum, nodes := d.U64(), d.U64(), d.U64(), d.U64(), d.U32()
	if d.Err != nil || used != 1<<20 || nodes != 2 || available <= 8<<20 || available >= 16<<20 || maximum <= used {
		t.Fatal("wrong global capacity/usage", used, available, maximum, nodes, d.Err)
	}
	// Filesystem clients use the same committed member pool, without requiring
	// dashboard/admin access on the owner serving the usage counter.
	capacity, err := c.Space(ctx)
	if err != nil {
		t.Fatal(err)
	}
	localCapacity, err := source.node.bucketQuotaCapacity(bucket.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if capacity.Used != 1<<20 || capacity.Free <= localCapacity.Available || !capacity.QuotaLimited {
		t.Fatalf("filesystem space omitted remote storage: %+v, local %+v", capacity, localCapacity)
	}

}
