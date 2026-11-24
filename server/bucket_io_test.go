// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestBucketTrafficMeasuresPayloadNotSparseLength(t *testing.T) {
	source, _ := startAutomaticDaemonPair(t, false)
	addReplicaDataVolume(t, source.node, "traffic-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "traffic")
	ctx := context.Background()
	actor := userPrincipal(t, source.node)
	id := volume.ObjectID{3}
	opt := func() volume.Options { return volume.Options{RequestID: volume.ID(client.NewRequestID())} }
	data := bytes.Repeat([]byte("x"), 4096)
	if _, err := b.Put(ctx, actor, id, data, opt()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.GetRange(ctx, actor, id, 0, 1024); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, _, err := b.CopyTo(ctx, actor, id, 0, 2048, &out); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutRange(ctx, actor, id, 64, make([]byte, 512), opt()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Append(ctx, actor, id, make([]byte, 10), opt()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CAS(ctx, actor, id, 4, data[4:12], []byte("12345678"), opt()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CAS(ctx, actor, id, 4, data[4:12], []byte("badvalue"), opt()); err == nil {
		t.Fatal("failed CAS accepted")
	}
	if _, err := b.Trunc(ctx, actor, id, 2<<20, opt()); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, actor, id, opt()); err != nil {
		t.Fatal(err)
	}
	read, written := source.node.userTraffic(actor.User.ID)
	if read != 3072 || written != 4626 {
		t.Fatal("user traffic differs from bucket traffic", read, written)
	}
	snap := source.node.CollectStats(client.StatsFilter{})
	for _, row := range snap.Buckets {
		if row.ID == client.ID(b.ID()) {
			if row.IO.LogicalReadBytes != 3072 || row.IO.LogicalWriteBytes != 4626 {
				t.Fatal("incorrect frontend traffic", row.IO)
			}
			if row.IO.PhysicalReadBytes != 0 || row.IO.PhysicalWriteBytes != 0 {
				t.Fatal("logical traffic misreported as physical")
			}
			return
		}
	}
	t.Fatal("missing bucket stats")
}
