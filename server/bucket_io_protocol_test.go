// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"testing"
	"time"
)

func TestBucketTrafficIncludesStreamingAcceptedObjectProtocol(t *testing.T) {
	source, _ := startAutomaticDaemonPair(t, false)
	addReplicaDataVolume(t, source.node, "metered-protocol", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, source.node, "metered-protocol")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{3}
	if _, err = c.Put(ctx, id, bytes.Repeat([]byte("x"), 5<<20), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.GetRange(ctx, id, 0, 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Append(ctx, id, bytes.Repeat([]byte("y"), 2<<20), client.AppendOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.PutRange(ctx, id, 1<<20, bytes.Repeat([]byte("w"), 2<<20), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CAS(ctx, id, 0, bytes.Repeat([]byte("x"), 4096), bytes.Repeat([]byte("z"), 4096), client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true}}); err != nil {
		t.Fatal(err)
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if data, _, err := c.Get(ctx, id); err != nil || len(data) != 7<<20 {
		t.Fatal("complete readback", len(data), err)
	}
	traffic := b.ioSnapshot()
// Bucket counters aggregate concurrent operations. Housekeeping may read
	// the object prefix while this test performs its exact eight MiB readback.
	// It never writes this non-S3 payload, so write accounting stays exact.
	if traffic.LogicalReadBytes < 8<<20 || traffic.LogicalWriteBytes != (9<<20)+4096 {
		t.Fatal("streaming/accepted protocol escaped metering", traffic)
	}
}
