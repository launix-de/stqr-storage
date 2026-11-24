// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestNativeBlockDiscardUsesWriteGrantOnlyDuringLease(t *testing.T) {
	source, _ := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "block-discard")
	message, user := createUserMessage("block-user", "block-test-password")
	if _, status := source.node.dispatchAs(message, nil, userPrincipal(t, source.node)); status != 201 {
		t.Fatal("user create", status)
	}
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.ID(user)
	e.U32(uint32(BucketRead | BucketWrite))
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketGrant, e.B); status != 200 {
		t.Fatal("grant", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, "block-user", "block-test-password", client.ID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{1}
	if _, err = c.Put(ctx, oid, []byte("stripe"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = c.Delete(ctx, oid, client.DeleteOptions{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("ordinary client bypassed DELETE grant", err)
	}
	if err = c.BeginBlockMaintenance(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err = c.Delete(ctx, oid, client.DeleteOptions{Accepted: true}); err != nil {
		t.Fatal("native discard with WRITE grant", err)
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.Get(ctx, oid); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("discard retained stripe", err)
	}
}
