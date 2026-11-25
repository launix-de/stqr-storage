// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestObjectLeaseRetainsGrantsAndNBDTrimUsesNativeReplacement(t *testing.T) {
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
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, "block-user", "block-test-password", client.ID(b.ID()), "bd")
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
	if err = c.BeginObjectLease(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err = c.Delete(ctx, oid, client.DeleteOptions{Accepted: true}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("exclusive lease expanded DELETE grant", err)
	}
	if _, err = c.Trunc(ctx, oid, 64, client.TruncOptions{}); err != nil {
		t.Fatal("generic lease rejected native TRUNC", err)
	}
	catalog := nbd.NewCatalog(c)
	disk, err := catalog.Create(ctx, nbd.Metadata{Name: "write-only-discard", Size: nbd.StripeSize})
	if err != nil {
		t.Fatal(err)
	}
	if err = catalog.WriteAt(ctx, disk, bytes.Repeat([]byte("x"), int(nbd.StripeSize)), 0); err != nil {
		t.Fatal(err)
	}
	if err = catalog.Trim(ctx, disk, 0, nbd.StripeSize); err != nil {
		t.Fatal("client trim fallback", err)
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if err = catalog.ReadAt(ctx, disk, got, 512); err != nil || !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatal("trim retained client data", err)
	}
	stripe, err := c.Stat(ctx, nbd.StripeID(disk.ID, 0))
	if err != nil || stripe.Length != 0 {
		t.Fatal("WRITE-only trim retained stripe payload", stripe, err)
	}
}
