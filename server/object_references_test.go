// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestTicket32ObjectReferencesReadonlyAndReconnect(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "references", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "references")
	user := bucketProtocolUser(t, s)
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dial := func() *client.Client {
		c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a := dial()
	defer a.Close()
	for _, maintenance := range []func(context.Context) error{a.ResetAbandonedObjectReferences, a.ReclaimBucketMetadata} {
		if err := maintenance(ctx); !errors.Is(err, client.ErrUnauthorized) {
			t.Fatal("maintenance allowed outside exclusive lease", err)
		}
	}
	if _, err := a.CanReclaimDependencies(ctx); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("dependency proof allowed outside exclusive lease", err)
	}
	z := dial()
	defer z.Close()
	id := client.ObjectID{42}
	pin, claim := client.NewRequestID(), client.NewRequestID()
	if err := a.PinObject(ctx, id, pin); err != nil {
		t.Fatal(err)
	}
	if err := a.PinObject(ctx, id, pin); err != nil {
		t.Fatal("pin retry", err)
	}
	if err := z.ClaimObjectRetirement(ctx, id, claim); !errors.Is(err, client.ErrWouldBlock) {
		t.Fatal("claim ignored pin", err)
	}
	a.Close()
	a = dial()
	defer a.Close()
	if err := z.ClaimObjectRetirement(ctx, id, claim); !errors.Is(err, client.ErrWouldBlock) {
		t.Fatal("transport loss dropped durable pin", err)
	}
	if err := a.UnpinObject(ctx, id, pin); err != nil {
		t.Fatal(err)
	}
	if err := a.UnpinObject(ctx, id, pin); err != nil {
		t.Fatal("unpin retry", err)
	}
	if err := z.ClaimObjectRetirement(ctx, id, claim); err != nil {
		t.Fatal(err)
	}
	if err := a.PinObject(ctx, id, pin); !errors.Is(err, client.ErrWouldBlock) {
		t.Fatal("pin ignored retirement claim", err)
	}
	if _, err := z.Put(ctx, id, []byte("not allowed"), client.PutOptions{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("reference claim conferred WRITE", err)
	}
	if err := z.Delete(ctx, id, client.DeleteOptions{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("reference claim conferred DELETE", err)
	}
	if err := z.ReleaseObjectRetirement(ctx, id, claim); err != nil {
		t.Fatal(err)
	}
	if err := a.PinObject(ctx, id, pin); err != nil {
		t.Fatal(err)
	}
	if err := a.UnpinObject(ctx, id, pin); err != nil {
		t.Fatal(err)
	}
	if err := b.allVolumes()[0].Scrub(); err != nil {
		t.Fatal(err)
	}
}
