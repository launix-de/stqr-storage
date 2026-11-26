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

func TestBucketAccessOrdinaryUserKeepsRightsAfterNameReuse(t *testing.T) {
	s := testServer(t)
	original, _ := createProtocolBucket(t, s, "access-reused")
	user := bucketProtocolUser(t, s)
	d := original.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.DialBucket(ctx, host.URL, user.Name, "bucket-test-password", d.Name, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d = original.Describe()
	d.Name = "access-original"
	if _, err = s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	replacement, _ := createProtocolBucket(t, s, "access-reused")
	d = replacement.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead | BucketWrite}}
	if _, err = s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	permissions, err := c.BucketAccess(ctx)
	if err != nil || permissions != client.BucketPermissionRead || c.BucketID() != client.ID(original.ID()) {
		t.Fatal("metadata followed a reused name instead of the bound identity", permissions, err)
	}
	other, err := client.DialBucket(ctx, host.URL, user.Name, "bucket-test-password", d.Name, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if permissions, err = other.BucketAccess(ctx); err != nil || permissions != client.BucketPermissionRead|client.BucketPermissionWrite || other.BucketID() != client.ID(replacement.ID()) {
		t.Fatal("replacement positive control has wrong access", permissions, err)
	}
}

func TestBucketAccessOrdinaryUserRevocationAndLock(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "access-revoked")
	user := bucketProtocolUser(t, s)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connect := func() (*client.Client, error) {
		return client.DialBucket(ctx, host.URL, user.Name, "bucket-test-password", b.ID().String(), "fs")
	}
	if c, err := connect(); !errors.Is(err, client.ErrUnauthorized) {
		if c != nil {
			c.Close()
		}
		t.Fatal("missing grant allowed access", err)
	}
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead | BucketWrite}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	c, err := connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if permissions, err := c.BucketAccess(ctx); err != nil || permissions != client.BucketPermissionRead|client.BucketPermissionWrite {
		t.Fatal("writable positive control", permissions, err)
	}
	d = b.Describe()
	d.State = BucketLocked
	if _, err = s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if permissions, err := c.BucketAccess(ctx); err == nil && permissions != client.BucketPermissionRead {
		t.Fatal("live locked connection retained writable access", permissions)
	}
	locked, err := connect()
	if err != nil {
		t.Fatal(err)
	}
	defer locked.Close()
	if permissions, err := locked.BucketAccess(ctx); err != nil || permissions != client.BucketPermissionRead {
		t.Fatal("locked bucket did not report read-only access", permissions, err)
	}
	d = b.Describe()
	d.Grants = nil
	if _, err = s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if permissions, err := locked.BucketAccess(ctx); err == nil {
		t.Fatal("revoked connection disclosed permissions", permissions)
	}
	if c, err := connect(); !errors.Is(err, client.ErrUnauthorized) {
		if c != nil {
			c.Close()
		}
		t.Fatal("revocation allowed a new connection", err)
	}
}
