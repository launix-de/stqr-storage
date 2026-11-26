// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestBucketAccessClientEffectivePermissions(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "access", "hdd", 8<<20)
	user := bucketProtocolUser(t, s)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name   string
		grant  BucketPermissions
		locked bool
		want   client.BucketPermissions
	}{
		{"read-write", BucketRead | BucketWrite, false, client.BucketPermissionRead | client.BucketPermissionWrite},
		{"read-only", BucketRead, false, client.BucketPermissionRead},
		{"append-only", BucketRead | BucketAppend, false, client.BucketPermissionRead | client.BucketPermissionAppend},
		{"locked", BucketAllPermissions, true, client.BucketPermissionRead},
		{strings.Repeat("long-name", 12), BucketRead | BucketWrite, false, client.BucketPermissionRead | client.BucketPermissionWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := createProtocolBucket(t, s, tc.name)
			d := b.Describe()
			d.Grants = []BucketGrant{{UserID: user.ID, Permissions: tc.grant}}
			if tc.locked {
				d.State = BucketLocked
			}
			if _, err := s.buckets.Save(d, d.Revision); err != nil {
				t.Fatal(err)
			}
			for _, selector := range []string{tc.name, b.ID().String()} {
				c, err := client.DialBucket(ctx, host.URL, user.Name, "bucket-test-password", selector, "fs")
				if err != nil {
					t.Fatal(err)
				}
				got, err := c.BucketAccess(ctx)
				c.Close()
				if err != nil || got != tc.want {
					t.Fatalf("selector %q: permissions %v, error %v", selector, got, err)
				}
			}
			usage, err := b.logicalUsage()
			if err != nil || usage != 0 {
				t.Fatalf("access query wrote to bucket: usage %d, error %v", usage, err)
			}
		})
	}
}

func TestBucketAccessClientAfterRename(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "access-before-rename")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	a := s.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.DialBucket(ctx, host.URL, a.Username, a.Password, b.Describe().Name, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d := b.Describe()
	d.Name = "access-after-rename"
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	got, err := c.BucketAccess(ctx)
	if err != nil || got != client.BucketPermissions(BucketAllPermissions) {
		t.Fatal(got, err)
	}
}
