// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestBucketNameConnectionCanonicalIDAndRename(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "names", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "my files Ä")
	var mu sync.Mutex
	var selectors []string
	handler := s.Handler()
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/ws" {
			mu.Lock()
			selectors = append(selectors, r.Header.Get(client.BucketHeader))
			mu.Unlock()
		}
		handler.ServeHTTP(w, r)
	}))
	defer host.Close()
	a := s.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.DialBucket(ctx, host.URL, a.Username, a.Password, "my files Ä", "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.BucketID() != client.ID(b.ID()) {
		t.Fatal("name did not bind canonical ID")
	}
	oid := client.ObjectID{1}
	if _, err = c.Put(ctx, oid, []byte("original bucket"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	d := b.Describe()
	d.Name = "renamed"
	if _, err = s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	replacement, _ := createProtocolBucket(t, s, "my files Ä")
	other, err := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(replacement.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err = other.Put(ctx, oid, []byte("different bucket"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	s.wsMu.Lock()
	for ws := range s.sockets {
		if ws.authorization.bucket == b {
			ws.conn.Close()
		}
	}
	s.wsMu.Unlock()
	var data []byte
	for i := 0; i < 5; i++ {
		data, _, err = c.Get(ctx, oid)
		if err == nil {
			break
		}
	}
	if err != nil || string(data) != "original bucket" {
		t.Fatal("rename/reconnect retargeted connection", string(data), err)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, selector := range selectors {
		if i > 0 && selector != "my files Ä" && selector != b.ID().String() && selector != replacement.ID().String() {
			t.Fatal("unexpected selector", selector)
		}
	}
	if len(selectors) < 3 || selectors[len(selectors)-1] != b.ID().String() {
		t.Fatal("reconnect used mutable name", selectors)
	}
}

func TestBucketNameConnectionAuthorizationAndFormat(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "named-grants")
	user := bucketProtocolUser(t, s)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx := context.Background()
	if c, err := client.DialBucket(ctx, host.URL, user.Name, "bucket-test-password", "named-grants", "fs"); !errors.Is(err, client.ErrUnauthorized) {
		if c != nil {
			c.Close()
		}
		t.Fatal("name bypassed ACL", err)
	}
	a := s.cfg.Auth.BootstrapAdmin
	for _, selector := range []string{b.ID().String(), "named-grants", "name:named-grants"} {
		c, err := client.DialBucket(ctx, host.URL, a.Username, a.Password, selector, "fs")
		if err != nil {
			t.Fatal(err)
		}
		if c.BucketID() != client.ID(b.ID()) {
			t.Fatal("canonical ID differs")
		}
		c.Close()
	}
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	c, err := client.DialBucket(ctx, host.URL, user.Name, "bucket-test-password", "named-grants", "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Put(ctx, client.ObjectID{1}, []byte("denied"), client.PutOptions{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("name grant allowed write", err)
	}
	for _, tc := range []struct {
		selector, format string
		status           uint64
	}{{"missing-bucket", "fs", 404}, {systemBucket.String(), "fs", 403}, {"named-grants", "bd", 409}} {
		conn, err := client.DialBucket(ctx, host.URL, a.Username, a.Password, tc.selector, tc.format)
		if conn != nil {
			conn.Close()
		}
		var status *client.StatusError
		if !errors.As(err, &status) || uint64(status.Status) != tc.status {
			t.Fatal(tc, err)
		}
	}
	// Hexadecimal names have an explicit, unambiguous spelling.
	literal := "0123456789abcdef0123456789abcdef"
	hexName, _ := createProtocolBucket(t, s, literal)
	named, err := client.DialBucket(ctx, host.URL, a.Username, a.Password, "name:"+literal, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer named.Close()
	if named.BucketID() != client.ID(hexName.ID()) {
		t.Fatal("hex-looking name resolved as ID")
	}
}
