// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func dialTestSocket(t *testing.T, host, username, password, bucket string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	r, _ := http.NewRequest(http.MethodGet, host, nil)
	r.SetBasicAuth(username, password)
	if bucket != "" {
		r.Header.Set(client.BucketHeader, bucket)
		r.Header.Set(client.FormatHeader, "fs")
	}
	d := websocket.Dialer{Subprotocols: []string{"stqr.v2"}}
	return d.Dial("ws"+strings.TrimPrefix(host, "http")+"/api/ws", r.Header)
}
func TestConnectionChecksBucketGrantAtUpgrade(t *testing.T) {
	s := testServer(t)
	user := bucketProtocolUser(t, s)
	b, _ := createProtocolBucket(t, s, "connect-grant")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()), "fs"); !errors.Is(err, client.ErrUnauthorized) {
		if c != nil {
			c.Close()
		}
		t.Fatal("missing grant was not rejected at Dial", err)
	}
	if c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID{99}, "fs"); !errors.Is(err, client.ErrNotFound) {
		if c != nil {
			c.Close()
		}
		t.Fatal("unknown bucket was not rejected at Dial", err)
	}
	conn, response, err := dialTestSocket(t, host.URL, user.Name, "bucket-test-password", "")
	if err == nil {
		conn.Close()
		t.Fatal("normal user connected without a bucket")
	}
	if response == nil || response.StatusCode != 400 {
		t.Fatal(response, err)
	}
	response.Body.Close()
	description := b.Describe()
	description.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	if _, err = s.buckets.Save(description, description.Revision); err != nil {
		t.Fatal(err)
	}
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s.wsMu.Lock()
	var authorization *connectionAuthorization
	for ws := range s.sockets {
		if ws.principal.User.ID == user.ID {
			authorization = ws.authorization
			break
		}
	}
	s.wsMu.Unlock()
	if authorization == nil || authorization.bucket != b || authorization.bucketID != b.ID() || authorization.admin || authorization.clusterPeer || authorization.permissions != BucketRead {
		t.Fatal("bucket capability not stored in connection")
	}
	if _, err = c.Put(ctx, client.ObjectID{1}, []byte("denied"), client.PutOptions{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("cached read-only connection could write", err)
	}
}
func TestConnectionDataPathHasNoAccountOrGrantLookups(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cached-grant-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "cached-access")
	user := bucketProtocolUser(t, s)
	description := b.Describe()
	description.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketAllPermissions}}
	if _, err := s.buckets.Save(description, description.Revision); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{42}
	if _, err = c.Put(ctx, oid, []byte("initial"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before := s.system.IOStats()
	for i := 0; i < 5; i++ {
		if _, _, err = c.Get(ctx, oid); err != nil {
			t.Fatal(err)
		}
		if _, err = c.Stat(ctx, oid); err != nil {
			t.Fatal(err)
		}
		_, err = c.Append(ctx, oid, []byte("!"), client.AppendOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.CAS(ctx, oid, 0, []byte("initial"), []byte("initial"), client.CASOptions{MutationOptions: client.MutationOptions{}}); err != nil {
			t.Fatal(err)
		}
	}
	after := s.system.IOStats()
	if after.ReadOps != before.ReadOps || after.WriteOps != before.WriteOps {
		t.Fatalf("data path touched account/catalog storage: before %+v after %+v", before, after)
	}
	// The connection remains a valid capability if the admin device is temporarily
	// unavailable; object operations must not perform hidden re-authentication.
	if err = s.system.Close(); err != nil {
		t.Fatal(err)
	}
	if data, _, err := c.Get(ctx, oid); err != nil || !bytes.HasPrefix(data, []byte("initial")) {
		t.Fatal("cached grant required another admin-volume lookup", err)
	}
}
func TestConnectionCannotSwitchBucketOrBecomeAdministrative(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "bound-a")
	other, _ := createProtocolBucket(t, s, "bound-b")
	admin := s.cfg.Auth.BootstrapAdmin
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	conn, _, err := dialTestSocket(t, host.URL, admin.Username, admin.Password, b.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Admin credentials on a data socket do not turn it into the dashboard channel.
	request := client.Message{Kind: client.Request, Opcode: client.NodeConfigGet, ID: client.NewRequestID()}
	bucketReplyResult(t, requestMessage(t, conn, request), 403)
	e := client.Encoder{}
	e.ID(client.ID(other.ID()))
	e.B = append(e.B, make([]byte, 32)...)
	e.U64(0)
	e.U64(0)
	e.ID(client.ID{})
	request = client.Message{Kind: client.Request, Opcode: 0x0107, ID: client.NewRequestID(), Body: e.B}
	bucketReplyResult(t, requestMessage(t, conn, request), 403)
}
func TestConnectionCachesAdminAndPeerRoles(t *testing.T) {
	s := testServer(t)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	admin := s.cfg.Auth.BootstrapAdmin
	conn, _, err := dialTestSocket(t, host.URL, admin.Username, admin.Password, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	before := s.system.IOStats()
	request := client.Message{Kind: client.Request, Opcode: client.NodeConfigGet, ID: client.NewRequestID()}
	bucketReplyResult(t, requestMessage(t, conn, request), 200)
	after := s.system.IOStats()
	if after.ReadOps != before.ReadOps {
		t.Fatal("admin role was looked up again")
	}
	peerID := volume.ID{11}
	peer, _, err := dialTestSocket(t, host.URL, "node-"+peerID.String(), s.cfg.Auth.ClusterToken, "")
	if err != nil {
		t.Fatal(err)
	}
	// The client receives the upgrade before the server has necessarily
	// published its session. A protocol response is the registration barrier.
	defer peer.Close()
	bucketReplyResult(t, requestMessage(t, peer, client.Message{Kind: client.Request, Opcode: client.Ping, ID: client.NewRequestID(), Body: make([]byte, 8)}), 200)
	s.wsMu.Lock()
	var capability *connectionAuthorization
	for ws := range s.sockets {
		if ws.principal.User.ID == peerID.String() {
			capability = ws.authorization
		}
	}
	s.wsMu.Unlock()
	if capability == nil || !capability.clusterPeer || capability.admin || capability.bucket != nil {
		t.Fatal("peer role not cached separately")
	}
	bucketReplyResult(t, requestMessage(t, peer, client.Message{Kind: client.Request, Opcode: client.UserList, ID: client.NewRequestID()}), 403)
	// Authentication grants a transport role, never cluster membership/ownership.
	bucketReplyResult(t, requestMessage(t, peer, client.Message{Kind: client.Request, Opcode: 0x1000, ID: client.NewRequestID()}), 400)
	bad, response, err := dialTestSocket(t, host.URL, "node-"+peerID.String(), "wrong-token", "")
	if err == nil {
		bad.Close()
		t.Fatal("invalid cluster token accepted")
	}
	if response == nil || response.StatusCode != 401 {
		t.Fatal(response, err)
	}
	response.Body.Close()
}
func TestConnectionPasswordChangeInvalidatesCachedAccess(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "password-data", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "password-revocation")
	user := bucketProtocolUser(t, s)
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id, _ := parseID(user.ID)
	body := client.Encoder{}
	body.ID(client.ID(id))
	body.Text("replacement-password")
	_, status, _ := adminBucketMessage(t, s, client.UserSetPassword, body.B)
	if status != 200 {
		t.Fatal(status)
	}
	if _, _, err = c.Get(ctx, client.ObjectID{1}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("old cached credentials survived password change", err)
	}
}
