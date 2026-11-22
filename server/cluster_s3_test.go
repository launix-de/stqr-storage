// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestDaemonS3ExtendsAndReadsThroughEitherNode(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	for _, n := range []*daemonTestNode{source, target} {
		addTestVolume(t, n.node, "data", "hdd", 4<<20)
	}
	bucket, _ := createProtocolBucket(t, source.node, "cluster-s3")
	user := bucketProtocolUser(t, source.node)
	uid, _ := parseID(user.ID)
	grant := func(permissions BucketPermissions) {
		t.Helper()
		e := client.Encoder{}
		e.ID(client.ID(bucket.ID()))
		e.ID(client.ID(uid))
		e.U32(uint32(permissions))
		if _, status, _ := adminBucketMessage(t, source.node, client.BucketGrant, e.B); status != 200 {
			t.Fatal("global S3 grant", status)
		}
	}
	grant(BucketAllPermissions)
	e := client.Encoder{}
	e.ID(client.ID(uid))
	e.Text(testS3Access)
	e.Text(testS3Secret)
	if _, status := source.node.dispatchAs(userMessage(client.UserS3KeySet, e), nil, userPrincipal(t, source.node)); status != 200 {
		t.Fatal("global S3 key", status)
	}
	payload := bytes.Repeat([]byte{0x5c}, 1<<20)
	for i := 0; i < 6; i++ {
		path := fmt.Sprintf("/s3/cluster-s3/key-%d", i)
		response := s3TestRequest(t, source.node, http.MethodPut, path, payload, nil)
		if response.Code != 200 {
			logDaemonRangeState(t, source, target)
			t.Fatalf("S3 PUT %d: status=%d body=%s", i, response.Code, response.Body.String())
		}
	}
	for i := 0; i < 6; i++ {
		path := fmt.Sprintf("/s3/cluster-s3/key-%d", i)
		response := s3TestRequest(t, target.node, http.MethodGet, path, nil, nil)
		if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), payload) {
			t.Fatal("S3 remote GET", i, response.Code)
		}
	}
	response := s3TestRequest(t, target.node, http.MethodGet, "/s3/cluster-s3?list-type=2", nil, nil)
	if response.Code != 200 {
		t.Fatal("S3 global LIST", response.Code, response.Body.String())
	}
	for i := 0; i < 6; i++ {
		if !strings.Contains(response.Body.String(), fmt.Sprintf("<Key>key-%d</Key>", i)) {
			t.Fatal("S3 LIST omitted remote key", i)
		}
	}
	var logical uint64
	for _, n := range []*daemonTestNode{source, target} {
		var local uint64
		n.node.mu.RLock()
		for _, v := range n.node.volumes {
			if !v.config.System {
				local += v.store.Stats().LogicalBytes
			}
		}
		n.node.mu.RUnlock()
		if local == 0 {
			t.Fatal("S3 extension left one node unused")
		}
		logical += local
	}
	if logical <= 4<<20 {
		t.Fatal("S3 test did not exceed a single node")
	}
	response = s3TestRequest(t, source.node, http.MethodPut, "/s3/cluster-s3/does-not-fit", bytes.Repeat([]byte{0x2a}, 2<<20), nil)
	if response.Code != 507 {
		t.Fatal("physical capacity failure lost its 507 status", response.Code, response.Body.String())
	}
	response = s3TestRequest(t, target.node, http.MethodGet, "/s3/cluster-s3/does-not-fit", nil, nil)
	if response.Code != 404 {
		t.Fatal("rejected S3 PUT published an object", response.Code)
	}
	// Frontend internals need GET/CAS even for write-only or delete-only HTTP
	// grants. Their private sessions must retain that original grant throughout.
	grant(BucketWrite)
	response = s3TestRequest(t, source.node, http.MethodPut, "/s3/cluster-s3/write-only", []byte("write-only value"), nil)
	if response.Code != 200 {
		t.Fatal("write-only S3 internals", response.Code, response.Body.String())
	}
	response = s3TestRequest(t, target.node, http.MethodGet, "/s3/cluster-s3/write-only", nil, nil)
	if response.Code != 403 {
		t.Fatal("write-only grant acquired public READ", response.Code)
	}
	grant(BucketDelete)
	response = s3TestRequest(t, target.node, http.MethodDelete, "/s3/cluster-s3/write-only", nil, nil)
	if response.Code != 204 {
		t.Fatal("delete-only S3 internals", response.Code, response.Body.String())
	}
}

func TestFrontendCredentialsStayPrivateAndBucketBound(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "frontend-boundary")
	p := userPrincipal(t, source.node)
	p.frontendPermission = BucketRead
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	name, secret, release, err := source.node.frontendCredentials(ctx, bucket.ID(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	wire, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(name, frontendCredentialPrefix))
	if err != nil {
		t.Fatal(err)
	}
	wire[len(wire)-1] ^= 1 // forged live-operation ID, otherwise valid catalog binding
	forged := frontendCredentialPrefix + base64.RawURLEncoding.EncodeToString(wire)
	if conn, err := client.Dial(ctx, target.node.cfg.Node.Advertise, forged, secret, client.ID(bucket.ID())); err == nil {
		conn.Close()
		t.Fatal("cluster token invented a user session without a live frontend operation")
	}
	for _, credentials := range []struct {
		password string
		bucket   client.ID
	}{{"incorrect-cluster-credential", client.ID(bucket.ID())}, {secret, client.ID{0xfa}}} {
		conn, err := client.Dial(ctx, target.node.cfg.Node.Advertise, name, credentials.password, credentials.bucket)
		if err == nil {
			conn.Close()
			t.Fatal("invalid frontend credential was accepted")
		}
	}
	req, err := http.NewRequest(http.MethodGet, target.node.cfg.Node.Advertise+"/api/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(name, secret)
	response, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("frontend credential exposed administrative HTTP API", response.StatusCode)
	}
	conn, err := client.Dial(ctx, target.node.cfg.Node.Advertise, name, secret, client.ID(bucket.ID()))
	if err != nil {
		t.Fatal("valid private frontend refused", err)
	}
	defer conn.Close()
	// An account change revokes existing private sessions and prevents their
	// old revision-bound credentials from silently reconnecting as that user.
	e := client.Encoder{}
	uid, _ := parseID(p.User.ID)
	e.ID(client.ID(uid))
	e.Text("replacement-password-for-test")
	if _, status := source.node.dispatchAs(userMessage(client.UserSetPassword, e), nil, p); status != 200 {
		t.Fatal("account revision update", status)
	}
	if _, _, err := conn.Get(ctx, client.ObjectID{1}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("revoked frontend session remained usable", err)
	}
	reopened, err := client.Dial(ctx, target.node.cfg.Node.Advertise, name, secret, client.ID(bucket.ID()))
	if err == nil {
		reopened.Close()
		t.Fatal("old frontend credential reconnected after account change")
	}
}

func TestNodeDrainLetsActiveFrontendFinishBeforeMembershipLeave(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, target.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "frontend-drain")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for {
		patch, known := source.node.routingPatch(bucket.ID(), [32]byte{})
		if known && patch.Entries[0].Locations[0].Node == target.node.control.ledger.node {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("empty source range did not relocate", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	p := userPrincipal(t, source.node)
	p.frontendPermission = BucketRead
	connection, err := source.node.openFrontend(ctx, bucket.ID(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	e := client.Encoder{}
	e.ID(source.node.control.ledger.node)
	_, status, _ := adminBucketMessage(t, source.node, client.NodeDrain, e.B)
	if status != 202 {
		t.Fatal(status)
	}
	for !source.node.drainClosing.Load() {
		if ctx.Err() != nil {
			t.Fatal("drain did not reach frontend barrier", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	if source.node.retiring() {
		t.Fatal("active frontend issuer left membership prematurely")
	}
	if newConnection, err := source.node.openFrontend(ctx, bucket.ID(), p); err == nil {
		newConnection.Close()
		t.Fatal("drain accepted a fresh frontend operation")
	}
	// Already admitted work can still authenticate its remote object session.
	if _, _, err := connection.Get(ctx, client.ObjectID{1}); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("drain prevented an existing frontend from finishing", err)
	}
	connection.Close()
	select {
	case <-source.node.stopRequest:
	case <-ctx.Done():
		t.Fatal("drain did not finish after frontend released", ctx.Err())
	}
}
