// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
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

func TestDaemonS3MultipartCopyAndExpiryAfterMigration(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 16<<20)
	remote := addTestVolume(t, target.node, "data", "hdd", 16<<20)
	bucket, _ := createProtocolBucket(t, source.node, "fop-files")
	user := bucketProtocolUser(t, source.node)
	uid, _ := parseID(user.ID)
	e := client.Encoder{}
	e.ID(client.ID(bucket.ID()))
	e.ID(client.ID(uid))
	e.U32(uint32(BucketAllPermissions))
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketGrant, e.B); status != 200 {
		t.Fatal(status)
	}
	e = client.Encoder{}
	e.ID(client.ID(uid))
	e.Text(testS3Access)
	e.Text(testS3Secret)
	if _, status := source.node.dispatchAs(userMessage(client.UserS3KeySet, e), nil, userPrincipal(t, source.node)); status != 200 {
		t.Fatal(status)
	}
	upload := startS3Multipart(t, source.node, "moved", nil)
	payload := bytes.Repeat([]byte{0x76}, 2<<20)
	part := s3TestRequest(t, source.node, "PUT", "/s3/fop-files/moved?uploadId="+upload+"&partNumber=1", payload, nil)
	requireS3Status(t, part, 200)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID{})
	vid, _ := parseID(remote.VolumeID)
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	completion := completeS3XML(client.S3CompletedPart{Number: 1, ETag: part.Header().Get("ETag")})
	requireS3Status(t, s3TestRequest(t, source.node, "POST", "/s3/fop-files/moved?uploadId="+upload, completion, nil), 200)
	requireS3Status(t, s3TestRequest(t, source.node, "PUT", "/s3/fop-files/copied", nil, map[string]string{"X-Amz-Copy-Source": "/fop-files/moved"}), 200)
	for _, n := range []*daemonTestNode{source, target} {
		got := s3TestRequest(t, n.node, "GET", "/s3/fop-files/copied", nil, nil)
		requireS3Status(t, got, 200)
		if !bytes.Equal(got.Body.Bytes(), payload) {
			t.Fatal("remote copy changed bytes")
		}
	}
	owner, err := source.node.findAccount(user.Name)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := source.node.openFrontend(ctx, bucket.ID(), principal{User: owner, frontendPermission: BucketWrite})
	if err != nil {
		t.Fatal(err)
	}
	faulted := client.NewS3(multipartBeforePublishStore{S3Store: connection, target: client.S3MetadataID("recovered")})
	pending, err := faulted.CreateMultipart(ctx, "recovered", owner.ID, "", "", client.S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	recoveryData := "recover through the remote owner"
	recoveryPart, err := faulted.UploadPart(ctx, pending.ID, 1, strings.NewReader(recoveryData), uint64(len(recoveryData)), client.S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = faulted.CompleteMultipart(ctx, pending.ID, []client.S3CompletedPart{{Number: 1, ETag: recoveryPart.ETag}}, client.S3PutOptions{}); err == nil {
		t.Fatal("publication failure was not injected")
	}
	connection.Close()
	source = restartDaemonTestNode(t, source)
	if err = source.node.maintainS3(ctx, time.Now()); err != nil {
		t.Fatal("remote completion recovery after frontend restart", err)
	}
	recovered := s3TestRequest(t, target.node, "GET", "/s3/fop-files/recovered", nil, nil)
	requireS3Status(t, recovered, 200)
	if recovered.Body.String() != recoveryData {
		t.Fatal("multipart recovery changed payload")
	}
	abandoned := startS3Multipart(t, source.node, "expired", nil)
	requireS3Status(t, s3TestRequest(t, source.node, "PUT", "/s3/fop-files/expired?uploadId="+abandoned+"&partNumber=1", payload, nil), 200)
	allocated := target.node.volumes[remote.VolumeID].store.Stats().DataPhysical
	// Cleanup runs on the node which no longer owns any object in this bucket.
	if err = source.node.maintainS3(ctx, time.Now().Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	requireS3Status(t, s3TestRequest(t, source.node, "GET", "/s3/fop-files/expired?uploadId="+abandoned, nil, nil), 404)
	if target.node.volumes[remote.VolumeID].store.Stats().DataPhysical >= allocated {
		t.Fatal("remote expiry did not reclaim upload data")
	}
	requireS3Status(t, s3TestRequest(t, target.node, "GET", "/s3/fop-files/copied", nil, nil), 200)
}

func TestFrontendIssuerOfflineRemainsRetryable(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "issuer-wait")
	p := userPrincipal(t, source.node)
	p.frontendPermission = BucketRead
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	name, password, release, err := source.node.frontendCredentials(ctx, bucket.ID(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	source.close()
	request := httptest.NewRequest("GET", "http://localhost/api/ws", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	request.SetBasicAuth(name, password)
	request.Header.Set(client.BucketHeader, bucket.ID().String())
	response := httptest.NewRecorder()
	target.node.Handler().ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatal("unreachable issuer reported as invalid credentials", response.Code, response.Body.String())
	}
}
