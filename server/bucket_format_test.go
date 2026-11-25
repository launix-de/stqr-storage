// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

func TestBucketFormatNativeContract(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "format-native")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	user := bucketProtocolUser(t, s)
	a := s.cfg.Auth.BootstrapAdmin
	open := func(name, password, format string) int {
		h := http.Header{}
		h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(name+":"+password)))
		h.Set(client.BucketHeader, b.ID().String())
		if format != "" {
			h.Set(client.FormatHeader, format)
		}
		c, r, err := (&websocket.Dialer{Subprotocols: []string{"stqr.v2"}}).Dial("ws"+strings.TrimPrefix(host.URL, "http")+"/api/ws", h)
		if err == nil {
			c.Close()
			return 101
		}
		if r == nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		return r.StatusCode
	}
	if b.Describe().Format != "" {
		t.Fatal("fresh bucket has a format")
	}
	if got := open(a.Username, a.Password, ""); got != 400 {
		t.Fatal("missing format", got)
	}
	if got := open(user.Name, "bucket-test-password", "fs"); got != 403 {
		t.Fatal("unauthorized bind", got)
	}
	if b.Describe().Format != "" {
		t.Fatal("failed opening changed format")
	}
	for _, format := range []string{"my-client<v2>", "my-client<v2>", "s3"} {
		want := 101
		if format == "s3" {
			want = 409
		}
		if got := open(a.Username, a.Password, format); got != want {
			t.Fatal(format, got, want)
		}
	}
	if b.Describe().Format != "my-client<v2>" {
		t.Fatal(b.Describe().Format)
	}
	d := b.Describe()
	d.Format = "fs"
	if _, err := s.buckets.Save(d, d.Revision); err == nil {
		t.Fatal("format overwritten")
	}
	for _, format := range []string{"", "\n", " fs", "fs ", "a\tb", strings.Repeat("a", 256), string([]byte{255})} {
		if validBucketFormat(format, false) {
			t.Fatalf("invalid format accepted: %q", format)
		}
	}
}

func TestBucketFormatConcurrentFirstOpen(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "format-race")
	p := userPrincipal(t, s)
	const n = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			format := "fs"
			if i%2 == 1 {
				format = "s3"
			}
			errs[i] = s.openBucketFormat(context.Background(), b.ID().String(), format, p)
		}()
	}
	close(start)
	wg.Wait()
	winner := b.Describe().Format
	if winner != "fs" && winner != "s3" {
		t.Fatal(winner)
	}
	for i, err := range errs {
		format := "fs"
		if i%2 == 1 {
			format = "s3"
		}
		if format == winner && err != nil || format != winner && statusOf(err) != 409 {
			t.Fatal(i, winner, err)
		}
	}
}

func TestBucketFormatCloneAndRestart(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil)
	s := node.node
	src, _ := createProtocolBucket(t, s, "format-source")
	dst, _ := createProtocolBucket(t, s, "format-clone")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.openBucketFormat(ctx, src.ID().String(), "memcp", userPrincipal(t, s)); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(src.ID()))
	e.ID(client.ID(dst.ID()))
	if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, s)); err != nil {
		t.Fatal(err)
	}
	if dst.Describe().Format != "memcp" {
		t.Fatal("clone lost format")
	}
	node = restartDaemonTestNode(t, node)
	for _, id := range []string{src.ID().String(), dst.ID().String()} {
		if err := node.node.openBucketFormat(ctx, id, "memcp", userPrincipal(t, node.node)); err != nil {
			t.Fatal("reopen", err)
		}
		if err := node.node.openBucketFormat(ctx, id, "fs", userPrincipal(t, node.node)); statusOf(err) != 409 {
			t.Fatal("restart lost format", err)
		}
	}
}

func TestBucketFormatClusterFirstOpen(t *testing.T) {
	a, b := startAutomaticDaemonPair(t, false)
	bucket, _ := createProtocolBucket(t, a.node, "cluster-format")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := make(chan struct{})
	actor := userPrincipal(t, a.node)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, node := range []*daemonTestNode{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			format := []string{"fs", "s3"}[i]
			errs[i] = node.node.openBucketFormat(ctx, bucket.ID().String(), format, actor)
		}()
	}
	close(start)
	wg.Wait()
	winner := bucket.Describe().Format
	if winner == "" {
		t.Fatal("no winner", errs)
	}
	for i, node := range []*daemonTestNode{a, b} {
		other, err := node.node.Bucket(bucket.ID())
		if err != nil || other.Describe().Format != winner {
			t.Fatal("cluster disagrees", err)
		}
		if []string{"fs", "s3"}[i] == winner && errs[i] != nil || []string{"fs", "s3"}[i] != winner && statusOf(errs[i]) != 409 {
			t.Fatal(winner, errs)
		}
	}
}

func TestBucketFormatReadOnlyAndS3(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "read-only-format")
	user := bucketProtocolUser(t, s)
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if err := s.openBucketFormat(context.Background(), b.ID().String(), "fs", principal{User: user}); err != nil {
		t.Fatal("read grant must allow opening", err)
	}
	if _, _, err := s.openS3(context.Background(), b, userPrincipal(t, s)); statusOf(err) != 409 {
		t.Fatal("S3 accepted FS bucket", err)
	}
	fresh, _ := createProtocolBucket(t, s, "s3-format")
	_, closeAPI, err := s.openS3(context.Background(), fresh, userPrincipal(t, s))
	if err != nil {
		t.Fatal(err)
	}
	closeAPI()
	if fresh.Describe().Format != "s3" {
		t.Fatal("S3 did not bind")
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	a := s.cfg.Auth.BootstrapAdmin
	_, err = client.Dial(context.Background(), host.URL, a.Username, a.Password, client.ID(fresh.ID()), "fs")
	var response *client.StatusError
	if !errors.As(err, &response) || response.Status != 409 {
		t.Fatal("SDK mismatch", err)
	}
}

func TestBucketFormatBackgroundAndInvalidUpgradeDoNotBind(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "unset-format")
	for _, check := range []func() error{
		func() error { return s.maintainS3(context.Background(), time.Now()) },
		func() error { return s.maintainS3Page(context.Background(), time.Now(), &s3MaintenanceCursor{}) },
	} {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	req, err := http.NewRequest(http.MethodGet, host.URL+"/api/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	a := s.cfg.Auth.BootstrapAdmin
	req.SetBasicAuth(a.Username, a.Password)
	req.Header.Set(client.BucketHeader, b.ID().String())
	req.Header.Set(client.FormatHeader, "custom-client")
	req.Header.Set("Sec-WebSocket-Protocol", "stqr.v2")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatal(response.StatusCode)
	}
	if b.Describe().Format != "" {
		t.Fatal("maintenance or malformed upgrade bound an unset bucket", b.Describe().Format)
	}
}

func TestRetiredDiskManagementIsNotAdvertised(t *testing.T) {
	s := testServer(t)
	profile := s.protocolVersion()
	for _, op := range []uint16{0x011e, 0x011f, 0x203e, 0x203f} {
		if _, ok := profile.Operations[fmt.Sprintf("%04x", op)]; ok {
			t.Fatalf("retired operation %04x advertised", op)
		}
		_, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: op, ID: client.NewRequestID()}, nil, userPrincipal(t, s))
		if status < 400 {
			t.Fatalf("retired operation %04x accepted: %d", op, status)
		}
	}
}
