// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

func TestJSClientBrowserPolicy(t *testing.T) {
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "browser-policy")
	a := s.cfg.Auth.BootstrapAdmin
	request := func(path, origin string, protocols []string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.RemoteAddr = "127.0.0.1:1234"
		req.SetBasicAuth(a.Username, a.Password)
		req.Header.Set("Origin", origin)
		req.Header.Set(client.BucketHeader, b.ID().String())
		req.Header.Set(client.FormatHeader, "opaque")
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 16)))
		req.Header.Set("Sec-WebSocket-Protocol", strings.Join(protocols, ", "))
		out := httptest.NewRecorder()
		s.Handler().ServeHTTP(out, req)
		return out
	}
	if got := request("http://localhost/api/ws", "http://untrusted.invalid", []string{"stqr.v2"}); got.Code != 403 {
		t.Fatal("unlisted origin", got.Code)
	}
	s.mu.Lock()
	s.cfg.BrowserOrigins = []string{"http://127.0.0.0/8"}
	s.mu.Unlock()
	if !s.browserOriginAllowed(httptest.NewRequest("GET", "http://localhost/api/ws", nil)) {
		t.Fatal("originless native call rejected")
	}
	req := httptest.NewRequest("OPTIONS", "http://localhost/api/bucket-connect", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	req.Header.Set("Origin", "http://127.1.2.3:8123")
	out := httptest.NewRecorder()
	s.Handler().ServeHTTP(out, req)
	if out.Code != 204 || out.Header().Get("Access-Control-Allow-Origin") != "http://127.1.2.3:8123" {
		t.Fatal("CIDR origin preflight", out.Code)
	}
	if got := request("http://localhost/api/ws", "http://127.1.2.3:8123", []string{"stqr.v2", "bucket." + base64.RawURLEncoding.EncodeToString([]byte(b.ID().String()))}); got.Code != 400 {
		t.Fatal("duplicate selector", got.Code)
	}
	if got := request("http://localhost/api/bucket-tunnel?node="+newID(), "http://127.1.2.3:8123", []string{"stqr.v2"}); got.Code != 503 {
		t.Fatal("unlisted target", got.Code, got.Body.String())
	}
	if b.Describe().Format != "" {
		t.Fatal("invalid requests bound a format")
	}
}

func TestJSClientTunnelDelegationScope(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, target.node, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "browser-scope")
	other, _ := createProtocolBucket(t, source.node, "other-browser-scope")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := userPrincipal(t, source.node)
	name, password, release, err := source.node.browserTunnelCredentials(ctx, p, b.ID().String(), "opaque")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	open := func(bucket, format, name, password string) int {
		h := http.Header{}
		h.Set(client.BucketHeader, bucket)
		h.Set(client.FormatHeader, format)
		h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(name+":"+password)))
		ws, r, err := (&websocket.Dialer{Subprotocols: []string{"stqr.v2"}}).Dial("ws"+strings.TrimPrefix(target.node.cfg.Node.Advertise, "http")+"/api/ws", h)
		if err == nil {
			ws.Close()
			return 101
		}
		if r == nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		return r.StatusCode
	}
	if got := open(other.ID().String(), "opaque", name, password); got != 401 {
		t.Fatal("other bucket", got)
	}
	if got := open(b.ID().String(), "different", name, password); got != 401 {
		t.Fatal("other format", got)
	}
	if got := open(b.ID().String(), "opaque", name, password+"00"); got != 401 {
		t.Fatal("forged credential", got)
	}
	if got := open(b.ID().String(), "opaque", name, password); got != 101 {
		t.Fatal("valid scoped credential", got)
	}
	release()
	if got := open(b.ID().String(), "opaque", name, password); got != 401 {
		t.Fatal("released grant", got)
	}
}

func TestJSClientTunnelRevocation(t *testing.T) {
	for _, action := range []string{"grant", "disabled"} {
		t.Run(action, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t)
			addTestVolume(t, target.node, "data", "hdd", 8<<20)
			b, _ := createProtocolBucket(t, source.node, "browser-revoke")
			user := bucketProtocolUser(t, source.node)
			uid, _ := parseID(user.ID)
			grant := client.Encoder{}
			grant.ID(client.ID(b.ID()))
			grant.ID(client.ID(uid))
			grant.U32(uint32(BucketAllPermissions))
			if _, status, _ := adminBucketMessage(t, source.node, client.BucketGrant, grant.B); status != 200 {
				t.Fatal("grant", status)
			}
			headers := http.Header{}
			headers.Set(client.BucketHeader, b.ID().String())
			headers.Set(client.FormatHeader, "opaque")
			headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user.Name+":bucket-test-password")))
			ws, response, err := (&websocket.Dialer{Subprotocols: []string{"stqr.v2"}}).Dial("ws"+strings.TrimPrefix(source.node.cfg.Node.Advertise, "http")+"/api/bucket-tunnel?node="+target.node.cfg.Node.ID, headers)
			if err != nil {
				if response != nil {
					t.Fatalf("open tunnel %d: %v", response.StatusCode, err)
				}
				t.Fatal(err)
			}
			defer ws.Close()
			if action == "grant" {
				revoke := client.Encoder{}
				revoke.ID(client.ID(b.ID()))
				revoke.ID(client.ID(uid))
				if _, status, _ := adminBucketMessage(t, source.node, client.BucketRevoke, revoke.B); status != 200 {
					t.Fatal("revoke", status)
				}
			} else {
				update := client.Encoder{}
				update.ID(client.ID(uid))
				update.Text(user.Name)
				update.U8(0)
				update.Bool(true)
				if _, status, _ := adminBucketMessage(t, source.node, client.UserUpdate, update.B); status != 200 {
					t.Fatal("disable", status)
				}
			}
			ws.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, _, err = ws.ReadMessage(); err == nil {
				t.Fatal("revoked tunnel remained live")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("revocation did not close tunnel")
			}
		})
	}
}
