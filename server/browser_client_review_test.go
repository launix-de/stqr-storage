// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

func TestJSClientUnicodeFormat(t *testing.T) {
	for _, routing := range []string{"direct", "gateway"} {
		t.Run(routing, func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "unicode-data", "hdd", 32<<20)
			b, _ := createProtocolBucket(t, s, "unicode-format")
			host := httptest.NewServer(s.Handler())
			defer host.Close()
			a := s.cfg.Auth.BootstrapAdmin
			const format = "形式-ä-📦"
			runJSClient(t, map[string]any{"endpoint": host.URL, "username": a.Username, "password": a.Password, "bucket": b.Describe().Name, "bucketID": b.ID().String(), "routing": routing, "format": format})
			if b.Describe().Format != format {
				t.Fatal("opaque Unicode format was changed")
			}
		})
	}
}

func TestJSClientTunnelExpiryWhileAdmissionBusy(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, target.node, "expiry-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "expiry-bucket")
	a := source.node.cfg.Auth.BootstrapAdmin
	req, err := http.NewRequest("GET", source.node.cfg.Node.Advertise+"/api/session", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(a.Username, a.Password)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("session status", response.StatusCode)
	}
	var session struct {
		CSRF string `json:"csrf_token"`
	}
	if err = json.NewDecoder(response.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	var cookie *http.Cookie
	for _, c := range response.Cookies() {
		if c.Name == "stqr_session" {
			cookie = c
		}
	}
	if cookie == nil || session.CSRF == "" {
		t.Fatal("missing authenticated session")
	}
	headers := http.Header{}
	headers.Set("Cookie", cookie.String())
	headers.Set(client.BucketHeader, b.ID().String())
	headers.Set(client.FormatHeader, "opaque")
	ws, _, err := (&websocket.Dialer{Subprotocols: []string{"stqr.v2", "csrf." + session.CSRF}}).Dial("ws"+strings.TrimPrefix(source.node.cfg.Node.Advertise, "http")+"/api/bucket-tunnel?node="+target.node.cfg.Node.ID, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	ping := func() error {
		return ws.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Request, Opcode: client.Ping, ID: client.NewRequestID(), Body: make([]byte, 8)}))
	}
	// Positive control: the authenticated relay is operational before expiration.
	if err = ping(); err != nil {
		t.Fatal(err)
	}
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := ws.ReadMessage()
	if err != nil {
		t.Fatal("live tunnel did not relay", err)
	}
	message, err := client.DecodeMessage(raw)
	if err != nil || connectionTestStatus(message.Body) != 200 {
		t.Fatal("live tunnel did not answer PING", err)
	}
	// Hold only the gateway's administrative barrier. The remote owner remains
	// usable, so expiration must close the delegation without waiting for it.
	source.node.opMu.Lock()
	defer source.node.opMu.Unlock()
	source.node.authMu.Lock()
	entry := source.node.sessions[cookie.Value]
	entry.Expires = time.Now().Add(2 * time.Second)
	source.node.sessions[cookie.Value] = entry
	source.node.authMu.Unlock()
	time.Sleep(4 * time.Second)
	if err = ping(); err != nil {
		return
	}
	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = ws.ReadMessage()
	if err == nil {
		t.Fatal("expired session still relays authenticated requests while gateway admission is busy")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("expired tunnel did not close while gateway admission is busy")
	}
}
