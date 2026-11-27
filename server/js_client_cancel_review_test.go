// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

func TestJSClientCancelledLeaseDoesNotLeaveExclusiveOwner(t *testing.T) {
	for _, mode := range []string{"pending", "lost-ack"} {
		t.Run(mode, func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "cancel-data", "hdd", 8<<20)
			b, _ := createProtocolBucket(t, s, "cancel-lease")
			backend := httptest.NewServer(s.Handler())
			defer backend.Close()
			target, _ := url.Parse(backend.URL)
			proxy := httputil.NewSingleHostReverseProxy(target)
			var held, dropped atomic.Bool
			release := func() {
				if held.Swap(false) {
					s.opMu.Unlock()
				}
			}
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/__js_review/") {
					switch strings.TrimPrefix(r.URL.Path, "/__js_review/") {
					case "hold":
						s.opMu.Lock()
						held.Store(true)
						writeJSON(w, 200, map[string]bool{"held": true})
						return
					case "release":
						release()
						writeJSON(w, 200, map[string]bool{"released": true})
						return
					}
					id, err := parseID(r.URL.Query().Get("id"))
					if err != nil {
						http.Error(w, "invalid request", 400)
						return
					}
					deadline := time.Now().Add(3 * time.Second)
					for {
						pending := false
						s.wsMu.Lock()
						for ws := range s.sockets {
							ws.objectMu.Lock()
							_, ok := ws.objectContexts[client.ID(id)]
							ws.objectMu.Unlock()
							pending = pending || ok
						}
						s.wsMu.Unlock()
						path := strings.TrimPrefix(r.URL.Path, "/__js_review/")
						ready := path == "admitted" && pending || path == "dropped" && dropped.Load() || path == "settled" && !pending && !b.underMaintenance()
						if ready || time.Now().After(deadline) {
							if !ready && path != "settled" {
								http.Error(w, "barrier not reached", 500)
								return
							}
							writeJSON(w, 200, map[string]bool{"maintenance": b.underMaintenance()})
							return
						}
						time.Sleep(time.Millisecond)
					}
				}
				if r.URL.Path != "/api/ws" {
					proxy.ServeHTTP(w, r)
					return
				}
				upstream, _, err := (&websocket.Dialer{Subprotocols: websocket.Subprotocols(r)}).Dial("ws"+strings.TrimPrefix(backend.URL, "http")+"/api/ws", nil)
				if err != nil {
					http.Error(w, "upstream failed", 502)
					return
				}
				defer upstream.Close()
				downstream, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}, CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				var workers sync.WaitGroup
				done := make(chan struct{}, 2)
				relay := func(dst, src *websocket.Conn, owner bool) {
					defer workers.Done()
					defer func() { done <- struct{}{} }()
					for {
						kind, raw, err := src.ReadMessage()
						if err != nil {
							return
						}
						if owner && mode == "lost-ack" {
							m, err := client.DecodeMessage(raw)
							if err == nil && m.Kind == client.Response && m.Opcode == client.BucketObjectLease && connectionTestStatus(m.Body) == 200 {
								dropped.Store(true)
								continue
							}
						}
						if dst.WriteMessage(kind, raw) != nil {
							return
						}
					}
				}
				workers.Add(2)
				go relay(upstream, downstream, false)
				go relay(downstream, upstream, true)
				<-done
				downstream.Close()
				upstream.Close()
				workers.Wait()
			}))
			defer host.Close()
			defer release()
			a := s.cfg.Auth.BootstrapAdmin
			input, _ := json.Marshal(map[string]string{"endpoint": host.URL, "username": a.Username, "password": a.Password, "bucket": b.ID().String(), "mode": mode})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "node", "js_client_cancel_review.mjs")
			cmd.Stdin = bytes.NewReader(input)
			output, err := cmd.CombinedOutput()
			if err != nil || !bytes.Contains(output, []byte("JS_CANCEL_OK")) {
				t.Fatalf("lease cancellation: %v\n%s", err, output)
			}
		})
	}
}
