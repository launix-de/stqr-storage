// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestDashboardDeletionPendingAndOrphanUsersBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	for _, fails := range []bool{false, true} {
		name := "commit"
		if fails {
			name = "retry"
		}
		t.Run(name, func(t *testing.T) {
			n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil)
			s := n.node
			users := map[string]client.ID{}
			for _, name := range []string{"first", "second", "orphan", "shared", "administrator", "kept-owner"} {
				users[name] = createDeleteTestUser(t, s, name)
			}
			e := client.Encoder{}
			e.ID(users["administrator"])
			e.Text("administrator")
			e.U8(1)
			e.Bool(false)
			if _, status := s.dispatchAs(userMessage(client.UserUpdate, e), nil, userPrincipal(t, s)); status != 200 {
				t.Fatal(status)
			}
			buckets := map[string]*Bucket{}
			for _, name := range []string{"first", "second", "drop", "keep", "other"} {
				buckets[name], _ = createProtocolBucket(t, s, name)
			}
			for _, name := range []string{"first", "second"} {
				grantUserDeleteBucket(t, s, buckets[name], users[name], BucketRead)
			}
			for _, name := range []string{"orphan", "shared", "administrator"} {
				grantUserDeleteBucket(t, s, buckets["drop"], users[name], BucketRead)
			}
			grantUserDeleteBucket(t, s, buckets["other"], users["shared"], BucketRead)
			grantUserDeleteBucket(t, s, buckets["keep"], users["kept-owner"], BucketRead)
			gate := &deleteBlockingParticipants{entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(gate.release) })
			s.controller.mu.Lock()
			gate.controlParticipants = s.controller.coordinator.participants
			s.controller.coordinator.participants = gate
			s.controller.mu.Unlock()
			gate.reject.Store(fails)
			gate.enabled.Store(true)
			handler := s.Handler()
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/_test/delete-entered" {
					select {
					case <-gate.entered:
						writeJSON(w, 200, true)
					default:
						writeJSON(w, 200, false)
					}
					return
				}
				if r.URL.Path == "/_test/release-delete" && r.Method == "POST" {
					release.Do(func() { close(gate.release) })
					writeJSON(w, 200, true)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer host.Close()
			a := s.cfg.Auth.BootstrapAdmin
			ids := map[string]string{}
			for name, id := range users {
				ids[name] = id.String()
			}
			bucketIDs := map[string]string{}
			for name, b := range buckets {
				bucketIDs[name] = b.ID().String()
			}
			input, _ := json.Marshal(map[string]any{"url": host.URL, "username": a.Username, "password": a.Password, "users": ids, "buckets": bucketIDs, "fails": fails})
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "node", "dashboard_delete_pending_browser.mjs")
			cmd.Stdin = bytes.NewReader(input)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("pending deletion UI: %v\n%s", err, output)
			}
			t.Log(string(output))
			for _, name := range []string{"first", "second", "orphan"} {
				if _, err := s.findAccount(name); err == nil {
					t.Fatal("account was not removed", name)
				}
			}
			for _, name := range []string{"shared", "administrator", "kept-owner"} {
				if _, err := s.findAccount(name); err != nil {
					t.Fatal("protected account removed", name, err)
				}
			}
			if buckets["second"].Describe().State != BucketActive {
				t.Fatal("user deletion opt-out lost")
			}
		})
	}
}
