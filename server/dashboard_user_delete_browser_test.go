// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestDashboardUserDeleteBucketsBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	s := testServer(t)
	users := map[string]client.ID{}
	for _, name := range []string{"keep", "remove", "other"} {
		m, id := createUserMessage(name, "secret")
		if _, status := s.dispatchAs(m, nil, userPrincipal(t, s)); status != 201 {
			t.Fatal(status)
		}
		users[name] = id
	}
	kept, _ := createProtocolBucket(t, s, "keep-data")
	removed, _ := createProtocolBucket(t, s, "remove-data")
	shared, _ := createProtocolBucket(t, s, "shared-data")
	grantUserDeleteBucket(t, s, kept, users["keep"], BucketAllPermissions)
	grantUserDeleteBucket(t, s, removed, users["remove"], BucketAllPermissions)
	grantUserDeleteBucket(t, s, shared, users["remove"], BucketAllPermissions)
	grantUserDeleteBucket(t, s, shared, users["other"], BucketRead)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	input, _ := json.Marshal(map[string]string{"url": host.URL, "username": s.cfg.Auth.BootstrapAdmin.Username, "password": s.cfg.Auth.BootstrapAdmin.Password, "keep": users["keep"].String(), "remove": users["remove"].String()})
	cmd := exec.CommandContext(ctx, "node", "dashboard_user_delete_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("user deletion browser: %v\n%s", err, output)
	}
	for _, name := range []string{"keep", "remove"} {
		if _, err := s.findAccount(name); err == nil {
			t.Fatal("browser did not delete account", name)
		}
	}
	if kept.Describe().State != BucketActive || shared.Describe().State != BucketActive || removed.Describe().State != BucketDeleting {
		t.Fatal("browser deletion choice was not applied")
	}
	t.Log(string(output))
}
