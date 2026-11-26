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
)

func TestDashboardBucketCopyBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	s := testServer(t)
	b, _ := createProtocolBucket(t, s, "copy-my-id")
	ids := map[string]string{"copy-my-id": b.ID().String()}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	input, _ := json.Marshal(map[string]any{"url": host.URL, "username": a.Username, "password": a.Password, "ids": ids})
	cmd := exec.CommandContext(ctx, "node", "dashboard_bucket_copy_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bucket copy browser: %v\n%s", err, out)
	}
	t.Log(string(out))
}
