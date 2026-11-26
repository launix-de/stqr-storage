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

func TestDashboardBucketFormatBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	s := testServer(t)
	ids := map[string]string{}
	for _, format := range []string{"fs", "s3", "bd", "memcp", ""} {
		name := "format-" + format
		if format == "" {
			name = "format-fresh"
		}
		b, _ := createProtocolBucket(t, s, name)
		ids[name] = b.ID().String()
		if format != "" {
			if err := s.openBucketFormat(context.Background(), b.ID().String(), format, userPrincipal(t, s)); err != nil {
				t.Fatal(err)
			}
		}
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	input, _ := json.Marshal(map[string]any{"url": host.URL, "username": a.Username, "password": a.Password, "ids": ids})
	cmd := exec.CommandContext(ctx, "node", "dashboard_format_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("format browser: %v\n%s", err, out)
	}
	t.Log(string(out))
}
