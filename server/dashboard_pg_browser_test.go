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

func TestDashboardCentralPGBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 8<<20)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	input, _ := json.Marshal(map[string]string{"url": host.URL, "username": s.cfg.Auth.BootstrapAdmin.Username, "password": s.cfg.Auth.BootstrapAdmin.Password})
	cmd := exec.CommandContext(ctx, "node", "dashboard_pg_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("PG browser: %v\n%s", err, output)
	}
	t.Log(string(output))
}
