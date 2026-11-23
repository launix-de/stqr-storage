// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestDashboardQuotaBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	n, _ := startAutomaticDaemonPair(t, false)
	addTestVolume(t, n.node, "data", "hdd", 8<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := n.node.cfg.Auth.BootstrapAdmin
	input, _ := json.Marshal(map[string]string{"url": n.node.cfg.Node.Advertise, "username": admin.Username, "password": admin.Password})
	cmd := exec.CommandContext(ctx, "node", "dashboard_quota_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("quota browser: %v\n%s", err, output)
	}
	t.Log(string(output))
}
