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

func TestDashboardPeerVolumesBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	source := testServer(t)
	target := peerTestNode(t, source.cfg)
	// This fixture deliberately supplies the shared account image and connected
	// peer. It does not claim to exercise membership admission or system replication.
	var accounts accountsImage
	if err := source.readBlob("accounts", &accounts); err != nil {
		t.Fatal(err)
	}
	if err := target.writeBlob("accounts", accounts); err != nil {
		t.Fatal(err)
	}
	targetHost := httptest.NewServer(target.Handler())
	defer targetHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	node, _ := parseID(target.cfg.Node.ID)
	p, err := source.dialPeer(ctx, targetHost.URL, client.ID(node))
	if err != nil {
		t.Fatal(err)
	}
	if err = source.startPeerObservation(p); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(source.Handler())
	defer host.Close()
	input, err := json.Marshal(map[string]string{"url": host.URL, "username": source.cfg.Auth.BootstrapAdmin.Username, "password": source.cfg.Auth.BootstrapAdmin.Password, "target": node.String()})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "node", "dashboard_peer_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser check: %v\n%s", err, output)
	}
	t.Log(string(output))
	source.mu.RLock()
	for _, v := range source.volumes {
		if v.config.Name == "browser-remote" {
			t.Error("browser created remote volume on the source node")
		}
	}
	source.mu.RUnlock()
	target.mu.RLock()
	found := false
	for _, v := range target.volumes {
		if v.config.Name == "browser-remote" {
			found = v.store.Size() == 2<<20
		}
	}
	target.mu.RUnlock()
	if !found {
		t.Fatal("remote browser volume missing or not grown")
	}
}
