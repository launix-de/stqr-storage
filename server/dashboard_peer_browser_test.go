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
	host := httptest.NewServer(source.Handler())
	defer host.Close()
	target, _ := openPendingPeer(t, source, host.URL)
	id, _ := parseID(target.running.Node.ID)
	awaitPeerObservation(t, source, client.ID(id), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	// Upgrade only the advertised software release, retaining safe v1 semantics.
	// Reconnect performs the real authenticated discovery; the browser must show
	// the older LOCAL node in red without disabling its ordinary administration.
	profile := target.protocolVersion()
	profile.Version = "browser-newer"
	profile.Release = source.protocolVersion().Release + 1
	target.compatibility.Store(&profile)
	source.peerStatsMu.RLock()
	connection := source.peerStats[client.ID(id)].connection
	source.peerStatsMu.RUnlock()
	connection.close()
	awaitPeerObservation(t, source, client.ID(id), func(o *peerObservation) bool {
		return o.online && o.snapshot != nil && o.connection.Profile.Release == profile.Release
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	input, err := json.Marshal(map[string]string{"url": host.URL, "username": source.running.Auth.BootstrapAdmin.Username, "password": source.running.Auth.BootstrapAdmin.Password, "target": id.String()})
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
