// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestDashboardMembershipProjection(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, node, "dashboard_membership_test.mjs").CombinedOutput()
	if err != nil {
		t.Fatalf("membership projection: %v\n%s", err, output)
	}
}

func TestDashboardForceDisconnect(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, node, "dashboard_force_test.mjs").CombinedOutput()
	if err != nil {
		t.Fatalf("exclusion dialog and codec: %v\n%s", err, output)
	}
}

func TestConfigViewKeepsOfflineCommittedMembers(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	check := func() {
		t.Helper()
		view := source.node.configView()
		members, ok := view["member_nodes"].([]string)
		epoch := source.node.currentSystem.Load().manifest.Membership.Epoch
		if !ok || len(members) != 2 || view["membership_epoch"] != strconv.FormatUint(epoch, 10) || view["mode"] != "cluster" {
			t.Fatal("missing committed membership", view["membership_epoch"], view["mode"])
		}
	}
	check()
	target.close()
	source = restartDaemonTestNode(t, source)
	check() // A fresh process has no peer report, but still has both members.
}

func TestCollectStatsReadsControlActivityWithoutLedgerLock(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil)
	controller := node.node.controller
	controller.mu.Lock()
	defer controller.mu.Unlock()
	l := node.node.control.ledger
	claim, err := l.beginRequest(controlAdministration, []client.ID{l.node})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.reserve(claim); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	result := make(chan client.StatsSnapshot, 1)
	go func() { result <- node.node.CollectStats(client.StatsFilter{}) }()
	var snapshot client.StatsSnapshot
	select {
	case snapshot = <-result:
		l.mu.Unlock()
	case <-time.After(time.Second):
		l.mu.Unlock()
		t.Fatal("statistics blocked on the control ledger")
	}
	if !snapshot.NeedRebuild || snapshot.ReasonBits&(1<<9) == 0 || snapshot.Coordinator != l.node || snapshot.Round != claim.Round {
		t.Fatal("pending control not shown in statistics")
	}
	proof, err := l.abortClaim(claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.cancelRequest(claim.Round, client.Certificate{proof}); err != nil {
		t.Fatal(err)
	}
	if activity := l.activity.Load(); activity == nil || activity.Busy {
		t.Fatal("finished control still shown as active")
	}
}

// Real daemon membership and browser requests; the fenced node and its files
// are disposable fixtures, never the user's running cluster.
func TestDashboardForceDisconnectBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	source, target := startAutomaticDaemonPair(t, false)
	targetID := target.node.cfg.Node.ID
	target.close()
	source = restartDaemonTestNode(t, source)
	admin := source.node.cfg.Auth.BootstrapAdmin
	input, err := json.Marshal(map[string]string{"url": source.node.cfg.Node.Advertise, "username": admin.Username, "password": admin.Password, "target": targetID})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "dashboard_force_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser exclusion: %v\n%s", err, output)
	}
	t.Log(string(output))
	membership := source.node.currentSystem.Load().manifest.Membership
	if len(membership.Members) != 1 || len(membership.Excluded) != 1 || membership.Excluded[0].String() != targetID {
		t.Fatal("browser exclusion was not committed")
	}
}
