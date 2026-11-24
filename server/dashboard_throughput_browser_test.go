// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestDashboardThroughputBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	n, _ := startAutomaticDaemonPair(t, false)
	v := addReplicaDataVolume(t, n.node, "throughput-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, n.node, "throughput-browser")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	actor := userPrincipal(t, n.node)
	id := volume.ObjectID{4}
	if _, err := b.Put(ctx, actor, id, make([]byte, 4096), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				_, _, _ = b.GetRange(ctx, actor, id, 0, 4096)
				_, _ = b.PutRange(ctx, actor, id, 0, make([]byte, 4096), volume.Options{RequestID: volume.ID(client.NewRequestID())})
			}
		}
	}()
	defer func() { cancel(); <-done }()
	admin := n.node.cfg.Auth.BootstrapAdmin
	input, _ := json.Marshal(map[string]string{"url": n.node.cfg.Node.Advertise, "username": admin.Username, "password": admin.Password, "bucket": b.ID().String(), "volume": v.Identity().VolumeID.String()})
	cmd := exec.CommandContext(ctx, "node", "dashboard_throughput_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("throughput browser: %v\n%s", err, out)
	}
	t.Log(string(out))
}
