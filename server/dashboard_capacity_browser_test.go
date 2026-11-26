// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDashboardGlobalCapacityBarsBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	source, target := startAutomaticDaemonPair(t, false)
	addReplicaDataVolume(t, source.node, "capacity-left", "hdd", 8<<20)
	addReplicaDataVolume(t, target.node, "capacity-right", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, source.node, "capacity-browser-data")
	payload := bytes.Repeat([]byte("capacity payload"), 32768)
	if _, err := b.Put(context.Background(), userPrincipal(t, source.node), volume.ObjectID{7}, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, s := range []*Server{source.node, target.node} {
		for _, v := range s.CollectStats(client.StatsFilter{}).Volumes {
			total += v.CapacityPhysical
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	input, _ := json.Marshal(map[string]string{"url": source.node.cfg.Node.Advertise, "username": admin.Username, "password": admin.Password, "capacity": strconv.FormatUint(total, 10), "minimumUsed": strconv.Itoa(len(payload))})
	cmd := exec.CommandContext(ctx, "node", "dashboard_capacity_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("global capacity UI: %v\n%s", err, out)
	}
	t.Log(string(out))
}
