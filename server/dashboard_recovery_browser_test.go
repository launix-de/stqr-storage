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

	"github.com/launix-de/stqr-storage/client"
)

func TestDashboardRecoveryCodec(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, node, "dashboard_recovery_test.mjs").CombinedOutput()
	if err != nil {
		t.Fatalf("recovery codec: %v\n%s", err, output)
	}
}

func TestDashboardVolumeRecoveryBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	source, target := startAutomaticDaemonPair(t, false)
	v := addTestVolume(t, source.node, "rescued", "hdd", 4<<20)
	path := source.node.volumes[v.VolumeID].config.Path
	bucket, _ := createProtocolBucket(t, source.node, "browser-recovery")
	admin := source.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	object := client.ObjectID{7}
	if _, err := c.Put(ctx, object, []byte("browser rescued data"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	source.close()
	input, err := json.Marshal(map[string]string{"url": target.node.cfg.Node.Advertise, "username": admin.Username, "password": admin.Password, "source": source.node.cfg.Node.ID, "target": target.node.cfg.Node.ID, "path": path, "volume": v.VolumeID})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "node", "dashboard_recovery_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser recovery: %v\n%s", err, output)
	}
	t.Log(string(output))
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, _, err := c.Get(ctx, object)
	if err != nil || string(got) != "browser rescued data" {
		t.Fatal("browser takeover data", string(got), err)
	}
}
