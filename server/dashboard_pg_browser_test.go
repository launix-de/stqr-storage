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
	"github.com/launix-de/stqr-storage/volume"
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

func TestDashboardPGReplicationBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	addTestVolume(t, target.node, "data", "hdd", 8<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	source.node.mu.RLock()
	credentials := source.node.cfg.Auth.BootstrapAdmin
	source.node.mu.RUnlock()
	input, _ := json.Marshal(map[string]string{"url": "http://" + source.listener.Addr().String(), "username": credentials.Username, "password": credentials.Password})
	cmd := exec.CommandContext(ctx, "node", "dashboard_pg_replication_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("PG replication browser: %v\n%s", err, output)
	}
	t.Log(string(output))
	var bucket *Bucket
	for _, description := range source.node.bucketSnapshots() {
		if description.Name == "browser-replicated-db" {
			if description.Policy.Redundancy.Mode != "replication" || description.Policy.Redundancy.Replicas != 2 || description.Policy.Redundancy.ACK != "all" || description.Policy.Compression != "gzip" {
				t.Fatalf("browser did not save the replicated policy: %+v", description.Policy)
			}
			bucket, err = source.node.Bucket(newVolumeID(description.ID))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if bucket == nil {
		t.Fatal("browser did not create its bucket")
	}
	actor := userPrincipal(t, source.node)
	object := volume.ObjectID{1}
	if _, err := bucket.Put(ctx, actor, object, []byte("browser-configured ALL replication"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	follower, err := target.node.Bucket(bucket.ID())
	if err != nil {
		t.Fatal(err)
	}
	data, _, err := follower.Get(ctx, actor, object)
	if err != nil || string(data) != "browser-configured ALL replication" {
		t.Fatalf("browser-configured replica unreadable: %q, %v", data, err)
	}
}
