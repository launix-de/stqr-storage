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

func runJSClient(t *testing.T, cfg map[string]any) {
	t.Helper()
	input, _ := json.Marshal(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "js_client_native.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("JS native: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("JS_NATIVE_OK")) {
		t.Fatal(string(out))
	}
	t.Log(string(out))
}
func TestJSClientNative(t *testing.T) {
	for _, routing := range []string{"direct", "gateway"} {
		t.Run(routing, func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "data", "hdd", 32<<20)
			b, _ := createProtocolBucket(t, s, "js-client")
			host := httptest.NewServer(s.Handler())
			defer host.Close()
			a := s.cfg.Auth.BootstrapAdmin
			runJSClient(t, map[string]any{"endpoint": host.URL, "username": a.Username, "password": a.Password, "bucket": b.Describe().Name, "bucketID": b.ID().String(), "routing": routing})
		})
	}
}

func TestJSClientCluster(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, target.node, "data", "hdd", 128<<20)
	b, _ := createProtocolBucket(t, source.node, "js-cluster")
	a := source.node.cfg.Auth.BootstrapAdmin
	for _, routing := range []string{"direct", "gateway"} {
		t.Run(routing, func(t *testing.T) {
			runJSClient(t, map[string]any{"endpoint": source.node.cfg.Node.Advertise, "username": a.Username, "password": a.Password, "bucket": b.Describe().Name, "bucketID": b.ID().String(), "routing": routing})
		})
	}
}

func TestJSClientBrowser(t *testing.T) {
	module := os.Getenv("STQR_TEST_BROWSER_MODULE")
	if module == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for browser integration")
	}
	source, target := startAutomaticDaemonPair(t)
	addTestVolume(t, target.node, "data", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, source.node, "js-browser")
	target.node.mu.Lock()
	target.node.cfg.BrowserOrigins = []string{source.node.cfg.Node.Advertise}
	target.node.mu.Unlock()
	a := source.node.cfg.Auth.BootstrapAdmin
	input, _ := json.Marshal(map[string]any{"url": source.node.cfg.Node.Advertise, "target": target.node.cfg.Node.Advertise, "username": a.Username, "password": a.Password, "bucket": b.ID().String()})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "js_client_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("JS browser: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("JS_BROWSER_OK")) {
		t.Fatal(string(out))
	}
	t.Log(string(out))
}
