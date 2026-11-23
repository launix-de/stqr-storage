// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDashboardEndlessPagesBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE to the local Playwright module")
	}
	s := testServer(t)
	var accounts accountsImage
	if err := s.readBlob("accounts", &accounts); err != nil {
		t.Fatal(err)
	}
	hash := accounts.Users[0].PasswordHash
	for i := 0; i < 220; i++ {
		accounts.Users = append(accounts.Users, account{ID: volume.ID(client.NewRequestID()).String(), Name: fmt.Sprintf("tenant-user-%03d", i), Role: "USER", PasswordHash: hash})
		b := newTestBucket(t, fmt.Sprintf("tenant-bucket-%03d", i))
		if _, err := s.buckets.Save(b.Describe(), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.writeBlob("accounts", accounts); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	input, _ := json.Marshal(map[string]string{"url": host.URL, "username": s.cfg.Auth.BootstrapAdmin.Username, "password": s.cfg.Auth.BootstrapAdmin.Password})
	cmd := exec.CommandContext(ctx, "node", "dashboard_pages_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("endless browser: %v\n%s", err, output)
	}
	t.Log(string(output))
}
