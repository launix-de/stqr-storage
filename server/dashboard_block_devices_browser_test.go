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
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

func TestDashboardBlockDevicesBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	for _, mode := range []string{"writable", "readonly", "offline"} {
		readonly, offline := mode == "readonly", mode == "offline"
		t.Run(mode, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			addTestVolume(t, source.node, "browser-blocks", "hdd", 32<<20)
			b, _ := createProtocolBucket(t, source.node, "browser-disks")
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			a := source.node.cfg.Auth.BootstrapAdmin
			c, err := nbd.Open(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { c.Close() }()
			m, err := c.Create(ctx, nbd.Metadata{Name: "system", Size: nbd.StripeSize + 512})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.Create(ctx, nbd.Metadata{Name: "data <archive>", Size: 16 << 20}); err != nil {
				t.Fatal(err)
			}
			if readonly {
				c.Close()
				waitFsckReleased(t, b)
				c, err = nbd.Open(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), true)
				if err != nil {
					t.Fatal(err)
				}
			}
			if offline {
				c.Close()
				waitFsckReleased(t, b)
			}
			input, _ := json.Marshal(map[string]any{"url": target.node.cfg.Node.Advertise, "username": a.Username, "password": a.Password, "readonly": readonly, "offline": offline})
			cmd := exec.CommandContext(ctx, "node", "dashboard_block_devices_browser.mjs")
			cmd.Stdin = bytes.NewReader(input)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("block UI: %v\n%s", err, output)
			}
			t.Log(string(output))
			if offline {
				c, err = nbd.Open(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), true)
				if err != nil {
					t.Fatal(err)
				}
			}
			actual, err := c.Lookup(ctx, m.Name)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(9007199254741504)
			if readonly {
				want = m.Size
			}
			if actual.Size != want || actual.ID != m.ID {
				t.Fatal("browser size or identity", actual)
			}
		})
	}
}
