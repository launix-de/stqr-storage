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
	"github.com/launix-de/stqr-storage/volume"
)

func TestDashboardStorageVolumeGrowthBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	source, target := startAutomaticDaemonPair(t, false)
	file := addTestVolume(t, source.node, "grow-storage-file", "hdd", 4<<20)
	other := addTestVolume(t, target.node, "leave-this-volume", "hdd", 4<<20)
	ram, err := source.node.AddVolume(newID(), AddVolumeRequest{Name: "grow-storage-ram", Class: "ram", Source: 2, SizeBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := createProtocolBucket(t, source.node, "storage-growth-data")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, target.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{0x63}
	payload := bytes.Repeat([]byte("confirmed storage-volume data\x00"), 1024)
	before, err := c.Put(ctx, oid, payload, client.PutOptions{RequestID: client.NewRequestID()})
	if err != nil {
		t.Fatal(err)
	}
	m := source.node.volumes[file.VolumeID]
	identity := m.store.Identity()
	path := m.config.Path
	if _, err = m.store.Stat(volume.Key{Bucket: b.ID(), Object: volume.ObjectID(oid)}); err != nil {
		t.Fatal("fixture data not on resized volume", err)
	}
	input, _ := json.Marshal(map[string]string{"url": target.node.cfg.Node.Advertise, "username": a.Username, "password": a.Password, "node": source.node.cfg.Node.ID, "file": file.VolumeID, "ram": ram.VolumeID})
	cmd := exec.CommandContext(ctx, "node", "dashboard_volume_growth_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("storage growth UI: %v\n%s", err, output)
	}
	t.Log(string(output))
	if m.store.Size() != 8<<20 || source.node.volumes[ram.VolumeID].store.Size() != 4<<20 || target.node.volumes[other.VolumeID].store.Size() != 4<<20 {
		t.Fatal("wrong volume or capacity changed")
	}
	actual := m.store.Identity()
	actual.Size = identity.Size
	if actual != identity {
		t.Fatal("growth changed volume identity")
	}
	st, err := os.Stat(path)
	if err != nil || st.Size() != 8<<20 {
		t.Fatal("storage file not grown", st, err)
	}
	got, after, err := c.Get(ctx, oid)
	if err != nil || !bytes.Equal(got, payload) || after.Length != before.Length {
		t.Fatal("growth changed confirmed objects", err)
	}
	c.Close()
	source = restartDaemonTestNode(t, source)
	if source.node.volumes[file.VolumeID].store.Size() != 8<<20 || source.node.volumes[file.VolumeID].config.SizeBytes != 8<<20 {
		t.Fatal("storage growth lost at restart")
	}
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, _, err = c.Get(ctx, oid)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("restart lost confirmed objects", err)
	}
}
