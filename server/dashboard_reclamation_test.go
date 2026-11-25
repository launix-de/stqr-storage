// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os/exec"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestTicket32JavaScriptNativeCASReuse(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js required for JavaScript/native interoperability")
	}
	s := testServer(t)
	addTestVolume(t, s, "js-reuse", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "js-reuse")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{32}
	if _, err := c.Put(ctx, id, bytes.Repeat([]byte("a"), 8192), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	v := b.allVolumes()[0]
	before := v.Stats()
	input, err := json.Marshal(map[string]string{"url": host.URL, "username": admin.Username, "password": admin.Password, "bucket": b.ID().String(), "node": s.cfg.Node.ID, "object": hex.EncodeToString(id[:])})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, node, "--experimental-default-type=module", "dashboard_reclamation.mjs")
	cmd.Stdin = bytes.NewReader(input)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("JavaScript/native interoperability: %v\n%s", err, out)
	}
	got, _, err := c.Get(ctx, id)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{99}, 8192)) {
		t.Fatalf("Go could not read JS result: %v", err)
	}
	after := v.Stats()
	if after.Objects != before.Objects || after.DataPhysical != before.DataPhysical || after.LogicalBytes != before.LogicalBytes {
		t.Fatalf("JS CAS retained payload: before=%+v after=%+v", before, after)
	}
	if err := v.Scrub(); err != nil {
		t.Fatal(err)
	}
}
