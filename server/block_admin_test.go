// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

func decodeBlockAdminPage(t *testing.T, body []byte) ([]nbd.Metadata, string, bool, bool) {
	t.Helper()
	d := client.Decoder{B: body}
	status := d.V()
	d.Blob()
	if status != 200 {
		t.Fatalf("block admin status %d: %x", status, body)
	}
	live, readonly := d.Bool(), d.Bool()
	d.ID()
	items, next, err := nbd.DecodeManagementResult(d.Blob())
	if err != nil || d.Done() != nil {
		t.Fatal(err, d.Err)
	}
	return items, next, live, readonly
}

func TestBlockDeviceAdminRequiresExplicitCompatibility(t *testing.T) {
	current := protocolProfile{WireMin: 1, WireMax: 1, Operations: currentOperationSemantics()}
	for _, op := range []uint16{client.BlockDeviceList, client.BlockDeviceResize, client.BucketBlockControlRegister, client.BucketBlockControl} {
		if err := checkPeerOperation(current, legacyProfile(), op); statusOf(err) != 426 {
			t.Fatal("legacy operation accepted", op, err)
		}
		if err := checkPeerOperation(current, current, op); err != nil {
			t.Fatal(op, err)
		}
	}
}

func TestBlockDeviceDashboardRoutesToLiveExporter(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "block-admin", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, source.node, "dashboard-disks")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := source.node.cfg.Auth.BootstrapAdmin
	c, err := nbd.Open(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), false)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	m, err := c.Create(ctx, nbd.Metadata{Name: "system", Size: 4096})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x71}, 512)
	if err = c.WriteAt(ctx, m, payload, 3584); err != nil {
		t.Fatal(err)
	}
	call := func(op uint16, r nbd.ManagementRequest) ([]byte, int) {
		t.Helper()
		return target.node.dispatchAs(client.Message{Kind: client.Request, Opcode: op, ID: client.NewRequestID(), Body: blockAdminBody(b.ID(), client.ID{}, r.MarshalBinary())}, nil, userPrincipal(t, source.node))
	}
	body, status := call(client.BlockDeviceList, nbd.ManagementRequest{Action: 1, Limit: 50})
	if status != 200 {
		t.Fatalf("list status %d: %x", status, body)
	}
	items, _, live, ro := decodeBlockAdminPage(t, body)
	if len(items) != 1 || items[0] != m || !live || ro {
		t.Fatal(items, live, ro)
	}
	r := nbd.ManagementRequest{Action: 2, Name: m.Name, Device: m.ID, Expected: m.Size, Size: 8192}
	body, status = call(client.BlockDeviceResize, r)
	if status != 200 {
		t.Fatalf("grow status %d: %x", status, body)
	}
	items, _, _, _ = decodeBlockAdminPage(t, body)
	if len(items) != 1 || items[0].Size != 8192 {
		t.Fatal(items)
	}
	// A lost dashboard response can be retried with the original expected size.
	if body, status = call(client.BlockDeviceResize, r); status != 200 {
		t.Fatalf("retry %d: %x", status, body)
	}
	if _, status = call(client.BlockDeviceResize, nbd.ManagementRequest{Action: 2, Name: m.Name, Device: m.ID, Expected: m.Size, Size: 16384}); status != 409 {
		t.Fatal("stale size accepted", status)
	}
	got := make([]byte, 1024)
	if err = c.ReadAt(ctx, items[0], got, 3584); err != nil || !bytes.Equal(got[:512], payload) || !bytes.Equal(got[512:], make([]byte, 512)) {
		t.Fatal("live data or zero tail damaged", err)
	}
	// The original lease remains exclusive and the exporter can continue writing.
	if other, e := client.Dial(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID())); e == nil {
		other.Close()
		t.Fatal("dashboard released exporter lease")
	}
	if err = c.WriteAt(ctx, items[0], payload, 7680); err != nil {
		t.Fatal(err)
	}
}

func TestBlockDeviceDashboardStoppedReadonlyAndLegacyOwners(t *testing.T) {
	source, _ := startAutomaticDaemonPair(t, false)
	s := source.node
	addTestVolume(t, s, "block-admin", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "dashboard-offline-disks")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	c, err := nbd.Open(ctx, s.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), false)
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Create(ctx, nbd.Metadata{Name: "disk", Size: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	waitFsckReleased(t, b)
	call := func(op uint16, r nbd.ManagementRequest) ([]byte, int) {
		return s.dispatchAs(client.Message{Kind: client.Request, Opcode: op, ID: client.NewRequestID(), Body: blockAdminBody(b.ID(), client.ID{}, r.MarshalBinary())}, nil, userPrincipal(t, s))
	}
	body, status := call(client.BlockDeviceList, nbd.ManagementRequest{Action: 1, Limit: 50})
	if status != 200 {
		t.Fatalf("stopped list %d: %x", status, body)
	}
	_, _, live, _ := decodeBlockAdminPage(t, body)
	if live {
		t.Fatal("stopped exporter reported live")
	}
	if body, status = call(client.BlockDeviceResize, nbd.ManagementRequest{Action: 2, Name: m.Name, Device: m.ID, Expected: m.Size, Size: 8192}); status != 200 {
		t.Fatalf("stopped resize %d: %x", status, body)
	}
	readonly, err := nbd.Open(ctx, s.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), true)
	if err != nil {
		t.Fatal(err)
	}
	body, status = call(client.BlockDeviceList, nbd.ManagementRequest{Action: 1, Limit: 50})
	if status != 200 {
		t.Fatalf("readonly list %d: %x", status, body)
	}
	_, _, live, ro := decodeBlockAdminPage(t, body)
	if !live || !ro {
		t.Fatal(live, ro)
	}
	if _, status = call(client.BlockDeviceResize, nbd.ManagementRequest{Action: 2, Name: m.Name, Device: m.ID, Expected: 8192, Size: 16384}); status != 403 {
		t.Fatal("readonly resized", status)
	}
	readonly.Close()
	waitFsckReleased(t, b)
	legacy, err := client.Dial(ctx, s.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if err = legacy.BeginBlockMaintenance(ctx, false); err != nil {
		t.Fatal(err)
	}
	if _, status = call(client.BlockDeviceList, nbd.ManagementRequest{Action: 1, Limit: 50}); status != 409 {
		t.Fatal("unregistered lease bypassed", status)
	}
	message := client.Message{Kind: client.Request, Opcode: client.BlockDeviceList, Body: blockAdminBody(b.ID(), client.ID{}, nbd.ManagementRequest{Action: 1, Limit: 50}.MarshalBinary())}
	p := userPrincipal(t, s)
	p.User.Role = "USER"
	if _, status = s.dispatchAs(message, nil, p); status != 403 {
		t.Fatal("nonadmin list", status)
	}
	p = userPrincipal(t, s)
	p.connection = &connectionAuthorization{bucket: b}
	if _, status = s.dispatchAs(message, nil, p); status != 403 {
		t.Fatal("bucket-scoped admin escaped scope", status)
	}
}
