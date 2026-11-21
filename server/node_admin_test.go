// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func targetedAdmin(t *testing.T, target client.ID, opcode uint16, body []byte, operation client.ID) client.Message {
	t.Helper()
	b, err := (client.NodeAdmin{Target: target, Opcode: opcode, Body: body}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return client.Message{Kind: client.Request, Opcode: client.NodeAdminRequest, ID: operation, Body: b}
}

func TestNodeAdminTargetsVolumesAndPreservesReceipts(t *testing.T) {
	source := testServer(t)
	target := peerTestNode(t, source.cfg)
	// Explicit fixture setup only. A real joining node must receive this account
	// image through the durable EVERYWHERE/control commit before admission.
	var accounts accountsImage
	if err := source.readBlob("accounts", &accounts); err != nil {
		t.Fatal(err)
	}
	if err := target.writeBlob("accounts", accounts); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	node, _ := parseID(target.cfg.Node.ID)
	p, err := source.dialPeer(ctx, host.URL, client.ID(node))
	if err != nil {
		t.Fatal(err)
	}
	if err = source.startPeerObservation(p); err != nil {
		t.Fatal(err)
	}
	sourceHost := httptest.NewServer(source.Handler())
	defer sourceHost.Close()
	conn, _, err := dialTestSocket(t, sourceHost.URL, source.cfg.Auth.BootstrapAdmin.Username, source.cfg.Auth.BootstrapAdmin.Password, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	e := client.Encoder{}
	e.Text("remote-only")
	e.U8(0)
	e.Text("")
	e.U8(0)
	e.U64(1 << 20)
	request := targetedAdmin(t, client.ID(node), client.VolumeAdd, e.B, client.NewRequestID())
	first := requestMessage(t, conn, request)
	result := bucketReplyResult(t, first, 201)
	ack := client.Decoder{B: result}
	operation := ack.ID()
	state, hasEntity := ack.U8(), ack.Bool()
	vid := ack.ID()
	if ack.Done() != nil || operation != request.ID || state != 2 || !hasEntity {
		t.Fatal("invalid delegated add acknowledgement")
	}
	second := requestMessage(t, conn, request)
	if !bytes.Equal(second.Body, first.Body) {
		t.Fatal("proxy retry did not preserve target receipt")
	}
	source.mu.RLock()
	sourceCount := len(source.volumes)
	source.mu.RUnlock()
	target.mu.RLock()
	targetCount := len(target.volumes)
	targetVolume := target.volumes[vid.String()]
	target.mu.RUnlock()
	if sourceCount != 1 || targetCount != 2 || targetVolume == nil || targetVolume.config.Name != "remote-only" {
		t.Fatal("remote add changed the wrong node or duplicated a volume")
	}
	if _, err = source.operation(operation.String()); err == nil {
		t.Fatal("target operation receipt was copied into source's local admin volume")
	}
	grow := client.Encoder{}
	grow.ID(vid)
	grow.U64(2 << 20)
	bucketReplyResult(t, requestMessage(t, conn, targetedAdmin(t, client.ID(node), client.VolumeGrow, grow.B, client.NewRequestID())), 200)
	if targetVolume.store.Size() != 2<<20 {
		t.Fatal("remote grow did not reach owning node")
	}
	status := client.Encoder{}
	status.ID(request.ID)
	bucketReplyResult(t, requestMessage(t, conn, targetedAdmin(t, client.ID(node), client.AdminOperationStatus, status.B, client.NewRequestID())), 200)
	// Missing target must not fall back to mutating the contacted node.
	bucketReplyResult(t, requestMessage(t, conn, targetedAdmin(t, client.ID{99}, client.VolumeAdd, e.B, client.NewRequestID())), 503)
	// Revalidate delegated rights at the target rather than trusting a role sent
	// by the source or cached at the originating dashboard connection.
	accounts.Users[0].Role = "USER"
	if err := target.writeBlob("accounts", accounts); err != nil {
		t.Fatal(err)
	}
	bucketReplyResult(t, requestMessage(t, conn, targetedAdmin(t, client.ID(node), client.NodeConfigGet, nil, client.NewRequestID())), 403)
}
