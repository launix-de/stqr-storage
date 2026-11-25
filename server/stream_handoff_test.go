// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestStreamingPutFinishesWithMapRedirectAfterHandoff(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	for _, n := range []*daemonTestNode{source, target} {
		addTestVolume(t, n.node, "data", "hdd", 8<<20)
	}
	bucket, _ := createProtocolBucket(t, source.node, "stream-cutover")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	conn, _, err := dialTestSocket(t, source.node.cfg.Node.Advertise, admin.Username, admin.Password, bucket.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	write := func(m client.Message) {
		t.Helper()
		if err := conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(m)); err != nil {
			t.Fatal(err)
		}
	}
	read := func() client.Message {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, wire, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		m, err := client.DecodeMessage(wire)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	oid := client.ObjectID{0x90}
	before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID(oid))
	payload := bytes.Repeat([]byte{0x54}, 2<<20)
	digest := sha256.Sum256(payload)
	request := client.NewRequestID()
	e := client.Encoder{}
	e.ID(client.ID(bucket.ID()))
	e.B = append(e.B, oid[:]...)
	e.U64(before.Revision)
	e.U64(before.Entries[0].OwnerEpoch)
	e.ID(before.Entries[0].Runtime)
	e.Bool(false)
	e.Bool(false)
	e.Bool(false)
	encodeObjectTransfer(&e, 1, uint64(len(payload)), digest, nil)
	write(client.Message{Kind: client.Request, Opcode: 0x0102, ID: request, Body: e.B})
	chunk := func(part uint64) {
		e := client.Encoder{}
		e.ID(request)
		e.U64(part)
		e.U64(part << 20)
		e.Blob(payload[part<<20 : (part+1)<<20])
		write(client.Message{Kind: client.FireAndForget, Opcode: 5, ID: client.NewRequestID(), Body: e.B})
	}
	chunk(0)
	// The PING barrier uses the same ordered receive queue. Its reply proves
	// the upload was staged before handoff, without polling private maps.
	ping := client.NewRequestID()
	write(client.Message{Kind: client.Request, Opcode: client.Ping, ID: ping, Body: make([]byte, 8)})
	if reply := read(); reply.ID != ping {
		t.Fatal("upload did not stay staged", reply.Opcode)
	}
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	var destination volume.ID
	for _, v := range target.node.volumes {
		if !v.config.System {
			destination = v.store.Identity().VolumeID
		}
	}
	plan, err := planRangeMove(before, moving, target.node.control.ledger.node, destination, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	chunk(1)
	end := client.Encoder{}
	end.ID(request)
	end.U64(uint64(len(payload)))
	end.B = append(end.B, digest[:]...)
	write(client.Message{Kind: client.FireAndForget, Opcode: 6, ID: client.NewRequestID(), Body: end.B})
	reply := read()
	d := client.Decoder{B: reply.Body}
	status, headers := d.V(), d.Blob()
	h := client.Decoder{B: headers}
	count, tag, patchWire := h.V(), h.V(), h.Blob()
	patch, err := client.DecodeMapPatch(patchWire)
	if reply.ID != request || status != 301 || count != 1 || tag != 2 || h.Done() != nil || d.Done() != nil || err != nil || patch.Entries[0].Locations[0].Node != target.node.control.ledger.node {
		t.Fatal("stream completion lost its adaptive map redirect", status, err)
	}
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Put(ctx, oid, payload, client.PutOptions{RequestID: request}); err != nil {
		t.Fatal("same logical streaming PUT could not retry at new owner", err)
	}
	got, _, err := c.Get(ctx, oid)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("stream retry changed payload", err)
	}
}
