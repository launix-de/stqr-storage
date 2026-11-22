// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

func TestClusterAdminSocketCanMutateUntilItsRoleIsRevoked(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	create, adminID := createUserMessage("cluster-admin", "cluster-admin-test-password")
	// The user schema's create role is explicit, not taken from the connection.
	e := client.Encoder{}
	e.ID(adminID)
	e.Text("cluster-admin")
	e.U8(1)
	e.Bool(false)
	e.Text("cluster-admin-test-password")
	create.Body = e.B
	if _, status := source.node.dispatchAs(create, nil, userPrincipal(t, source.node)); status != 201 {
		t.Fatal("create administrator", status)
	}
	conn, _, err := dialTestSocket(t, source.node.cfg.Node.Advertise, "cluster-admin", "cluster-admin-test-password", "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	child, _ := createUserMessage("created-through-admin-socket", "child-password")
	if err := conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(child)); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, wire, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	reply, err := client.DecodeMessage(wire)
	if err != nil || reply.ID != child.ID {
		t.Fatal("admin response", err)
	}
	d := client.Decoder{B: reply.Body}
	if status := d.V(); status != 201 {
		t.Fatal("revocable admin capability was not accepted", status)
	}
	if _, err := target.node.findAccount("created-through-admin-socket"); err != nil {
		t.Fatal("socket mutation was not replicated", err)
	}
	demote := updateUserRole(adminID, "cluster-admin", 0, false)
	if _, status := source.node.dispatchAs(demote, nil, userPrincipal(t, source.node)); status != 200 {
		t.Fatal("demotion", status)
	}
	forbidden, _ := createUserMessage("must-not-be-created", "unused-password")
	if err := conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(forbidden)); err == nil {
		_, wire, err = conn.ReadMessage()
		if err == nil {
			response, decodeErr := client.DecodeMessage(wire)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			r := client.Decoder{B: response.Body}
			if status := r.V(); status != 403 {
				t.Fatal("revoked admin socket accepted a mutation", status)
			}
		}
	}
	if _, err := source.node.findAccount("must-not-be-created"); err == nil {
		t.Fatal("revoked administrator changed source accounts")
	}
	if _, err := target.node.findAccount("must-not-be-created"); err == nil {
		t.Fatal("revoked administrator changed peer accounts")
	}
}
