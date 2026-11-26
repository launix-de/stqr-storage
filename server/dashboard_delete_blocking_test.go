// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type deleteBlockingParticipants struct {
	controlParticipants
	reject           atomic.Bool
	enabled          atomic.Bool
	entered, release chan struct{}
	once             sync.Once
}

func (p *deleteBlockingParticipants) Reserve(ctx context.Context, node client.ID, claim controlClaim) error {
	if p.enabled.CompareAndSwap(true, false) {
		p.once.Do(func() { close(p.entered) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.reject.Swap(false) {
		return fail(503, "test: participant temporarily unavailable")
	}
	return p.controlParticipants.Reserve(ctx, node, claim)
}

// Delay a real global transaction at an explicit protocol boundary. This uses
// no real users and no sleeps to decide whether the deletion has started.
func TestDashboardDeleteKeepsSocketResponsive(t *testing.T) {
	n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil)
	create, user := createUserMessage("delete-fixture", "secret")
	if _, status := n.node.dispatchAs(create, nil, userPrincipal(t, n.node)); status != 201 {
		t.Fatal(status)
	}
	a := n.node.cfg.Auth.BootstrapAdmin
	socket, _, err := dialTestSocket(t, n.node.cfg.Node.Advertise, a.Username, a.Password, "")
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	other, _, err := dialTestSocket(t, n.node.cfg.Node.Advertise, a.Username, a.Password, "")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	gate := &deleteBlockingParticipants{entered: make(chan struct{}), release: make(chan struct{})}
	n.node.controller.mu.Lock()
	gate.controlParticipants = n.node.controller.coordinator.participants
	n.node.controller.coordinator.participants = gate
	n.node.controller.mu.Unlock()
	gate.enabled.Store(true)
	var release sync.Once
	defer release.Do(func() { close(gate.release) })
	yes := true
	remove := userDeleteRequest(user, &yes)
	if err = socket.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(remove)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("delete did not enter transaction")
	}
	ping := client.Message{Kind: client.Request, Opcode: client.Ping, ID: client.NewRequestID(), Body: make([]byte, 8)}
	if err = socket.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(ping)); err != nil {
		t.Fatal(err)
	}
	other.SetReadDeadline(time.Now().Add(time.Second))
	if reply := requestMessage(t, other, ping); reply.ID != ping.ID {
		t.Fatal("other socket stalled")
	}
	socket.SetReadDeadline(time.Now().Add(time.Second))
	if _, data, err := socket.ReadMessage(); err != nil {
		t.Fatal("delayed user deletion blocked the dashboard socket", err)
	} else {
		reply, err := client.DecodeMessage(data)
		if err != nil || reply.ID != ping.ID {
			t.Fatal("expected independent ping response", err)
		}
		bucketReplyResult(t, reply, 200)
	}
	release.Do(func() { close(gate.release) })
	t.Log("delayed USER_DELETE leaves PING responsive on the same dashboard socket")
}
