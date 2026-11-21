// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

func awaitPeerObservation(t *testing.T, s *Server, id client.ID, ready func(*peerObservation) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s.peerStatsMu.RLock()
		p := s.peerStats[id]
		ok := p != nil && ready(p)
		s.peerStatsMu.RUnlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("peer observation did not reach expected state")
}

func TestPeerStatsPushDashboardAndReconnect(t *testing.T) {
	target := testServer(t)
	source := peerTestNode(t, target.cfg)
	targetHost := httptest.NewServer(target.Handler())
	defer targetHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	remoteID, _ := parseID(target.cfg.Node.ID)
	id := client.ID(remoteID)
	p, err := source.dialPeer(ctx, targetHost.URL, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.startPeerObservation(p); err != nil {
		t.Fatal(err)
	}
	awaitPeerObservation(t, source, id, func(o *peerObservation) bool { return o.snapshot != nil })
	source.peerStatsMu.RLock()
	first := *source.peerStats[id].snapshot
	source.peerStatsMu.RUnlock()
	// Wait for an actual periodic push, rather than an explicit stats request.
	awaitPeerObservation(t, source, id, func(o *peerObservation) bool { return o.snapshot.Sequence > first.Sequence })
	host := httptest.NewServer(source.Handler())
	defer host.Close()
	conn, _, err := dialTestSocket(t, host.URL, source.cfg.Auth.BootstrapAdmin.Username, source.cfg.Auth.BootstrapAdmin.Password, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	sub, request := client.NewRequestID(), client.NewRequestID()
	e := client.Encoder{}
	e.ID(sub)
	e.U8(1)
	e.V(0)
	e.V(0)
	e.V(0)
	e.Bool(false)
	e.U32(3000)
	if err = conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Request, Opcode: client.StatsSubscribe, ID: request, Body: e.B})); err != nil {
		t.Fatal(err)
	}
	assembler := client.NewStatsAssembler(2)
	nodes := map[client.ID]bool{}
	acked := false
	for len(nodes) < 2 || !acked {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		m, err := client.DecodeMessage(raw)
		if err != nil {
			t.Fatal(err)
		}
		if m.Kind == client.Response {
			bucketReplyResult(t, m, 200)
			acked = m.ID == request
			continue
		}
		subscription, part, err := client.DecodeStatsPush(m.Body)
		if err != nil || subscription != sub {
			t.Fatal("dashboard push", err)
		}
		snapshot, err := assembler.Add(part)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot != nil {
			nodes[snapshot.Node] = true
		}
	}
	if !nodes[id] {
		t.Fatal("dashboard omitted remote node")
	}
	p.close()
	awaitPeerObservation(t, source, id, func(o *peerObservation) bool { return !o.online && o.snapshot != nil })
	if got := source.statsSnapshots(client.StatsFilter{Scope: 1}); len(got) != 2 {
		t.Fatal("transport loss removed peer statistics")
	}
	if got := source.statsSnapshots(client.StatsFilter{Scope: 0}); len(got) != 1 {
		t.Fatal("LOCAL scope forwarded remote telemetry")
	}
	path := target.configPath
	targetHost.Close()
	if err = target.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	nextHost := httptest.NewServer(restarted.Handler())
	defer nextHost.Close()
	next, err := source.dialPeer(ctx, nextHost.URL, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.startPeerObservation(next); err != nil {
		t.Fatal(err)
	}
	awaitPeerObservation(t, source, id, func(o *peerObservation) bool {
		return o.online && o.snapshot != nil && o.snapshot.BootID != first.BootID
	})
	if source.cfg.Generation != 1 || restarted.cfg.Generation != 1 {
		t.Fatal("heartbeat/reconnect changed placement generation")
	}
}
