// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestCompatibilityPolicy(t *testing.T) {
	baseline := legacyProfile()
	next := protocolProfile{Version: "next", Release: 2, WireMin: 1, WireMax: 1, Operations: map[string]operationSemantics{fmt.Sprintf("%04x", peerReplicaPrepare): {Emit: 2, Min: 2, Max: 2}}}
	for _, op := range []uint16{client.Ping, client.StatsGet, client.StatsSubscribe} {
		if err := checkPeerOperation(next, baseline, op); err != nil {
			t.Fatalf("safe opcode %x: %v", op, err)
		}
	}
	for _, pair := range [][2]protocolProfile{{baseline, next}, {next, baseline}} {
		if err := checkPeerOperation(pair[0], pair[1], peerReplicaPrepare); statusOf(err) != 426 {
			t.Fatalf("unsafe mutation allowed: %v", err)
		}
	}
	// A transitional implementation that can emit v1 and accept v1/v2 is safe.
	bridge := next
	bridge.Operations = map[string]operationSemantics{fmt.Sprintf("%04x", peerReplicaPrepare): {Emit: 1, Min: 1, Max: 2}}
	if err := checkPeerOperation(bridge, baseline, peerReplicaPrepare); err != nil {
		t.Fatal(err)
	}
	if err := checkPeerOperation(next, bridge, peerReplicaPrepare); statusOf(err) != 426 {
		t.Fatal("bridge still emits v1, cannot talk to v2-only receiver")
	}
}

func TestCompatibilityHeaderValidation(t *testing.T) {
	if p, err := decodeProfile(http.Header{}); err != nil || p.WireMin != 1 {
		t.Fatal(p, err)
	}
	for _, raw := range []string{`{`, `{"version":"x","wire_min":2,"wire_max":1}`, `{"version":"x","wire_min":1,"wire_max":1,"operations":{"101c":{"emit":2,"min":1,"max":1}}}`, `{"version":"x","wire_min":1,"wire_max":1,"operations":{"nope":{"emit":1,"min":1,"max":1}}}`} {
		header := http.Header{}
		header.Set(compatibilityHeader, raw)
		if _, err := decodeProfile(header); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

// Bypass the sending-side gate to prove an authenticated old/misbehaving peer
// cannot evade the receiving-side check by sending a frame directly.
func uncheckedVersionRequest(t *testing.T, p *peerConnection, op uint16) error {
	t.Helper()
	id := client.NewRequestID()
	pending := peerPending{opcode: op, result: make(chan peerReply, 1)}
	p.mu.Lock()
	p.pending[id] = pending
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }()
	select {
	case p.outgoing <- client.Message{Kind: client.Request, Opcode: op, ID: id}:
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked")
	}
	select {
	case reply := <-pending.result:
		return reply.err
	case <-time.After(3 * time.Second):
		t.Fatal("reply blocked")
		return nil
	}
}

func TestPeerVersionMixedSemantics(t *testing.T) {
	target := testServer(t)
	source := peerTestNode(t, target.cfg)
	old := legacyProfile()
	old.Release = 1
	source.compatibility.Store(&old)
	profile := target.protocolVersion()
	profile.Version = "new-COW-fixture"
	profile.Release = source.protocolVersion().Release + 1
	profile.Operations = map[string]operationSemantics{fmt.Sprintf("%04x", peerReplicaPrepare): {Emit: 2, Min: 2, Max: 2}}
	target.compatibility.Store(&profile)
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	id, _ := parseID(target.cfg.Node.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := source.dialPeer(ctx, host.URL, client.ID(id))
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	verifyPeerPing(t, p)
	verifyPeerSnapshot(t, p)
	if _, err := p.request(ctx, peerReplicaPrepare, nil); statusOf(err) != 426 {
		t.Fatalf("sending gate: %v", err)
	}
	if err := uncheckedVersionRequest(t, p, peerReplicaPrepare); statusOf(err) != 426 {
		t.Fatalf("receiving gate must reject before decoding bad body: %v", err)
	}
	view := source.versionsView()
	if !view[source.cfg.Node.ID].Outdated || view[target.cfg.Node.ID].Outdated || !view[target.cfg.Node.ID].Restricted {
		t.Fatalf("wrong mixed-version view: %+v", view)
	}
	// Returning to a compatible build clears the status on the next authenticated
	// exchange. Merely disconnecting does not forget the observed release.
	source.rememberVersion(client.ID(id), source.protocolVersion())
	if source.versionsView()[source.cfg.Node.ID].Outdated {
		t.Fatal("stale upgrade warning")
	}
}

func TestPeerWireConflictPreservesOwnership(t *testing.T) {
	target := testServer(t)
	source := peerTestNode(t, target.cfg)
	profile := target.protocolVersion()
	profile.Version = "wire2-fixture"
	profile.Release = source.protocolVersion().Release + 1
	profile.WireMin = 2
	profile.WireMax = 2
	target.compatibility.Store(&profile)
	before := target.configView()
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	id, _ := parseID(target.cfg.Node.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if p, err := source.dialPeer(ctx, host.URL, client.ID(id)); statusOf(err) != 426 {
		if p != nil {
			p.close()
		}
		t.Fatalf("wire conflict: %v", err)
	}
	status := source.versionsView()[target.cfg.Node.ID]
	if !status.Incompatible || !source.versionsView()[source.cfg.Node.ID].Outdated {
		t.Fatalf("missing diagnostic: %+v", status)
	}
	if target.cfg.Node.Draining != source.cfg.Node.Draining {
		t.Fatal("wire conflict changed node state")
	}
	after := target.configView()
	if fmt.Sprint(before["member_nodes"]) != fmt.Sprint(after["member_nodes"]) || before["membership_epoch"] != after["membership_epoch"] {
		t.Fatal("wire conflict changed committed membership")
	}
	if target.shortStopping.Load() || source.shortStopping.Load() {
		t.Fatal("version conflict stopped a daemon")
	}
	// Even an extreme protocol mismatch keeps the stable bootstrap usable.
	if err := checkPeerOperation(profile, legacyProfile(), client.Ping); err != nil {
		t.Fatal(err)
	}
}

func TestPeerVersionClaimsRequireIdentity(t *testing.T) {
	target := testServer(t)
	source := peerTestNode(t, target.cfg)
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if p, err := source.dialPeer(ctx, host.URL, client.ID{42}); err == nil {
		p.close()
		t.Fatal("accepted wrong identity")
	}
	if len(source.versionsView()) != 1 {
		t.Fatal("unbound response poisoned version observations")
	}
}

func TestCompatibilityUnknownAndWrappedOperations(t *testing.T) {
	baseline := legacyProfile()
	next := baseline
	next.Operations = map[string]operationSemantics{"203c": {Emit: 1, Min: 1, Max: 1}}
	if err := checkPeerOperation(baseline, next, 0x203c); statusOf(err) != 426 {
		t.Fatal("unknown legacy opcode was implicitly advertised")
	}
	if err := checkPeerOperation(next, next, 0x203c); err != nil {
		t.Fatal(err)
	}
	next.Operations = map[string]operationSemantics{fmt.Sprintf("%04x", client.VolumeGrow): {Emit: 2, Min: 2, Max: 2}}
	wrapped, err := (client.PeerAdmin{Actor: client.ID{1}, Username: "admin", Operation: client.ID{2}, Opcode: client.VolumeGrow}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPeerMessage(next, baseline, client.Message{Opcode: client.PeerAdminRequest, Body: wrapped}); statusOf(err) != 426 {
		t.Fatal("inner admin semantics bypassed")
	}
	target := testServer(t)
	target.compatibility.Store(&next)
	auth := &connectionAuthorization{clusterPeer: true, peerProfile: baseline}
	auth.peerVerified.Store(true)
	_, status := target.dispatchAs(client.Message{Kind: client.Request, Opcode: client.PeerAdminRequest, Body: wrapped}, nil, principal{connection: auth})
	if status != 426 {
		t.Fatalf("wrapped request reached administrator lookup: %d", status)
	}
	// Private frontend object requests must not lose their issuer's semantics
	// when they become bucket credentials instead of cluster-peer credentials.
	frontendProfile := next
	frontendProfile.Operations = map[string]operationSemantics{"0102": {Emit: 2, Min: 2, Max: 2}}
	target.compatibility.Store(&frontendProfile)
	frontend := &connectionAuthorization{frontendPermission: BucketWrite, peerProfile: baseline}
	_, status = target.dispatchAs(client.Message{Kind: client.Request, Opcode: 0x0102}, nil, principal{connection: frontend})
	if status != 426 {
		t.Fatalf("frontend PUT bypassed semantic gate: %d", status)
	}
}

func TestNodeVersionsSubscriptionIsOptIn(t *testing.T) {
	auth := &connectionAuthorization{admin: true}
	s := &Server{running: Config{Node: NodeConfig{ID: client.ID{1}.String()}}, sockets: map[*wsSession]struct{}{}}
	ws := &wsSession{server: s, authorization: auth, responses: make(chan []byte, 4)}
	s.sockets[ws] = struct{}{}
	s.publishVersions()
	if len(ws.responses) != 0 {
		t.Fatal("unsolicited new opcode sent to legacy admin")
	}
	p := principal{connection: auth}
	_, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: nodeVersionsMessage, Body: []byte{1}}, ws, p)
	if status != 200 {
		t.Fatal(status)
	}
	s.publishVersions()
	frame, err := client.DecodeMessage(<-ws.responses)
	if err != nil || frame.Kind != client.Push || frame.Opcode != nodeVersionsMessage {
		t.Fatal(frame, err)
	}
	s.publishVersions()
	if len(ws.responses) != 0 {
		t.Fatal("unchanged version view was pushed again")
	}
	_, status = s.dispatchAs(client.Message{Kind: client.Request, Opcode: nodeVersionsMessage, Body: []byte{0}}, ws, p)
	if status != 200 {
		t.Fatal(status)
	}
	s.publishVersions()
	if len(ws.responses) != 0 {
		t.Fatal("push after unsubscribe")
	}
	_, status = s.dispatchAs(client.Message{Kind: client.Request, Opcode: nodeVersionsMessage, Body: []byte{1}}, ws, principal{connection: &connectionAuthorization{}})
	if status != 403 {
		t.Fatal("non-admin received version telemetry", status)
	}
}
