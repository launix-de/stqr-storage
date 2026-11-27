// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/launix-de/stqr-storage/client"
	"testing"
)

func TestDirectedCOWCompatibilityActivation(t *testing.T) {
	first, _ := startAutomaticDaemonPair(t, false)
	// Use a fixed committed view and independent authenticated observations so
	// heartbeat arrivals cannot race the deliberately outdated test profiles.
	probe := &Server{controller: first.node.controller, running: first.node.running}
	view := first.node.currentSystem.Load()
	if view == nil || len(view.manifest.Membership.Members) != 2 {
		t.Fatal("two-member fixture missing")
	}
	probe.currentSystem.Store(view)
	current := probe.protocolVersion()
	var remote client.ID
	for _, m := range view.manifest.Membership.Members {
		if m.ID.String() != probe.running.Node.ID {
			remote = m.ID
		}
	}
	profile := func() protocolProfile {
		p := current
		p.Operations = make(map[string]operationSemantics, len(current.Operations))
		for k, v := range current.Operations {
			p.Operations[k] = v
		}
		return p
	}
	for _, tc := range []struct {
		name   string
		change func(*protocolProfile)
		ok     bool
	}{
		{"all-current", func(*protocolProfile) {}, true},
		{"old-control", func(p *protocolProfile) {
			p.ProtocolVersion = 3
			p.Operations[fmt.Sprintf("%04x", client.PeerControlApply)] = operationSemantics{Emit: 3, Min: 3, Max: 3}
		}, false},
		{"old-inheritance", func(p *protocolProfile) {
			p.Operations[fmt.Sprintf("%04x", peerCowCapture)] = operationSemantics{Emit: 2, Min: 2, Max: 2}
		}, false},
		{"old-catalog-transfer", func(p *protocolProfile) {
			p.Operations[fmt.Sprintf("%04x", peerSystemChunk)] = operationSemantics{Emit: 2, Min: 2, Max: 2}
		}, false},
		{"old-clone", func(p *protocolProfile) {
			p.Operations[fmt.Sprintf("%04x", client.BucketClone)] = operationSemantics{Emit: 3, Min: 3, Max: 3}
		}, false},
		{"legacy-unknown", func(p *protocolProfile) { *p = legacyProfile() }, false},
		{"future-build-compatible-operations", func(p *protocolProfile) { p.ProtocolVersion = ProtocolVersion + 1 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := profile()
			tc.change(&p)
			probe.versions.peers = map[string]protocolProfile{remote.String(): p}
			err := probe.requireDirectedCOWCompatibility(context.Background())
			if tc.ok && err != nil || !tc.ok && statusOf(err) != 426 {
				t.Fatal("activation gate", err)
			}
			if probe.currentSystem.Load() != view || probe.shortStopping.Load() {
				t.Fatal("compatibility gate changed cluster ownership/state")
			}
		})
	}
	probe.versions.peers = nil
	if statusOf(probe.requireDirectedCOWCompatibility(context.Background())) != 426 {
		t.Fatal("unknown member allowed schema-3 activation")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(probe.requireDirectedCOWCompatibility(ctx), context.Canceled) {
		t.Fatal("cancelled activation did not stop")
	}
}

func TestDirectedCOWRetainsSafeLegacyPeerOperations(t *testing.T) {
	current := (&Server{}).protocolVersion()
	previous := current
	previous.ProtocolVersion = 3
	previous.Operations = make(map[string]operationSemantics, len(current.Operations))
	for k, v := range current.Operations {
		previous.Operations[k] = v
	}
	for op, revision := range directedCOWRequiredSemantics() {
		previous.Operations[fmt.Sprintf("%04x", op)] = operationSemantics{Emit: revision - 1, Min: revision - 1, Max: revision - 1}
	}
	for _, op := range []uint16{client.Ping, client.StatsGet, 0x0100, 0x0101, 0x0102} {
		if err := checkPeerOperation(current, previous, op); err != nil {
			t.Fatalf("unchanged opcode %#x blocked: %v", op, err)
		}
	}
	for op := range directedCOWRequiredSemantics() {
		for _, pair := range [][2]protocolProfile{{current, previous}, {previous, current}} {
			if statusOf(checkPeerOperation(pair[0], pair[1], op)) != 426 {
				t.Fatalf("changed operation %#x accepted from old peer", op)
			}
		}
	}
	// Even a single-node process may not advertise old semantics and publish a
	// catalog it promised its peers that it did not understand.
	s := &Server{}
	s.compatibility.Store(&previous)
	if statusOf(s.requireDirectedCOWCompatibility(context.Background())) != 426 {
		t.Fatal("self-declared old semantics ignored")
	}
}
