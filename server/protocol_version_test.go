package server

import (
	"github.com/launix-de/stqr-storage/client"
	"testing"
)

func TestProtocolVersionIgnoresBuildIdentity(t *testing.T) {
	s := testServer(t)
	local := s.protocolVersion()
	peer := local
	peer.Version = "different-package-version"
	peer.Revision = "ffffffffffffffffffffffffffffffffffffffff"
	peer.Release = 9999999999 // historical package builders emitted a timestamp
	id := client.ID{9}
	s.rememberVersion(id, peer)
	for node, status := range s.versionsView() {
		if status.Outdated || status.Incompatible || status.Restricted {
			t.Fatalf("build identity changed protocol status for %s: %+v", node, status)
		}
	}
	peer.ProtocolVersion = local.ProtocolVersion + 1
	s.rememberVersion(id, peer)
	if !s.versionsView()[s.cfg.Node.ID].Outdated || s.versionsView()[id.String()].Outdated {
		t.Fatal("manual protocol ordering ignored")
	}
	peer.ProtocolVersion = 0 // no trustworthy manual number in the old header
	peer.Release = 1
	s.rememberVersion(id, peer)
	if s.versionsView()[id.String()].Outdated {
		t.Fatal("legacy software release treated as protocol number")
	}
}
