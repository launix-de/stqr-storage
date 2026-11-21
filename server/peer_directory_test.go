// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"net"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func restartPeerAtAddress(t *testing.T, path string) (*Server, *httptest.Server) {
	t.Helper()
	s, err := Open(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", s.running.Node.Listen)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	host := httptest.NewUnstartedServer(s.Handler())
	host.Listener.Close()
	host.Listener = listener
	host.Start()
	t.Cleanup(func() { host.Close(); s.Close() })
	return s, host
}

func TestPeerDirectoryReconnectsWithoutOriginalInitiator(t *testing.T) {
	seed := testServer(t)
	seedHost := httptest.NewServer(seed.Handler())
	defer seedHost.Close()
	b, bh := openPendingPeer(t, seed, seedHost.URL)
	c, ch := openPendingPeer(t, seed, seedHost.URL)
	bID, _ := parseID(b.running.Node.ID)
	cID, _ := parseID(c.running.Node.ID)
	awaitPeerObservation(t, b, client.ID(cID), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	awaitPeerObservation(t, c, client.ID(bID), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	cluster, bpath, cpath := seed.running.ClusterID, b.configPath, c.configPath
	// Stop all three nodes. The initial seed stays stopped for the whole restart
	// check; no helper parameter or test-side dial is used by the returning nodes.
	seedHost.Close()
	seed.Close()
	bh.Close()
	b.Close()
	ch.Close()
	c.Close()
	// Deliberately erase the YAML contact caches. The system objects must win.
	for _, path := range []string{bpath, cpath} {
		cfg, err := loadConfig(path, Options{})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Peers = nil
		if err := saveConfig(path, cfg); err != nil {
			t.Fatal(err)
		}
	}
	cr, _ := restartPeerAtAddress(t, cpath)
	br, _ := restartPeerAtAddress(t, bpath)
	awaitPeerObservation(t, cr, client.ID(bID), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	awaitPeerObservation(t, br, client.ID(cID), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	if br.running.ClusterID != cluster || cr.running.ClusterID != cluster || br.running.Node.ID != bID.String() || cr.running.Node.ID != cID.String() {
		t.Fatal("restart forked the cluster or changed node identities")
	}
	for _, s := range []*Server{br, cr} {
		s.mu.RLock()
		peers := append([]PeerConfig(nil), s.cfg.Peers...)
		s.mu.RUnlock()
		if len(peers) != 2 {
			t.Fatal("restart lost discovered contacts", len(peers))
		}
	}
	t.Log("discovery/telemetry reconnected without the old initiator; membership/rebuild authority remains pending")
}

func TestPeerDirectoryUnionIsCanonicalAndRejectsIdentityCollision(t *testing.T) {
	a := []client.NodeAddress{{Node: client.ID{1}, Addresses: []string{"http://a:7419"}}}
	b := []client.NodeAddress{{Node: client.ID{2}, Addresses: []string{"http://b:7419"}}, {Node: client.ID{1}, Addresses: []string{"http://a:7419", "http://a2:7419"}}}
	ab, err := mergePeerAddresses(a, b)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := mergePeerAddresses(b, a)
	if err != nil || !reflect.DeepEqual(ab, ba) {
		t.Fatal("discovery depends on broadcast arrival order", err)
	}
	if _, err := mergePeerAddresses(a, []client.NodeAddress{{Node: client.ID{2}, Addresses: []string{"http://a:7419"}}}); err == nil {
		t.Fatal("address changed node identity")
	}
}
