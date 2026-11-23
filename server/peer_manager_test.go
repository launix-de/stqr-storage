// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

// This fixture uses only normal Open/config/advertisement. It does not call
// dialPeer/startPeerObservation or copy an accounts image into the candidate.
func openPendingPeer(t *testing.T, seed *Server, seedURL string) (*Server, *httptest.Server) {
	t.Helper()
	host := httptest.NewUnstartedServer(http.NotFoundHandler())
	seed.mu.RLock()
	c := copyConfig(seed.cfg)
	seed.mu.RUnlock()
	seedID := c.Node.ID
	c.Node.ID, c.Node.Name = newID(), "pending-node"
	c.Node.DataDir = "."
	c.Node.Listen = host.Listener.Addr().String()
	c.Node.Advertise = "http://" + host.Listener.Addr().String()
	c.Auth.BootstrapAdmin = BootstrapAdmin{}
	c.JoinPending = true
	c.BucketCatalogVersion = 0
	c.PeerDirectoryVersion = 0
	c.PlacementGroupsVersion = 0
	c.Volumes = nil
	c.Peers = []PeerConfig{{NodeID: seedID, URL: seedURL}}
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := saveConfig(path, c); err != nil {
		host.Close()
		t.Fatal(err)
	}
	s, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		host.Close()
		t.Fatal(err)
	}
	host.Config.Handler = s.Handler()
	host.Start()
	t.Cleanup(func() { host.Close(); s.Close() })
	return s, host
}

func TestAutomaticPeerDiscoveryPendingAdministrationAndRestart(t *testing.T) {
	seed := testServer(t)
	seedHost := httptest.NewServer(seed.Handler())
	defer seedHost.Close()
	pending, host := openPendingPeer(t, seed, seedHost.URL)
	id, _ := parseID(pending.running.Node.ID)
	awaitPeerObservation(t, seed, client.ID(id), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	seed.peerStatsMu.RLock()
	first := *seed.peerStats[client.ID(id)].snapshot
	seed.peerStatsMu.RUnlock()
	if first.NodeState != 4 || first.MembershipEpoch != 0 || first.Generation != 0 || first.MapRevision != 0 {
		t.Fatal("pending node advertised active authority")
	}
	seed.mu.RLock()
	known := append([]PeerConfig(nil), seed.cfg.Peers...)
	seed.mu.RUnlock()
	if len(known) != 1 || known[0].NodeID != id.String() || known[0].URL != host.URL {
		t.Fatal("seed did not persist advertised contact")
	}
	var accounts accountsImage
	if err := pending.readBlob("accounts", &accounts); err != nil || len(accounts.Users) != 0 {
		t.Fatal("pending node invented a bootstrap account", err)
	}
	r, _ := http.NewRequest(http.MethodGet, host.URL+"/api/session", nil)
	r.SetBasicAuth(seed.running.Auth.BootstrapAdmin.Username, seed.running.Auth.BootstrapAdmin.Password)
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("pending node accepted ordinary authentication", response.StatusCode)
	}
	conn, _, err := dialTestSocket(t, seedHost.URL, seed.running.Auth.BootstrapAdmin.Username, seed.running.Auth.BootstrapAdmin.Password, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	configReply := bucketReplyResult(t, requestMessage(t, conn, targetedAdmin(t, client.ID(id), client.NodeConfigGet, nil, client.NewRequestID())), 200)
	d := client.Decoder{B: configReply}
	var view struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal([]byte(d.Text()), &view); err != nil || d.Done() != nil || view.Mode != "joining" {
		t.Fatal("pending config unavailable through authenticated seed", err)
	}
	e := client.Encoder{}
	e.Text("candidate-volume")
	e.U8(0)
	e.Text("")
	e.U8(0)
	e.U64(1 << 20)
	bucketReplyResult(t, requestMessage(t, conn, targetedAdmin(t, client.ID(id), client.VolumeAdd, e.B, client.NewRequestID())), 201)
	pending.mu.RLock()
	beforeRestartGeneration := pending.cfg.Generation
	pending.mu.RUnlock()
	path := pending.configPath
	host.Close()
	if err := pending.Close(); err != nil {
		t.Fatal(err)
	}
	awaitPeerObservation(t, seed, client.ID(id), func(o *peerObservation) bool { return !o.online && o.snapshot != nil })
	// The same configured endpoint returns, without test-side dial/observer calls.
	restarted, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	listener, err := net.Listen("tcp", restarted.running.Node.Listen)
	if err != nil {
		t.Fatal(err)
	}
	next := httptest.NewUnstartedServer(restarted.Handler())
	next.Listener.Close()
	next.Listener = listener
	next.Start()
	defer next.Close()
	awaitPeerObservation(t, seed, client.ID(id), func(o *peerObservation) bool {
		return o.online && o.snapshot != nil && o.snapshot.BootID != first.BootID
	})
	restarted.mu.RLock()
	generation := restarted.cfg.Generation
	restarted.mu.RUnlock()
	if !restarted.joinPending() || generation != beforeRestartGeneration {
		t.Fatal("reconnect admitted the node or changed generation")
	}
}
