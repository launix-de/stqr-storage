// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"gopkg.in/yaml.v3"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// These are isolated transport fixtures, not an admitted storage cluster.
func peerTestNode(t *testing.T, base Config) *Server {
	t.Helper()
	c := copyConfig(base)
	c.Node.ID = newID()
	c.Node.Name = "peer-transport-fixture"
	c.Node.DataDir = "."
	c.Node.Advertise = ""
	c.Volumes = nil
	c.Peers = nil
	c.VolumesDir = "volumes"
	c.BucketCatalogVersion = 0
	path := filepath.Join(t.TempDir(), "node.yaml")
	if e := saveConfig(path, c); e != nil {
		t.Fatal(e)
	}
	s, e := Open(Options{ConfigPath: path})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func peerStatsFilter(e *client.Encoder) { e.U8(0); e.V(0); e.V(0); e.V(0); e.Bool(false) }
func verifyPeerPing(t *testing.T, p *peerConnection) {
	t.Helper()
	e := client.Encoder{}
	e.U64(123)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := p.request(ctx, client.Ping, e.B)
	if err != nil || !bytes.Equal(b, e.B) {
		t.Fatal("peer ping", err)
	}
}
func verifyPeerSnapshot(t *testing.T, p *peerConnection) client.ID {
	var bootID client.ID
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	subscription := client.NewRequestID()
	e := client.Encoder{}
	e.ID(subscription)
	peerStatsFilter(&e)
	e.U32(3000)
	if _, err := p.request(ctx, client.StatsSubscribe, e.B); err != nil {
		t.Fatal(err)
	}
	var parts [][]byte
	var digest [32]byte
	var sequence uint64
	for {
		m, err := p.nextPush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if m.Opcode != client.StatsSnapshotMessage {
			t.Fatal("unexpected push", m.Opcode)
		}
		d := client.Decoder{B: m.Body}
		id, source, boot, seq := d.ID(), d.ID(), d.ID(), d.U64()
		index, count := d.U32(), d.U32()
		var sum [32]byte
		copy(sum[:], d.Take(32))
		data := d.Blob()
		if d.Done() != nil || id != subscription || source != p.Node || boot == (client.ID{}) || count == 0 || count > 64 || index >= count {
			t.Fatal("invalid native peer snapshot")
		}
		if parts == nil {
			parts = make([][]byte, count)
			digest = sum
			sequence = seq
		}
		if len(parts) != int(count) || digest != sum || sequence != seq {
			t.Fatal("mixed snapshot")
		}
		parts[index] = append([]byte(nil), data...)
		bootID = boot
		complete := true
		for _, part := range parts {
			complete = complete && part != nil
		}
		if !complete {
			continue
		}
		data = bytes.Join(parts, nil)
		if sha256.Sum256(data) != digest {
			t.Fatal("snapshot digest")
		}
		snapshot := client.Decoder{B: data}
		if snapshot.U16() != 1 || snapshot.ID() != p.Node {
			t.Fatal("snapshot identity/schema")
		}
		break
	}
	e = client.Encoder{}
	e.ID(subscription)
	if _, err := p.request(ctx, client.StatsUnsubscribe, e.B); err != nil {
		t.Fatal(err)
	}
	return bootID
}
func TestPeerNativeIdentityPingAndStats(t *testing.T) {
	target := testServer(t)
	source := peerTestNode(t, target.cfg)
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	node, _ := parseID(target.cfg.Node.ID)
	p, err := source.dialPeer(ctx, host.URL, client.ID(node))
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	verifyPeerPing(t, p)
	verifyPeerSnapshot(t, p)
	if len(p.Membership.Members) != 1 {
		t.Fatal("transport handshake admitted a member")
	}
	if _, err = p.request(ctx, client.UserList, nil); statusOf(err) != 403 {
		t.Fatal("peer gained administrative rights", err)
	}
	if _, err = p.request(ctx, client.RebuildOfferMessage, nil); statusOf(err) != 501 {
		t.Fatal("unimplemented rebuild received an ACK", err)
	}
}
func TestPeerNativeRejectsIdentityAndClusterMismatch(t *testing.T) {
	target := testServer(t)
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	source := peerTestNode(t, target.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if p, err := source.dialPeer(ctx, host.URL, client.ID{99}); statusOf(err) != 409 {
		if p != nil {
			p.close()
		}
		t.Fatal("unexpected node accepted", err)
	}
	foreign := copyConfig(target.cfg)
	foreign.ClusterID = newID()
	other := peerTestNode(t, foreign)
	if p, err := other.dialPeer(ctx, host.URL, client.ID{}); statusOf(err) != 409 {
		if p != nil {
			p.close()
		}
		t.Fatal("foreign cluster accepted", err)
	}
	bad := copyConfig(target.cfg)
	bad.Auth.ClusterToken = "wrong-token"
	other = peerTestNode(t, bad)
	if p, err := other.dialPeer(ctx, host.URL, client.ID{}); statusOf(err) != 401 {
		if p != nil {
			p.close()
		}
		t.Fatal("wrong token accepted", err)
	}
	p, err := source.dialPeer(ctx, host.URL, client.ID{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	cluster, _ := parseID(target.cfg.ClusterID)
	body, _ := (client.NodeHello{Cluster: client.ID(cluster), Node: client.ID{88}, Membership: 1, Boot: source.boot}).MarshalBinary()
	if _, err = p.request(ctx, client.NodeHelloMessage, body); statusOf(err) != 403 {
		t.Fatal("spoofed authenticated node accepted", err)
	}
}
func TestPeerNativeRequiresBindingBeforeStats(t *testing.T) {
	target := testServer(t)
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	node := "node-" + newID()
	conn, _, err := dialTestSocket(t, host.URL, node, target.cfg.Auth.ClusterToken, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	e := client.Encoder{}
	peerStatsFilter(&e)
	bucketReplyResult(t, requestMessage(t, conn, client.Message{Kind: client.Request, Opcode: client.StatsGet, ID: client.NewRequestID(), Body: e.B}), 403)
}
func TestPeerNativeDisconnectReconnectAndMultiplex(t *testing.T) {
	target := testServer(t)
	source := peerTestNode(t, target.cfg)
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := source.dialPeer(ctx, host.URL, client.ID{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				e := client.Encoder{}
				e.U64(uint64(n*100 + j))
				b, err := p.request(ctx, client.Ping, e.B)
				if err != nil {
					failures <- err
					return
				}
				if !bytes.Equal(b, e.B) {
					failures <- errors.New("correlation mismatch")
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	before := target.cfg.Generation
	target.wsMu.Lock()
	for ws := range target.sockets {
		ws.close()
	}
	target.wsMu.Unlock()
	select {
	case <-p.ctx.Done():
	case <-ctx.Done():
		t.Fatal("disconnect did not close peer")
	}
	if target.cfg.Generation != before {
		t.Fatal("disconnect changed generation")
	}
	p, err = source.dialPeer(ctx, host.URL, p.Node)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	verifyPeerPing(t, p)
}

// Opt-in real second-host check. The URL must reach a separately running fixture
// through an SSH forward. Its configuration must be a dedicated disposable file.
func TestPeerExternalTransport(t *testing.T) {
	path, address := os.Getenv("STQR_TEST_PEER_CONFIG"), os.Getenv("STQR_TEST_PEER_URL")
	if path == "" && address == "" {
		t.Skip("requires dedicated external peer fixture")
	}
	if path == "" || address == "" {
		t.Fatal("both external peer settings are required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err = yaml.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	source := peerTestNode(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	expected, _ := parseID(cfg.Node.ID)
	p, err := source.dialPeer(ctx, address, client.ID(expected))
	if err != nil {
		t.Fatal(err)
	}
	verifyPeerPing(t, p)
	beforeBoot := verifyPeerSnapshot(t, p)
	restart := os.Getenv("STQR_TEST_PEER_RESTART_PM2") == "1"
	if restart {
		if cfg.Node.Name != "peer-transport-fixture" {
			t.Fatal("restart requires the named disposable fixture")
		}
		if output, e := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "stqr", "pm2", "restart", "stqr-peer-transport").CombinedOutput(); e != nil {
			t.Fatalf("fixture restart: %v (%s)", e, output)
		}
		select {
		case <-p.ctx.Done():
		case <-ctx.Done():
			t.Fatal("remote process restart did not close the peer")
		}
	} else {
		p.close()
	}
	for {
		p, err = source.dialPeer(ctx, address, client.ID(expected))
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	defer p.close()
	verifyPeerPing(t, p)
	afterBoot := verifyPeerSnapshot(t, p)
	if restart && beforeBoot == afterBoot {
		t.Fatal("remote process BootID did not change")
	}
	t.Log("native NODE_HELLO, correlated PING, pushed snapshot digest and reconnect verified against external fixture")
}
