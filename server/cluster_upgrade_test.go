// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

// The previous deployed build already had discovery and local catalogs, but no
// control/range journals. Upgrade must retain the original data and join-pending
// identity; discovery itself must not become a second independent membership.
func TestDaemonUpgradeFromDiscoveryOnlyCluster(t *testing.T) {
	legacy := func(path string, cfg *Config) *daemonTestNode {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if cfg == nil {
			value, e := loadConfig(path, Options{})
			if e != nil {
				t.Fatal(e)
			}
			cfg = &value
		}
		cfg.Node.Listen = ln.Addr().String()
		cfg.Node.Advertise = "http://" + ln.Addr().String()
		if err := saveConfig(path, *cfg); err != nil {
			t.Fatal(err)
		}
		s, err := openTransportTestNode(Options{ConfigPath: path})
		if err != nil {
			ln.Close()
			t.Fatal(err)
		}
		h := &http.Server{Handler: s.Handler()}
		go h.Serve(ln)
		d := &daemonTestNode{node: s, http: h, listener: ln, path: path}
		t.Cleanup(d.close)
		return d
	}
	root := t.TempDir()
	source := legacy(filepath.Join(root, "source", "node.yaml"), nil)
	local := addTestVolume(t, source.node, "original-data", "hdd", 4<<20)
	draft := newTestBucket(t, "upgrade-data")
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	oid := client.ObjectID{42}
	data := bytes.Repeat([]byte("existing acknowledged data"), 200)
	before, err := c.Put(ctx, oid, data, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	config := copyConfig(source.node.cfg)
	config.Node.ID = newID()
	config.Node.DataDir = "."
	config.Volumes = nil
	config.JoinPending = true
	config.Peers = []PeerConfig{{NodeID: source.node.cfg.Node.ID, URL: source.node.cfg.Node.Advertise}}
	config.ControlVersion, config.RangeVersion, config.QuotaVersion, config.PeerDirectoryVersion, config.BucketCatalogVersion, config.PlacementGroupsVersion = 0, 0, 0, 0, 0, 0
	target := legacy(filepath.Join(root, "target", "node.yaml"), &config)
	targetID, _ := parseID(target.node.cfg.Node.ID)
	awaitPeerObservation(t, source.node, client.ID(targetID), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	source.close()
	target.close()
	// No connect flag on either restart; return the old joiner first.
	target = restartDaemonTestNode(t, target)
	source = restartDaemonTestNode(t, source)
	deadline := time.Now().Add(15 * time.Second)
	for {
		a, b := source.node.currentSystem.Load(), target.node.currentSystem.Load()
		if a != nil && b != nil && a.root == b.root && len(a.manifest.Membership.Members) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("legacy upgrade did not admit discovered peer")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if source.node.volumes[local.VolumeID] == nil || target.node.volumes[local.VolumeID] != nil {
		t.Fatal("upgrade changed local volume ownership")
	}
	c, err = client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(draft.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, info, err := c.Get(ctx, oid)
	if err != nil || !bytes.Equal(got, data) || info.Length != before.Length {
		t.Fatal("upgrade lost old acknowledged object/version", err)
	}
}
