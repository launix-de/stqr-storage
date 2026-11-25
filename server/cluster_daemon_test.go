// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type daemonTestNode struct {
	node      *Server
	http      *http.Server
	listener  net.Listener
	automatic bool
	path      string
}

// Unlike transport fixtures, these use production Open and its default
// controller, bootstrap, automatic peer managers and metadata publication.
func startDaemonTestNode(t *testing.T, path string, config *Config, automatic ...bool) *daemonTestNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if config != nil {
		config.Node.Listen = ln.Addr().String()
		config.Node.Advertise = "http://" + ln.Addr().String()
		if err := saveConfig(path, *config); err != nil {
			ln.Close()
			t.Fatal(err)
		}
	} else {
		opts := Options{ConfigPath: path, values: map[string]string{"listen": ln.Addr().String(), "advertise": "http://" + ln.Addr().String()}}
		cfg, err := loadConfig(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := saveConfig(path, cfg); err != nil {
			t.Fatal(err)
		}
	}
	auto := len(automatic) > 0 && automatic[0]
	s, err := Open(Options{ConfigPath: path, manualRanges: !auto})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	h := &http.Server{Handler: s.Handler()}
	go h.Serve(ln)
	d := &daemonTestNode{node: s, http: h, listener: ln, path: path, automatic: auto}
	t.Cleanup(func() { d.close() })
	return d
}
func (d *daemonTestNode) close() { d.http.Close(); d.node.Close() }

func restartDaemonTestNode(t *testing.T, d *daemonTestNode) *daemonTestNode {
	t.Helper()
	address := d.listener.Addr().String()
	d.close()
	ln, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Options{ConfigPath: d.path, manualRanges: !d.automatic})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	h := &http.Server{Handler: s.Handler()}
	go h.Serve(ln)
	n := &daemonTestNode{node: s, http: h, listener: ln, path: d.path, automatic: d.automatic}
	t.Cleanup(n.close)
	return n
}

func TestDaemonControlBootstrapAndAutomaticJoin(t *testing.T) {
	source := startDaemonTestNode(t, filepath.Join(t.TempDir(), "source.yaml"), nil)
	if view := source.node.currentSystem.Load(); view == nil || len(view.manifest.Membership.Members) != 1 {
		t.Fatal("default daemon did not bootstrap committed system state")
	}
	config := copyConfig(source.node.cfg)
	config.Node.ID = newID()
	config.Node.Name = "candidate"
	config.Node.DataDir = "."
	config.Volumes = nil
	config.Peers = []PeerConfig{{NodeID: source.node.cfg.Node.ID, URL: source.node.cfg.Node.Advertise}}
	config.ControlVersion = 0
	config.RangeVersion = 0
	config.QuotaVersion = 0
	config.PeerDirectoryVersion = 0
	config.BucketCatalogVersion = 0
	config.PlacementGroupsVersion = 0
	config.JoinPending = true
	config.Auth.BootstrapAdmin = BootstrapAdmin{}
	target := startDaemonTestNode(t, filepath.Join(t.TempDir(), "target.yaml"), &config)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		a, b := source.node.currentSystem.Load(), target.node.currentSystem.Load()
		if a != nil && b != nil && a.root == b.root && len(a.manifest.Membership.Members) == 2 && !target.node.joinPending() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	a, b := source.node.currentSystem.Load(), target.node.currentSystem.Load()
	if a == nil || b == nil || a.root != b.root || len(a.manifest.Membership.Members) != 2 || target.node.joinPending() {
		source.node.control.ledger.mu.Lock()
		slot := source.node.control.ledger.slot
		req := source.node.control.ledger.request
		source.node.control.ledger.mu.Unlock()
		t.Fatalf("automatic admission incomplete: source=%v target=%v slot=%v request=%v", a != nil, b != nil, slot != nil, req != nil)
	}
	var users accountsImage
	if err := target.node.readBlob("accounts", &users); err != nil || len(users.Users) != 1 {
		t.Fatal("candidate accounts were not copied", err)
	}
	actor := userPrincipal(t, source.node)
	e := client.Encoder{}
	e.ID(client.NewRequestID())
	e.Text("replicated-user")
	e.U8(0)
	e.Bool(false)
	e.Text("normal-admin-test-password")
	m := client.Message{Kind: client.Request, Opcode: client.UserCreate, ID: client.NewRequestID(), Body: e.B}
	_, status := source.node.dispatchAs(m, nil, actor)
	if status != 201 {
		t.Fatal("global user creation failed", status)
	}
	left, err := source.node.findAccount("replicated-user")
	if err != nil {
		t.Fatal(err)
	}
	right, err := target.node.findAccount("replicated-user")
	if err != nil || left.ID != right.ID || left.PasswordHash != right.PasswordHash {
		t.Fatal("global account change not applied", err)
	}
	// A direct fresh authentication on the admitted candidate uses the same
	// native verifier, not the bootstrap password of an independent server.
	r, _ := http.NewRequestWithContext(context.Background(), "GET", target.node.cfg.Node.Advertise+"/", nil)
	r.SetBasicAuth("replicated-user", "normal-admin-test-password")
	if _, err := target.node.authenticate(r); err != nil {
		t.Fatal("admitted user cannot authenticate", err)
	}
	configBytes, err := os.ReadFile(target.path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(configBytes, []byte("normal-admin-test-password")) {
		t.Fatal("user password leaked into node config")
	}
	addTestVolume(t, source.node, "objects", "hdd", 4<<20)
	addTestVolume(t, target.node, "objects", "hdd", 4<<20)
	bucket, _ := createProtocolBucket(t, source.node, "routed-objects")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	objects, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	object := client.ObjectID{7}
	payload := bytes.Repeat([]byte("routed native payload"), 8192)
	if _, err := objects.Put(ctx, object, payload, client.PutOptions{}); err != nil {
		t.Fatal("adaptive PUT from non-owner", err)
	}
	other, _ := createUserMessage("other-coordinator", "other-coordinator-password")
	if _, status := target.node.dispatchAs(other, nil, actor); status != 201 {
		t.Fatal("new member cannot coordinate global administration", status)
	}
	target.close()
	got, _, err := objects.Get(ctx, object)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("known owner depended on stopped bootstrap", err)
	}
	if err := source.node.prepareShortShutdown(ctx); err != nil {
		t.Fatal("SHORT prepare", err)
	}
	source.node.mu.RLock()
	paused := source.node.cfg.Node.Paused
	source.node.mu.RUnlock()
	if !paused {
		t.Fatal("SHORT did not persist paused intent")
	}
	source.close()
	target = restartDaemonTestNode(t, target)
	if _, err := target.node.findAccount("replicated-user"); err != nil {
		t.Fatal("returning non-initiator lost system authority", err)
	}
	returning, err := client.Dial(ctx, target.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer returning.Close()
	type result struct {
		data []byte
		err  error
	}
	read := make(chan result, 1)
	go func() { data, _, err := returning.Get(ctx, object); read <- result{data, err} }()
	select {
	case r := <-read:
		t.Fatal("read should wait for paused required owner", r.err)
	case <-time.After(150 * time.Millisecond):
	}
	source = restartDaemonTestNode(t, source)
	select {
	case r := <-read:
		if r.err != nil || !bytes.Equal(r.data, payload) {
			t.Fatal("owner restart did not resume waiting read", r.err)
		}
	case <-ctx.Done():
		t.Fatal("waiting read did not resume", ctx.Err())
	}
	// Interrupt a real production coordinator after durable local preparation,
	// before transferring the new graph. Open must not wait for peer transports
	// that cannot start until it returns; its worker resumes after reconnect.
	pending, _ := createUserMessage("resumed-coordinator", "resumed-coordinator-password")
	prepareErr := func() error {
		c := source.node.controller
		c.mu.Lock()
		defer c.mu.Unlock()
		l := c.coordinator.ledger
		l.mu.Lock()
		base := cloneControlBase(l.current)
		l.mu.Unlock()
		required := membershipIDs(base.Membership)
		claim, err := c.coordinator.begin(ctx, controlAdministration, required)
		if err != nil {
			return err
		}
		draft, err := source.node.prepareAdminDraft(pending, actor)
		if err != nil {
			return err
		}
		defer draft.close()
		roots := make(map[client.ID]volume.ID)
		for _, node := range required {
			info, err := c.info(ctx, node)
			if err != nil {
				return err
			}
			roots[node] = info.root
		}
		bundle, err := draft.server.captureSystemImage(base.Membership, base.Sequence+1, base.Generation, base.MapRevision, roots, c.patches(base.Membership, draft.server.bucketSnapshots()))
		if err != nil {
			return err
		}
		if err := source.node.stageSystemBundle(bundle); err != nil {
			return err
		}
		e := client.Encoder{}
		e.V(uint64(draft.status))
		e.Blob(draft.result)
		if err := c.coordinator.prepared(ctx, controlProposal{Claim: claim, Kind: controlAdministration, Image: [32]byte(bundle.Root)}, e.B); err != nil {
			return err
		}
		source.node.cancel()
		return nil
	}()
	if prepareErr != nil {
		t.Fatal(prepareErr)
	}
	source = restartDaemonTestNode(t, source)
	for until := time.Now().Add(8 * time.Second); ; {
		_, left := source.node.findAccount("resumed-coordinator")
		_, right := target.node.findAccount("resumed-coordinator")
		if left == nil && right == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal("prepared coordinator did not resume after daemon reopen", left, right)
		}
		time.Sleep(25 * time.Millisecond)
	}
	checkDaemonRangeHandoff(t, source, target, bucket.ID(), returning)
}
