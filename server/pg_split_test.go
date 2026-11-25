package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type auditSplitNet struct {
	cut  atomic.Bool
	mu   sync.RWMutex
	side map[string]int
}

func (g *auditSplitNet) other(a, b string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.side[a] != g.side[b]
}
func auditSplitStart(t *testing.T, g *auditSplitNet, path string, cfg *Config, side int, address string) *daemonTestNode {
	t.Helper()
	if address == "" {
		address = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		c, e := loadConfig(path, Options{ConfigPath: path, values: map[string]string{"listen": ln.Addr().String(), "advertise": "http://" + ln.Addr().String()}})
		if e != nil {
			t.Fatal(e)
		}
		cfg = &c
	}
	cfg.Node.Listen = ln.Addr().String()
	cfg.Node.Advertise = "http://" + ln.Addr().String()
	if err = saveConfig(path, *cfg); err != nil {
		t.Fatal(err)
	}
	s, err := Open(Options{ConfigPath: path, manualRanges: true})
	if err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	g.side[s.cfg.Node.ID] = side
	g.mu.Unlock()
	handler := s.Handler()
	h := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, _, _ := r.BasicAuth()
		if g.cut.Load() && strings.HasPrefix(name, "node-") && !strings.HasPrefix(name, frontendCredentialPrefix) && g.other(s.cfg.Node.ID, strings.TrimPrefix(name, "node-")) {
			http.Error(w, "audit network partition", 503)
			return
		}
		handler.ServeHTTP(w, r)
	})}
	go h.Serve(ln)
	d := &daemonTestNode{node: s, http: h, listener: ln, path: path}
	t.Cleanup(d.close)
	return d
}
func auditSplitWait(t *testing.T, f func() bool) {
	t.Helper()
	until := time.Now().Add(12 * time.Second)
	for !f() {
		if time.Now().After(until) {
			t.Fatal("audit wait timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func (g *auditSplitNet) split(nodes []*daemonTestNode) {
	g.cut.Store(true)
	for _, d := range nodes {
		s := d.node
		var ps []*peerConnection
		s.peerStatsMu.RLock()
		for id, p := range s.peerStats {
			if g.other(s.cfg.Node.ID, id.String()) {
				ps = append(ps, p.connection)
			}
		}
		s.peerStatsMu.RUnlock()
		for _, p := range ps {
			p.close()
		}
		s.wsMu.Lock()
		for ws := range s.sockets {
			if ws.authorization != nil && ws.authorization.clusterPeer && g.other(s.cfg.Node.ID, ws.principal.User.ID) {
				ws.close()
			}
		}
		s.wsMu.Unlock()
	}
}
func auditSplitPG(t *testing.T, s *Server, name string, nodes []*daemonTestNode) volume.ID {
	t.Helper()
	id := volume.ID(client.NewRequestID())
	p := storagePolicy(DefaultBucketPolicy())
	mask := PlacementMask{NodeSelection: "list", Storage: &p}
	for _, n := range nodes {
		mask.Nodes = append(mask.Nodes, PlacementNode{NodeID: n.node.cfg.Node.ID, Mode: VolumesAll})
	}
	raw, _ := json.Marshal(mask)
	e := client.Encoder{}
	e.U16(2)
	e.ID(client.ID(id))
	e.U64(0)
	e.Text(name)
	e.Blob(raw)
	_, status, reply := adminBucketMessage(t, s, placementGroupSet, e.B)
	if status != 201 && status != 202 && status != 200 {
		t.Fatalf("PG %d %v", status, reply)
	}
	return id
}
func TestPGRebuildDisjointNetworkSplit(t *testing.T) {
	g := &auditSplitNet{side: map[string]int{}}
	var nodes []*daemonTestNode
	source := auditSplitStart(t, g, filepath.Join(t.TempDir(), "a.yaml"), nil, 0, "")
	nodes = append(nodes, source)
	for i := 1; i < 4; i++ {
		source.node.mu.RLock()
		cfg := copyConfig(source.node.cfg)
		source.node.mu.RUnlock()
		cfg.Node.ID = newID()
		cfg.Node.Name = "island-node"
		cfg.Node.DataDir = "."
		cfg.Volumes = nil
		cfg.Peers = []PeerConfig{{NodeID: source.node.cfg.Node.ID, URL: source.node.cfg.Node.Advertise}}
		cfg.ControlVersion = 0
		cfg.RangeVersion = 0
		cfg.QuotaVersion = 0
		cfg.PeerDirectoryVersion = 0
		cfg.BucketCatalogVersion = 0
		cfg.PlacementGroupsVersion = 0
		cfg.JoinPending = true
		cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
		nodes = append(nodes, auditSplitStart(t, g, filepath.Join(t.TempDir(), "n.yaml"), &cfg, i%2, ""))
		count := len(nodes)
		auditSplitWait(t, func() bool {
			v := source.node.currentSystem.Load()
			if v == nil || len(v.manifest.Membership.Members) != count {
				return false
			}
			for _, n := range nodes {
				w := n.node.currentSystem.Load()
				if w == nil || w.root != v.root {
					return false
				}
			}
			return true
		})
	}
	// Existing HTTP fixture helpers take credentials from the local config;
	// joined nodes authenticate the replicated administrator with source credentials.
	for _, n := range nodes[1:] {
		n.node.mu.Lock()
		n.node.cfg.Auth.BootstrapAdmin = source.node.cfg.Auth.BootstrapAdmin
		n.node.mu.Unlock()
	}
	vols := make([]*volume.Volume, 4)
	for i, n := range nodes {
		v := addTestVolume(t, n.node, "data", "hdd", 16<<20)
		vid, _ := parseID(v.VolumeID)
		vols[i] = n.node.volumes[vid.String()].store
	}
	auditSplitWait(t, func() bool {
		d, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		return len(d) >= 4
	})
	groups := []volume.ID{auditSplitPG(t, source.node, "intranet", []*daemonTestNode{nodes[0], nodes[2]}), auditSplitPG(t, nodes[1].node, "internet", []*daemonTestNode{nodes[1], nodes[3]})}
	buckets := []*Bucket{replicaTestBucket(t, nodes[0].node, groups[0], "media"), replicaTestBucket(t, nodes[1].node, groups[1], "php")}
	id := volume.ObjectID{1}
	payload := []byte("before split")
	for i, b := range buckets {
		if _, err := b.Put(context.Background(), userPrincipal(t, nodes[i].node), id, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	// Finite quota credits start on membership's first node, even if PG excludes it.
	first := source.node.currentSystem.Load().manifest.Membership.Members[0].ID
	var creditWriter int
	for i := 0; i < 2; i++ {
		if nodes[i].node.control.ledger.node != first {
			creditWriter = i
			break
		}
	}
	finite := centralBucket(t, nodes[creditWriter].node, groups[creditWriter], "finite-local-pg", 1<<20)
	credit := findQuota(source.node.currentSystem.Load(), client.ID(finite.ID()))
	for _, n := range credit.Nodes {
		if n.Bytes > 0 {
			t.Logf("initial finite credits: %d bytes on %s; writer=%s", n.Bytes, n.Node, nodes[creditWriter].node.control.ledger.node)
		}
	}
	leaseBucket := replicaTestBucket(t, nodes[0].node, groups[0], "exclusive")
	// Bind application formats while global metadata coordination is available.
	for _, bucket := range append(append([]*Bucket{}, buckets...), finite, leaseBucket) {
		if err := source.node.openBucketFormat(context.Background(), bucket.ID().String(), "fs", userPrincipal(t, source.node)); err != nil {
			t.Fatal(err)
		}
	}
	g.split(nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	for i, b := range buckets {
		a := nodes[i].node.cfg.Auth.BootstrapAdmin
		c, err := client.Dial(ctx, nodes[i].node.cfg.Node.Advertise, a.Username, a.Password, client.ID(b.ID()), "fs")
		if err != nil {
			t.Fatal("split fresh auth", err)
		}
		if _, err = c.Put(ctx, client.ObjectID(id), []byte("after split"), client.PutOptions{}); err != nil {
			t.Fatal("split put", err)
		}
		data, _, err := c.Get(ctx, client.ObjectID(id))
		if err != nil || string(data) != "after split" {
			t.Fatal("split get", err)
		}
		c.Close()
		t.Logf("side %d fresh auth/read/write passed", i)
		before, _ := nodes[i].node.routingPatch(b.ID(), id)
		dest := nodes[i+2].node.control.ledger.node
		plan, err := planRangeMove(before, before.Entries[0], dest, vols[i+2].Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err = nodes[i].node.scopes.move(ctx, plan); err != nil {
			t.Fatal("isolated scoped move", err)
		}
		t.Logf("side %d move done; generation before=%d after=%d", i, before.Generation, nodes[i].node.currentSystem.Load().manifest.Generation)
		dstBucket, _ := nodes[i+2].node.Bucket(b.ID())
		data, _, err = dstBucket.Get(ctx, userPrincipal(t, nodes[i+2].node), id)
		if err != nil || string(data) != "after split" {
			t.Fatal("moved data", err)
		}
		// A second independent range transition in each fragment, still without
		// any global generation advance. The original object stays on the new owner.
		current, _ := nodes[i+2].node.routingPatch(b.ID(), volume.ObjectID{0x80})
		_, right, e := client.SplitRange(current.Entries[0])
		if e != nil {
			t.Fatal(e)
		}
		second, e := planRangeMove(current, right, nodes[i].node.control.ledger.node, vols[i].Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
		if e != nil {
			t.Fatal(e)
		}
		if e = nodes[i+2].node.scopes.move(ctx, second); e != nil {
			t.Fatal("second isolated move", e)
		}
		t.Logf("side %d second move completed; global generation=%d", i, nodes[i].node.currentSystem.Load().manifest.Generation)
	}
	for i, group := range groups {
		for _, n := range []*daemonTestNode{nodes[i], nodes[i+2]} {
			info := n.node.scopes.pgInfo(group.String())
			if info == nil || info.Generation != "2" || info.State != "idle" {
				t.Fatalf("PG generation not committed independently: %+v", info)
			}
		}
	}
	quotaCtx, quotaCancel := context.WithTimeout(context.Background(), 3*time.Second)
	admin := source.node.cfg.Auth.BootstrapAdmin
	quotaClient, err := client.Dial(quotaCtx, nodes[creditWriter].node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(finite.ID()), "fs")
	if err != nil {
		quotaCancel()
		t.Fatal(err)
	}
	_, err = quotaClient.Put(quotaCtx, client.ObjectID{2}, bytes.Repeat([]byte{1}, 4096), client.PutOptions{})
	quotaClient.Close()
	quotaCancel()
	if err != nil {
		t.Fatal("PG-local credits did not remain available", err)
	}

	a := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, a.Username, a.Password, client.ID(leaseBucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(context.Background(), 2*time.Second)
	err = c.BeginObjectLease(short, false)
	stop()
	c.Close()
	if err != nil {
		t.Fatal("PG-scoped lease waited for foreign nodes", err)
	}

	// Restart a disk owner while the cross-fragment partition still holds.
	old := nodes[3]
	address := old.listener.Addr().String()
	old.node.mu.RLock()
	cfg := copyConfig(old.node.cfg)
	old.node.mu.RUnlock()
	old.close()
	nodes[3] = auditSplitStart(t, g, old.path, &cfg, 1, address)
	a = source.node.cfg.Auth.BootstrapAdmin
	restartCtx, restartCancel := context.WithTimeout(context.Background(), 3*time.Second)
	restarted, restartErr := client.Dial(restartCtx, nodes[3].node.cfg.Node.Advertise, a.Username, a.Password, client.ID(buckets[1].ID()), "fs")
	if restartErr != nil {
		restartCancel()
		t.Fatal("isolated restart auth", restartErr)
	}
	restartedData, _, restartErr := restarted.Get(restartCtx, client.ObjectID(id))
	restarted.Close()
	restartCancel()
	if restartErr != nil || string(restartedData) != "after split" {
		t.Fatal("isolated restart data", restartErr)
	}
	t.Log("disk owner restarted inside isolated fragment; fresh authentication and data read passed")
	nextVolume := addTestVolume(t, nodes[3].node, "restart-local-move", "hdd", 16<<20)
	nextID, _ := parseID(nextVolume.VolumeID)
	auditSplitWait(t, func() bool {
		_, err := nodes[3].node.controller.info(ctx, nodes[1].node.control.ledger.node)
		return err == nil
	})
	current, _ := nodes[3].node.routingPatch(buckets[1].ID(), id)
	resumed, resumeErr := planRangeMove(current, current.Entries[0], nodes[3].node.control.ledger.node, nextID, uint8(volume.HDD), volume.Raw, 1<<20)
	if resumeErr != nil {
		t.Fatal(resumeErr)
	}
	if resumeErr = nodes[3].node.scopes.move(ctx, resumed); resumeErr != nil {
		t.Fatal("next PG round after isolated restart", resumeErr)
	}
	if info := nodes[3].node.scopes.pgInfo(groups[1].String()); info.Generation != "3" || info.State != "idle" {
		t.Fatalf("PG counter did not continue after restart: %+v", info)
	}
	g.cut.Store(false)
	auditSplitWait(t, func() bool {
		for _, n := range nodes {
			n.node.peerStatsMu.RLock()
			live := 0
			for _, p := range n.node.peerStats {
				if p.online {
					live++
				}
			}
			n.node.peerStatsMu.RUnlock()
			if live < 3 {
				return false
			}
		}
		return true
	})
	auditSplitWait(t, func() bool {
		for i, b := range buckets {
			p, _ := nodes[1-i].node.routingPatch(b.ID(), id)
			if p.Entries[0].Locations[0].Node != nodes[i+2].node.control.ledger.node {
				return false
			}
		}
		return true
	})
	for i, b := range buckets {
		foreign := nodes[1-i].node
		p, _ := foreign.routingPatch(b.ID(), id)
		t.Logf("reconnected foreign map bucket %d owner=%s want=%s", i, p.Entries[0].Locations[0].Node, nodes[i+2].node.control.ledger.node)
		if p.Entries[0].Locations[0].Node != nodes[i+2].node.control.ledger.node {
			t.Fatal("missing automatic archive catchup at reconnect")
		}
	}
	// An ordinary global metadata checkpoint preserves both independent histories.
	e := client.Encoder{}
	e.ID(client.ID(buckets[0].ID()))
	e.Text("media-rejoined")
	_, status, reply := adminBucketMessage(t, source.node, client.BucketRename, e.B)
	if status != 200 {
		t.Fatalf("rejoin global control %d %v", status, reply)
	}
	for i, b := range buckets {
		for _, n := range nodes {
			p, _ := n.node.routingPatch(b.ID(), id)
			if p.Entries[0].Locations[0].Node != nodes[i+2].node.control.ledger.node {
				t.Fatal("histories did not merge")
			}
		}
	}
	t.Log("explicit common checkpoint merges both island handoffs")
}

func TestPGWorkerDoesNotWaitForUnrelatedPG(t *testing.T) {
	g := &auditSplitNet{side: map[string]int{}}
	var nodes []*daemonTestNode
	source := auditSplitStart(t, g, filepath.Join(t.TempDir(), "a.yaml"), nil, 0, "")
	nodes = append(nodes, source)
	for i := 1; i < 4; i++ {
		source.node.mu.RLock()
		cfg := copyConfig(source.node.cfg)
		source.node.mu.RUnlock()
		cfg.Node.ID = newID()
		cfg.Node.Name = "island-node"
		cfg.Node.DataDir = "."
		cfg.Volumes = nil
		cfg.Peers = []PeerConfig{{NodeID: source.node.cfg.Node.ID, URL: source.node.cfg.Node.Advertise}}
		cfg.ControlVersion = 0
		cfg.RangeVersion = 0
		cfg.QuotaVersion = 0
		cfg.PeerDirectoryVersion = 0
		cfg.BucketCatalogVersion = 0
		cfg.PlacementGroupsVersion = 0
		cfg.JoinPending = true
		cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
		nodes = append(nodes, auditSplitStart(t, g, filepath.Join(t.TempDir(), "n.yaml"), &cfg, i%2, ""))
		count := len(nodes)
		auditSplitWait(t, func() bool {
			v := source.node.currentSystem.Load()
			if v == nil || len(v.manifest.Membership.Members) != count {
				return false
			}
			for _, n := range nodes {
				w := n.node.currentSystem.Load()
				if w == nil || w.root != v.root {
					return false
				}
			}
			return true
		})
	}
	// Existing HTTP fixture helpers take credentials from the local config;
	// joined nodes authenticate the replicated administrator with source credentials.
	for _, n := range nodes[1:] {
		n.node.mu.Lock()
		n.node.cfg.Auth.BootstrapAdmin = source.node.cfg.Auth.BootstrapAdmin
		n.node.mu.Unlock()
	}
	vols := make([]*volume.Volume, 4)
	for i, n := range nodes {
		v := addTestVolume(t, n.node, "data", "hdd", 16<<20)
		vid, _ := parseID(v.VolumeID)
		vols[i] = n.node.volumes[vid.String()].store
	}
	auditSplitWait(t, func() bool {
		d, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		return len(d) >= 4
	})

	// Make the shared source the deterministic coordinator of both PGs.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].node.cfg.Node.ID < nodes[j].node.cfg.Node.ID })
	g.mu.Lock()
	for i, n := range nodes {
		g.side[n.node.cfg.Node.ID] = i % 2
	}
	g.mu.Unlock()
	source = nodes[0]
	dataVolume := func(n *daemonTestNode) volume.ID {
		for _, v := range n.node.volumes {
			if !v.config.System {
				return v.store.Identity().VolumeID
			}
		}
		t.Fatal("missing data volume")
		return volume.ID{}
	}
	good := auditSplitPG(t, source.node, "healthy-local", []*daemonTestNode{nodes[0], nodes[2]})
	bad := auditSplitPG(t, source.node, "blocked-cross-fragment", []*daemonTestNode{nodes[0], nodes[1]})
	live := replicaTestBucket(t, source.node, good, "healthy")
	waiting := replicaTestBucket(t, source.node, bad, "waiting")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, b := range []*Bucket{live, waiting} {
		if _, err := b.Put(ctx, userPrincipal(t, source.node), volume.ObjectID{1}, bytes.Repeat([]byte("x"), 64<<10), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := source.node.routingPatch(waiting.ID(), volume.ObjectID{1})
	pending, err := planRangeMove(before, before.Entries[0], nodes[1].node.control.ledger.node, dataVolume(nodes[1]), uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.reserve(pending); err != nil {
		t.Fatal(err)
	}
	if _, err = source.node.scopes.call(ctx, pending.Target, peerRangeReserve, mustRangeWire(pending)); err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.fence(pending.id()); err != nil {
		t.Fatal(err)
	}
	g.split(nodes)
	// Also leave an unrelated global metadata reservation waiting on the
	// unavailable fragment. It must not fence healthy PG payload work.
	source.node.controller.mu.Lock()
	defer source.node.controller.mu.Unlock()
	globalCtx, globalCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, globalErr := source.node.controller.coordinator.begin(globalCtx, controlAdministration, membershipIDs(source.node.currentSystem.Load().manifest.Membership))
	globalCancel()
	if globalErr == nil {
		t.Fatal("global ALL unexpectedly completed across the split")
	}
	source.node.control.ledger.mu.Lock()
	reserved := source.node.control.ledger.slot != nil
	source.node.control.ledger.mu.Unlock()
	if !reserved {
		t.Fatal("missing local global metadata reservation")
	}
	source.node.mu.Lock()
	source.node.volumes[dataVolume(source).String()].config.State = "draining"
	source.node.mu.Unlock()
	source.node.scopes.start()
	auditSplitWait(t, func() bool {
		patch, _ := source.node.routingPatch(live.ID(), volume.ObjectID{1})
		return patch.Entries[0].Locations[0].Node == nodes[2].node.control.ledger.node && source.node.scopes.pgInfo(good.String()).Generation == "1"
	})
	source.node.scopes.mu.Lock()
	stillWaiting := source.node.scopes.active(source.node.scopes.txns[pending.id()])
	source.node.scopes.mu.Unlock()
	if !stillWaiting {
		t.Fatal("test did not retain an unavailable PG generation")
	}
	t.Log("healthy PG completed automatic rebalance while another PG was fenced and global metadata ALL waited on the same node/coordinator")
}
