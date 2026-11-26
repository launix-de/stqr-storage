// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestStorageWarmupProductionOrdinaryCOWClientAndRoleBoundaries(t *testing.T) {
	path, bucket, object, adminName, adminPassword := warmupFixture(t)
	setup := startDaemonTestNode(t, path, nil)
	password := "cold-client-password"
	var readerID client.ID
	for _, name := range []string{"cold-reader", "cold-denied"} {
		msg, id := createUserMessage(name, password)
		if body, status := setup.node.dispatchAs(msg, nil, userPrincipal(t, setup.node)); status != 201 {
			t.Fatalf("create ordinary account: %d %x", status, body)
		}
		if name == "cold-reader" {
			readerID = id
		}
	}
	lower := bucket
	copyBucket, _ := createProtocolBucket(t, setup.node, "cold-inherited-copy")
	clone := client.Encoder{}
	clone.ID(lower)
	clone.ID(client.ID(copyBucket.ID()))
	if _, status, reply := adminBucketMessage(t, setup.node, client.BucketClone, clone.B); status != 200 {
		t.Fatal("create real clone link", status, reply)
	}
	bucket = client.ID(copyBucket.ID())
	for _, store := range copyBucket.allVolumes() {
		h, err := store.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.RecordInfo(volume.Key{Bucket: volume.ID(bucket), Object: volume.ObjectID(object)})
		h.Close()
		if !errors.Is(err, volume.ErrNotFound) {
			t.Fatal("clone fixture has a local data record", err)
		}
	}
	grant := client.Encoder{}
	grant.ID(bucket)
	grant.ID(readerID)
	grant.U32(uint32(BucketRead))
	_, status, reply := adminBucketMessage(t, setup.node, client.BucketGrant, grant.B)
	if status != 200 {
		t.Fatal("grant reader", status, reply)
	}
	accountingDir := setup.node.accountingDir
	setup.close()
	if err := os.RemoveAll(accountingDir); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	ln.Close()
	cfg, err := loadConfig(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Node.Listen = address
	cfg.Node.Advertise = "http://" + address
	if err := saveConfig(path, cfg); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var dataWrites, dataSyncs atomic.Uint64
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{ConfigPath: path, manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO {
			return &coldReadOnlyCounters{VolumeIO: &blockedCensusIO{VolumeIO: v, entered: entered, release: release}, writes: &dataWrites, syncs: &dataSyncs}
		}}, io.Discard)
	}()
	defer func() {
		unblock()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("production daemon did not close")
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no canonical census block")
	}
	url := "http://" + address
	hc := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for {
		req, _ := http.NewRequest("GET", url+"/api/session", nil)
		req.SetBasicAuth(adminName, adminPassword)
		r, e := hc.Do(req)
		if e == nil {
			r.Body.Close()
			if r.StatusCode != 200 {
				t.Fatal("admin authentication", r.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("production listener unavailable", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	wire := client.EncodeMessage(client.Message{Kind: client.Request, Opcode: client.NodeConfigGet, ID: client.NewRequestID()})
	req, _ := http.NewRequest("POST", url+"/api/message", bytes.NewReader(wire))
	response, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("unauthenticated metadata access", response.StatusCode)
	}
	if _, status := warmHTTP(t, hc, url, "cold-reader", password, client.NodeConfigGet, nil); status != 403 {
		t.Fatal("ordinary user acquired admin read", status)
	}
	if _, status := warmHTTP(t, hc, url, "cold-reader", password, client.NodeHelloMessage, nil); status != 403 {
		t.Fatal("ordinary user acquired cluster-peer role", status)
	}
	dialctx, dialcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer dialcancel()
	if c, err := client.Dial(dialctx, url, "cold-denied", password, client.BucketID(bucket), "fs"); !errors.Is(err, client.ErrUnauthorized) {
		if c != nil {
			c.Close()
		}
		t.Fatal("bucket without grant admitted", err)
	}
	if c, err := client.Dial(dialctx, url, "cold-reader", password, client.BucketID(lower), "fs"); !errors.Is(err, client.ErrUnauthorized) {
		if c != nil {
			c.Close()
		}
		t.Fatal("copy-only user acquired lower bucket grant", err)
	}
	beforeWrites, beforeSyncs := dataWrites.Load(), dataSyncs.Load()
	reader, err := client.Dial(dialctx, url, "cold-reader", password, client.BucketID(bucket), "fs")
	if err != nil {
		t.Fatal("ordinary granted reader could not connect", err)
	}
	defer reader.Close()
	read := func() {
		t.Helper()
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		data, _, err := reader.Get(c, object)
		if err != nil || string(data) != "persistent warmup payload" {
			t.Fatal("ordinary cold GET", string(data), err)
		}
	}
	read()
	for _, op := range []uint16{client.ObjectAccepted, client.BucketCreate, client.VolumeGrow, client.NodeConfigSet, peerPGRebuildRequest} {
		if _, status := warmHTTP(t, hc, url, adminName, adminPassword, op, nil); status != 503 {
			t.Fatalf("cold mutation/control %04x returned %d", op, status)
		}
	}
	read() // A rejected writer/control request must not queue ahead of this read.
	if dataWrites.Load() != beforeWrites || dataSyncs.Load() != beforeSyncs {
		t.Fatal("inherited cold read wrote or synced data volumes", beforeWrites, dataWrites.Load(), beforeSyncs, dataSyncs.Load())
	}
	unblock()
	deadline = time.Now().Add(5 * time.Second)
	for {
		request, _ := http.NewRequest("GET", url+"/api/health", nil)
		request.SetBasicAuth(adminName, adminPassword)
		result, err := hc.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(result.Body)
		result.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, []byte(`"active"`)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("warmup never became active", string(body))
		}
		time.Sleep(10 * time.Millisecond)
	}
	writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Second)
	defer writeCancel()
	if _, err := reader.Put(writeCtx, client.ObjectID{99}, []byte("forbidden"), client.PutOptions{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("read-only grant could mutate after readiness", err)
	}
	read()
}

// Count actual data-provider calls independently of mapped/positioned read
// accounting. Snapshotting inherited reads must not mutate either data volume.
type coldReadOnlyCounters struct {
	volume.VolumeIO
	writes, syncs *atomic.Uint64
}

func (d *coldReadOnlyCounters) WriteAt(p []byte, off int64) (int, error) {
	d.writes.Add(1)
	return d.VolumeIO.WriteAt(p, off)
}
func (d *coldReadOnlyCounters) Sync() error { d.syncs.Add(1); return d.VolumeIO.Sync() }

func TestStorageWarmupEmbeddedRecoveryStageRejectsBeforeControlLock(t *testing.T) {
	path, _, _, _, _ := warmupFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	s, err := Open(Options{ConfigPath: path, manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO {
		return &blockedCensusIO{VolumeIO: v, entered: entered, release: release}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); s.Close() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no blocked production census")
	}
	// A cold embedded control call must return before entering opMu, just like
	// AddVolume/GrowVolume; no backing path should be opened while unavailable.
	missing := filepath.Join(t.TempDir(), "not-an-existing-volume")
	s.opMu.Lock()
	done := make(chan error, 1)
	go func() { _, err := s.StageRecoveryVolume("rescued", missing); done <- err }()
	returned := false
	select {
	case err := <-done:
		returned = true
		if statusOf(err) != 503 {
			t.Error("cold recovery stage not unavailable", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("cold recovery stage queued on the control lock")
	}
	s.opMu.Unlock()
	if !returned {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("recovery stage remained stuck after lock release")
		}
	}
	unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.WaitStorageReady(ctx); err != nil {
		t.Fatal(err)
	}
	// A genuine post-readiness call reaches ordinary path validation, proving
	// the control was deferred rather than permanently disabled by the guard.
	if _, err := s.StageRecoveryVolume("rescued", missing); !os.IsNotExist(err) {
		t.Fatal("ready recovery stage did not reach ordinary path validation", err)
	}
}

// This fixture uses the real range-handoff protocol to separate physical owners
// while preserving one identical policy/PG and a stable non-detaching clone link.
func TestStorageWarmupRemoteLowerOrdinaryCOWRead(t *testing.T) {
	first, second := startAutomaticDaemonPair(t, false)
	a := addReplicaDataVolume(t, first.node, "cold-copy-data", "hdd", 16<<20)
	b := addReplicaDataVolume(t, second.node, "cold-lower-data", "hdd", 16<<20)
	reviewWaitDest(t, first.node, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sourceID, copyID := second.node.control.ledger.node, first.node.control.ledger.node
	if sourceID == copyID {
		t.Fatal("fixture nodes are not distinct")
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	centralPGRequest(t, first.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(first.node, a), whitelistReplicaMask(second.node, b)}})
	lower := replicaTestBucket(t, first.node, group, "remote-cold-lower")
	copyBucket := replicaTestBucket(t, first.node, group, "remote-cold-copy")
	anchor := replicaTestBucket(t, first.node, group, "remote-cold-anchor")
	object := client.ObjectID{42}
	payload := []byte("ordinary inherited bytes from the other cold node")
	admin := first.node.cfg.Auth.BootstrapAdmin
	original, err := client.Dial(ctx, first.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(lower.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = original.Put(ctx, object, payload, client.PutOptions{}); err != nil {
		original.Close()
		t.Fatal(err)
	}
	if err = original.Sync(ctx); err != nil {
		original.Close()
		t.Fatal(err)
	}
	original.Close()
	moveTo := func(bucket volume.ID, node *daemonTestNode, store *volume.Volume) {
		t.Helper()
		patch, ok := first.node.routingPatch(bucket, volume.ObjectID(object))
		if !ok || len(patch.Entries) != 1 || len(patch.Entries[0].Locations) != 1 {
			t.Fatal("missing initial single-owner range", patch)
		}
		if patch.Entries[0].Locations[0].Node != node.node.control.ledger.node || patch.Entries[0].Locations[0].Volume != client.ID(store.Identity().VolumeID) {
			from := first
			if patch.Entries[0].Locations[0].Node == second.node.control.ledger.node {
				from = second
			}
			plan, err := planRangeMove(patch, patch.Entries[0], node.node.control.ledger.node, store.Identity().VolumeID, uint8(store.Identity().Class), volume.Raw, 2<<20)
			if err != nil {
				t.Fatal("plan real owner separation", err)
			}
			if err = from.node.scopes.move(ctx, plan); err != nil {
				t.Fatal("commit real owner separation", err)
			}
		}
		for _, peer := range []*daemonTestNode{first, second} {
			current, ok := peer.node.routingPatch(bucket, volume.ObjectID(object))
			if !ok || len(current.Entries) != 1 || len(current.Entries[0].Locations) != 1 || current.Entries[0].Locations[0].Node != node.node.control.ledger.node || current.Entries[0].Locations[0].Volume != client.ID(store.Identity().VolumeID) {
				t.Fatal("owner separation was not committed on both peers", current)
			}
		}
	}
	moveTo(lower.ID(), second, b)
	moveTo(copyBucket.ID(), first, a)
	moveTo(anchor.ID(), first, a)
	// The copy node must also have a canonical OBJ2 record, so BOTH production
	// nodes are genuinely cold and paused in accounting during the later GET.
	if _, err = anchor.Put(ctx, userPrincipal(t, first.node), volume.ObjectID{99}, []byte("cold anchor"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	clone := client.Encoder{}
	clone.ID(client.ID(lower.ID()))
	clone.ID(client.ID(copyBucket.ID()))
	if _, status, reply := adminBucketMessage(t, first.node, client.BucketClone, clone.B); status != 200 {
		t.Fatal("clone across committed owners", status, reply)
	}
	if copyBucket.Describe().CloneDetach || !cowSameStorage(lower.Describe().Policy, copyBucket.Describe().Policy) {
		t.Fatal("fixture would materialize/detach instead of retaining a clone link")
	}
	password := "remote-cold-reader-password"
	var readerID client.ID
	for _, name := range []string{"remote-copy-reader", "remote-no-grant"} {
		m, id := createUserMessage(name, password)
		if body, status := first.node.dispatchAs(m, nil, userPrincipal(t, first.node)); status != 201 {
			t.Fatalf("create USER: %d %x", status, body)
		}
		if name == "remote-copy-reader" {
			readerID = id
			user, err := first.node.findAccount(name)
			if err != nil || user.Role != "USER" {
				t.Fatal("positive reader is not an ordinary USER", user.Role, err)
			}
		}
	}
	grant := client.Encoder{}
	grant.ID(client.ID(copyBucket.ID()))
	grant.ID(readerID)
	grant.U32(uint32(BucketRead))
	if _, status, reply := adminBucketMessage(t, first.node, client.BucketGrant, grant.B); status != 200 {
		t.Fatal("grant copy only", status, reply)
	}
	assertNoLocalCopy := func() {
		t.Helper()
		for _, peer := range []*daemonTestNode{first, second} {
			for _, store := range peer.node.quotaVolumes() {
				h, err := store.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				_, err = h.RecordInfo(volume.Key{Bucket: copyBucket.ID(), Object: volume.ObjectID(object)})
				h.Close()
				if !errors.Is(err, volume.ErrNotFound) {
					t.Fatal("inherited fixture unexpectedly has a local copy record", err)
				}
			}
		}
	}
	assertNoLocalCopy()
	if first.node.currentSystem.Load().root != second.node.currentSystem.Load().root {
		t.Fatal("setup did not commit the same system generation")
	}
	link := copyBucket.Describe().ClonePeers
	if len(link) != 1 || link[0].Bucket != lower.ID().String() {
		t.Fatal("missing stable lower link", link)
	}
	beforeMaps := make(map[volume.ID]client.MapPatch)
	for _, id := range []volume.ID{copyBucket.ID(), lower.ID()} {
		beforeMaps[id], _ = first.node.routingPatch(id, volume.ObjectID(object))
	}
	copyDescription := copyBucket.Describe()
	configs := []string{first.path, second.path}
	addresses := []string{first.node.cfg.Node.Advertise, second.node.cfg.Node.Advertise}
	dirs := []string{first.node.accountingDir, second.node.accountingDir}
	volumes := []client.ID{client.ID(a.Identity().VolumeID), client.ID(b.Identity().VolumeID)}
	first.close()
	second.close()
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	type coldNode struct {
		entered, release     chan struct{}
		once                 sync.Once
		reads, writes, syncs atomic.Uint64
		cancel               context.CancelFunc
		done                 chan error
	}
	cold := []*coldNode{{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan error, 1)}, {entered: make(chan struct{}), release: make(chan struct{}), done: make(chan error, 1)}}
	for i, node := range cold {
		runctx, stop := context.WithCancel(context.Background())
		node.cancel = stop
		i, node := i, node
		go func() {
			node.done <- Run(runctx, Options{ConfigPath: configs[i], manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO {
				return &remoteColdProofIO{VolumeIO: v, entered: node.entered, release: node.release, reads: &node.reads, writes: &node.writes, syncs: &node.syncs}
			}}, io.Discard)
		}()
	}
	defer func() {
		for _, n := range cold {
			n.once.Do(func() { close(n.release) })
			n.cancel()
		}
		for _, n := range cold {
			select {
			case err := <-n.done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(8 * time.Second):
				t.Error("remote fixture daemon did not close")
			}
		}
	}()
	for i, n := range cold {
		select {
		case <-n.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("node %d never blocked canonical census", i)
		}
	}
	hc := &http.Client{Timeout: time.Second}
	for _, address := range addresses {
		until := time.Now().Add(5 * time.Second)
		for {
			req, _ := http.NewRequest("GET", address+"/api/session", nil)
			req.SetBasicAuth(admin.Username, admin.Password)
			r, err := hc.Do(req)
			if err == nil {
				r.Body.Close()
				if r.StatusCode != 200 {
					t.Fatal("authenticated production listener", r.StatusCode)
				}
				break
			}
			if time.Now().After(until) {
				t.Fatal("listener unavailable during cold census", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	// Validate the actual replayed owner map through each productive listener,
	// rather than inferring remote ownership from the existence of two nodes.
	for _, address := range addresses {
		for _, want := range []struct {
			bucket       volume.ID
			node, device client.ID
		}{{copyBucket.ID(), copyID, volumes[0]}, {lower.ID(), sourceID, volumes[1]}} {
			query := client.Encoder{}
			query.ID(client.ID(want.bucket))
			query.B = append(query.B, object[:]...)
			raw, status := warmHTTP(t, hc, address, admin.Username, admin.Password, client.MapGetMessage, query.B)
			if status != 200 {
				t.Fatal("replayed owner map unavailable", status)
			}
			patch, err := client.DecodeMapPatch(bucketReplyResult(t, client.Message{Body: raw}, 200))
			if err != nil || len(patch.Entries) != 1 || len(patch.Entries[0].Locations) != 1 || patch.Entries[0].Locations[0].Node != want.node || patch.Entries[0].Locations[0].Volume != want.device {
				t.Fatal("replayed map has wrong physical owner", patch, err)
			}
			before := beforeMaps[want.bucket]
			if patch.Membership != before.Membership || patch.Generation != before.Generation || patch.Revision != before.Revision || !reflect.DeepEqual(patch.Entries, before.Entries) {
				t.Fatal("cold replay changed committed generation/range authority", before, patch)
			}
		}
	}
	for _, address := range addresses {
		raw, status := warmHTTP(t, hc, address, admin.Username, admin.Password, client.BucketListPage, adminPageBody("", 10, "", copyBucket.ID().String()))
		if status != 200 {
			t.Fatal("clone metadata unavailable while cold", status)
		}
		d := client.Decoder{B: bucketReplyResult(t, client.Message{Body: raw}, 200)}
		var page adminTestPage
		if err := json.Unmarshal([]byte(d.Text()), &page); err != nil || d.Done() != nil || len(page.Buckets) != 1 {
			t.Fatal("invalid cold clone metadata", err)
		}
		got := page.Buckets[0]
		if got.ID != copyDescription.ID || got.Revision != copyDescription.Revision || got.CloneDetach || !reflect.DeepEqual(got.ClonePeers, link) || got.Policy != copyDescription.Policy {
			t.Fatal("cold replay changed stable clone link/policy/generation", got.BucketDescription, copyDescription)
		}
	}
	dialctx, dialcancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer dialcancel()
	for _, probe := range []struct {
		address, name string
		bucket        volume.ID
	}{{addresses[0], "remote-no-grant", copyBucket.ID()}, {addresses[1], "remote-copy-reader", lower.ID()}} {
		c, err := client.Dial(dialctx, probe.address, probe.name, password, client.BucketID(probe.bucket), "fs")
		if c != nil {
			c.Close()
		}
		if !errors.Is(err, client.ErrUnauthorized) {
			t.Fatal("ordinary negative grant control admitted", probe.name, probe.bucket, err)
		}
	}
	beforeWrites := [2]uint64{cold[0].writes.Load(), cold[1].writes.Load()}
	beforeSyncs := [2]uint64{cold[0].syncs.Load(), cold[1].syncs.Load()}
	beforeRemoteReads := cold[1].reads.Load()
	reader, err := client.Dial(dialctx, addresses[0], "remote-copy-reader", password, client.BucketID(copyBucket.ID()), "fs")
	if err != nil {
		t.Fatal("copy-only USER could not open inherited format through cold peer", err)
	}
	defer reader.Close()
	for i := 0; i < 2; i++ {
		readctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		got, _, err := reader.Get(readctx, object)
		stop()
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("ordinary remote-lower cold GET", string(got), err)
		}
	}
	if cold[1].reads.Load() <= beforeRemoteReads {
		t.Fatal("other physical lower provider was not read")
	}
	for i, n := range cold {
		if n.writes.Load() != beforeWrites[i] || n.syncs.Load() != beforeSyncs[i] {
			t.Fatal("cold remote inherited read wrote/synced a data provider", i, beforeWrites[i], n.writes.Load(), beforeSyncs[i], n.syncs.Load())
		}
		select {
		case <-n.release:
			t.Fatal("fixture released census before positive read")
		default:
		}
	}
	t.Logf("copy owner=%s lower owner=%s; both canonical censuses blocked; lower provider reads %d->%d; data writes/syncs unchanged on both nodes; stable link=%s; system generation=%d map revision=%d", copyID, sourceID, beforeRemoteReads, cold[1].reads.Load(), link[0].Link, beforeMaps[copyBucket.ID()].Generation, beforeMaps[copyBucket.ID()].Revision)
}

type remoteColdProofIO struct {
	volume.VolumeIO
	entered, release     chan struct{}
	blocked              atomic.Bool
	reads, writes, syncs *atomic.Uint64
}

func (d *remoteColdProofIO) ReadAt(p []byte, off int64) (int, error) {
	d.reads.Add(1)
	n, err := d.VolumeIO.ReadAt(p, off)
	if n >= 8 && bytes.Contains(p[:n], []byte("OBJ2")) && d.blocked.CompareAndSwap(false, true) {
		close(d.entered)
		<-d.release
	}
	return n, err
}
func (d *remoteColdProofIO) WriteAt(p []byte, off int64) (int, error) {
	d.writes.Add(1)
	return d.VolumeIO.WriteAt(p, off)
}
func (d *remoteColdProofIO) Sync() error { d.syncs.Add(1); return d.VolumeIO.Sync() }
