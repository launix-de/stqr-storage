// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// This pauses an actual canonical OBJ2 read, not Open or HTTP itself. Further
// reads use independent handles and must proceed while the census owns RLock.
type blockedCensusIO struct {
	volume.VolumeIO
	entered, release chan struct{}
	blocked          atomic.Bool
}

func (b *blockedCensusIO) ReadAt(p []byte, off int64) (int, error) {
	n, err := b.VolumeIO.ReadAt(p, off)
	if n >= 8 && bytes.Contains(p[:n], []byte("OBJ2")) && b.blocked.CompareAndSwap(false, true) {
		close(b.entered)
		<-b.release
	}
	return n, err
}
func warmupFixture(t *testing.T) (string, client.ID, client.ObjectID, string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.yaml")
	d := startDaemonTestNode(t, path, nil)
	addTestVolume(t, d.node, "data", "hdd", 8<<20)
	bucket, _ := createProtocolBucket(t, d.node, "warmup")
	auth := d.node.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, d.node.cfg.Node.Advertise, auth.Username, auth.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	id := client.ObjectID{42}
	if _, err = c.Put(ctx, id, []byte("persistent warmup payload"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Close()
	accountingDir := d.node.accountingDir
	forceColdAccountingOnTestStop(t, d.node)
	d.close()
	// A legacy/unclean deployment may lack this derived sidecar. Its absence
	// must not prevent the real daemon listener from becoming available.
	if err = os.RemoveAll(accountingDir); err != nil {
		t.Fatal(err)
	}
	return path, client.ID(bucket.ID()), id, auth.Username, auth.Password
}
func warmHTTP(t *testing.T, hc *http.Client, url, user, password string, opcode uint16, body []byte) ([]byte, int) {
	t.Helper()
	wire := client.EncodeMessage(client.Message{Kind: client.Request, Opcode: opcode, ID: client.NewRequestID(), Body: body})
	req, _ := http.NewRequest("POST", url+"/api/message", bytes.NewReader(wire))
	req.SetBasicAuth(user, password)
	r, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}

	m, err := client.DecodeMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	decoder := client.Decoder{B: m.Body}
	status := decoder.V()
	if decoder.Err != nil {
		t.Fatal(decoder.Err)
	}
	return m.Body, int(status)
}
func TestStorageWarmupProductionListenerReadsAndMutationGate(t *testing.T) {
	path, bucket, id, user, password := warmupFixture(t)
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
	if err = saveConfig(path, cfg); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var backend *blockedCensusIO
	opts := Options{ConfigPath: path, manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO {
		backend = &blockedCensusIO{VolumeIO: v, entered: entered, release: release}
		return backend
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, opts, io.Discard) }()
	defer func() {
		unblock()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal("Open failed", err)
	case <-time.After(5 * time.Second):
		t.Fatal("census did not reach canonical record")
	}
	hc := &http.Client{Timeout: time.Second}
	url := "http://" + address
	deadline := time.Now().Add(3 * time.Second)
	for {
		req, _ := http.NewRequest("GET", url+"/api/session", nil)
		req.SetBasicAuth(user, password)
		r, e := hc.Do(req)
		if e == nil {
			r.Body.Close()
			if r.StatusCode != 200 {
				t.Fatal("bootstrap Basic auth", r.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real daemon listener blocked behind census", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, status := warmHTTP(t, hc, url, user, password, client.NodeConfigGet, nil); status != 200 {
		t.Fatal("config unavailable", status)
	}
	if _, status := warmHTTP(t, hc, url, user, password, client.StatsGet, []byte{0, 0, 0, 0, 0}); status != 200 {
		t.Fatal("stats unavailable", status)
	}
	c, err := client.Dial(context.Background(), url, user, password, client.BucketID(bucket), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	readctx, readcancel := context.WithTimeout(context.Background(), time.Second)
	defer readcancel()
	got, _, err := c.Get(readctx, id)
	if err != nil || string(got) != "persistent warmup payload" {
		t.Fatal("GET blocked/incorrect during census", string(got), err)
	}
	if _, status := warmHTTP(t, hc, url, user, password, client.ObjectAccepted, nil); status != 503 {
		t.Fatal("cold mutation", status)
	}
	// A rejected writer must not queue ahead of the next reader on RWMutex.
	if _, _, err = c.Get(readctx, id); err != nil {
		t.Fatal("GET after rejected writer", err)
	}
	unblock()
	for {
		wctx, wcancel := context.WithTimeout(context.Background(), time.Second)
		_, err = c.Put(wctx, client.ObjectID{44}, []byte("ready"), client.PutOptions{})
		wcancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline.Add(3 * time.Second)) {
			t.Fatal("never became write-ready", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if backend == nil || !backend.blocked.Load() {
		t.Fatal("test did not block physical record read")
	}
}

func TestStorageWarmupCacheHitAndExactCounters(t *testing.T) {
	path, bucket, _, _, _ := warmupFixture(t)
	s, err := Open(Options{ConfigPath: path, manualRanges: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = s.WaitStorageReady(ctx); err != nil {
		s.Close()
		t.Fatal(err)
	}
	image := s.quotaUsage.Load()
	var wantLogical, wantPhysical, wantObjects uint64
	for _, usage := range image.volumes {
		for _, u := range usage {
			if u.Bucket == volume.ID(bucket) {
				wantLogical += u.Logical
				wantPhysical += u.Physical
				wantObjects += u.Objects
			}
		}
	}
	if wantLogical != uint64(len("persistent warmup payload")) || wantPhysical == 0 || wantObjects != 1 {
		s.Close()
		t.Fatal("inexact restored counters", wantLogical, wantPhysical, wantObjects)
	}
	dir := s.accountingDir
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) == 0 {
		t.Fatal("no derived checkpoint sidecar", err)
	}
	var recordReads atomic.Uint64
	s, err = Open(Options{ConfigPath: path, manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO { return &countCensusIO{VolumeIO: v, records: &recordReads} }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.WaitStorageReady(ctx); err != nil {
		t.Fatal(err)
	}
	if recordReads.Load() != 0 {
		t.Fatal("cache-hit startup reread object records", recordReads.Load())
	}
	image = s.quotaUsage.Load()
	var logical, physical, objects uint64
	for _, usage := range image.volumes {
		for _, u := range usage {
			if u.Bucket == volume.ID(bucket) {
				logical += u.Logical
				physical += u.Physical
				objects += u.Objects
			}
		}
	}
	if logical != wantLogical || physical != wantPhysical || objects != wantObjects {
		t.Fatal("cache changed quota counters", logical, physical, objects)
	}
}

type countCensusIO struct {
	volume.VolumeIO
	records *atomic.Uint64
}

func (c *countCensusIO) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.VolumeIO.ReadAt(p, off)
	if n >= 8 && bytes.Contains(p[:n], []byte("OBJ2")) {
		c.records.Add(1)
	}
	return n, err
}

func TestStorageWarmupCancelAndUnknownUsage(t *testing.T) {
	path, bucket, _, _, _ := warmupFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s, err := Open(Options{ConfigPath: path, manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO {
		return &blockedCensusIO{VolumeIO: v, entered: entered, release: release}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); s.Close() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("missing background census")
	}
	b, err := s.Bucket(volume.ID(bucket))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.accountedUsage(); statusOf(err) != 503 {
		t.Fatal("cold usage must be unknown", err)
	}
	snapshot := s.CollectStats(client.StatsFilter{})
	if snapshot.NodeState != 2 || snapshot.ReasonBits&(1<<10) == 0 || len(snapshot.Buckets) != 0 {
		t.Fatal("cold counters advertised as ready/zero", snapshot)
	}
	var physical, provisionalAdmission uint64
	for _, v := range snapshot.Volumes {
		physical += v.CapacityPhysical
		provisionalAdmission += v.AdmissionFreePhysical
	}
	if physical == 0 || provisionalAdmission == 0 {
		t.Fatal("fixture must expose physical totals before admission readiness")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = s.WaitStorageReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// A cancelled startup must not wait for an accounting census to finish.
	s.cancel()
	unblock()
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel warmup")
	}
}
func TestStorageWarmupNewEmptyNodeReady(t *testing.T) {
	s, err := Open(Options{ConfigPath: filepath.Join(t.TempDir(), "node.yaml"), manualRanges: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.storageWarming.Load() {
		t.Fatal("new empty cluster requires a census")
	}
}

func TestStorageWarmupReplaysSourceFenceBeforeReads(t *testing.T) {
	path, bucket, id, _, _ := warmupFixture(t)
	s, err := Open(Options{ConfigPath: path, manualRanges: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = s.WaitStorageReady(ctx); err != nil {
		s.Close()
		t.Fatal(err)
	}
	before, ok := s.routingPatch(volume.ID(bucket), volume.ObjectID(id))
	if !ok {
		s.Close()
		t.Fatal("missing map")
	}
	left, _, err := client.SplitRange(before.Entries[0])
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	before.Addresses = append(before.Addresses, client.NodeAddress{Node: client.ID{0xee}, Addresses: []string{"http://127.0.0.1:1"}})
	plan, err := planRangeMove(before, left, client.ID{0xee}, volume.ID{0xdd}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	wire, err := plan.marshal()
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	r := s.scopes
	r.mu.Lock()
	_, err = r.persist(rangeReserved, rangeEvent{ID: plan.id(), Plan: wire})
	if err == nil {
		_, err = r.persist(rangeFenced, rangeEvent{ID: plan.id()})
	}
	r.mu.Unlock()
	dir := s.accountingDir
	forceColdAccountingOnTestStop(t, s)
	if e := s.Close(); err == nil {
		err = e
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s, err = Open(Options{ConfigPath: path, manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO {
		return &blockedCensusIO{VolumeIO: v, entered: entered, release: release}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); s.Close() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no cold census")
	}
	if s.scopes == nil {
		t.Fatal("HTTP could start without replayed fences")
	}
	b, err := s.Bucket(volume.ID(bucket))
	if err != nil {
		t.Fatal(err)
	}
	if data, _, err := b.Get(ctx, userPrincipal(t, s), volume.ObjectID(id)); statusOf(err) != 503 {
		t.Fatal("served fenced old-source copy", string(data), err)
	}
	if err = s.checkObjectOwner(volume.ID(bucket), volume.ObjectID{0x90}, true); err != nil {
		t.Fatal("unaffected range blocked", err)
	}
}

func TestStorageWarmupReadonlyCOWDoesNotWriteStorage(t *testing.T) {
	path, bucket, id, _, _ := warmupFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s, err := Open(Options{ConfigPath: path, manualRanges: true, wrapDataVolumeIO: func(v volume.VolumeIO) volume.VolumeIO {
		return &blockedCensusIO{VolumeIO: v, entered: entered, release: release}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); s.Close() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no blocked census")
	}
	var beforeWrites, beforeSyncs uint64
	for _, v := range s.quotaVolumes() {
		stats := v.IOStats()
		beforeWrites += stats.WriteOps
		beforeSyncs += stats.SyncOps
	}
	capture := client.Encoder{}
	capture.ID(bucket)
	capture.B = append(capture.B, id[:]...)
	capture.Bool(false)
	capture.Bool(false)
	raw, err := s.cowPeerMessage(context.Background(), client.Message{Kind: client.Request, Opcode: peerCowCapture, Body: capture.B})
	if err != nil {
		t.Fatal(err)
	}
	d := client.Decoder{B: raw}
	if !d.Bool() {
		t.Fatal("missing read capture")
	}
	snapshot := d.ID()
	read := client.Encoder{}
	read.ID(snapshot)
	read.U64(0)
	read.U64(uint64(len("persistent warmup payload")))
	raw, err = s.cowPeerMessage(context.Background(), client.Message{Kind: client.Request, Opcode: peerCowRead, Body: read.B})
	if err != nil {
		t.Fatal(err)
	}
	d = client.Decoder{B: raw}
	if string(d.Blob()) != "persistent warmup payload" || d.Done() != nil {
		t.Fatal("incorrect inherited read")
	}
	closeBody := client.Encoder{}
	closeBody.ID(snapshot)
	if _, err = s.cowPeerMessage(context.Background(), client.Message{Kind: client.Request, Opcode: peerCowClose, Body: closeBody.B}); err != nil {
		t.Fatal(err)
	}
	var writes, syncs uint64
	for _, v := range s.quotaVolumes() {
		stats := v.IOStats()
		writes += stats.WriteOps
		syncs += stats.SyncOps
	}
	if writes != beforeWrites || syncs != beforeSyncs {
		t.Fatal("read-only COW wrote/synced storage", beforeWrites, writes, beforeSyncs, syncs)
	}
	for _, op := range []uint16{peerVerifyFrontend, peerCowCapture, peerCowRead, peerCowClose, peerCowInventory} {
		if !storageWarmReadMessage(client.Message{Opcode: op}) {
			t.Fatalf("read operation %04x blocked", op)
		}
	}
	for _, op := range []uint16{peerCowMaterialize, peerCowSync, client.ObjectReference, client.ObjectAccepted, client.VolumeGrow, client.BucketCreate, client.ConnectionSync, peerPGRebuildRequest} {
		if storageWarmReadMessage(client.Message{Opcode: op}) {
			t.Fatalf("mutation %04x admitted", op)
		}
	}
}

// These tests exercise genuinely cold/legacy recovery. A clean modern stop also
// retains exact O(bucket) aggregates inside its image, so removing only the
// separate advisory cache no longer makes the next process cold. Cancel this
// owned fixture's background jobs and replace its accounting hooks with an
// explicitly unkeyed equivalent predicate before stopping it; no image/canonical
// payload is edited and no accounting census or persistence fence is started.
func forceColdAccountingOnTestStop(t *testing.T, s *Server) {
	t.Helper()
	s.cancel()
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.RLock()
	var stores []*volume.Volume
	for _, managed := range s.volumes {
		if managed.store != s.system {
			stores = append(stores, managed.store)
		}
	}
	s.mu.RUnlock()
	for _, store := range stores {
		if _, err := store.ConfigureAccounting(volume.AccountingOptions{Include: func(volume.Key, bool) bool { return true }}); err != nil {
			t.Fatal(err)
		}
	}
}
