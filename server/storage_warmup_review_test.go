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
