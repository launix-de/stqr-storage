// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestRestartRetentionServerFileHandoff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.yaml")
	opts := Options{ConfigPath: path, transportOnly: true, values: map[string]string{"system-volume-bytes": "262144"}}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	added := addTestVolume(t, s, "persistent", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "retained-data")
	bucketID := b.ID()
	p := quotaPrincipal(t, s)
	ctx := context.Background()
	object := volume.ObjectID{1}
	deleted := volume.ObjectID{2}
	if _, err = b.Put(ctx, p, object, []byte("base"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Put(ctx, p, deleted, []byte("gone"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	store := s.volumes[added.VolumeID].store
	before := store.IOStats().SyncOps
	if _, err = b.CAS(ctx, p, object, 0, []byte("base"), []byte("new"), volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	request := volume.ID(client.NewRequestID())
	if _, err = b.Append(ctx, p, object, []byte("-append"), volume.Options{Accepted: true, RequestID: request}); err != nil {
		t.Fatal(err)
	}
	if err = b.Delete(ctx, p, deleted, volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	mark, ok := s.acceptedMark(bucketID)
	if !ok {
		t.Fatal("missing accepted mark")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if store.IOStats().SyncOps != before {
		t.Fatal("ordinary node close secretly synced data")
	}
	raw, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(raw, []byte("restart_retention_version: 1")) {
		t.Fatal("old decoder guard missing", err)
	}
	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.boot == mark.Boot {
		t.Fatal("reused process boot")
	}
	b, err = w.Bucket(bucketID)
	if err != nil {
		t.Fatal(err)
	}
	p = quotaPrincipal(t, w)
	got, _, err := b.Get(ctx, p, object)
	if err != nil || string(got) != "new-append" {
		t.Fatal("accepted file state lost", string(got), err)
	}
	if _, _, err = b.Get(ctx, p, deleted); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("accepted deletion lost", err)
	}
	if _, err = b.Append(ctx, p, object, []byte("-append"), volume.Options{Accepted: true, RequestID: request}); err != nil {
		t.Fatal(err)
	}
	got, _, err = b.Get(ctx, p, object)
	if err != nil || string(got) != "new-append" {
		t.Fatal("append repeated after restart", string(got), err)
	}
	proof := w.restart.prefixes[restartPrefixKey{mark.Boot, bucketID}]
	if proof == nil || proof.Sequence < mark.Sequence || proof.Confirmed >= proof.Sequence {
		t.Fatal("accepted mark falsely confirmed or forgotten", proof)
	}
	for _, m := range w.volumes {
		if m.config.System {
			continue
		} // existing administrative startup writes are separate
		if m.store.IOStats().SyncOps != 0 {
			t.Fatalf("restore initiated data Sync on %s: %+v", m.config.Name, m.store.IOStats())
		}
	}
}
func TestRestartRetentionRequiredManifestBlocksStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.yaml")
	opts := Options{ConfigPath: path, transportOnly: true}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	current := filepath.Join(s.restart.directory, "current")
	if err = os.Remove(current); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(opts); !errors.Is(err, volume.ErrRecovery) {
		t.Fatal("missing required handoff became an empty node", err)
	}
}

func restartSyncRequest(bucket volume.ID, mark client.CommitMark) client.Message {
	e := client.Encoder{}
	e.ID(client.ID(bucket))
	e.ID(mark.Boot)
	e.U64(mark.Sequence)
	return client.Message{Kind: client.Request, Opcode: client.BucketSync, ID: client.NewRequestID(), Body: e.B}
}
func TestRestartRetentionThreeBootsOldPrefixAndUnknown(t *testing.T) {
	ctx := context.Background()
	opts := Options{ConfigPath: filepath.Join(t.TempDir(), "node.yaml"), transportOnly: true, values: map[string]string{"system-volume-bytes": "262144"}}
	first, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	added := addTestVolume(t, first, "three-boots", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, first, "three-boots")
	bucket := b.ID()
	object := volume.ObjectID{61}
	if _, err = b.Put(ctx, quotaPrincipal(t, first), object, []byte("first"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	a, ok := first.acceptedMark(bucket)
	if !ok {
		t.Fatal("first mark missing")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	b, err = second.Bucket(bucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.PutRange(ctx, quotaPrincipal(t, second), object, 0, []byte("later"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	z, ok := second.acceptedMark(bucket)
	if !ok {
		t.Fatal("second mark missing")
	}
	if z.Boot == a.Boot {
		t.Fatal("boot reused")
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	store := third.volumes[added.VolumeID].store
	if store.IOStats().SyncOps != 0 || len(third.restart.prefixes) != 2 {
		t.Fatal("restore synced or lost proof")
	}
	actor := quotaPrincipal(t, third)
	unknown := client.CommitMark{Boot: client.NewRequestID(), Sequence: 1}
	for _, tc := range []struct {
		mark   client.CommitMark
		status int
	}{{unknown, 410}, {client.CommitMark{Boot: a.Boot, Sequence: a.Sequence + 1}, 412}} {
		if _, status, err := third.executeSync(ctx, restartSyncRequest(bucket, tc.mark), nil, actor); err == nil || status != tc.status {
			t.Fatal("unproven prefix accepted", status, err)
		}
	}
	if store.IOStats().SyncOps != 0 {
		t.Fatal("unknown prefix started fence")
	}
	for i, mark := range []client.CommitMark{a, z} {
		before := store.IOStats().SyncOps
		raw, status, err := third.executeSync(ctx, restartSyncRequest(bucket, mark), nil, actor)
		if err != nil || status != 200 {
			t.Fatal("retained prefix rejected", status, err)
		}
		confirmed, err := client.DecodeCommitMark(raw)
		if err != nil || confirmed.Boot != mark.Boot || confirmed.Sequence < mark.Sequence {
			t.Fatal("wrong boot confirmed", confirmed, err)
		}
		if i == 0 && store.IOStats().SyncOps <= before {
			t.Fatal("old Sync did not persist retained data")
		}
		if i == 1 && store.IOStats().SyncOps != before {
			t.Fatal("already persisted prefix repeated fence")
		}
	}
	b, err = third.Bucket(bucket)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Get(ctx, actor, object)
	if err != nil || string(got) != "later" {
		t.Fatal("latest data lost", string(got), err)
	}
	if err = third.Close(); err != nil {
		t.Fatal(err)
	}
	fourth, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer fourth.Close()
	for _, mark := range []client.CommitMark{a, z} {
		if _, status, err := fourth.executeSync(ctx, restartSyncRequest(bucket, mark), nil, quotaPrincipal(t, fourth)); err != nil || status != 200 {
			t.Fatal("confirmed proof lost", status, err)
		}
	}
	if fourth.volumes[added.VolumeID].store.IOStats().SyncOps != 0 {
		t.Fatal("confirmed proof started fence")
	}
}

// Keep the actual client alive across a file-backed owner restart. Reading the
// retained bytes is independent of explicitly confirming its old boot prefix.
func TestRestartRetentionClientReconnectAndOldFence(t *testing.T) {
	var active atomic.Pointer[Server]
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := active.Load()
		if s == nil {
			http.Error(w, "restarting", 503)
			return
		}
		s.Handler().ServeHTTP(w, r)
	}))
	defer gateway.Close()
	opts := Options{ConfigPath: filepath.Join(t.TempDir(), "node.yaml"), transportOnly: true, values: map[string]string{"advertise": gateway.URL, "system-volume-bytes": "262144"}}
	first, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	added := addTestVolume(t, first, "reconnect", "hdd", 16<<20)
	bucket, _ := createProtocolBucket(t, first, "reconnect")
	active.Store(first)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := first.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, gateway.URL, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	object := client.ObjectID{91}
	if _, err = c.Put(ctx, object, []byte("retained socket write"), client.PutOptions{Accepted: true, RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	active.Store(nil)
	second, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	active.Store(second)
	store := second.volumes[added.VolumeID].store
	got, _, err := c.Get(ctx, object)
	if err != nil || string(got) != "retained socket write" {
		t.Fatal("same client did not reconnect to retained bytes", string(got), err)
	}
	if store.IOStats().SyncOps != 0 {
		t.Fatal("reconnection/read implicitly synced")
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal("same client cannot explicitly confirm old accepted prefix", err)
	}
	if store.IOStats().SyncOps == 0 {
		t.Fatal("explicit client fence did not reach retained volume")
	}
}
