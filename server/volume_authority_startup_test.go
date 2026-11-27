// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Open may expose reads before background recovery completes. The authority
// cut must be durable before storage readiness permits mutations. Both immediate
// and background startup failures must release ownership after their owner closes.
func TestAuthorityFenceStartupRepairAndErrorClose(t *testing.T) {
	n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	added := addTestVolume(t, n.node, "startup-authority", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, n.node, "startup-authority")
	key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}
	want := []byte("must survive constructor repair and failure")
	if _, err := b.Put(context.Background(), quotaPrincipal(t, n.node), key.Object, want, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	path := n.node.volumes[added.VolumeID].config.Path
	n.close()
	disk, err := volume.New(path, 0, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	h, err := disk.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.SetLocalMetadata(nil)
	h.Close()
	disk.Close()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reopened, err := Open(Options{ConfigPath: n.path, manualRanges: true})
	if err != nil {
		t.Fatal("legacy startup did not await missing header", err)
	}
	if err = reopened.WaitStorageReady(ctx); err != nil {
		reopened.Close()
		t.Fatal("startup did not confirm authority", err)
	}
	dataVolume := reopened.volumes[added.VolumeID].store
	h, err = dataVolume.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := h.LocalMetadata()
	h.Close()
	if err != nil || len(proof) == 0 {
		reopened.Close()
		t.Fatal("storage readiness returned without authority proof", err)
	}
	if !dataVolume.IsDurableThrough(dataVolume.Stats().Generation) {
		reopened.Close()
		t.Fatal("storage readiness acknowledged unconfirmed header")
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	disk, err = volume.New(path, 0, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	h, err = disk.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	bad := []byte("invalid authority proof")
	err = h.SetLocalMetadata(bad)
	h.Close()
	disk.Close()
	if err != nil {
		t.Fatal(err)
	}
	failed, err := Open(Options{ConfigPath: n.path, manualRanges: true})
	if err == nil {
		err = failed.WaitStorageReady(ctx)
		if closeErr := failed.Close(); closeErr != nil {
			t.Fatal("background startup failure could not close", closeErr)
		}
	}
	if err == nil {
		t.Fatal("corrupt authority accepted")
	}
	// An immediate Open error closes itself; a returned server is explicitly
	// closed above after its readiness failure. Either path must release claims.
	disk, err = volume.New(path, 0, volume.HDD)
	if err != nil {
		t.Fatal("constructor error leaked volume lock", err)
	}
	h, err = disk.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	gotProof, err := h.LocalMetadata()
	if err != nil || !bytes.Equal(gotProof, bad) {
		h.Close()
		disk.Close()
		t.Fatal("failed startup rewrote corrupt proof", err)
	}
	got, _, err := h.Get(key)
	if err != nil || !bytes.Equal(got, want) {
		h.Close()
		disk.Close()
		t.Fatal("constructor damaged existing data", err)
	}
	err = h.SetLocalMetadata(proof)
	h.Close()
	disk.Close()
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := Open(Options{ConfigPath: n.path, manualRanges: true})
	if err != nil {
		t.Fatal("repair could not reopen constructor", err)
	}
	if err = healthy.WaitStorageReady(ctx); err != nil {
		healthy.Close()
		t.Fatal("repaired authority never became ready", err)
	}
	healthy.Close()
}
