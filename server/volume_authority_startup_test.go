// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// The legacy constructor returns only after its authority cut is durable. The
// separate background-startup integration uses the same unlocked fence helper
// before lifting its mutation gate instead of blocking Open.
func TestAuthorityFenceLegacyConstructorRepairAndErrorClose(t *testing.T) {
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
	reopened, err := Open(Options{ConfigPath: n.path, manualRanges: true})
	if err != nil {
		t.Fatal("legacy startup did not await missing header", err)
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
		t.Fatal("constructor returned without authority proof", err)
	}
	if !dataVolume.IsDurableThrough(dataVolume.Stats().Generation) {
		reopened.Close()
		t.Fatal("legacy Open acknowledged unconfirmed header")
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
		failed.Close()
		t.Fatal("corrupt authority accepted")
	}
	// Do not call failed.Close here: Open's own error path must join workers and
	// release the device claim. Reopening immediately proves that ownership ends.
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
	healthy.Close()
}
