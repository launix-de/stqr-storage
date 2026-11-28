// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Restart records are node-owned OS writeback metadata, not a durability ACK.
// Their activation and old-boot Sync checks are separate from this codec.
type restartPrefixKey struct {
	Boot   client.ID
	Bucket volume.ID
}
type restartVolumeCut struct {
	Volume     volume.ID
	Generation uint64
}
type restartPrefix struct {
	Boot                client.ID
	Bucket              volume.ID
	Sequence, Confirmed uint64
	Unavailable         bool
	Volumes             []restartVolumeCut
}
type restartManifest struct {
	Schema                 uint16
	Handoff, Cluster, Node volume.ID
	Boot                   client.ID
	Volumes                []volume.RestartReceipt
	Prefixes               []restartPrefix
}
type restartIntent struct {
	Schema  uint16
	Handoff volume.ID
	Volumes []volume.ID
}
type restartState struct {
	cluster, node volume.ID
	directory     string
	ready         bool
	manifest      *restartManifest
	sources       map[volume.ID]volume.RestartReceipt
	stores        map[volume.ID]*volume.Volume        // identity/receipt-verified startup bindings, immutable while serving
	prefixes      map[restartPrefixKey]*restartPrefix // Server.durability.mu
}

// No Sync/fsync here: a sealed image survives an orderly process handoff in
// OS writeback state, but the caller has not requested physical durability.
func writeRestartRecord(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".handoff-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	hash := sha256.New()
	if err = f.Chmod(0600); err == nil {
		err = json.NewEncoder(io.MultiWriter(f, hash)).Encode(value)
	}
	if err == nil {
		var n int
		sum := hash.Sum(nil)
		n, err = f.Write(sum)
		if err == nil && n != len(sum) {
			err = io.ErrShortWrite
		}
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}
func readRestartRecord(path string, value any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() || stat.Size() < sha256.Size {
		return volume.ErrCorrupt
	}
	hash := sha256.New()
	decoder := json.NewDecoder(io.TeeReader(io.NewSectionReader(f, 0, stat.Size()-sha256.Size), hash))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return volume.ErrCorrupt
	}
	var seal [sha256.Size]byte
	if _, err = f.ReadAt(seal[:], stat.Size()-sha256.Size); err != nil {
		return err
	}
	if !bytes.Equal(hash.Sum(nil), seal[:]) {
		return volume.ErrCorrupt
	}
	return nil
}
