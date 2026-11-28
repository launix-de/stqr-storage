// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/launix-de/stqr-storage/volume"
)

// Caller has stopped publication and joined serving/background jobs, then
// acquired node admission. Holding durability.mu across Volume calls would
// reverse mutation's lock order; capture primitive refs before reading stores.
func (s *Server) retainRestartLocked(r *restartState) error {
	if r == nil {
		return nil
	}
	handoff, err := volume.NewID()
	if err != nil {
		return err
	}
	s.mu.RLock()
	stores := make([]*volume.Volume, 0, len(s.volumes))
	for _, managed := range s.volumes {
		stores = append(stores, managed.store)
	}
	s.mu.RUnlock()
	// Collect identity after releasing node/config locks. Retention covers whole
	// stores, including deleted buckets and internal admin objects.
	cohort := map[volume.ID]*volume.Volume{}
	for _, store := range stores {
		id := store.Identity()
		if id.Class == volume.RAM || id.Class == volume.RAMCache {
			continue
		}
		if id.ClusterID != r.cluster || id.NodeID != r.node {
			return volume.ErrIdentity
		}
		cohort[id.VolumeID] = store
	}
	intent := restartIntent{Schema: 1, Handoff: handoff}
	for id := range cohort {
		intent.Volumes = append(intent.Volumes, id)
	}
	sort.Slice(intent.Volumes, func(i, j int) bool { return intent.Volumes[i].String() < intent.Volumes[j].String() })
	if err = writeRestartRecord(filepath.Join(r.directory, "required"), intent); err != nil {
		return err
	}
	m := restartManifest{Schema: 1, Handoff: handoff, Cluster: r.cluster, Node: r.node, Boot: s.boot}
	receipts := map[volume.ID]volume.RestartReceipt{}
	for _, id := range intent.Volumes {
		receipt, err := cohort[id].RetainAccepted(filepath.Join(r.directory, handoff.String(), id.String()+".image"), handoff)
		if err != nil {
			return err
		}
		m.Volumes = append(m.Volumes, receipt)
		receipts[id] = receipt
	}
	type pendingPrefix struct {
		prefix restartPrefix
		refs   []durabilityVolume
	}
	var pending []pendingPrefix
	s.durability.mu.Lock()
	for _, p := range r.prefixes {
		copy := *p
		copy.Volumes = append([]restartVolumeCut(nil), p.Volumes...)
		m.Prefixes = append(m.Prefixes, copy)
	}
	for bucket, state := range s.durability.buckets {
		if state.sequence == 0 {
			continue
		}
		p := pendingPrefix{prefix: restartPrefix{Boot: s.boot, Bucket: bucket, Sequence: state.sequence, Confirmed: state.durable}}
		for store, generation := range state.volumes {
			p.refs = append(p.refs, durabilityVolume{store, generation})
		}
		pending = append(pending, p)
	}
	s.durability.mu.Unlock()
	for _, item := range pending {
		p := item.prefix
		if b, err := s.Bucket(p.Bucket); err == nil && b.storageDescription().Policy.StorageStrategy == "ram" {
			p.Unavailable = true
		}
		for _, ref := range item.refs {
			id := ref.v.Identity()
			if id.Class == volume.RAM || id.Class == volume.RAMCache {
				p.Unavailable = true
				continue
			}
			if ref.v.TryIsDurableThrough(ref.generation) {
				continue
			}
			if cohort[id.VolumeID] != ref.v {
				p.Unavailable = true
				continue
			}
			p.Volumes = append(p.Volumes, restartVolumeCut{id.VolumeID, ref.generation})
		}
		if !p.Unavailable && len(p.Volumes) == 0 {
			p.Confirmed = p.Sequence
		}
		m.Prefixes = append(m.Prefixes, p)
	}
	for i := range m.Prefixes {
		p := &m.Prefixes[i]
		if p.Confirmed >= p.Sequence {
			p.Volumes = nil
		}
		for _, cut := range p.Volumes {
			receipt, ok := receipts[cut.Volume]
			if !ok || cut.Generation > receipt.Generation {
				p.Unavailable = true
			}
		}
		sort.Slice(p.Volumes, func(i, j int) bool { return p.Volumes[i].Volume.String() < p.Volumes[j].Volume.String() })
	}
	sort.Slice(m.Prefixes, func(i, j int) bool {
		a, b := m.Prefixes[i], m.Prefixes[j]
		if a.Boot != b.Boot {
			return a.Boot.String() < b.Boot.String()
		}
		return a.Bucket.String() < b.Bucket.String()
	})
	if err := writeRestartRecord(filepath.Join(r.directory, "current"), m); err != nil {
		return err
	}
	r.manifest = &m
	return nil
}

// The caller has closed every borrowed retained view and still owns the node
// lock. The newly published complete image contains every needed dirty page and
// old boot proof. Reclaim only exact private UUID image directories; unrelated
// entries and symlinks are never followed or removed recursively.
func (r *restartState) reclaimObsoleteImages() {
	if r == nil || r.manifest == nil {
		return
	}
	dirs, err := os.ReadDir(r.directory)
	if err != nil {
		return
	}
	for _, dir := range dirs {
		name := dir.Name()
		if !dir.IsDir() || name == r.manifest.Handoff.String() || len(name) != 32 {
			continue
		}
		if _, err := hex.DecodeString(name); err != nil {
			continue
		}
		path := filepath.Join(r.directory, name)
		files, err := os.ReadDir(path)
		if err != nil {
			continue
		}
		safe := true
		for _, file := range files {
			base, ok := strings.CutSuffix(file.Name(), ".image")
			if !ok || len(base) != 32 || file.IsDir() || file.Type()&os.ModeSymlink != 0 {
				safe = false
				break
			}
			if _, err := hex.DecodeString(base); err != nil {
				safe = false
				break
			}
		}
		if !safe {
			continue
		}
		for _, file := range files {
			_ = os.Remove(filepath.Join(path, file.Name()))
		}
		_ = os.Remove(path)
	}
}
