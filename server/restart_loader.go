// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Separate from startup activation: required evidence cannot become empty data.
func restartDirectory(directory string, cluster, node volume.ID) string {
	return filepath.Join(directory, "writeback", cluster.String(), node.String())
}

func loadRestartState(directory string, cluster, node volume.ID) (*restartState, error) {
	r := &restartState{directory: restartDirectory(directory, cluster, node), cluster: cluster, node: node, sources: map[volume.ID]volume.RestartReceipt{}, stores: map[volume.ID]*volume.Volume{}, prefixes: map[restartPrefixKey]*restartPrefix{}}
	var intent restartIntent
	err := readRestartRecord(filepath.Join(r.directory, "required"), &intent)
	if os.IsNotExist(err) {
		if _, other := os.Stat(filepath.Join(r.directory, "current")); !os.IsNotExist(other) {
			return nil, fmt.Errorf("%w: missing restart intent", volume.ErrRecovery)
		}
		return r, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: restart intent: %v", volume.ErrRecovery, err)
	}
	var m restartManifest
	if err = readRestartRecord(filepath.Join(r.directory, "current"), &m); err != nil {
		return nil, fmt.Errorf("%w: restart manifest: %v", volume.ErrRecovery, err)
	}
	if cluster == (volume.ID{}) || node == (volume.ID{}) || m.Schema != 1 || intent.Schema != 1 || m.Handoff == (volume.ID{}) || m.Handoff != intent.Handoff || m.Cluster != cluster || m.Node != node || m.Boot == (client.ID{}) || len(intent.Volumes) != len(m.Volumes) {
		return nil, volume.ErrRecovery
	}
	required := map[volume.ID]bool{}
	for _, id := range intent.Volumes {
		if required[id] || id == (volume.ID{}) {
			return nil, volume.ErrRecovery
		}
		required[id] = true
	}
	for _, receipt := range m.Volumes {
		if !required[receipt.Volume] || receipt.Handoff != m.Handoff || receipt.Cluster != cluster || receipt.Node != node || receipt.BaselineGeneration == 0 || receipt.ConfirmedGeneration > receipt.BaselineGeneration || receipt.Generation < receipt.BaselineGeneration || receipt.HeaderDigest == ([32]byte{}) {
			return nil, volume.ErrRecovery
		}
		delete(required, receipt.Volume)
		r.sources[receipt.Volume] = receipt
	}
	for _, p := range m.Prefixes {
		key := restartPrefixKey{p.Boot, p.Bucket}
		if p.Boot == (client.ID{}) || p.Sequence == 0 || p.Confirmed > p.Sequence || r.prefixes[key] != nil {
			return nil, volume.ErrRecovery
		}
		seen := map[volume.ID]bool{}
		for _, cut := range p.Volumes {
			receipt, ok := r.sources[cut.Volume]
			if cut.Volume == (volume.ID{}) || cut.Generation == 0 || seen[cut.Volume] || !p.Unavailable && (!ok || cut.Generation > receipt.Generation) {
				return nil, volume.ErrRecovery
			}
			seen[cut.Volume] = true
		}
		if p.Confirmed < p.Sequence && !p.Unavailable && len(p.Volumes) == 0 {
			return nil, volume.ErrRecovery
		}
		copy := p
		copy.Volumes = append([]restartVolumeCut(nil), p.Volumes...)
		r.prefixes[key] = &copy
	}
	r.manifest = &m
	return r, nil
}
func (r *restartState) imagePath(receipt volume.RestartReceipt) string {
	return filepath.Join(r.directory, receipt.Handoff.String(), receipt.Volume.String()+".image")
}
func (r *restartState) source(id volume.ID) *volume.RestartSource {
	if r == nil {
		return nil
	}
	receipt, ok := r.sources[id]
	if !ok {
		return nil
	}
	return &volume.RestartSource{Path: r.imagePath(receipt), Receipt: receipt}
}

func (r *restartState) validateConfigured(configured []VolumeConfig) error {
	if r == nil || r.manifest == nil {
		return nil
	}
	pending := make(map[volume.ID]bool, len(r.sources))
	for id := range r.sources {
		pending[id] = true
	}
	for _, c := range configured {
		id, err := parseID(c.ID)
		if err != nil {
			return volume.ErrRecovery
		}
		if pending[id] {
			class, err := classFromString(c.Class)
			if err != nil || class == volume.RAM || class == volume.RAMCache {
				return volume.ErrRecovery
			}
			delete(pending, id)
		}
	}
	if len(pending) != 0 {
		return fmt.Errorf("%w: required retained store is not configured", volume.ErrRecovery)
	}
	return nil
}

func (r *restartState) bindStores(stores map[string]*managedVolume) error {
	for id := range r.sources {
		managed := stores[id.String()]
		if managed == nil || managed.store.Identity().VolumeID != id {
			return volume.ErrRecovery
		}
		r.stores[id] = managed.store
	}
	return nil
}
