// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"github.com/launix-de/stqr-storage/volume"
)

// Snapshot/recovery uses the fenced BEFORE placement, even after the source's
// local map has committed the target. It must never resolve through that newer
// map and mistake an unavailable RAM target for a missing source object.
func (r *rangeManager) selectSnapshotSource(p rangePlan, key volume.Key) (*volume.Volume, error) {
	b, err := r.server.Bucket(key.Bucket)
	if err != nil {
		return nil, err
	}
	description := b.Describe()
	if description.DesiredPolicy != nil && p.Before.Entries[0].PolicyVersion == description.DesiredPolicy.Version {
		description.Policy = *description.DesiredPolicy
	}
	pin := volume.ID(p.Before.Entries[0].Locations[0].Volume)
	var selected *volume.Volume
	for _, store := range b.volumesFor(description) {
		if pin != (volume.ID{}) && store.Identity().VolumeID != pin || !r.server.rangeClass(description.Policy, p.Before.Entries[0]).Allows(store.Identity().Class) || !r.server.volumeAllowedForEntry(p.Before.Bucket, p.Before.Entries[0], store.Identity().VolumeID, r.server.currentSystem.Load()) {
			continue
		}
		h, err := store.Open(false)
		if err != nil {
			return nil, err
		}
		_, err = h.RecordInfo(key)
		if err == nil && pin == (volume.ID{}) {
			shadow, e := h.ImportShadow(key)
			if e != nil {
				err = e
			} else if shadow {
				err = volume.ErrNotFound
			}
		}
		h.Close()
		if errors.Is(err, volume.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if selected != nil && selected != store {
			return nil, fail(503, "source has ambiguous physical copies")
		}
		if selected == nil {
			selected = store
		}
	}
	if selected == nil {
		return nil, volume.ErrNotFound
	}
	return selected, nil
}
