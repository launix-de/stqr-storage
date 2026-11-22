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
	var latest volume.Info
	for _, store := range b.volumesFor(description) {
		if pin != (volume.ID{}) && store.Identity().VolumeID != pin || !bucketClass(description.Policy).Allows(store.Identity().Class) {
			continue
		}
		h, err := store.Open(false)
		if err != nil {
			return nil, err
		}
		info, err := h.RecordInfo(key)
		h.Close()
		if errors.Is(err, volume.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if selected == nil || info.Version > latest.Version || info.Version == latest.Version && info.Deleted {
			selected, latest = store, info
		}
	}
	if selected == nil {
		return nil, volume.ErrNotFound
	}
	return selected, nil
}
