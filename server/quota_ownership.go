// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type quotaOwnershipStamp struct {
	placement  *placementImage
	view       *systemView
	legacy     uint64
	general    bool
	standalone bool
}

// System image and placement pointers change for account edits, receipts and
// map envelope revisions too. They are not ownership changes. Compare only the
// immutable inputs actually used by the predicate; the volume can retain its
// totals when changed buckets have no records on that volume.
func (stamp quotaOwnershipStamp) ChangedOwnershipBuckets(previous any) ([]volume.ID, bool) {
	before, ok := previous.(quotaOwnershipStamp)
	if !ok || stamp.general != before.general || stamp.standalone != before.standalone {
		return nil, false
	}
	if stamp.standalone {
		return nil, true
	}
	var previousOrigins, nextOrigins []volumeRecoveryOrigin
	if before.view != nil {
		previousOrigins = before.view.manifest.VolumeRecoveries
	}
	if stamp.view != nil {
		nextOrigins = stamp.view.manifest.VolumeRecoveries
	}
	if !reflect.DeepEqual(previousOrigins, nextOrigins) {
		return nil, false
	}
	buckets := make(map[client.ID]bool)
	for _, image := range []*placementImage{before.placement, stamp.placement} {
		if image != nil {
			for id := range image.bounds {
				buckets[id] = true
			}
		}
	}
	var changed []volume.ID
	for id := range buckets {
		var oldEntries, newEntries []client.RangeEntry
		if before.placement != nil {
			oldEntries = before.placement.bucketEntries(id)
		}
		if stamp.placement != nil {
			newEntries = stamp.placement.bucketEntries(id)
		}
		if !reflect.DeepEqual(oldEntries, newEntries) {
			changed = append(changed, volume.ID(id))
		}
	}
	return changed, true
}

func (s *Server) refreshQuotaOwnership(v *volume.Volume) error {
	image := s.placement.Load()
	view := s.currentSystem.Load()
	id := v.Identity().VolumeID
	s.mu.RLock()
	node, _ := parseID(s.cfg.Node.ID)
	managed := s.volumes[id.String()]
	general := managed != nil && !managed.config.RecoveryOnly
	s.mu.RUnlock()
	stamp := quotaOwnershipStamp{placement: image, view: view, general: general, standalone: s.controller == nil}
	if s.controller == nil {
		s.mu.RLock()
		stamp.legacy = s.cfg.Generation
		s.mu.RUnlock()
	}
	return v.SetOwnershipFilter(stamp, func(k volume.Key, imported bool) bool {
		if s.controller == nil {
			return true
		}
		if image == nil {
			return false
		}
		entry, known := image.lookup(client.ID(k.Bucket), client.ObjectID(k.Object))
		if !known || entry.State == 4 || entry.State == 5 || len(entry.Locations) == 0 {
			return false
		}
		if len(entry.Locations) > 1 {
			for _, loc := range entry.Locations {
				if loc.Role <= 1 && loc.Node == client.ID(node) {
					if loc.Volume != (client.ID{}) {
						return loc.Volume == client.ID(id)
					}
					return !imported && general
				}
			}
			return false
		}
		if entry.Locations[0].Node != client.ID(node) {
			return false
		}
		if entry.Locations[0].Volume != (client.ID{}) {
			return entry.Locations[0].Volume == client.ID(id)
		}
		if imported {
			return false
		}
		if view != nil {
			for _, origin := range view.manifest.VolumeRecoveries {
				if origin.Complete && origin.Before.Bucket == client.ID(k.Bucket) && sameRangeAuthority(origin.After, entry) {
					return hasControlNode(origin.Volumes, client.ID(id))
				}
			}
		}
		return general
	})
}

// Recovery admission measures the prospective owner map before publishing it,
// including staged rescued disks. HOLD bytes cannot silently bypass a finite
// physical quota merely because they were absent from the old live map.
func (b *Bucket) plannedPhysicalUsage(view *systemView) (uint64, error) {
	s := b.server
	stores := s.quotaVolumes()
	s.mu.RLock()
	for _, v := range s.recoveryVolumes {
		stores = append(stores, v.store)
	}
	s.mu.RUnlock()
	node, _ := parseID(s.cfg.Node.ID)
	var total uint64
	for _, v := range stores {
		vid := v.Identity().VolumeID
		h, err := v.Open(false)
		if err != nil {
			return 0, err
		}
		n, err := h.StorageUsageWhere(func(k volume.Key) bool {
			if k.Bucket != b.ID() {
				return false
			}
			entry, known := view.placement.lookup(client.ID(k.Bucket), client.ObjectID(k.Object))
			return known && entry.State != 4 && entry.State != 5 && len(entry.Locations) > 0 && entry.Locations[0].Node == client.ID(node) && s.volumeAllowedForEntry(client.ID(k.Bucket), entry, vid, view)
		})
		h.Close()
		if err != nil {
			return 0, err
		}
		total = addCapacity(total, n)
	}
	return total, nil
}
