// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"slices"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type quotaOwnershipStamp struct {
	node, volume volume.ID
	placement    *placementImage
	view         *systemView
	legacy       uint64
	general      bool
	standalone   bool
}

// System image and placement pointers change for account edits, receipts and
// map envelope revisions too. They are not ownership changes. Compare only the
// immutable inputs actually used by the predicate; the volume can retain its
// totals when changed buckets have no records on that volume.
func (stamp quotaOwnershipStamp) ChangedOwnershipBuckets(previous any) ([]volume.ID, bool) {
	before, ok := previous.(quotaOwnershipStamp)
	if !ok || stamp.general != before.general || stamp.standalone != before.standalone || stamp.node != before.node || stamp.volume != before.volume {
		return nil, false
	}
	if stamp.standalone {
		return nil, true
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
		if !slices.Equal(before.ownershipSegments(volume.ID(id)), stamp.ownershipSegments(volume.ID(id))) {
			changed = append(changed, volume.ID(id))
		}
	}
	return changed, true
}

// Normalize the predicate rather than map metadata: epochs, policy labels and
// splits do not change accounting when every affected object remains owned by
// the same volume. Recovery-origin rules are evaluated by the actual predicate.
type ownershipSegment struct {
	start              client.ObjectID
	ordinary, imported bool
}

func (stamp quotaOwnershipStamp) ownershipSegments(bucket volume.ID) []ownershipSegment {
	if stamp.placement == nil {
		return nil
	}
	var out []ownershipSegment
	for _, entry := range stamp.placement.bucketEntries(client.ID(bucket)) {
		key := volume.Key{Bucket: bucket, Object: volume.ObjectID(entry.Start)}
		next := ownershipSegment{start: entry.Start, ordinary: stamp.includesRecord(key, false), imported: stamp.includesRecord(key, true)}
		if len(out) > 0 && out[len(out)-1].ordinary == next.ordinary && out[len(out)-1].imported == next.imported {
			continue
		}
		out = append(out, next)
	}
	// A missing bucket and a present but wholly unowned bucket have identical
	// predicates. Keep future filter installation independent of this delta.
	if len(out) == 1 && !out[0].ordinary && !out[0].imported {
		return nil
	}
	return out
}

func (stamp quotaOwnershipStamp) OwnershipForEntireBucket(bucket volume.ID) (bool, bool) {
	if stamp.standalone {
		return true, true
	}
	if stamp.node == (volume.ID{}) || stamp.volume == (volume.ID{}) {
		return false, false
	}
	segments := stamp.ownershipSegments(bucket)
	if len(segments) == 0 {
		return false, true
	}
	if len(segments) == 1 && segments[0].ordinary == segments[0].imported {
		return segments[0].ordinary, true
	}
	return false, false
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
	stamp := quotaOwnershipStamp{node: node, volume: id, placement: image, view: view, general: general, standalone: s.controller == nil}
	if s.controller == nil {
		s.mu.RLock()
		stamp.legacy = s.cfg.Generation
		s.mu.RUnlock()
	}
	return v.SetOwnershipFilter(stamp, stamp.includesRecord)
}

// Shared by accounting and semantic-delta classification. It reads immutable
// snapshots only and cannot acquire server/volume locks.
func (stamp quotaOwnershipStamp) includesRecord(k volume.Key, imported bool) bool {
	if stamp.standalone {
		return true
	}
	if stamp.placement == nil {
		return false
	}
	entry, known := stamp.placement.lookup(client.ID(k.Bucket), client.ObjectID(k.Object))
	if !known || entry.State == 4 || entry.State == 5 || len(entry.Locations) == 0 {
		return false
	}
	if len(entry.Locations) > 1 {
		for _, loc := range entry.Locations {
			if loc.Role <= 1 && loc.Node == client.ID(stamp.node) {
				if loc.Volume != (client.ID{}) {
					return loc.Volume == client.ID(stamp.volume)
				}
				return !imported && stamp.general
			}
		}
		return false
	}
	if entry.Locations[0].Node != client.ID(stamp.node) {
		return false
	}
	if entry.Locations[0].Volume != (client.ID{}) {
		return entry.Locations[0].Volume == client.ID(stamp.volume)
	}
	if imported {
		return false
	}
	if stamp.view != nil {
		for _, origin := range stamp.view.manifest.VolumeRecoveries {
			if origin.Complete && origin.Before.Bucket == client.ID(k.Bucket) && sameRangeAuthority(origin.After, entry) {
				return hasControlNode(origin.Volumes, client.ID(stamp.volume))
			}
		}
	}
	return stamp.general
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
