// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Existing heartbeat BucketStats carry maintained counters, not an additional
// dashboard poll or a scan of every object's index. CollectStats holds cfg.mu;
// this path only reads immutable images and cannot invert the volume lock order.
func (s *Server) collectQuotaStats(snap *client.StatsSnapshot, filter client.StatsFilter) {
	image := s.quotaUsage.Load()
	view := s.currentSystem.Load()
	policies := s.quotaPolicies.Load()
	if policies == nil {
		return
	}
	if image == nil {
		image = &quotaUsageImage{}
	}
	usage := make(map[volume.ID]volume.BucketStorage)
	for _, values := range image.volumes {
		for _, v := range values {
			u := usage[v.Bucket]
			u.Logical = addCapacity(u.Logical, v.Logical)
			u.Physical = addCapacity(u.Physical, v.Physical)
			u.Objects = addCapacity(u.Objects, v.Objects)
			usage[v.Bucket] = u
		}
	}
	reserved := make(map[client.ID]uint64)
	if view != nil {
		for _, r := range view.manifest.QuotaReserves {
			if r.Node == s.quotaNode {
				remaining := subtractCapacity(r.Backing, storagePhysical(image.all[volume.ID(r.Volume)], volume.ID(r.Bucket)))
				reserved[r.Bucket] = addCapacity(reserved[r.Bucket], remaining)
			}
		}
	}
	for _, d := range policies.descriptions {
		id, _ := bucketID(d.ID)
		if d.DeletionComplete || d.ID == systemBucket.String() || !matches(filter.Buckets, client.ID(id)) {
			continue
		}
		p := d.Policy
		u := usage[id]
		accounting := uint8(0)
		if p.Limits.Accounting == "physical" {
			accounting = 1
		}
		snap.Buckets = append(snap.Buckets, client.BucketStats{ID: client.ID(id), Accounting: accounting, Strategy: uint8(bucketClass(p)), LogicalBytes: u.Logical, AttributedPhysical: u.Physical, ObjectCount: u.Objects, Guaranteed: p.Limits.Guaranteed, Absolute: p.Limits.Absolute, AbsoluteUnlimited: p.Limits.Unlimited, GuaranteedReservedPhysical: reserved[client.ID(id)]})
	}
}

// A missing/stale member image is unknown, never a fabricated zero. These are
// advisory diagnostics; READY and mutation admission always use local proofs.
func (s *Server) addQuotaDiagnostics(entry *bucketListEntry) {
	p := entry.Policy
	if p.Limits.Guaranteed == 0 {
		return
	}
	view := s.currentSystem.Load()
	local := s.latest.Load()
	if view == nil || local == nil {
		return
	}
	images := map[client.ID]*client.StatsSnapshot{local.Node: local}
	s.peerStatsMu.RLock()
	for id, o := range s.peerStats {
		if o.snapshot != nil {
			images[id] = o.snapshot
		}
	}
	s.peerStatsMu.RUnlock()
	var logical, reserved uint64
	for _, m := range view.manifest.Membership.Members {
		image := images[m.ID]
		if image == nil || image.MembershipEpoch != view.manifest.Membership.Epoch {
			return
		}
		found := false
		for _, b := range image.Buckets {
			if b.ID.String() != entry.ID {
				continue
			}
			if b.Guaranteed != p.Limits.Guaranteed || b.Absolute != p.Limits.Absolute || b.AbsoluteUnlimited != p.Limits.Unlimited {
				return
			}
			logical = addCapacity(logical, b.LogicalBytes)
			reserved = addCapacity(reserved, b.GuaranteedReservedPhysical)
			found = true
			break
		}
		if !found {
			return
		}
	}
	shortfall := uint64(0)
	if p.Limits.Accounting == "logical" {
		shortfall = subtractCapacity(unusedGuarantee(p, logical), reserved)
	}
	entry.GuaranteedRemainingPhysical = &reserved
	entry.GuaranteeEstimateShortfall = &shortfall
}
