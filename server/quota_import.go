// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type quotaImportKey struct{ Bucket, Volume client.ID }
type quotaImportImage struct{ reserved map[quotaImportKey]uint64 }

// An import may move its OWN bucket's unused guarantee into the disjoint import
// reserve. The atomic image avoids entering the range mutex from a storage
// admission callback (which can itself run inside a scoped journal operation).
func (r *rangeManager) publishQuotaImportsLocked() {
	image := &quotaImportImage{reserved: make(map[quotaImportKey]uint64)}
	for _, t := range r.txns {
		if r.active(t) && t.plan.Target == r.node && t.remaining != 0 {
			k := quotaImportKey{Bucket: t.plan.Before.Bucket, Volume: client.ID(t.plan.Volume)}
			image.reserved[k] = addCapacity(image.reserved[k], t.remaining)
		}
	}
	r.server.quotaImports.Store(image)
}
func (s *Server) importReserved(bucket, device client.ID) uint64 {
	if i := s.quotaImports.Load(); i != nil {
		return i.reserved[quotaImportKey{bucket, device}]
	}
	return 0
}
func (s *Server) refreshVolumeQuota(store *volume.Volume) error {
	h, err := store.Open(false)
	if err != nil {
		return err
	}
	u, err := h.StorageUsage()
	h.Close()
	if err != nil {
		return err
	}
	return store.SetGuaranteedReserve(s.quotaReserve(client.ID(store.Identity().VolumeID), u))
}
func (s *Server) bucketVolumeGuarantee(bucket, device client.ID, usage []volume.BucketStorage) uint64 {
	var n uint64
	for _, view := range []*systemView{s.currentSystem.Load(), s.pendingQuotas.Load()} {
		if view == nil {
			continue
		}
		for _, r := range view.manifest.QuotaReserves {
			if r.Bucket == bucket && r.Volume == device && r.Node.String() == s.cfg.Node.ID {
				n = max(n, subtractCapacity(r.Backing, storagePhysical(usage, volume.ID(bucket))))
			}
		}
	}
	return n
}

func (s *Server) volumeBacksGuarantee(id volume.ID) bool {
	for _, view := range []*systemView{s.currentSystem.Load(), s.pendingQuotas.Load()} {
		if view != nil {
			for _, r := range view.manifest.QuotaReserves {
				if r.Node == s.quotaNode && r.Volume == client.ID(id) {
					return true
				}
			}
		}
	}
	return false
}

// Draining is explicit administrative intent; a SHORT/offline node is never a
// reason to revoke its promises or redistribute membership automatically.
func (s *Server) quotaBackingDrains() bool {
	view := s.currentSystem.Load()
	if view == nil {
		return false
	}
	local := s.CollectStats(client.StatsFilter{})
	images := map[client.ID]client.StatsSnapshot{local.Node: local}
	s.peerStatsMu.RLock()
	for id, o := range s.peerStats {
		if o.snapshot != nil {
			images[id] = *o.snapshot
		}
	}
	s.peerStatsMu.RUnlock()
	for _, r := range view.manifest.QuotaReserves {
		image, ok := images[r.Node]
		if !ok {
			continue
		}
		if image.NodeState == 1 {
			return true
		}
		for _, v := range image.Volumes {
			if v.ID == r.Volume && v.State == 2 {
				return true
			}
		}
	}
	return false
}

func (s *Server) quotaRefreshCoordinator(members []client.ID) client.ID {
	s.mu.RLock()
	localDraining := s.cfg.Node.Draining
	s.mu.RUnlock()
	s.peerStatsMu.RLock()
	defer s.peerStatsMu.RUnlock()
	for _, id := range members {
		if id == s.quotaNode {
			if localDraining {
				continue
			}
			return id
		}
		if p := s.peerStats[id]; p != nil && p.snapshot != nil && p.snapshot.NodeState == 1 {
			continue
		}
		return id
	}
	return client.ID{}
}
