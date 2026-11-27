// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"path/filepath"
)

func (s *Server) remainingPhysical(view *systemView, node, device client.ID, usage []volume.BucketStorage) uint64 {
	if view == nil {
		return 0
	}
	var n uint64
	for _, r := range view.manifest.QuotaReserves {
		if r.Node == node && r.Volume == device {
			remaining := subtractCapacity(r.Backing, storagePhysical(usage, volume.ID(r.Bucket)))
			n = addCapacity(n, subtractCapacity(remaining, s.importReserved(r.Bucket, device)))
		}
	}
	return n
}
func (s *Server) quotaReserve(device client.ID, usage []volume.BucketStorage) uint64 {
	node := s.quotaNode
	return max(s.remainingPhysical(s.currentSystem.Load(), client.ID(node), device, usage), s.remainingPhysical(s.pendingQuotas.Load(), client.ID(node), device, usage))
}
func (s *Server) quotaAdmission(store *volume.Volume, device client.ID) volume.Admission {
	return func(change volume.StorageChange) (uint64, error) {
		before, after := change.BeforeOwned, change.AfterOwned
		// Exact cross-volume budget admission: callbacks hold their Volume lock,
		// then this short reservation mutex. No storage/node lock is acquired here.
		q := &s.parallelQuota
		q.mu.Lock()
		defer q.mu.Unlock()
		ticket := q.slots[volume.ID(device)]
		var planned map[volume.ID]uint64
		policies := s.quotaPolicies.Load()
		if policies == nil {
			return s.quotaReserve(device, change.After), nil
		}
		for _, d := range policies.descriptions {
			p := quotaPolicy(d)
			if d.DeletionComplete || d.ID == systemBucket.String() || p.Limits.Unlimited || p.Limits.Accounting != "physical" {
				continue
			}
			bid, _ := bucketID(d.ID)
			prior, next := storagePhysical(before, bid), storagePhysical(after, bid)
			if next <= prior {
				continue
			}
			// Imported objects remain an uncommitted, fenced operating copy. Their
			// activation separately checks the destination's post-handoff allowance.
			var others uint64
			if image := s.quotaUsage.Load(); image != nil {
				for vid, usage := range image.volumes {
					if vid != volume.ID(device) {
						others = addCapacity(others, storagePhysical(usage, bid))
					}
				}
			}
			limit := p.Limits.Absolute
			if s.controller != nil {
				var err error
				limit, err = s.quotaAllowance(client.ID(bid))
				if err != nil {
					return 0, err
				}
			}
			pending := q.physical[bid]
			if ticket != nil {
				pending = subtractCapacity(pending, ticket.physical[bid])
			}
			others = addCapacity(others, pending)
			if others > limit || next > limit-others {
				waiting := false
				if s.quotas != nil && addCapacity(others, next) <= p.Limits.Absolute {
					waiting = s.quotas.requestActivation(bid, volume.ObjectID{0xff, 0x51}, addCapacity(others, next))
				}
				return 0, &physicalQuotaFull{Bucket: bid, Growth: next - prior, Waiting: waiting}
			}
			if ticket != nil {
				if planned == nil {
					planned = make(map[volume.ID]uint64)
				}
				planned[bid] = next - prior
			}
		}
		if ticket != nil {
			q.releasePhysicalLocked(ticket)
			if len(planned) > 0 && q.physical == nil {
				q.physical = make(map[volume.ID]uint64)
			}
			ticket.physical = planned
			for id, n := range planned {
				q.physical[id] = addCapacity(q.physical[id], n)
			}
		}
		return s.quotaReserve(device, change.After), nil
	}
}
func (s *Server) refreshQuotaProtectionLocked() error {
	if s.metadataPreview {
		return nil
	}
	if s.control != nil {
		l := s.control.ledger
		l.mu.Lock()
		pending := s.pendingQuotas.Load()
		valid := pending != nil && l.slot != nil && l.slot.Proposal != nil && l.slot.Proposal.Image == [32]byte(pending.root)
		l.mu.Unlock()
		if !valid {
			s.pendingQuotas.Store(nil)
		}
	}
	return s.applyQuotaProtectionLocked()
}
func (s *Server) applyQuotaProtectionLocked() error {
	s.quotaPolicies.Store(&quotaPolicyImage{descriptions: s.bucketSnapshots()})
	s.mu.RLock()
	node, _ := parseID(s.cfg.Node.ID)
	s.mu.RUnlock()
	if s.quotaNode == (client.ID{}) {
		s.quotaNode = client.ID(node)
	}
	if s.quotaHooked == nil {
		s.quotaHooked = make(map[volume.ID]*volume.Volume)
	}
	s.mu.RLock()
	stores := make([]*volume.Volume, 0, len(s.volumes))
	for _, m := range s.volumes {
		if m.store != s.system {
			stores = append(stores, m.store)
		}
	}
	s.mu.RUnlock()
	for _, v := range stores {
		id := v.Identity().VolumeID
		if s.storageWarming.Load() && !s.storageAccountingWarmed.Load() {
			stamp := s.quotaOwnership(v)
			_, err := v.ConfigureAccounting(volume.AccountingOptions{
				CachePath: filepath.Join(s.accountingDir, id.String()+".cache"),
				Stamp:     stamp, Include: stamp.includesRecord,
				Admission: s.quotaAdmission(v, client.ID(id)),
				Observer:  func(usage volume.StorageSnapshot) { s.publishQuotaUsage(id, usage.Owned, usage.All) },
			})
			if err != nil {
				return err
			}
			s.quotaHooked[id] = v
			// Unknown is never zero. No reserve/admission writes until all
			// configured data volumes have exact, published counters.
			continue
		}
		if err := s.refreshQuotaOwnership(v); err != nil {
			return err
		}
		if s.quotaHooked[id] != v {
			if err := v.SetAdmission(s.quotaAdmission(v, client.ID(id)), func(usage volume.StorageSnapshot) { s.publishQuotaUsage(id, usage.Owned, usage.All) }); err != nil {
				return err
			}
			s.quotaHooked[id] = v
		}
		h, err := v.Open(false)
		if err != nil {
			return err
		}
		u, err := h.StorageUsage()
		h.Close()
		if err != nil {
			return err
		}
		if err := v.SetGuaranteedReserve(s.quotaReserve(client.ID(id), u)); err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) restoreQuotaProtection() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.control != nil {
		l := s.control.ledger
		l.mu.Lock()
		var root volume.ObjectID
		if l.slot != nil && l.slot.Proposal != nil {
			root = volume.ObjectID(l.slot.Proposal.Image)
		}
		l.mu.Unlock()
		if root != (volume.ObjectID{}) {
			view, err := s.loadSystemView(root)
			if err != nil {
				return err
			}
			s.pendingQuotas.Store(view)
		}
	}
	return s.applyQuotaProtectionLocked()
}

// opMu remains held through the durable READY record. Thus a concurrent write
// cannot spend promised bytes between validating the backing and voting READY.
func (s *Server) prepareQuotaProtection(view *systemView) error {
	classes := make(map[client.ID]volume.Class, len(view.descriptions))
	for _, d := range view.descriptions {
		id, _ := bucketID(d.ID)
		classes[client.ID(id)] = bucketClass(quotaPolicy(d))
	}
	provisioned := make(map[client.ID]bool)
	for _, r := range view.manifest.QuotaReserves {
		if r.Node != s.quotaNode {
			continue
		}
		s.mu.RLock()
		m := s.volumes[r.Volume.String()]
		draining := s.cfg.Node.Draining
		s.mu.RUnlock()
		if m == nil {
			return volume.ErrFull
		}
		if m.config.State != "active" || draining {
			inherited := false
			if current := s.currentSystem.Load(); current != nil {
				for _, old := range current.manifest.QuotaReserves {
					if samePhysicalReservation(old, r) && old.Backing >= r.Backing {
						inherited = true
						break
					}
				}
			}
			if !inherited {
				return volume.ErrFull
			}
		}
		if !classes[r.Bucket].Allows(m.store.Identity().Class) {
			return volume.ErrFull
		}
		if !provisioned[r.Volume] {
			if err := m.store.EnsurePhysicalBacking(); err != nil {
				return err
			}
			provisioned[r.Volume] = true
		}
	}
	prior := s.pendingQuotas.Load()
	s.pendingQuotas.Store(view)
	if err := s.applyQuotaProtectionLocked(); err != nil {
		s.pendingQuotas.Store(prior)
		_ = s.applyQuotaProtectionLocked()
		return err
	}
	return nil
}

type physicalQuotaFull struct {
	Bucket  volume.ID
	Growth  uint64
	Waiting bool
}

func (e *physicalQuotaFull) Error() string { return "bucket physical quota is exhausted" }
func (e *physicalQuotaFull) Unwrap() error { return volume.ErrFull }
