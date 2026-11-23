// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (r *rangeManager) physicalFootprint(p rangePlan) (uint64, error) {
	b, err := r.server.Bucket(volume.ID(p.Before.Bucket))
	if err != nil {
		return 0, err
	}
	if quotaPolicy(b.storageDescription()).Limits.Accounting != "physical" || quotaPolicy(b.storageDescription()).Limits.Unlimited || p.Source == p.Target {
		return 0, nil
	}
	var n uint64
	for _, v := range r.sourceVolumes(p) {
		if err := r.server.refreshQuotaOwnership(v); err != nil {
			return 0, err
		}
		h, err := v.Open(false)
		if err != nil {
			return 0, err
		}
		usage, err := h.OwnedStorageUsage()
		if err != nil {
			h.Close()
			return 0, err
		}
		retained, err := h.StorageUsageWhere(func(k volume.Key) bool { return client.ID(k.Bucket) == p.Before.Bucket && !p.contains(k.Object) }, true)
		h.Close()
		if err != nil {
			return 0, err
		}
		n = addCapacity(n, subtractCapacity(storagePhysical(usage, volume.ID(p.Before.Bucket)), retained))
	}
	return n, nil
}

// The incoming copy is fenced operating space until COMMIT. Before READY, its
// future owner must cover all current physical usage with existing credits plus
// exactly the source debit carried in BOTH durable READYs. Compression/layout
// expansion can require additional ordinary credit transfers, never new quota.
func (r *rangeManager) checkPhysicalActivation(t *rangeTxn, transferred uint64) error {
	s := r.server
	s.opMu.Lock()
	defer s.opMu.Unlock()
	return r.checkPhysicalActivationLocked(t, transferred)
}
func (r *rangeManager) checkPhysicalActivationLocked(t *rangeTxn, transferred uint64) error {
	s := r.server
	b, err := s.Bucket(volume.ID(t.plan.Before.Bucket))
	if err != nil {
		return err
	}
	p := quotaPolicy(b.storageDescription())
	if p.Limits.Unlimited || p.Limits.Accounting != "physical" {
		return nil
	}
	allowance, err := s.quotaAllowance(t.plan.Before.Bucket)
	if err != nil {
		return err
	}
	used, err := b.physicalUsage()
	if err != nil {
		return err
	}
	store, err := r.destination(t.plan)
	if err != nil {
		return err
	}
	h, err := store.Open(false)
	if err != nil {
		return err
	}
	incoming, err := h.StorageUsageWhere(func(k volume.Key) bool {
		return client.ID(k.Bucket) == t.plan.Before.Bucket && t.plan.contains(k.Object)
	})
	h.Close()
	if err != nil {
		return err
	}
	if t.plan.Source == t.plan.Target {
		used = 0
		for _, v := range s.quotaVolumes() {
			h, err := v.Open(false)
			if err != nil {
				return err
			}
			retained, err := h.StorageUsageWhere(func(k volume.Key) bool { return k.Bucket == b.ID() && !t.plan.contains(k.Object) }, true)
			h.Close()
			if err != nil {
				return err
			}
			used = addCapacity(used, retained)
		}
	}
	used = addCapacity(used, incoming)
	limit := addCapacity(allowance, transferred)
	if limit > p.Limits.Absolute {
		return volume.ErrFull
	}
	if used > limit {
		if subtractCapacity(used, transferred) > p.Limits.Absolute {
			return volume.ErrFull
		}
		if s.quotas.requestActivation(b.ID(), volume.ObjectID(t.plan.Before.Begin), subtractCapacity(used, transferred)) {
			return fail(503, "waiting for physical handoff growth credits")
		}
		return volume.ErrFull
	}
	return nil
}

func (r *rangeManager) checkPhysicalSourceLocked(t *rangeTxn, transferred uint64) error {
	b, err := r.server.Bucket(volume.ID(t.plan.Before.Bucket))
	if err != nil {
		return err
	}
	p := quotaPolicy(b.storageDescription())
	if p.Limits.Unlimited || p.Limits.Accounting != "physical" || t.plan.Source == t.plan.Target {
		return nil
	}
	used, err := b.physicalUsage()
	if err != nil {
		return err
	}
	released, err := r.physicalFootprint(t.plan)
	if err != nil {
		return err
	}
	retained := subtractCapacity(used, released)
	allowance, err := r.server.quotaAllowance(t.plan.Before.Bucket)
	if err != nil {
		return err
	}
	required := addCapacity(retained, transferred)
	if required > allowance {
		if r.server.quotas.requestActivation(b.ID(), volume.ObjectID(t.plan.Before.Begin), required) {
			return fail(503, "waiting for physical source handoff credits")
		}
		return volume.ErrFull
	}
	return nil
}
