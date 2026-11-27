// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Storage callbacks run under a volume lock. The dedicated quota reservation
// mutex may be acquired here; it must never acquire volume/node/config/range
// locks. Configuration remains immutable while a mutation holds admission.
type quotaUsageImage struct {
	volumes map[volume.ID][]volume.BucketStorage
	all     map[volume.ID][]volume.BucketStorage
}
type quotaPolicyImage struct {
	source       *systemView // identifies bulk control installs which bypass publishBucket
	descriptions []BucketDescription
}

func (s *Server) publishQuotaUsage(id volume.ID, usage []volume.BucketStorage, all []volume.BucketStorage) {
	q := &s.parallelQuota
	q.mu.Lock()
	defer q.mu.Unlock()
	// Convert exactly this publication's reservations into committed usage in
	// one critical section. Other volumes cannot see both or neither charge.
	if t := q.slots[id]; t != nil {
		q.releasePhysicalLocked(t)
	}
	if charges := q.quarantine[id]; charges != nil {
		for bucket, n := range charges {
			q.physical[bucket] -= n
			if q.physical[bucket] == 0 {
				delete(q.physical, bucket)
			}
		}
		delete(q.quarantine, id)
	}
	for {
		prior := s.quotaUsage.Load()
		next := &quotaUsageImage{volumes: make(map[volume.ID][]volume.BucketStorage), all: make(map[volume.ID][]volume.BucketStorage)}
		if prior != nil {
			for id, u := range prior.volumes {
				next.volumes[id] = u
				next.all[id] = prior.all[id]
			}
		}
		next.volumes[id] = usage
		next.all[id] = all
		if s.quotaUsage.CompareAndSwap(prior, next) {
			return
		}
	}
}

type quotaRangeDelta struct {
	bucket                      client.ID
	sequence, logical, physical uint64
	source                      bool
}
type quotaRangeImage struct {
	sequence uint64
	deltas   []quotaRangeDelta
	blocked  map[client.ID]bool
	held     map[client.ID]uint64
}

func (r *rangeManager) publishQuotaRangesLocked() {
	image := &quotaRangeImage{sequence: r.journal.sequence, blocked: make(map[client.ID]bool), held: make(map[client.ID]uint64)}
	for _, t := range r.txns {
		if t.plan.ReplicaAdd || t.plan.ReplicaRemove || t.plan.ReplicaRelocate {
			continue
		}
		if t.plan.Source == r.node && t.plan.Source != t.plan.Target && t.sourceReady.RecordSequence != 0 && t.decision == nil && !t.aborted && !t.forceRetired {
			image.held[t.plan.Before.Bucket] = addCapacity(image.held[t.plan.Before.Bucket], t.quotaUsed)
		}
		if r.active(t) && t.plan.Source == r.node && t.plan.ram() && t.sourceBoot != r.server.boot && t.decision == nil {
			image.blocked[t.plan.Before.Bucket] = true
		}
		if t.decision == nil || t.plan.Source == t.plan.Target {
			continue
		}
		if t.plan.Source != r.node && t.plan.Target != r.node {
			continue
		}
		image.deltas = append(image.deltas, quotaRangeDelta{bucket: t.plan.Before.Bucket, sequence: t.commitProof.RecordSequence, logical: t.decision.LogicalUsed, physical: t.decision.QuotaUsed, source: t.plan.Source == r.node})
	}
	r.server.quotaRanges.Store(image)
}

func (s *Server) heldPhysicalQuota(id client.ID) uint64 {
	if image := s.quotaRanges.Load(); image != nil {
		return image.held[id]
	}
	return 0
}
