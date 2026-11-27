// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Admission is a lifetime barrier, never a substitute for a Volume's RWMutex.
// Stable single-copy mutations share it; catalog/lease/placement/close changes
// retain exclusive admission. Cold or nested paths unwind and revalidate before
// taking exclusive admission. No shared-to-exclusive upgrade is allowed.
type exclusiveMutationKey struct{}
type exclusiveMutationRetry struct{}

func (*exclusiveMutationRetry) Error() string { return "mutation needs exclusive admission" }

// Only server-encoded reference coordination may omit the logical-total proof:
// the root is bucket-local even in a clone graph, and an unlimited logical
// quota has no inherited-byte ceiling to enforce. Physical admission, guarantees,
// authority and lifetime barriers are still mandatory. The mutation caller also
// requires Accepted; a legacy durable operation keeps exclusive admission.
func parallelReferenceCoordination(b *Bucket, p principal, id volume.ObjectID) bool {
	d := b.storageDescription()
	return p.internalCoordination && !p.cowInternal && bucketLocalReferenceRoot(id) && d.Policy.Limits.Unlimited && d.Policy.Limits.Accounting == "logical"
}

func (s *Server) parallelMutationCandidate(b *Bucket, p principal, id volume.ObjectID) bool {
	if p.cowInternal || s.controller == nil || s.control == nil || s.replicas == nil || b.underMaintenance() {
		return false
	}
	d := b.storageDescription()
	// Ordinary inherited logical bytes require the exclusive COW proof. The
	// private unlimited reference root has no inherited contents or logical cap.
	if (len(d.ClonePeers) != 0 && !parallelReferenceCoordination(b, p, id)) || d.DesiredPolicy != nil || d.State != BucketActive || d.Policy.PlacementMode == "everywhere" {
		return false
	}
	entry, ok := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(id))
	return ok && entry.State == 0 && len(entry.Locations) == 1 && entry.Locations[0].Role == 0 && entry.Locations[0].Node == s.control.ledger.node
}

// Caller holds shared admission: publication of a changed map, authority plan,
// hooks or policy requires exclusive admission. This check does no Volume I/O.
func (s *Server) parallelMutationReady(b *Bucket, p principal, id volume.ObjectID) bool {
	return s.parallelMutationReadyProof(b, p, id, b.membershipProof(b.storageDescription(), volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}))
}
func (s *Server) parallelMutationReadyProof(b *Bucket, p principal, id volume.ObjectID, proof *cohortMembershipProof) bool {
	if !s.parallelMutationCandidate(b, p, id) {
		return false
	}
	a := s.authority
	if a == nil || a.wanted == nil || a.wantedRevision != a.revision.Load() || s.authorityPending(b.ID(), id) {
		return false
	}
	d := b.storageDescription()
	entry, _ := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(id))
	device := volume.ID(entry.Locations[0].Volume)
	if device == (volume.ID{}) {
		if len(proof.volumes) == 1 {
			device = proof.volumes[0].Identity().VolumeID
		} else if len(proof.possible) == 1 {
			device = proof.possible[0].Identity().VolumeID
		} else {
			return false
		}
	}
	s.mu.RLock()
	m := s.volumes[device.String()]
	s.mu.RUnlock()
	if m == nil || s.quotaHooked[device] != m.store || !m.store.AccountingReady() {
		return false
	}
	image := s.quotaPolicies.Load()
	if image == nil {
		return false
	}
	if parallelReferenceCoordination(b, p, id) {
		return true
	}
	u := &b.usage
	u.mu.Lock()
	defer u.mu.Unlock()
	placement, revision := b.usageStamp()
	return u.valid && u.placement == placement && u.generation == revision && u.replicaProofValid && u.replicaProofSequence == s.replicas.sequence.Load() && u.replicaProofPlacement == placement && u.replicaProofRevision == d.Revision
}

// Register before acquiring any Volume handle so a cold logicalUsage reader
// cannot hold usage.mu while waiting for this publisher's Volume lock.
func (b *Bucket) beginParallelUsage(physicalOnly bool) bool {
	u := &b.usage
	u.mu.Lock()
	defer u.mu.Unlock()
	placement, revision := b.usageStamp()
	if !physicalOnly && (!u.valid || u.placement != placement || u.generation != revision) {
		return false
	}
	u.activeAdmissions++
	return true
}
func (b *Bucket) endParallelUsage() {
	u := &b.usage
	u.mu.Lock()
	u.activeAdmissions--
	u.mu.Unlock()
}

// Called only under exclusive admission. A cold usage measurement sees no
// parallel publishers. Primary prepares increment sequence under exclusive
// admission; map changes invalidate the zero-pending proof as well. Followers
// cannot introduce primary growth without a placement publication.
func (s *Server) prepareParallelMutation(b *Bucket, p principal, id volume.ObjectID) error {
	if !s.parallelMutationCandidate(b, p, id) || parallelReferenceCoordination(b, p, id) {
		return nil
	}
	if _, err := b.logicalUsage(); err != nil {
		return err
	}
	growth, err := s.pendingReplicaGrowth(b.ID())
	if err != nil {
		return err
	}
	u := &b.usage
	u.mu.Lock()
	defer u.mu.Unlock()
	placement, revision := b.usageStamp()
	u.replicaProofSequence = s.replicas.sequence.Load()
	u.replicaProofPlacement, u.replicaProofRevision = placement, revision
	u.replicaProofValid = growth == 0
	return nil
}

// Only bounded in-flight tickets are stored. No object metadata, object version
// or persistent reservation is introduced. The volume handle pins each slot:
// install AFTER acquiring its write lock, clean up BEFORE releasing that lock.
// This mutex must never be held while acquiring a Volume/server/range lock or
// waiting for storage/network I/O. Admission and observer callbacks may use it
// under their existing Volume lock; no reverse acquisition is permitted.
type parallelQuotaState struct {
	mu         sync.Mutex
	slots      map[volume.ID]*mutationQuotaTicket
	physical   map[volume.ID]uint64
	quarantine map[volume.ID]map[volume.ID]uint64
}

// The physical-only kind skips no engine admission callback. It omits only a
// meaningless logical-growth reservation for the private unlimited root.
type mutationQuotaKind uint8

const (
	mutationLogicalQuota mutationQuotaKind = iota
	mutationPhysicalOnlyQuota
)

type mutationQuotaTicket struct {
	kind     mutationQuotaKind
	bucket   *Bucket
	volume   volume.ID
	growth   uint64
	physical map[volume.ID]uint64
}

func nextMutationLength(op uint16, off, n uint64, old volume.Info, opt volume.Options) (uint64, error) {
	prior := old.Length
	if old.Deleted {
		prior = 0
	}
	switch op {
	case 0x0102:
		return n, nil
	case 0x0103:
		if n > math.MaxUint64-off {
			return 0, volume.ErrRange
		}
		return max(prior, off+n), nil
	case 0x0104:
		if n > math.MaxUint64-prior {
			return 0, volume.ErrRange
		}
		return prior + n, nil
	case 0x0105:
		return n, nil
	case 0x0106:
		return 0, nil
	case 0x0108:
		if opt.WholeObject {
			return n, nil
		}
		if n > math.MaxUint64-off {
			return 0, volume.ErrRange
		}
		return max(prior, off+n), nil
	}
	return prior, nil
}

func (s *Server) reserveMutationQuota(b *Bucket, d BucketDescription, device volume.ID, key volume.Key, op uint16, off, n uint64, old volume.Info, repeated bool, opt volume.Options, kind mutationQuotaKind) (*mutationQuotaTicket, error) {
	if kind == mutationPhysicalOnlyQuota && (!opt.Accepted || !bucketLocalReferenceRoot(key.Object) || !d.Policy.Limits.Unlimited || d.Policy.Limits.Accounting != "logical") {
		return nil, &exclusiveMutationRetry{}
	}
	next, err := nextMutationLength(op, off, n, old, opt)
	if err != nil {
		return nil, err
	}
	prior := old.Length
	if old.Deleted {
		prior = 0
	}
	growth := uint64(0)
	if kind != mutationPhysicalOnlyQuota && !repeated && next > prior {
		growth = next - prior
	}
	u := &b.usage
	u.mu.Lock()
	defer u.mu.Unlock()
	placement, revision := b.usageStamp()
	if kind != mutationPhysicalOnlyQuota && (!u.valid || u.placement != placement || u.generation != revision) {
		return nil, &exclusiveMutationRetry{}
	}
	used := addCapacity(u.bytes, u.reservedGrowth)
	if !d.Policy.Limits.Unlimited && d.Policy.Limits.Accounting == "logical" && growth > 0 {
		limit := d.Policy.Limits.Absolute
		if s.controller != nil {
			limit, err = s.quotaAllowance(client.ID(b.ID()))
			if err != nil {
				return nil, err
			}
		}
		if used > limit || growth > limit-used {
			if used >= d.Policy.Limits.Absolute || growth > d.Policy.Limits.Absolute-used {
				return nil, &prepublicationQuotaFull{}
			}
			if s.quotas != nil && s.quotas.request(b.ID(), key.Object, growth) {
				return nil, fail(503, "waiting for bucket growth credits")
			}
			return nil, &prepublicationQuotaFull{}
		}
	}
	if growth > math.MaxUint64-u.reservedGrowth {
		return nil, volume.ErrRange
	}
	t := &mutationQuotaTicket{kind: kind, bucket: b, volume: device, growth: growth}
	q := &s.parallelQuota
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.slots == nil {
		q.slots = make(map[volume.ID]*mutationQuotaTicket)
	}
	if q.slots[device] != nil {
		return nil, volume.ErrBusy
	}
	q.slots[device] = t
	u.reservedGrowth += growth
	u.activeTickets++
	return t, nil
}

func (q *parallelQuotaState) releasePhysicalLocked(t *mutationQuotaTicket) {
	for bucket, n := range t.physical {
		q.physical[bucket] -= n
		if q.physical[bucket] == 0 {
			delete(q.physical, bucket)
		}
	}
	t.physical = nil
}

// Must run while this mutation still owns its Volume write handle. Publication
// callbacks have already converted physical reservations into committed usage.
// Ambiguous unpublished physical charges are quarantined until exact accounting
// is republished; an unavailable volume cannot silently donate uncertain space.
// Logical publication is independent of a later failed physical reclamation:
// preserve the known logical delta while quarantining uncertain physical growth.
func (s *Server) finishMutationQuota(t *mutationQuotaTicket, old, next uint64, repeated bool, logicalErr, physicalErr error) {
	q := &s.parallelQuota
	q.mu.Lock()
	if q.slots[t.volume] == t {
		delete(q.slots, t.volume)
		if mutationQuotaAmbiguous(physicalErr) && len(t.physical) > 0 {
			if q.quarantine == nil {
				q.quarantine = make(map[volume.ID]map[volume.ID]uint64)
			}
			q.quarantine[t.volume] = t.physical
			t.physical = nil
		} else {
			q.releasePhysicalLocked(t)
		}
	}
	q.mu.Unlock()
	u := &t.bucket.usage
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reservedGrowth -= t.growth
	u.activeTickets--
	t.bucket.updateUsage(old, next, repeated, logicalErr)
}

// Shared callers must never run the all-volume refresh or authority planner.
// Any stale proof restarts before payload consumption under exclusive admission.
func parallelForcedExclusive(ctx context.Context) bool {
	forced, _ := ctx.Value(exclusiveMutationKey{}).(bool)
	return forced
}

func mutationQuotaAmbiguous(err error) bool {
	// Backend ENOSPC can wrap both ErrFull and ErrRecovery. Recovery dominates
	// the capacity/precondition classification: uncertain publication must not
	// donate reserved bytes to another volume.
	if errors.Is(err, volume.ErrRecovery) {
		return true
	}
	return err != nil && !errors.Is(err, volume.ErrPrecondition) && !errors.Is(err, volume.ErrFull) && !errors.Is(err, volume.ErrRange) && !errors.Is(err, volume.ErrNotFound)
}
