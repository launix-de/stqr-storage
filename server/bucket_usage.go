// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"math"
	"sync"
)

// The cache contains one scalar per bucket, not one entry per object. Growth
// admission must not walk a million-object index for every ten-byte APPEND.
// A committed placement image invalidates the count after handoff/checkpoint;
// exclusive legacy writes hold this mutex through commit; parallel publishers
// reserve growth and settle committed deltas in short critical sections.
type bucketUsage struct {
	mu                    sync.Mutex
	valid                 bool
	bytes                 uint64
	reservedGrowth        uint64
	activeTickets         uint64
	activeAdmissions      uint64
	replicaProofSequence  uint64
	replicaProofPlacement *placementImage
	replicaProofRevision  uint64
	replicaProofValid     bool
	placement             *placementImage
	generation            uint64 // bucket catalog revision; never an object-content counter
}

// Observation must never reconstruct a COW namespace while holding node
// admission or block behind a publisher settling its usage counters. Unknown
// logical usage is unavailable, not zero and not permission to exceed a quota.
func (b *Bucket) observedCOWLogicalUsage() (uint64, error) {
	u := &b.usage
	if !u.mu.TryLock() {
		return 0, fail(503, "bucket logical usage is being updated")
	}
	defer u.mu.Unlock()
	placement, revision := b.usageStamp()
	if !u.valid || u.placement != placement || u.generation != revision {
		return 0, fail(503, "inherited bucket logical usage is not available")
	}
	return u.bytes, nil
}

func (b *Bucket) usageStamp() (*placementImage, uint64) {
	s := b.server
	if s.controller != nil {
		return s.placement.Load(), b.storageDescription().Revision
	}
	return nil, b.storageDescription().Revision
}

func (b *Bucket) logicalUsage() (uint64, error) {
	if b.server.storageWarming.Load() {
		return 0, b.server.storageUnavailable()
	}
	u := &b.usage
	u.mu.Lock()
	defer u.mu.Unlock()
	placement, generation := b.usageStamp()
	if u.valid && u.placement == placement && u.generation == generation {
		return u.bytes, nil
	}
	// Never scan volumes while a parallel publisher can need this mutex to
	// settle its counters under its Volume handle. Exclusive cold admission
	// waits for those publishers and reconstructs once; stats report unknown.
	if u.activeAdmissions != 0 {
		return 0, fail(503, "bucket usage awaits active publishers")
	}
	var value uint64
	var err error
	if len(b.storageDescription().ClonePeers) > 0 {
		value, err = b.cowLogicalUsage()
	} else {
		value, err = b.scanLogicalUsage()
	}
	if err != nil {
		return 0, err
	}
	current, currentGeneration := b.usageStamp()
	if current != placement || currentGeneration != generation {
		return 0, fail(503, "bucket placement changed during usage measurement")
	}
	u.valid, u.bytes, u.placement, u.generation = true, value, placement, generation
	return value, nil
}

// Caller holds usage.mu. Exclusive callers have closed their Volume handle;
// parallel callers settle before Close while activeAdmissions prevents a cold
// reader from scanning volumes under this mutex. Cache hits never take a Volume
// lock, preventing an inversion or a refresh between publication and adjustment.
func (b *Bucket) updateUsage(old, next uint64, repeated bool, err error) {
	u := &b.usage
	if err != nil {
		// Rejected preconditions/capacity do not change logical lengths.
		// Immutable create-if-absent probes and capacity handoff retries are
		// common; invalidating here turns each retry into a full bucket scan.
		// Ambiguous backend errors still invalidate the count conservatively.
		if !errors.Is(err, volume.ErrPrecondition) &&
			!(errors.Is(err, volume.ErrFull) && !errors.Is(err, volume.ErrRecovery)) {
			u.valid = false
		}
		return
	}
	if repeated || !u.valid {
		return
	}
	placement, generation := b.usageStamp()
	if placement != u.placement || generation != u.generation || old > u.bytes || next > math.MaxUint64-(u.bytes-old) {
		u.valid = false
		return
	}
	u.bytes = u.bytes - old + next
}

// Fully pinned single-copy maps have one physical record per logical object.
// Maintained ownership counters therefore give the exact logical total without
// walking every object on a cold quota/dashboard read. Unbound maps, replicas
// and inherited COW views keep the logical merge path.
func (b *Bucket) pinnedLogicalUsage() (uint64, bool, error) {
	if b.server.storageWarming.Load() {
		return 0, true, b.server.storageUnavailable()
	}
	s := b.server
	if s.controller == nil || len(b.storageDescription().ClonePeers) != 0 {
		return 0, false, nil
	}
	placement := s.placement.Load()
	if placement == nil {
		return 0, false, nil
	}
	entries := placement.bucketEntries(client.ID(b.ID()))
	if len(entries) == 0 {
		return 0, false, nil
	}
	for _, entry := range entries {
		if entry.State != 0 || len(entry.Locations) != 1 || entry.Locations[0].Role != 0 || entry.Locations[0].Volume == (client.ID{}) {
			return 0, false, nil
		}
	}
	stamp := inventoryStamp(entries, client.ID{})
	var total uint64
	for _, v := range s.quotaVolumes() {
		if err := s.refreshQuotaOwnership(v); err != nil {
			return 0, true, err
		}
		h, err := v.Open(false)
		if err != nil {
			return 0, true, err
		}
		usage, err := h.OwnedStorageUsage()
		h.Close()
		if err != nil {
			return 0, true, err
		}
		for _, u := range usage {
			if u.Bucket != b.ID() {
				continue
			}
			if u.Logical > math.MaxUint64-total {
				return 0, true, volume.ErrRange
			}
			total += u.Logical
		}
	}
	if inventoryStamp(s.placement.Load().bucketEntries(client.ID(b.ID())), client.ID{}) != stamp {
		return 0, true, fail(503, "bucket ownership changed during usage measurement")
	}
	return total, true, nil
}
