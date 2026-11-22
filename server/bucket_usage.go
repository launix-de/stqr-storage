// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"sync"
)

// The cache contains one scalar per bucket, not one entry per object. Growth
// admission must not walk a million-object index for every ten-byte APPEND.
// A committed placement image invalidates the count after handoff/checkpoint;
// local writes adjust it while holding this mutex through the storage commit.
type bucketUsage struct {
	mu         sync.Mutex
	valid      bool
	bytes      uint64
	placement  *placementImage
	generation uint64 // legacy/local placement has no immutable range image
}

func (b *Bucket) usageStamp() (*placementImage, uint64) {
	s := b.server
	if s.controller != nil {
		return s.placement.Load(), 0
	}
	s.mu.RLock()
	generation := s.cfg.Generation
	s.mu.RUnlock()
	return nil, generation
}

func (b *Bucket) logicalUsage() (uint64, error) {
	u := &b.usage
	u.mu.Lock()
	defer u.mu.Unlock()
	placement, generation := b.usageStamp()
	if u.valid && u.placement == placement && u.generation == generation {
		return u.bytes, nil
	}
	value, err := b.scanLogicalUsage()
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

// Caller holds usage.mu and has closed its volume write handle. Readers always
// acquire usage.mu before opening scan handles, preventing a lock inversion and
// a count refreshed between a commit and its incremental adjustment.
func (b *Bucket) updateUsage(old, next uint64, repeated bool, err error) {
	u := &b.usage
	if err != nil {
		u.valid = false
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
