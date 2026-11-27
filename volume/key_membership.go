// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync/atomic"
)

const keyMembershipBytes = 4 << 20
const keyMembershipBatch = 512
const keyMembershipHashes = 4

type keyMembership struct {
	words    []atomic.Uint64
	complete atomic.Bool
	identity ID
	epoch    uint64
}

func (f *keyMembership) positions(key indexKey, visit func(uint64)) {
	digest := sha256.Sum256(key[:])
	a := binary.LittleEndian.Uint64(digest[:8])
	step := binary.LittleEndian.Uint64(digest[8:16]) | 1
	for i := 0; i < keyMembershipHashes; i++ {
		visit((a + uint64(i)*step) & (keyMembershipBytes*8 - 1))
	}
}
func (f *keyMembership) add(key indexKey) {
	f.positions(key, func(bit uint64) { f.words[bit/64].Or(uint64(1) << (bit % 64)) })
}
func (v *Volume) mayContainKey(key indexKey) bool {
	f := v.keyMembership.Load()
	if f == nil || !f.complete.Load() {
		return true
	}
	maybe := true
	f.positions(key, func(bit uint64) {
		if f.words[bit/64].Load()&(uint64(1)<<(bit%64)) == 0 {
			maybe = false
		}
	})
	return maybe
}

// MayContainObject is a conservative, lock-free index-membership hint. False
// proves absence at this call; true still requires normal lookup/authorization.
// Tombstones count as present. No object data or metadata is cached here.
// A negative hint is not an ownership lease or a multi-key snapshot: callers
// deriving routing decisions must retain their normal concurrency/placement
// guards, since a later writer may insert the key immediately after this call.
func (v *Volume) MayContainObject(key Key) bool { return v.mayContainKey(objectKey(key)) }

// MayContainRequest includes retained retry receipts, independently of bucket.
func (v *Volume) MayContainRequest(id ID) bool { return v.mayContainKey(requestKey(id)) }

// MayContainReplica includes prepared, decided and retained published records.
func (v *Volume) MayContainReplica(id ID) bool { return v.mayContainKey(replicaKey(id)) }

// MayContainPending includes the per-object pending replica marker.
func (v *Volume) MayContainPending(key Key) bool { return v.mayContainKey(pendingKey(key)) }

func (v *Volume) invalidateKeyMembership() {
	v.keyMembershipEpoch.Add(1)
	v.keyMembership.Store(nil)
}
func (v *Volume) addMembershipKey(key indexKey) {
	if f := v.keyMembership.Load(); f != nil {
		f.add(key)
	}
}

// WarmKeyMembership builds a fixed 4MiB RAM hint from index keys only. It does
// not read OBJ2 descriptors/payloads, persist anything, or start a goroutine.
// The caller owns scheduling/cancellation. Concurrent builders return ErrBusy.
// Each bounded scan holds the read lock; writers add keys before publication,
// including insertions behind the cursor. Deletions never clear bits. Aborted
// writes and accumulated stale keys can only cause safe false positives.
// Incomplete/cancelled builds, failed/closed volumes and root replacements
// fall back to ordinary lookups. A new/reopened volume is UNKNOWN until this
// method succeeds. Invalidating an index also rejects any obsolete collector.
func (v *Volume) WarmKeyMembership(ctx context.Context) error {
	return v.warmKeyMembership(ctx, nil)
}
func (v *Volume) warmKeyMembership(ctx context.Context, afterBatch func()) error {
	if ctx == nil {
		return ErrRange
	}
	if !v.keyMembershipBuild.TryLock() {
		return ErrBusy
	}
	defer v.keyMembershipBuild.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var f *keyMembership
	err := func() error {
		v.mu.Lock()
		defer v.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := v.check(false); err != nil {
			return err
		}
		if prior := v.keyMembership.Load(); prior != nil && prior.complete.Load() {
			f = nil
			return nil
		}
		f = &keyMembership{words: make([]atomic.Uint64, keyMembershipBytes/8)}
		f.identity = v.sb.Identity.VolumeID
		f.epoch = v.keyMembershipEpoch.Load()
		v.keyMembership.Store(f)
		return nil
	}()
	if err != nil || f == nil {
		return err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			v.keyMembership.CompareAndSwap(f, nil)
		}
	}()
	var cursor indexKey
	haveCursor := false
	stop := errors.New("membership batch complete")
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count := 0
		err := func() error {
			v.mu.RLock()
			defer v.mu.RUnlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := v.check(false); err != nil {
				return err
			}
			if v.keyMembership.Load() != f || v.keyMembershipEpoch.Load() != f.epoch || v.sb.Identity.VolumeID != f.identity {
				return ErrBusy
			}
			var lower *indexKey
			if haveCursor {
				lower = &cursor
			}
			return v.walkIndexView(lower, nil, func(entry indexEntry) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if haveCursor && !lessKey(cursor, entry.Key) {
					return nil
				}
				f.add(entry.Key)
				cursor = entry.Key
				haveCursor = true
				count++
				if count == keyMembershipBatch {
					return stop
				}
				return nil
			})
		}()
		if err != nil && !errors.Is(err, stop) {
			return err
		}
		if afterBatch != nil {
			afterBatch()
		}
		if count == keyMembershipBatch {
			continue
		}
		v.mu.RLock()
		err = ctx.Err()
		if err == nil {
			err = v.check(false)
		}
		if err == nil && (v.keyMembership.Load() != f || v.keyMembershipEpoch.Load() != f.epoch || v.sb.Identity.VolumeID != f.identity) {
			err = ErrBusy
		}
		if err == nil {
			f.complete.Store(true)
			succeeded = true
		}
		v.mu.RUnlock()
		return err
	}
}
