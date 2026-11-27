// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Authority is control metadata, not an implicit persistence request for a user
// write. Plan/stage while opMu proves the cohort and import allowance; persist
// captured cuts after dropping all node/replica/range mutexes. Only confirmed
// device records may justify recovery or clear a changed range's admission gate.
// Accepted headers can survive partially (e.g. a concurrent explicit Sync), so
// retain their scopes until their exact generation is confirmed or superseded.
// The private registry is O(volumes+ranges), never O(objects).
type authorityTarget struct {
	store              *volume.Volume
	record             volumeAuthorityRecord
	budget, generation uint64
}
type authorityBatch struct {
	revision uint64
	targets  []authorityTarget
	done     chan struct{}
	err      error // published by closing done
}
type authorityInterval struct {
	begin, end client.ObjectID
	toEnd      bool
}
type authorityPendingView struct {
	buckets map[volume.ID][]authorityInterval
}

// A stage failure is scoped only after a complete desired-image calculation.
// Unaffected writes may proceed; the affected ranges keep their pending gate.
type authorityStageError struct{ err error }

func (e *authorityStageError) Error() string { return e.err.Error() }
func (e *authorityStageError) Unwrap() error { return e.err }

type authorityWait struct {
	batch *authorityBatch
	store *volume.Volume
}

func (*authorityWait) Error() string { return "range authority checkpoint is pending" }
func (*authorityWait) Unwrap() error { return fail(503, "range authority checkpoint is pending") }
func (w *authorityWait) wait(ctx context.Context) error {
	if w.store != nil {
		return w.store.CheckpointThrough(0)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.batch.done:
		return w.batch.err
	}
}
func (s *Server) authorityPending(bucket volume.ID, object volume.ObjectID) bool {
	if s.authority == nil {
		return false
	}
	p := s.authority.pending.Load()
	if p == nil {
		return false
	}
	ranges := p.buckets[bucket]
	at := sort.Search(len(ranges), func(i int) bool { return bytes.Compare(ranges[i].begin[:], object[:]) > 0 }) - 1
	return at >= 0 && (ranges[at].toEnd || bytes.Compare(object[:], ranges[at].end[:]) < 0)
}
func authorityScopeAdd(out map[volume.ID][]authorityInterval, record volumeAuthorityRecord) {
	for _, claim := range record.Entries {
		end, toEnd, err := client.RangeEnd(claim.Entry.Start, claim.Entry.PrefixBits)
		if err == nil {
			key := volume.ID(claim.Bucket)
			out[key] = append(out[key], authorityInterval{claim.Entry.Start, end, toEnd})
		}
	}
}

// Compare entries independently: changing one range does not block every other
// range sharing a data device. Header identity changes affect all its claims.
func authorityScopeDiff(out map[volume.ID][]authorityInterval, a, b volumeAuthorityRecord) {
	if a.Cluster != b.Cluster || a.Node != b.Node || a.Volume != b.Volume {
		authorityScopeAdd(out, a)
		authorityScopeAdd(out, b)
		return
	}
	type claimKey struct {
		bucket client.ID
		start  client.ObjectID
		prefix uint16
		epoch  uint64
	}
	key := func(e volumeAuthorityEntry) claimKey {
		return claimKey{e.Bucket, e.Entry.Start, e.Entry.PrefixBits, e.Entry.OwnerEpoch}
	}
	prior := make(map[claimKey]volumeAuthorityEntry, len(a.Entries))
	for _, entry := range a.Entries {
		prior[key(entry)] = entry
	}
	for _, entry := range b.Entries {
		old, ok := prior[key(entry)]
		if !ok || !reflect.DeepEqual(old, entry) {
			if ok {
				authorityScopeAdd(out, volumeAuthorityRecord{Entries: []volumeAuthorityEntry{old}})
			}
			authorityScopeAdd(out, volumeAuthorityRecord{Entries: []volumeAuthorityEntry{entry}})
		}
		delete(prior, key(entry))
	}
	for _, entry := range prior {
		authorityScopeAdd(out, volumeAuthorityRecord{Entries: []volumeAuthorityEntry{entry}})
	}
}
func (s *Server) publishAuthorityPendingLocked() {
	a := s.authority
	pending := make(map[volume.ID][]authorityInterval)
	for id, wanted := range a.wanted {
		authorityScopeDiff(pending, a.records[id], wanted)
	}
	for id, staged := range a.staged {
		authorityScopeDiff(pending, a.records[id], staged.record)
	}
	for bucket, ranges := range pending {
		sort.Slice(ranges, func(i, j int) bool { return bytes.Compare(ranges[i].begin[:], ranges[j].begin[:]) < 0 })
		merged := ranges[:0]
		for _, next := range ranges {
			if len(merged) == 0 {
				merged = append(merged, next)
				continue
			}
			last := &merged[len(merged)-1]
			if last.toEnd || bytes.Compare(next.begin[:], last.end[:]) <= 0 {
				if next.toEnd || !last.toEnd && bytes.Compare(next.end[:], last.end[:]) > 0 {
					last.end = next.end
					last.toEnd = next.toEnd
				}
			} else {
				merged = append(merged, next)
			}
		}
		pending[bucket] = merged
	}
	if len(pending) == 0 {
		a.pending.Store(nil)
	} else {
		a.pending.Store(&authorityPendingView{buckets: pending})
	}
}

// ensureVolumeAuthorityLocked never waits for a device. Successful staging
// returns a token whose callers MUST await after releasing their outer locks.
func (s *Server) ensureVolumeAuthorityLocked(allowances ...map[volume.ID]uint64) error {
	if s.controller == nil || s.authority == nil {
		return nil
	}
	a := s.authority
	if !a.dirty.Load() && a.batch == nil && len(a.staged) == 0 {
		return nil
	}
	targets, err := s.planVolumeAuthorityLocked(allowances...)
	if err != nil {
		return err
	}
	s.publishAuthorityPendingLocked()
	if a.batch != nil {
		return &authorityWait{batch: a.batch}
	}
	if len(targets) == 0 {
		a.dirty.Store(false)
		return nil
	}
	if a.staged == nil {
		a.staged = make(map[volume.ID]authorityTarget)
	}
	compact := s.compactAuthorityReady()
	for i := range targets {
		target := &targets[i]
		if target.generation != 0 {
			continue
		}
		h, err := target.store.Open(true)
		if err != nil {
			return &authorityStageError{err: err}
		}
		current, err := h.LocalMetadata()
		var raw []byte
		if err == nil {
			raw, err = marshalStableVolumeAuthority(target.record, compact || len(current) >= 4 && string(current[:4]) == "STVA", current)
		}
		if err == nil {
			err = h.SetLocalMetadataWithOptions(raw, volume.Options{Accepted: true, InFlightAllowance: target.budget})
		}
		if err == nil {
			target.generation, err = h.Generation()
		}
		h.Close()
		if err != nil {
			s.publishAuthorityPendingLocked()
			if errors.Is(err, volume.ErrBusy) && !errors.Is(err, volume.ErrWritebackPressure) {
				return &authorityWait{store: target.store}
			}
			return &authorityStageError{err: err}
		}
		a.staged[target.store.Identity().VolumeID] = *target
	}
	s.publishAuthorityPendingLocked()
	return s.startAuthorityBatchLocked(targets)
}
func (s *Server) startAuthorityBatchLocked(targets []authorityTarget) error {
	a := s.authority
	batch := &authorityBatch{revision: a.revision.Load(), targets: targets, done: make(chan struct{})}
	// Synchronize Add with Close's closed flag; Add must precede workers.Wait.
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return fail(503, "node is closing")
	}
	a.batch = batch
	s.workers.Add(1)
	go s.checkpointAuthorityBatch(batch)
	s.mu.RUnlock()
	return &authorityWait{batch: batch}
}
func (s *Server) checkpointAuthorityBatch(batch *authorityBatch) {
	defer s.workers.Done()
	for _, target := range batch.targets {
		if err := target.store.CheckpointThrough(target.generation); err != nil {
			batch.err = err
			break
		}
		s.opMu.Lock()
		id := target.store.Identity().VolumeID
		s.authority.records[id] = target.record
		if staged, ok := s.authority.staged[id]; ok && staged.generation == target.generation {
			delete(s.authority.staged, id)
		}
		s.publishAuthorityPendingLocked()
		s.opMu.Unlock()
	}
	s.opMu.Lock()
	a := s.authority
	a.batch = nil
	if batch.err == nil && a.revision.Load() == batch.revision {
		a.dirty.Store(false)
	}
	// Recompute current scopes before releasing a stale cohort gate. A topology
	// update during Sync must never be mistaken for this batch's confirmed view.
	if a.dirty.Load() {
		if _, err := s.planVolumeAuthorityLocked(); err != nil && batch.err == nil {
			batch.err = err
		}
	}
	s.publishAuthorityPendingLocked()
	close(batch.done)
	s.opMu.Unlock()
	if batch.err == nil {
		s.continueAuthorityProgress()
	}
}

// Drive topology changes that arrived during a fence even when its initiating
// request was unaffected and therefore had no reason to wait for the token.
func (s *Server) continueAuthorityProgress() {
	for {
		s.opMu.Lock()
		if s.ctx.Err() != nil || s.authority.batch != nil || !s.authority.dirty.Load() {
			s.opMu.Unlock()
			return
		}
		err := s.ensureVolumeAuthorityLocked()
		s.opMu.Unlock()
		var wait *authorityWait
		if errors.As(err, &wait) && wait.store != nil {
			if wait.wait(s.ctx) != nil {
				return
			}
			continue
		}
		return
	}
}
func (s *Server) ensureVolumeAuthority() error {
	for {
		s.opMu.Lock()
		err := s.ensureVolumeAuthorityLocked()
		s.opMu.Unlock()
		var wait *authorityWait
		if !errors.As(err, &wait) {
			return err
		}
		if err = wait.wait(s.ctx); err != nil {
			return err
		}
	}
}
func (s *Server) ensureObjectAuthorityLocked(key volume.Key) error {
	err := s.ensureVolumeAuthorityLocked()
	var wait *authorityWait
	var staged *authorityStageError
	if (errors.As(err, &wait) || errors.As(err, &staged)) && !s.authorityPending(key.Bucket, key.Object) {
		return nil
	}
	return err
}

// A recovered device remains staged and RecoveryOnly until owner+proof are
// confirmed together. Retry observes the accepted owner but cannot attach it
// merely because its volatile identity changed before the physical fence.
func (s *Server) reassignVolumeAuthorityLocked(store *volume.Volume, record volumeAuthorityRecord, node volume.ID, raw []byte) error {
	a := s.authority
	if a.batch != nil {
		return &authorityWait{batch: a.batch}
	}
	id := store.Identity().VolumeID
	prior, known := a.records[id]
	if !known {
		var err error
		prior, err = readVolumeAuthority(store)
		if err != nil {
			return err
		}
		a.records[id] = prior
	}
	staged, hasStaged := a.staged[id]
	if !hasStaged && store.Identity().NodeID == node && reflect.DeepEqual(prior, record) {
		return nil
	}
	target := authorityTarget{store: store, record: record}
	if hasStaged && reflect.DeepEqual(staged.record, record) && store.Identity().NodeID == node {
		target.generation = staged.generation
	} else {
		if err := store.ReassignIdentityWithMetadataOptions(store.Identity(), node, raw, volume.Options{Accepted: true}); err != nil {
			return err
		}
		h, err := store.Open(false)
		if err != nil {
			return err
		}
		target.generation, err = h.Generation()
		h.Close()
		if err != nil {
			return err
		}
	}
	if a.staged == nil {
		a.staged = make(map[volume.ID]authorityTarget)
	}
	a.staged[id] = target
	s.dirtyVolumeAuthority()
	s.publishAuthorityPendingLocked()
	return s.startAuthorityBatchLocked([]authorityTarget{target})
}
