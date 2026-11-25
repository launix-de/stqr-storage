// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"log"
	"runtime"
	"time"
)

type pgRebuildWorker struct{ wake, stop chan struct{} }

type pgRebuildCatalog struct {
	groups    map[string][]BucketDescription
	captured  time.Time
	snapshots []client.StatsSnapshot
	seen      map[client.ID]time.Time
}

// Collect catalog/telemetry once per node tick; PG workers never enumerate the
// full bucket catalog. There is no global network wait in this dispatcher.
func (r *rangeManager) startPGWorkers() {
	s := r.server
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		ticker := time.NewTicker(750 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			case <-r.capacityWake:
			}
			if s.joinPending() || s.shortStopping.Load() || s.clusterPaused() || s.forceFrozen() {
				continue
			}
			now := time.Now()
			catalog := &pgRebuildCatalog{groups: map[string][]BucketDescription{"": nil}, captured: now, seen: map[client.ID]time.Time{r.node: now}}
			catalog.snapshots = []client.StatsSnapshot{s.CollectStats(client.StatsFilter{})}
			s.peerStatsMu.RLock()
			for _, p := range s.peerStats {
				if p.online && p.snapshot != nil && p.connection.Capabilities&client.CapabilityRangeHandoff != 0 {
					catalog.snapshots = append(catalog.snapshots, *p.snapshot)
					catalog.seen[p.snapshot.Node] = p.lastSeen
				}
			}
			s.peerStatsMu.RUnlock()
			for _, d := range s.bucketSnapshots() {
				if d.ID == systemBucket.String() || d.DeletionComplete || d.State == BucketDeleting {
					continue
				}
				group := d.Policy.PlacementGroup
				if group != "" {
					members, _, err := s.pgMembers(d)
					if err != nil || !hasControlNode(members, r.node) {
						continue
					}
				}
				catalog.groups[group] = append(catalog.groups[group], d)
			}
			r.mu.Lock()
			for group, state := range r.pgStates {
				if state.Active != nil {
					if _, ok := catalog.groups[group.String()]; !ok {
						catalog.groups[group.String()] = nil
					}
				}
			}
			r.mu.Unlock()
			r.pgCatalog.Store(catalog)
			r.pgRuntime.workersMu.Lock()
			if r.pgRuntime.workers == nil {
				r.pgRuntime.workers = map[string]pgRebuildWorker{}
			}
			for group, worker := range r.pgRuntime.workers {
				if _, exists := catalog.groups[group]; !exists {
					close(worker.stop)
					delete(r.pgRuntime.workers, group)
				}
			}
			for group := range catalog.groups {
				worker, exists := r.pgRuntime.workers[group]
				if !exists {
					worker = pgRebuildWorker{wake: make(chan struct{}, 1), stop: make(chan struct{})}
					r.pgRuntime.workers[group] = worker
					s.mu.Lock()
					if !s.closed {
						s.workers.Add(1)
						go r.runPGWorker(group, worker)
					}
					s.mu.Unlock()
				}
				select {
				case worker.wake <- struct{}{}:
				default:
				}
			}
			r.pgRuntime.workersMu.Unlock()
		}
	}()
}
func (r *rangeManager) runPGWorker(group string, worker pgRebuildWorker) {
	defer r.server.workers.Done()
	s := r.server
	var reported string
	var reportedAt time.Time
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-worker.stop:
			return
		case <-worker.wake:
		}
		if s.joinPending() || s.shortStopping.Load() || s.clusterPaused() || s.forceFrozen() {
			continue
		}
		catalog := r.pgCatalog.Load()
		if catalog == nil {
			continue
		}
		descriptions := catalog.groups[group]
		plan, ok := r.pendingPGPlan(group)
		if !ok {
			plan, ok = r.nextPressureGroup(group)
		}
		if !ok {
			plan, ok = r.planNextGroup(group, descriptions)
		}
		var err error
		if ok {
			ctx, cancel := context.WithCancel(s.ctx)
			started := time.Now()
			err = r.move(ctx, plan)
			cancel()
			if err == nil {
				r.mu.Lock()
				elapsed := time.Since(started)
				if r.capacityMoveTime == 0 {
					r.capacityMoveTime = elapsed
				} else {
					r.capacityMoveTime = (r.capacityMoveTime*3 + elapsed) / 4
				}
				r.mu.Unlock()
				select {
				case worker.wake <- struct{}{}:
				default:
				}
			}
		} else if group == "" {
			// Long-lived cleanup/drain intents do not delay a runnable diff.
			// Their idle admission also observes work in the other PG workers.
			if s.cleanupIdle() {
				_ = r.collectMemos()
				_ = r.collectAbortedImports()
				_ = r.finishEmptyDrains()
				err = r.finishNodeDrain(s.ctx)
			}
		}
		runtime.Gosched()
		if err != nil && (err.Error() != reported || time.Since(reportedAt) > 30*time.Second) {
			log.Printf("STQR rebuild node=%s pg=%q: %v", r.node, group, err)
			reported, reportedAt = err.Error(), time.Now()
		}
	}
}
func (r *rangeManager) pendingPGPlan(group string) (rangePlan, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, p := range r.pgOffers {
		if id.String() == group && p.Source == r.node {
			return p, true
		}
	}
	for id, state := range r.pgStates {
		if id.String() != group || state.Active == nil {
			continue
		}
		p, err := decodeRangePlan(state.Active.Round.Plan)
		if err == nil && p.Source == r.node {
			return p, true
		}
	}
	for _, id := range r.order {
		t := r.txns[id]
		if t.plan.Source != r.node || !r.active(t) {
			continue
		}
		b, err := r.server.Bucket(newVolumeID(t.plan.Before.Bucket.String()))
		if err == nil && b.Describe().Policy.PlacementGroup == group {
			return t.plan, true
		}
	}
	return rangePlan{}, false
}

// A global slot only fences scopes it actually changes. PG payload work is not
// a completion condition for an unrelated global administrative generation.
func (r *rangeManager) blocksGlobalLocked(claim controlClaim) bool {
	if claim.GlobalBarrier {
		if r.busyLocked() || len(r.pgOffers) > 0 {
			return true
		}
		for _, state := range r.pgStates {
			if state.Active != nil {
				return true
			}
		}
	}
	for _, p := range r.pgOffers {
		if claim.Exclusion != nil || hasControlNode(claim.FrozenBuckets, p.Before.Bucket) {
			return true
		}
	}
	for _, t := range r.txns {
		if !r.active(t) {
			continue
		}
		b, err := r.server.Bucket(newVolumeID(t.plan.Before.Bucket.String()))
		if err != nil || b.Describe().Policy.PlacementGroup == "" || claim.Exclusion != nil || hasControlNode(claim.FrozenBuckets, t.plan.Before.Bucket) {
			return true
		}
	}
	for _, state := range r.pgStates {
		if state.Active == nil {
			continue
		}
		p, _ := decodeRangePlan(state.Active.Round.Plan)
		if claim.Exclusion != nil || hasControlNode(claim.FrozenBuckets, p.Before.Bucket) {
			return true
		}
	}
	return false
}
