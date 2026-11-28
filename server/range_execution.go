// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A worker and a foreground helper share the same execution. Helpers raise
// its priority while waiting; they never start a second snapshot transfer.
// Entries live only while callers hold a reference, including gate waiters.
type rangeExecution struct {
	gate   chan struct{}
	urgent atomic.Int64
	refs   int // protected by executionsMu
}
type rangeExecutionKey struct{}

func (r *rangeManager) executeMove(ctx context.Context, p rangePlan, foreground bool) error {
	id := p.id()
	runtime := &r.pgRuntime
	runtime.executionsMu.Lock()
	if runtime.executions == nil {
		runtime.executions = make(map[[32]byte]*rangeExecution)
	}
	e := runtime.executions[id]
	if e == nil {
		e = &rangeExecution{gate: make(chan struct{}, 1)}
		runtime.executions[id] = e
	}
	e.refs++
	if foreground {
		e.urgent.Add(1)
	}
	runtime.executionsMu.Unlock()
	defer func() {
		runtime.executionsMu.Lock()
		if foreground {
			e.urgent.Add(-1)
		}
		e.refs--
		if e.refs == 0 {
			delete(runtime.executions, id)
		}
		runtime.executionsMu.Unlock()
	}()
	select {
	case e.gate <- struct{}{}:
		defer func() { <-e.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The previous holder may have completed or cancelled the durable handoff.
	// Inspect it after acquiring the gate, not before waiting for the worker.
	r.mu.Lock()
	t := r.txns[id]
	finished := t != nil && t.sourceDone.RecordSequence != 0
	aborted := t != nil && t.aborted
	retired := t != nil && t.forceRetired
	r.mu.Unlock()
	if retired {
		return errControlConflict
	}
	if aborted {
		// The range stays aborted. Retry only its durable PG cancellation handshake:
		// a lost completion reply must not leave the generation blocking shutdown.
		if err := r.abortPG(ctx, p); err != nil {
			return err
		}
		return errControlConflict
	}
	if finished {
		return r.finishPG(ctx, p)
	}
	started := time.Now()
	err := r.moveSerial(context.WithValue(ctx, rangeExecutionKey{}, e), p)
	if err == nil {
		r.server.statistics.observeMove(time.Now(), time.Since(started), r.server.placementHalfLives())
	}
	return err
}

// Only borrow an already bound diff that actually retires the full volume's
// copy. Registering a capacity hint and choosing a new placement remain the
// planner's job. The caller must first release its object/range/volume locks.
func (r *rangeManager) stealSpace(ctx context.Context, bucket volume.ID, failed volume.ID) (bool, error) {
	if failed == (volume.ID{}) {
		return false, nil
	}
	r.mu.Lock()
	var plan rangePlan
	found := false
	useful := func(p rangePlan) bool {
		if p.Source != r.node || p.Before.Bucket != client.ID(bucket) || p.ReplicaAdd || p.ReplicaPromote || p.CodecOnly {
			return false
		}
		was, remains := false, false
		for _, entry := range p.Before.Entries {
			for _, loc := range entry.Locations {
				was = was || (loc.Node == r.node && loc.Volume == client.ID(failed))
			}
		}
		for _, entry := range p.After.Entries {
			for _, loc := range entry.Locations {
				remains = remains || (loc.Node == r.node && loc.Volume == client.ID(failed))
			}
		}
		// A split retains siblings on the old volume; the moving subrange is
		// sufficient to free space even if its siblings stay there.
		if !was {
			return false
		}
		if p.ReplicaRemove || p.ReplicaRelocate {
			return !remains
		}
		return p.Target != r.node || p.Volume != failed
	}
	for _, id := range r.order {
		t := r.txns[id]
		if r.active(t) && useful(t.plan) {
			plan, found = t.plan, true
			break
		}
	}
	if !found {
		for _, p := range r.pgOffers {
			if useful(p) {
				plan, found = p, true
				break
			}
		}
	}
	r.mu.Unlock()
	if !found {
		return false, nil
	}
	err := r.executeMove(ctx, plan, true)
	if errors.Is(err, errControlConflict) {
		// The worker may have durably cancelled this offer while the helper
		// waited for its execution gate. Retry the original object against
		// current placement/capacity; the obsolete offer is not an I/O fault.
		r.mu.Lock()
		t := r.txns[plan.id()]
		cancelled := t != nil && (t.aborted || t.forceRetired)
		r.mu.Unlock()
		if cancelled {
			return true, fail(503, "capacity handoff was cancelled; retry current placement")
		}
	}
	return true, err
}
