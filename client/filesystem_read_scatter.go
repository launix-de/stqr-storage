// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"sync"
)

const maxParallelObjectReads = 16

type fsReadSlotsKey struct{}

// Slots belong to one logical read, including recursive lower reads. A task
// never waits for a slot while holding one: a full batch executes its next
// child inline, so deep trees cannot deadlock or multiply the worker bound.
type fsReadScatter struct {
	ctx     context.Context
	cancel  context.CancelFunc
	slots   chan struct{}
	work    sync.WaitGroup
	failure sync.Once
	err     error
}

func newFSReadScatter(ctx context.Context) *fsReadScatter {
	slots, _ := ctx.Value(fsReadSlotsKey{}).(chan struct{})
	if slots == nil {
		slots = make(chan struct{}, maxParallelObjectReads-1)
		ctx = context.WithValue(ctx, fsReadSlotsKey{}, slots)
	}
	ctx, cancel := context.WithCancel(ctx)
	return &fsReadScatter{ctx: ctx, cancel: cancel, slots: slots}
}
func (g *fsReadScatter) fail(err error) {
	if err != nil {
		g.failure.Do(func() { g.err = err; g.cancel() })
	}
}
func (g *fsReadScatter) run(fn func() error) {
	if err := g.ctx.Err(); err != nil {
		g.fail(err)
		return
	}
	select {
	case g.slots <- struct{}{}:
		g.work.Add(1)
		go func() { defer g.work.Done(); defer func() { <-g.slots }(); g.fail(fn()) }()
	default:
		g.fail(fn())
	}
}
func (f *Filesystem) concurrentObjectReads() bool {
	for current := f; current != nil; current = current.lower {
		parallel, ok := current.store.(interface{ ConcurrentObjectRequests() bool })
		if !ok || !parallel.ConcurrentObjectRequests() {
			return false
		}
	}
	return true
}
