// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"context"
	"sync"
)

const maxParallelStripeReads = 16

type stripeReadScatter struct {
	ctx     context.Context
	cancel  context.CancelFunc
	slots   chan struct{}
	work    sync.WaitGroup
	failure sync.Once
	err     error
}

func newStripeReadScatter(ctx context.Context) *stripeReadScatter {
	ctx, cancel := context.WithCancel(ctx)
	return &stripeReadScatter{ctx: ctx, cancel: cancel, slots: make(chan struct{}, maxParallelStripeReads-1)}
}
func (g *stripeReadScatter) fail(err error) {
	if err != nil {
		g.failure.Do(func() { g.err = err; g.cancel() })
	}
}
func (g *stripeReadScatter) run(fn func() error) {
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
