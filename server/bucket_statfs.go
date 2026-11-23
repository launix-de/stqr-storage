// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (s *Server) executeBucketStatFS(ctx context.Context, m client.Message, p principal) ([]byte, error) {
	d := client.Decoder{B: m.Body}
	id := volume.ID(d.ID())
	if d.Done() != nil {
		return nil, fail(400, "invalid bucket space request")
	}
	b, err := s.Bucket(id)
	if err != nil {
		return nil, err
	}
	desc, err := b.authorize(p, BucketRead|BucketWrite|BucketAppend|BucketDelete)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.bucketAwaitingRecovery(client.ID(id)) {
		return nil, fail(503, "bucket has unavailable recovery ranges")
	}
	c, err := s.quotaCapacitySnapshot(ctx, id, uint8(bucketClass(desc.Policy)), principal{}, true)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Policy changes during a probe must not return a snapshot with a stale limit.
	if b.Describe().Revision != desc.Revision {
		return nil, fail(503, "bucket configuration changed during capacity measurement")
	}
	free := c.Available
	if !desc.Policy.Limits.Unlimited {
		free = min(free, subtractCapacity(desc.Policy.Limits.Absolute, c.Used))
	}
	// Saturate the aggregate while preserving Total = Used + Free.
	total := addCapacity(c.Used, free)
	free = total - c.Used
	e := client.Encoder{}
	e.U64(c.Used)
	e.U64(total)
	e.U64(free)
	e.U64(free)
	e.U64(desc.Policy.Limits.Absolute)
	e.Bool(!desc.Policy.Limits.Unlimited)
	return e.B, nil
}
