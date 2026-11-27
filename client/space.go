// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"syscall"
	"time"
)

const BucketStatFS uint16 = 0x011b

// BucketSpace is a live advisory snapshot, not a reservation. Used counts live
// object bytes (including filesystem metadata and objects awaiting fsck). For
// an unlimited COW bucket, Used instead measures allocated physical bytes in
// its eligible storage pool, without scanning the inherited namespace. Free
// is remaining eligible physical cluster space, capped by the bucket quota.
// Total = Used + Free. Compression and future metadata growth affect admission.
type BucketSpace struct {
	Used, Total, Free, Available, Quota uint64
	QuotaLimited                        bool
}

func (c *Client) Space(ctx context.Context) (BucketSpace, error) {
	// A df probe must finish even if an owner is unavailable and normal object
	// requests would keep retrying 503. Earlier caller deadlines still apply.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.ready(ctx); err != nil {
		return BucketSpace{}, err
	}
	e := Encoder{}
	e.ID(c.bucket)
	r, err := c.do(ctx, BucketStatFS, e.B, RequestID{}, false, nil)
	if err != nil {
		return BucketSpace{}, err
	}
	defer r.close()
	d := Decoder{B: r.result}
	out := BucketSpace{Used: d.U64(), Total: d.U64(), Free: d.U64(), Available: d.U64(), Quota: d.U64(), QuotaLimited: d.Bool()}
	if d.Done() != nil || out.Free > out.Total || out.Used != out.Total-out.Free || out.Available > out.Free {
		return BucketSpace{}, ErrProtocol
	}
	return out, nil
}

func (s *SubscriptionStore) Space(ctx context.Context) (BucketSpace, error) {
	if s == nil || s.client == nil {
		return BucketSpace{}, ErrClosed
	}
	ctx, release, err := s.backend(ctx)
	if err != nil {
		return BucketSpace{}, err
	}
	defer release()
	return s.client.Space(ctx)
}

func (f *Filesystem) Space(ctx context.Context) (BucketSpace, error) {
	if s, ok := f.store.(interface {
		Space(context.Context) (BucketSpace, error)
	}); ok {
		return s.Space(ctx)
	}
	return BucketSpace{}, syscall.ENOTSUP
}
func (o *OverlayFilesystem) Space(ctx context.Context) (BucketSpace, error) {
	return o.upper.Space(ctx)
}
