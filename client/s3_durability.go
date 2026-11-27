// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"io"
)

type s3SyncStore interface{ Sync(context.Context) error }
type s3WriteContextKey struct{}
type s3WriteCut struct{}

// Native S3 writes are ordinary accepted object operations. S3, as the client
// adapter promising persistent publication, explicitly requests one fence at
// the outer successful API boundary. Nested multipart publication shares that
// cut. Successful retries also fence observed state: a previous publication
// may be visible but its earlier fence may have failed. Readers, failed
// operations, close and background deletion do not fence.
func (s *S3) beginWrite(ctx context.Context) (context.Context, func(*error)) {
	if _, ok := ctx.Value(s3WriteContextKey{}).(*s3WriteCut); ok {
		return ctx, func(*error) {}
	}
	state := &s3WriteCut{}
	ctx = context.WithValue(ctx, s3WriteContextKey{}, state)
	return ctx, func(err *error) {
		if *err == nil {
			if store, ok := s.store.(s3SyncStore); ok {
				*err = store.Sync(ctx)
			}
		}
	}
}

type s3AcceptedStore struct {
	S3Store
	fence s3SyncStore
}

func (s *s3AcceptedStore) Sync(ctx context.Context) error { return s.fence.Sync(ctx) }
func (s *s3AcceptedStore) CAS(ctx context.Context, id ObjectID, off uint64, before, after []byte, opt CASOptions) (ObjectInfo, error) {
	opt.Accepted = true
	info, err := s.S3Store.CAS(ctx, id, off, before, after, opt)
	return info, err
}
func (s *s3AcceptedStore) PutFrom(ctx context.Context, id ObjectID, src io.Reader, n uint64, opt PutOptions) (ObjectInfo, error) {
	opt.Accepted = true
	info, err := s.S3Store.PutFrom(ctx, id, src, n, opt)
	return info, err
}
func (s *s3AcceptedStore) Delete(ctx context.Context, id ObjectID, opt DeleteOptions) error {
	opt.Accepted = true
	err := s.S3Store.Delete(ctx, id, opt)
	return err
}
