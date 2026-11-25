// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Request-scoped adapter. The HTTP operation authorizes the user's grant first;
// the frontend's private operation grant permits internal reads/CAS needed even
// by write-only/delete-only users. It is never exposed as a native connection.
type s3Store struct {
	b *Bucket
	p principal
}

// All S3 operations, including copy and background multipart recovery, use
// bucket-object routing. The cleanup bounds private credentials to this caller.
func (s *Server) openS3(ctx context.Context, b *Bucket, p principal) (*client.S3, func(), error) {
	if err := s.openBucketFormat(ctx, b.ID().String(), "s3", p); err != nil {
		return nil, nil, err
	}
	if view := s.currentSystem.Load(); s.controller != nil && view != nil && len(view.manifest.Membership.Members) > 1 {
		connection, err := s.openFrontend(ctx, b.ID(), p, "s3")
		if err != nil {
			return nil, nil, err
		}
		return client.NewS3(connection), func() { _ = connection.Close() }, nil
	}
	return client.NewS3(s3Store{b: b, p: p}), func() {}, nil
}

func s3NativeError(err error) error {
	switch {
	case errors.Is(err, volume.ErrNotFound):
		return client.ErrNotFound
	case errors.Is(err, volume.ErrPrecondition):
		return client.ErrPrecondition
	case errors.Is(err, volume.ErrFull):
		return client.ErrFull
	case err != nil && objectStatus(err) == 507:
		return client.ErrFull
	case errors.Is(err, volume.ErrRange):
		return client.ErrRange
	}
	return err
}
func s3Info(i volume.Info) client.ObjectInfo {
	return client.ObjectInfo{Length: i.Length}
}
func (s s3Store) Get(ctx context.Context, id client.ObjectID) ([]byte, client.ObjectInfo, error) {
	b, i, e := s.b.Get(ctx, s.p, volume.ObjectID(id))
	return b, s3Info(i), s3NativeError(e)
}
func (s s3Store) GetRange(ctx context.Context, id client.ObjectID, off, n uint64) ([]byte, client.ObjectInfo, error) {
	b, i, e := s.b.GetRange(ctx, s.p, volume.ObjectID(id), off, n)
	return b, s3Info(i), s3NativeError(e)
}
func (s s3Store) CAS(ctx context.Context, id client.ObjectID, off uint64, expected, replacement []byte, opt client.CASOptions) (client.ObjectInfo, error) {
	i, e := s.b.CAS(ctx, s.p, volume.ObjectID(id), off, expected, replacement, volume.Options{RequestID: volume.ID(opt.RequestID), ExpectedLength: opt.ExpectedLength, CompareOffset: opt.CompareOffset, Compare: opt.Compare, CreateOnly: opt.Absent, WholeObject: opt.Scope == client.CASObject})
	return s3Info(i), s3NativeError(e)
}
func (s s3Store) PutFrom(ctx context.Context, id client.ObjectID, src io.Reader, n uint64, opt client.PutOptions) (client.ObjectInfo, error) {
	i, e := s.b.PutReader(ctx, s.p, volume.ObjectID(id), src, n, volume.Options{RequestID: volume.ID(opt.RequestID), CreateOnly: opt.Absent, ExpectedLength: opt.ExpectedLength, CompareOffset: opt.CompareOffset, Compare: opt.Compare})
	return s3Info(i), s3NativeError(e)
}
func (s s3Store) GetRangeTo(ctx context.Context, id client.ObjectID, off, n uint64, dst io.Writer) (client.ObjectInfo, error) {
	i, _, e := s.b.CopyTo(ctx, s.p, volume.ObjectID(id), off, n, dst)
	return s3Info(i), s3NativeError(e)
}

func (s s3Store) GetRangeToMatching(ctx context.Context, id client.ObjectID, off, n uint64, condition client.ReadCondition, dst io.Writer) (client.ObjectInfo, error) {
	snapshot, e := os.CreateTemp("", "stqr-s3-read-*")
	if e != nil {
		return client.ObjectInfo{}, e
	}
	if e = os.Remove(snapshot.Name()); e != nil {
		snapshot.Close()
		return client.ObjectInfo{}, e
	}
	defer snapshot.Close()
	i, _, e := s.b.copyToChecked(ctx, s.p, volume.ObjectID(id), off, n, snapshot, &volume.Options{ExpectedLength: condition.Length, CompareOffset: condition.Offset, Compare: condition.Bytes})
	if e != nil {
		return s3Info(i), s3NativeError(e)
	}
	if _, e = snapshot.Seek(0, io.SeekStart); e != nil {
		return s3Info(i), e
	}
	_, e = io.Copy(bucketContextWriter{ctx: ctx, w: dst}, snapshot)
	return s3Info(i), e
}
func (s s3Store) Delete(ctx context.Context, id client.ObjectID, opt client.DeleteOptions) error {
	return s3NativeError(s.b.Delete(ctx, s.p, volume.ObjectID(id), volume.Options{RequestID: volume.ID(opt.RequestID), ExpectedLength: opt.ExpectedLength, CompareOffset: opt.CompareOffset, Compare: opt.Compare}))
}
func (s s3Store) ScanObjects(ctx context.Context, after *client.ObjectID, limit uint32) ([]client.ObjectEntry, *client.ObjectID, error) {
	d, e := s.b.authorize(s.p, BucketRead)
	if e != nil {
		return nil, nil, e
	}
	var cursor *volume.ObjectID
	if after != nil {
		id := volume.ObjectID(*after)
		cursor = &id
	}
	records, next, more, e := s.b.scanCluster(ctx, d, s.p, cursor, int(limit))
	if e != nil {
		return nil, nil, e
	}
	out := make([]client.ObjectEntry, 0, len(records))
	for _, r := range records {
		out = append(out, client.ObjectEntry{ID: client.ObjectID(r.Key.Object), Info: s3Info(r.Info)})
	}
	if !more {
		return out, nil, nil
	}
	id := client.ObjectID(next)
	return out, &id, nil
}
