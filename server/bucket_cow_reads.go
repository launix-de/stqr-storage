// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"io"

	"github.com/launix-de/stqr-storage/volume"
)

func (s *Server) cowLogicalSnapshot(ctx context.Context, bucket volume.ID, object volume.ObjectID, metadata bool) (*cowSnapshot, error) {
	for range 8 {
		g, err := s.cowGraph()
		if err != nil {
			return nil, err
		}
		proof, err := s.cowGraphDigest(g, bucket)
		if err != nil {
			return nil, err
		}
		snapshot, err := g.resolve(ctx, cowServerObjects{server: s, metadata: metadata, readReplica: true}, bucket, object, map[volume.ID]bool{})
		if err != nil {
			return nil, err
		}
		current, err := s.cowGraph()
		if err != nil {
			snapshot.close()
			return nil, err
		}
		after, err := s.cowGraphDigest(current, bucket)
		if err != nil {
			snapshot.close()
			return nil, err
		}
		if after == proof {
			return snapshot, nil
		}
		snapshot.close()
	}
	return nil, fail(503, "clone graph changed repeatedly during read")
}
func (b *Bucket) cowCopyTo(ctx context.Context, id volume.ObjectID, offset, length uint64, dst io.Writer, condition *volume.Options) (volume.Info, uint64, error) {
	snapshot, err := b.server.cowLogicalSnapshot(ctx, b.ID(), id, false)
	if err != nil {
		return volume.Info{}, 0, err
	}
	defer snapshot.close()
	if snapshot == nil || snapshot.Info.Deleted {
		return volume.Info{}, 0, volume.ErrNotFound
	}
	info := snapshot.Info
	if condition != nil {
		if condition.ExpectedLength != nil && *condition.ExpectedLength != info.Length {
			return info, 0, volume.ErrPrecondition
		}
		if condition.Compare != nil {
			off := condition.CompareOffset
			if off > info.Length || uint64(len(condition.Compare)) > info.Length-off {
				return info, 0, volume.ErrPrecondition
			}
			actual := make([]byte, len(condition.Compare))
			if len(actual) > 0 {
				_, err = snapshot.Data.ReadAt(actual, int64(off))
				if err != nil {
					return info, 0, err
				}
			}
			if !bytes.Equal(actual, condition.Compare) {
				return info, 0, volume.ErrPrecondition
			}
		}
	}
	if offset > info.Length {
		return info, 0, volume.ErrRange
	}
	length = min(length, info.Length-offset)
	n, err := io.CopyN(bucketContextWriter{ctx: ctx, w: dst}, io.NewSectionReader(snapshot.Data, int64(offset), int64(length)), int64(length))
	return info, uint64(n), err
}

func (s *Server) cowLogicalPage(ctx context.Context, bucket volume.ID, after *volume.ObjectID, limit int, local bool) ([]volume.ObjectRecord, volume.ObjectID, bool, error) {
	if limit < 1 || limit > 1000 {
		return nil, volume.ObjectID{}, false, volume.ErrRange
	}
	g, err := s.cowGraph()
	if err != nil {
		return nil, volume.ObjectID{}, false, err
	}
	proof, err := s.cowGraphDigest(g, bucket)
	if err != nil {
		return nil, volume.ObjectID{}, false, err
	}
	out, next, more, err := s.cowGraphPage(ctx, bucket, after, limit, local, g, s.placement.Load())
	if err != nil {
		return nil, next, false, err
	}
	current, err := s.cowGraph()
	if err != nil {
		return nil, next, false, err
	}
	now, err := s.cowGraphDigest(current, bucket)
	if err != nil {
		return nil, next, false, err
	}
	if now != proof {
		return nil, next, false, fail(503, "clone graph changed during inventory")
	}
	return out, next, more, nil
}
func (b *Bucket) cowLogicalUsage() (uint64, error) {
	var used uint64
	var after *volume.ObjectID
	for {
		page, next, more, err := b.server.cowLogicalPage(b.server.ctx, b.ID(), after, 128, true)
		if err != nil {
			return 0, err
		}
		for _, r := range page {
			if r.Info.Length > ^uint64(0)-used {
				return 0, volume.ErrRange
			}
			used += r.Info.Length
		}
		if !more {
			return used, nil
		}
		after = &next
	}
}

// A quota checkpoint may inspect a prospective clone graph while all affected
// namespaces are fenced, without publishing it or materializing any payload.
func (s *Server) cowGraphPage(ctx context.Context, bucket volume.ID, after *volume.ObjectID, limit int, local bool, g cowGraph, placement *placementImage) ([]volume.ObjectRecord, volume.ObjectID, bool, error) {
	seen := map[volume.ObjectID]bool{}
	more := false
	for _, peer := range g.component(bucket) {
		page, extra, err := s.cowPhysicalPage(ctx, peer, after, limit)
		if err != nil {
			return nil, volume.ObjectID{}, false, err
		}
		more = more || extra
		for _, id := range page {
			seen[id] = true
		}
	}
	ids := make([]volume.ObjectID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sortCowIDs(ids)
	if len(ids) > limit {
		ids = ids[:limit]
		more = true
	}
	var next volume.ObjectID
	var out []volume.ObjectRecord
	for _, id := range ids {
		next = id
		if local {
			owner := client.ID{}
			if s.controller != nil {
				entry, known := placement.lookup(client.ID(bucket), client.ObjectID(id))
				if !known || len(entry.Locations) == 0 {
					return nil, next, false, fail(503, "prospective clone owner unavailable")
				}
				owner = entry.Locations[0].Node
			}
			if !s.cowLocal(owner) {
				continue
			}
		}
		snapshot, err := g.resolve(ctx, cowServerObjects{server: s, metadata: true, readReplica: true}, bucket, id, map[volume.ID]bool{})
		if err != nil {
			return nil, next, false, err
		}
		if snapshot != nil && !snapshot.Info.Deleted {
			out = append(out, volume.ObjectRecord{Key: volume.Key{Bucket: bucket, Object: id}, Info: snapshot.Info})
		}
		snapshot.close()
	}
	return out, next, more, nil
}
