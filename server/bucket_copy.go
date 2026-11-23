// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Offline native copies preserve object addresses and payload, not versions or
// grants. A durable intent prevents a lost response from replaying a later clear.
// Cluster maintenance must acquire a distributed exclusion before using this path.
func (s *Server) executeBucketCopy(ctx context.Context, m client.Message, p principal) (out []byte, err error) {
	if err = s.requireAdmin(p); err != nil {
		return nil, err
	}
	d := client.Decoder{B: m.Body}
	srcID := volume.ID(d.ID())
	dstID := srcID
	if m.Opcode == client.BucketClone {
		dstID = volume.ID(d.ID())
	}
	if d.Done() != nil || srcID == systemBucket || dstID == systemBucket || m.Opcode == client.BucketClone && srcID == dstID {
		return nil, fail(400, "invalid clear/clone buckets")
	}
	s.opMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.opMu.Unlock()
		}
	}()
	if view := s.currentSystem.Load(); view != nil && len(view.manifest.Membership.Members) > 1 {
		return nil, fail(501, "clear/clone needs cluster-wide exclusive maintenance; only single-node supported")
	}
	receipt, previous, e := s.bucketReceipt(p, m)
	if e != nil {
		return nil, e
	}
	encode := func(n uint64) []byte { e := client.Encoder{}; e.U64(n); return e.B }
	if previous != nil {
		if receipt.Running || receipt.Status != 200 {
			return nil, fail(409, "previous copy/clear was interrupted; inspect destination before a new operation")
		}
		return encode(receipt.Objects), nil
	}
	src, e := s.Bucket(srcID)
	if e != nil {
		return nil, e
	}
	dst, e := s.Bucket(dstID)
	if e != nil {
		return nil, e
	}
	owners := map[*Bucket]*connectionAuthorization{}
	for _, b := range []*Bucket{src, dst} {
		if owners[b] != nil {
			continue
		}
		desc := b.Describe()
		if desc.State != BucketActive || desc.DesiredPolicy != nil || b.underMaintenance() {
			return nil, fail(409, "bucket must be active and idle")
		}
		s.wsMu.Lock()
		busy := false
		for ws := range s.sockets {
			if ws.authorization != nil && ws.authorization.bucket == b {
				busy = true
				break
			}
		}
		s.wsMu.Unlock()
		if busy {
			return nil, fail(409, "unmount/disconnect all bucket clients before clear/clone")
		}
		owners[b] = &connectionAuthorization{bucket: b, bucketID: b.ID(), admin: true, offlineCopy: true, permissions: BucketAllPermissions}
	}
	if src != dst {
		records, _, _, e := dst.scan(ctx, dst.Describe(), nil, 1)
		if e != nil {
			return nil, e
		}
		if len(records) != 0 {
			return nil, fail(409, "clone destination must be empty")
		}
	}
	receipt.Running = true
	desc := dst.Describe()
	desc.Receipts = append(desc.Receipts, receipt)
	if _, e = s.buckets.Save(desc, desc.Revision); e != nil {
		return nil, e
	}
	for b, a := range owners {
		b.mu.Lock()
		b.maintenance = a
		b.mu.Unlock()
	}
	s.opMu.Unlock()
	locked = false
	sourceRevision := src.Describe().Revision
	sourceStamp := inventoryStamp(s.placement.Load().bucketEntries(client.ID(src.ID())), client.ID{})
	var membership uint64
	if view := s.currentSystem.Load(); view != nil {
		membership = view.manifest.Membership.Epoch
	}
	count := uint64(0)
	defer func() {
		s.opMu.Lock()
		defer s.opMu.Unlock()
		if err == nil {
			if src.Describe().Revision != sourceRevision || inventoryStamp(s.placement.Load().bucketEntries(client.ID(src.ID())), client.ID{}) != sourceStamp {
				err = fail(409, "source changed during offline operation")
			}
			if view := s.currentSystem.Load(); view != nil && (view.manifest.Membership.Epoch != membership || len(view.manifest.Membership.Members) > 1) {
				err = fail(409, "membership changed during offline operation")
			}
		}
		desc := dst.Describe()
		for i := range desc.Receipts {
			if desc.Receipts[i].ID == receipt.ID {
				desc.Receipts[i].Running = false
				desc.Receipts[i].Objects = count
				if err != nil {
					desc.Receipts[i].Status = 500
				}
			}
		}
		_, saveErr := s.buckets.Save(desc, desc.Revision)
		err = errors.Join(err, saveErr)
		for b := range owners {
			b.mu.Lock()
			b.maintenance = nil
			b.mu.Unlock()
		}
	}()
	var after *volume.ObjectID
	for {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		records, next, more, e := src.scan(ctx, src.Describe(), after, 100)
		if e != nil {
			return nil, e
		}
		for _, record := range records {
			actor := p
			actor.connection = owners[src]
			opt := volume.Options{RequestID: volume.ID(client.NewRequestID())}
			if src == dst {
				version := record.Info.Version
				opt.ExpectedVersion = &version
				e = src.Delete(ctx, actor, record.Key.Object, opt)
			} else {
				// Finish each bounded source read before acquiring a destination
				// write handle: both buckets may share the same volume.
				targetActor := p
				targetActor.connection = owners[dst]
				opt.CreateOnly = true
				_, e = dst.Put(ctx, targetActor, record.Key.Object, nil, opt)
				for off := uint64(0); e == nil && off < record.Info.Length; {
					n := min(uint64(1<<20), record.Info.Length-off)
					var payload []byte
					payload, _, e = src.GetRange(ctx, actor, record.Key.Object, off, n)
					if e != nil {
						break
					}
					opt.RequestID = volume.ID(client.NewRequestID())
					opt.CreateOnly = false
					_, e = dst.PutRange(ctx, targetActor, record.Key.Object, off, payload, opt)
					off += n
				}
			}
			if e != nil {
				return nil, e
			}
			count++
		}
		if !more {
			break
		}
		cursor := next
		after = &cursor
	}
	return encode(count), nil
}
