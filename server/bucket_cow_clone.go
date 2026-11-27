// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func cowSameStorage(a, b BucketPolicy) bool {
	b.Version = a.Version
	b.Limits = a.Limits
	b.ReadAcceleration = a.ReadAcceleration
	return a == b
}
func (s *Server) cowMembers() []client.ID {
	if s.control == nil {
		return []client.ID{{}}
	}
	view := s.currentSystem.Load()
	if view == nil {
		return nil
	}
	return membershipIDs(view.manifest.Membership)
}
func (s *Server) cowPhysicalPage(ctx context.Context, bucket volume.ID, after *volume.ObjectID, limit int) ([]volume.ObjectID, bool, error) {
	seen := map[volume.ObjectID]bool{}
	more := false
	for _, node := range s.cowMembers() {
		e := client.Encoder{}
		e.ID(client.ID(bucket))
		var id volume.ObjectID
		if after != nil {
			id = *after
		}
		e.B = append(e.B, id[:]...)
		e.Bool(after != nil)
		e.U32(uint32(limit))
		body, err := s.cowCall(ctx, node, peerCowInventory, e.B)
		if err != nil {
			return nil, false, err
		}
		d := client.Decoder{B: body}
		more = d.Bool() || more
		for n := d.Count(1000); n > 0; n-- {
			var id volume.ObjectID
			copy(id[:], d.Take(32))
			seen[id] = true
		}
		if d.Done() != nil {
			return nil, false, client.ErrProtocol
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
	return ids, more, nil
}
func sortCowIDs(ids []volume.ObjectID) {
	sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
}
func (s *Server) cowSync(ctx context.Context, bucket volume.ID) error {
	for _, node := range s.cowMembers() {
		e := client.Encoder{}
		e.ID(client.ID(bucket))
		e.B = append(e.B, make([]byte, 32)...)
		if _, err := s.cowCall(ctx, node, peerCowSync, e.B); err != nil {
			return err
		}
	}
	return nil
}

// cowTransferableLogicalUsage is used under the single-node mutation/admin
// lock. The source usage includes every visible live object once. The reserved
// reference root is bucket-local, so subtract only the source's own visible
// live record, never a peer's root or a tombstone. A legacy physical copy with
// a foreign ORF1 owner still consumes source bytes, but is equally nontransferable;
// inspect metadata only and do not rewrite or retire any holder tokens here.
func (b *Bucket) cowTransferableLogicalUsage(ctx context.Context) (uint64, error) {
	used, err := b.logicalUsage()
	if err != nil {
		return 0, err
	}
	root, err := b.server.cowLogicalSnapshot(ctx, b.ID(), volume.ObjectID(client.ObjectReferenceRootID()), true)
	if err != nil {
		return 0, err
	}
	defer root.close()
	if root != nil && !root.Info.Deleted {
		if root.Info.Length > used {
			return 0, volume.ErrCorrupt
		}
		used -= root.Info.Length
	}
	return used, nil
}

func (s *Server) cloneCow(ctx context.Context, m client.Message, p principal) ([]byte, error) {
	if err := s.requireAdmin(p); err != nil {
		return nil, err
	}
	d := client.Decoder{B: m.Body}
	source, destination := volume.ID(d.ID()), volume.ID(d.ID())
	if d.Done() != nil || source == destination || source == systemBucket || destination == systemBucket {
		return nil, fail(400, "invalid clone namespaces")
	}
	if s.controller != nil {
		members := s.cowMembers()
		if len(members) == 0 {
			return nil, fail(503, "clone membership unavailable")
		}
		if members[0] != s.control.ledger.node {
			actor, err := parseID(p.User.ID)
			if err != nil {
				return nil, err
			}
			e := client.Encoder{}
			e.ID(client.ID(actor))
			e.Text(p.User.Name)
			e.ID(m.ID)
			e.U16(m.Opcode)
			e.Blob(m.Body)
			return (peerControlParticipants{runtime: s.control}).call(ctx, members[0], peerBucketCopy, e.B)
		}
	}
	s.copyCoordinator.Lock()
	defer s.copyCoordinator.Unlock()
	result := client.Encoder{}
	result.U64(0) // number of objects physically copied up front
	build := func(target *Server) error {
		receipt, previous, err := target.bucketReceipt(p, m)
		if err != nil {
			return err
		}
		if previous != nil {
			if receipt.Running || receipt.Status != 200 {
				return fail(409, "previous clone did not finish")
			}
			return nil
		}
		src, err := target.Bucket(source)
		if err != nil {
			return err
		}
		dst, err := target.Bucket(destination)
		if err != nil {
			return err
		}
		a, b := src.Describe(), dst.Describe()
		if !target.metadataPreview && !b.Policy.Limits.Unlimited && b.Policy.Limits.Accounting == "logical" {
			used, err := src.cowTransferableLogicalUsage(ctx)
			if err != nil {
				return err
			}
			if used > b.Policy.Limits.Absolute {
				return volume.ErrFull
			}
		}
		if a.State != BucketActive && a.State != BucketLocked || b.State != BucketActive || a.DesiredPolicy != nil || b.DesiredPolicy != nil || len(b.ClonePeers) > 0 {
			return fail(409, "clone needs active source and fresh destination")
		}
		graph, err := target.cowGraph()
		if err != nil {
			return err
		}
		if len(graph.component(source)) >= 4096 {
			return fail(409, "clone component has reached its traversal limit")
		}
		a.ClonePeers = append(a.ClonePeers, BucketClonePeer{Bucket: b.ID, Link: m.ID.String()})
		if b.Format != "" && b.Format != a.Format {
			return fail(409, "clone destination format differs from source")
		}
		b.Format = a.Format
		if b.Format != "" {
			b.Schema = 2
		}
		b.ClonePeers = append(b.ClonePeers, BucketClonePeer{Bucket: a.ID, Link: m.ID.String()})
		b.CloneDetach = !cowSameStorage(a.Policy, b.Policy)
		receipt.Running = false
		receipt.Status = 200
		receipt.Objects = 0
		a.Receipts = append(a.Receipts, receipt) // both endpoints retain external-reference sharing evidence
		b.Receipts = append(b.Receipts, receipt)
		return target.buckets.saveBatch([]BucketDescription{a, b})
	}
	if s.controller == nil {
		s.opMu.Lock()
		defer s.opMu.Unlock()
		receipt, previous, err := s.bucketReceipt(p, m)
		if err != nil {
			return nil, err
		}
		if previous != nil {
			if receipt.Status != 200 {
				return nil, fail(409, "previous clone failed")
			}
			return result.B, nil
		}
		for _, id := range []volume.ID{source, destination} {
			b, err := s.Bucket(id)
			if err != nil {
				return nil, err
			}
			if b.underMaintenance() {
				return nil, fail(409, "clone conflicts with bucket maintenance")
			}
		}
		page, _, err := s.cowPhysicalPage(ctx, destination, nil, 1)
		if err != nil {
			return nil, err
		}
		if len(page) > 0 {
			return nil, fail(409, "clone destination contains physical records; use a fresh bucket")
		}
		if err = s.cowSync(ctx, source); err != nil {
			return nil, err
		}
		if err = build(s); err != nil {
			return nil, err
		}
		if b, e := s.Bucket(destination); e == nil && b.storageDescription().CloneDetach {
			s.startBucketCleanup(destination)
		}
		return result.B, nil
	}
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.resume(ctx); err != nil {
		return nil, err
	}
	s.opMu.Lock()
	receipt, previous, err := s.bucketReceipt(p, m)
	s.opMu.Unlock()
	if err != nil {
		return nil, err
	}
	if previous != nil {
		if receipt.Status != 200 {
			return nil, fail(409, "previous clone failed")
		}
		return result.B, nil
	}
	_, _, err = c.change(ctx, nil, func() (*adminDraft, error) {
		page, _, err := s.cowPhysicalPage(ctx, destination, nil, 1)
		if err != nil {
			return nil, err
		}
		if len(page) > 0 {
			return nil, fail(409, "clone destination contains physical records; use a fresh bucket")
		}
		if err = s.cowSync(ctx, source); err != nil {
			return nil, err
		}
		s.opMu.Lock()
		preview, err := s.newAdminPreview()
		s.opMu.Unlock()
		if err != nil {
			return nil, err
		}
		draft := &adminDraft{server: preview, status: 200, result: result.B}
		if err = build(preview); err != nil {
			draft.close()
			return nil, err
		}
		return draft, nil
	}, client.ID(source), client.ID(destination))
	if err != nil {
		var native *client.StatusError
		if errors.As(err, &native) {
			return nil, fail(native.Status, err.Error())
		}
		return nil, err
	}
	return result.B, nil
}

// The protocol envelope carries a stable request ID, never an application
// pathname. Filesystems and block devices remain opaque bucket objects.
