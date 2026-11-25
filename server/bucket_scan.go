// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (b *Bucket) underMaintenance() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maintenance != nil
}

func (b *Bucket) blocksPlacementChange() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maintenance != nil && (b.lease == nil || b.lease.Released || !b.lease.ObjectIO || !b.lease.AllowHandoff)
}

func (ws *wsSession) releaseMaintenance() {
	if ws.authorization == nil || ws.authorization.bucket == nil {
		return
	}
	s := ws.server
	s.opMu.Lock()
	defer s.opMu.Unlock()
	b := ws.authorization.bucket
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lease != nil && !b.lease.Released {
		return
	}
	if b.maintenance == ws.authorization {
		b.maintenance = nil
	}
}

func (s *Server) executeMaintenance(ctx context.Context, op *objectMutation, d *client.Decoder, ws *wsSession, p principal, runtime client.ID) ([]byte, int, error) {
	if ws == nil || ws.authorization == nil {
		return nil, 0, fail(400, "maintenance requires a bucket socket")
	}
	b := op.bucket
	if op.message.Opcode == client.BucketMaintenanceEnd {
		if d.Done() != nil || !ws.authorization.leaseOwner.Load() || !b.leaseAllows(ws.authorization) {
			return nil, 0, ErrBucketForbidden
		}
		ids, _, err := s.leaseMembers()
		if l := ws.authorization.lease.Load(); l != nil && l.Group != (client.ID{}) {
			ids = append([]client.ID(nil), l.Members...)
		}
		if err != nil {
			return nil, 0, err
		}
		l := ws.authorization.lease.Load()
		if _, err = s.callLease(ctx, ids[0], 2, *l); err != nil {
			return nil, 0, err
		}
		return nil, 200, nil
	}

	if view := s.currentSystem.Load(); view != nil && s.controller != nil && (op.message.Opcode == client.BucketMaintenance || op.message.Opcode == client.BucketObjectLease) {
		objectIO := op.message.Opcode == client.BucketObjectLease
		if s.control != nil {
			s.control.ledger.mu.Lock()
			busy := s.control.ledger.slot != nil
			s.control.ledger.mu.Unlock()
			if busy {
				return nil, 0, fail(503, "cluster control transition in progress")
			}
		}
		readonly := false
		if objectIO {
			readonly = d.Bool()
		}
		if d.Done() != nil {
			return nil, 0, client.ErrProtocol
		}
		if err := s.beginClusterLease(ctx, b, ws, p, op.message.ID, objectIO, readonly); err != nil {
			return nil, 0, err
		}
		e := client.Encoder{B: []byte{1}}
		e.ID(ws.authorization.lease.Load().Token)
		return e.B, 200, nil
	}
	s.opMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.opMu.Unlock()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	description, err := b.authorize(p, BucketRead)
	if err != nil {
		return nil, 0, err
	}
	if description.State != BucketActive || description.DesiredPolicy != nil {
		return nil, 0, fail(409, "bucket lifecycle transition in progress")
	}
	if op.message.Opcode == client.BucketMaintenance {
		if err := s.checkReferenceAwareMaintenance(b, ws.authorization.objectReferences.Load()); err != nil {
			return nil, 0, err
		}
	}
	if op.message.Opcode == client.BucketMaintenance || op.message.Opcode == client.BucketObjectLease {
		objectIO := op.message.Opcode == client.BucketObjectLease
		if s.control != nil {
			s.control.ledger.mu.Lock()
			busy := s.control.ledger.slot != nil
			s.control.ledger.mu.Unlock()
			if busy {
				return nil, 0, fail(503, "cluster control transition in progress")
			}
		}
		readonly := false
		if objectIO {
			readonly = d.Bool()
		}
		if view := s.currentSystem.Load(); view != nil && len(view.manifest.Membership.Members) > 1 {
			return nil, 0, fail(501, "cluster-wide exclusive maintenance is not yet available")
		}
		if d.Done() != nil {
			return nil, 0, fail(400, "invalid maintenance request")
		}
		permission := BucketDelete
		if objectIO {
			permission = BucketRead
			if !readonly {
				permission = BucketWrite
			}
		}
		if _, err = b.authorize(p, permission); err != nil {
			return nil, 0, err
		}
		s.wsMu.Lock()
		busy := false
		for other := range s.sockets {
			if other != ws && other.authorization != nil && other.authorization.bucket == b {
				busy = true
				break
			}
		}
		s.wsMu.Unlock()
		if busy {
			return nil, 0, fail(409, "bucket is connected elsewhere; close other clients before acquiring an exclusive lease")
		}
		b.mu.Lock()
		b.maintenance = ws.authorization
		ws.authorization.objectLease.Store(objectIO)
		ws.authorization.leaseReadOnly.Store(objectIO && readonly)
		b.mu.Unlock()
		return nil, 200, nil
	}
	b.mu.RLock()
	owner := b.maintenance
	b.mu.RUnlock()
	if owner != ws.authorization && !b.leaseAllows(ws.authorization) {
		return nil, 0, fail(409, "exclusive maintenance must be acquired before scan")
	}
	var after *volume.ObjectID
	if d.Bool() {
		var id volume.ObjectID
		copy(id[:], d.Take(32))
		after = &id
	}
	limit := d.U32()
	if d.Done() != nil || limit == 0 || limit > 1000 {
		return nil, 0, fail(400, "invalid scan cursor/limit")
	}
	s.opMu.Unlock()
	locked = false
	records, next, more, err := b.scanCluster(ctx, description, p, after, int(limit))
	if err != nil {
		return nil, 0, err
	}
	e := client.Encoder{}
	e.U32(uint32(len(records)))
	for _, record := range records {
		e.B = append(e.B, record.Key.Object[:]...)
		encodeObjectInfo(&e, record.Info, runtime)
	}
	e.Bool(more)
	if more {
		e.B = append(e.B, next[:]...)
	}
	return e.B, 200, nil
}

// Merge copies before excluding tombstones: a newer tombstone must hide an old
// live copy. Cursor progress includes tombstones and the zero object address.
type replicaScanKey struct{}

func (b *Bucket) scan(ctx context.Context, description BucketDescription, after *volume.ObjectID, limit int) ([]volume.ObjectRecord, volume.ObjectID, bool, error) {
	return b.scanPlacement(ctx, description, after, limit, b.server.placement.Load(), nil)
}

func (b *Bucket) scanPlacement(ctx context.Context, description BucketDescription, after *volume.ObjectID, limit int, placement *placementImage, exclude func(client.RangeEntry) bool, views ...*systemView) ([]volume.ObjectRecord, volume.ObjectID, bool, error) {
	merged := map[volume.ObjectID]volume.ObjectRecord{}
	visited := map[volume.ObjectID]bool{}
	view := b.server.currentSystem.Load()
	if len(views) > 0 {
		view = views[0]
	}
	stores := b.volumesFor(description)
	if description.DesiredPolicy != nil {
		desired := description
		desired.Policy = *description.DesiredPolicy
		seen := map[*volume.Volume]bool{}
		for _, store := range stores {
			seen[store] = true
		}
		for _, store := range b.volumesFor(desired) {
			if !seen[store] {
				stores = append(stores, store)
				seen[store] = true
			}
		}
	}
	if len(views) > 0 && view != nil {
		seen := map[volume.ID]bool{}
		for _, store := range stores {
			seen[store.Identity().VolumeID] = true
		}
		for _, origin := range view.manifest.VolumeRecoveries {
			if origin.Complete && origin.Target == b.server.control.ledger.node && origin.Before.Bucket == client.ID(b.ID()) {
				for _, id := range origin.Volumes {
					staged := b.server.recoveryVolumes[volume.ID(id)]
					if staged != nil && !seen[volume.ID(id)] {
						stores = append(stores, staged.store)
						seen[volume.ID(id)] = true
					}
				}
			}
		}
	}
	for _, v := range stores {
		if err := ctx.Err(); err != nil {
			return nil, volume.ObjectID{}, false, err
		}
		h, err := v.Open(false)
		if err != nil {
			return nil, volume.ObjectID{}, false, err
		}
		page, err := h.ScanBucketObjects(b.ID(), after, limit+1)
		h.Close()
		if err != nil {
			return nil, volume.ObjectID{}, false, err
		}
		for _, r := range page {
			visited[r.Key.Object] = true
			if b.server.controller != nil {
				entry, known := placement.lookup(client.ID(b.ID()), client.ObjectID(r.Key.Object))
				owned := false
				for _, loc := range entry.Locations {
					if (loc.Role == 0 || ctx.Value(replicaScanKey{}) == true && loc.Role == 1) && loc.Node == b.server.control.ledger.node && entry.State != 4 && b.server.volumeAllowedForEntry(client.ID(b.ID()), entry, v.Identity().VolumeID, view) {
						owned = true
					}
				}
				if !known || !owned || exclude != nil && exclude(entry) {
					continue
				}
			}
			old, exists := merged[r.Key.Object]
			if exists && old.Info != r.Info {
				return nil, volume.ObjectID{}, false, fail(503, "inventory has conflicting physical copies")
			}
			if !exists {
				merged[r.Key.Object] = r
			}
		}
	}
	ids := make([]volume.ObjectID, 0, len(visited))
	for id := range visited {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	more := len(ids) > limit
	if more {
		ids = ids[:limit]
	}
	var next volume.ObjectID
	out := []volume.ObjectRecord{}
	for _, id := range ids {
		next = id
		if record, exists := merged[id]; exists && !record.Info.Deleted {
			out = append(out, record)
		}
	}
	return out, next, more, nil
}

// Read-only native inventory: no S3 names, no maintenance capability and no
// lexicographic key engine. Namespace frontends interpret their own records.
func (s *Server) executeObjectList(ctx context.Context, op *objectMutation, d *client.Decoder, p principal, runtime client.ID) ([]byte, int, error) {
	description, err := op.bucket.authorize(p, BucketRead)
	if err != nil {
		return nil, 0, err
	}
	var after *volume.ObjectID
	if d.Bool() {
		var id volume.ObjectID
		copy(id[:], d.Take(32))
		after = &id
	}
	limit := d.U32()
	if d.Done() != nil || limit == 0 || limit > 1000 {
		return nil, 0, fail(400, "invalid object scan")
	}
	var records []volume.ObjectRecord
	var next volume.ObjectID
	var more bool
	if len(description.ClonePeers) > 0 {
		records, next, more, err = s.cowLogicalPage(ctx, op.bucket.ID(), after, int(limit), false)
	} else {
		records, next, more, err = op.bucket.scanCluster(ctx, description, p, after, int(limit))
	}
	if err != nil {
		return nil, 0, err
	}
	e := client.Encoder{}
	e.U32(uint32(len(records)))
	for _, r := range records {
		e.B = append(e.B, r.Key.Object[:]...)
		objectRuntime := runtime
		if s.controller != nil {
			if entry, known := s.placement.Load().lookup(client.ID(op.bucket.ID()), client.ObjectID(r.Key.Object)); known {
				objectRuntime = entry.Runtime
			}
		}
		encodeObjectInfo(&e, r.Info, objectRuntime)
	}
	e.Bool(more)
	if more {
		e.B = append(e.B, next[:]...)
	}
	return e.B, 200, nil
}
