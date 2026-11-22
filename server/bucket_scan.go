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
	if b.maintenance == ws.authorization {
		b.maintenance = nil
	}
}

func (s *Server) executeMaintenance(ctx context.Context, op *objectMutation, d *client.Decoder, ws *wsSession, p principal, runtime client.ID) ([]byte, int, error) {
	if ws == nil || ws.authorization == nil {
		return nil, 0, fail(400, "maintenance requires a bucket socket")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	b := op.bucket
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
		if d.Done() != nil {
			return nil, 0, fail(400, "invalid maintenance request")
		}
		if _, err = b.authorize(p, BucketDelete); err != nil {
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
			return nil, 0, fail(409, "bucket is connected elsewhere; unmount all clients before fsck")
		}
		b.mu.Lock()
		b.maintenance = ws.authorization
		b.mu.Unlock()
		return nil, 200, nil
	}
	b.mu.RLock()
	owner := b.maintenance
	b.mu.RUnlock()
	if owner != ws.authorization {
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
	records, next, more, err := b.scan(ctx, description, after, int(limit))
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
func (b *Bucket) scan(ctx context.Context, description BucketDescription, after *volume.ObjectID, limit int) ([]volume.ObjectRecord, volume.ObjectID, bool, error) {
	merged := map[volume.ObjectID]volume.ObjectRecord{}
	for _, v := range b.volumesFor(description) {
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
			old, exists := merged[r.Key.Object]
			if !exists || r.Info.Version > old.Info.Version || r.Info.Version == old.Info.Version && r.Info.Deleted {
				merged[r.Key.Object] = r
			}
		}
	}
	ids := make([]volume.ObjectID, 0, len(merged))
	for id := range merged {
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
		if !merged[id].Info.Deleted {
			out = append(out, merged[id])
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
	records, next, more, err := op.bucket.scan(ctx, description, after, int(limit))
	if err != nil {
		return nil, 0, err
	}
	e := client.Encoder{}
	e.U32(uint32(len(records)))
	for _, r := range records {
		e.B = append(e.B, r.Key.Object[:]...)
		encodeObjectInfo(&e, r.Info, runtime)
	}
	e.Bool(more)
	if more {
		e.B = append(e.B, next[:]...)
	}
	return e.B, 200, nil
}
