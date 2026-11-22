// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerBucketInventory uint16 = 0x104d

func inventoryStamp(entries []client.RangeEntry, owner client.ID) [32]byte {
	selected := make([]client.RangeEntry, 0, len(entries))
	for _, entry := range entries {
		for _, loc := range entry.Locations {
			if loc.Role == 0 && (owner == (client.ID{}) || loc.Node == owner) {
				selected = append(selected, entry)
				break
			}
		}
	}
	b, _ := json.Marshal(selected)
	return sha256.Sum256(b)
}

func (s *Server) inventoryOwners(bucket volume.ID) []client.ID {
	seen := map[client.ID]bool{}
	for _, entry := range s.placement.Load().bucketEntries(client.ID(bucket)) {
		for _, loc := range entry.Locations {
			if loc.Role == 0 {
				seen[loc.Node] = true
			}
		}
	}
	owners := make([]client.ID, 0, len(seen))
	for node := range seen {
		owners = append(owners, node)
	}
	sort.Slice(owners, func(i, j int) bool { return bytes.Compare(owners[i][:], owners[j][:]) < 0 })
	return owners
}

// Refresh known owners first. If an old owner is unreachable, another live
// member may hold its committed handoff lineage. Only the resulting current
// owners are required to answer the inventory; unrelated offline nodes wait
// for no operation and are never declared failed or evicted.
func (s *Server) refreshInventoryMap(ctx context.Context, bucket volume.ID) error {
	failed := map[client.ID]bool{}
	tried := map[client.ID]bool{s.control.ledger.node: true}
	for _, node := range s.inventoryOwners(bucket) {
		if tried[node] {
			continue
		}
		tried[node] = true
		if err := s.scopes.syncArchiveFrom(ctx, node); err != nil {
			failed[node] = true
		}
	}
	if len(failed) > 0 {
		var alternatives []client.ID
		s.peerStatsMu.RLock()
		for node, observation := range s.peerStats {
			if observation.online && !tried[node] {
				alternatives = append(alternatives, node)
			}
		}
		s.peerStatsMu.RUnlock()
		for _, node := range alternatives {
			_ = s.scopes.syncArchiveFrom(ctx, node)
		}
	}
	for _, node := range s.inventoryOwners(bucket) {
		if failed[node] {
			return fail(503, "inventory requires an unavailable owner")
		}
	}
	return nil
}

func (b *Bucket) scanCluster(ctx context.Context, description BucketDescription, p principal, after *volume.ObjectID, limit int) ([]volume.ObjectRecord, volume.ObjectID, bool, error) {
	s := b.server
	if s.controller == nil {
		return b.scan(ctx, description, after, limit)
	}
	if limit < 1 || limit > 1000 {
		return nil, volume.ObjectID{}, false, volume.ErrRange
	}
	entries := s.placement.Load().bucketEntries(client.ID(b.ID()))
	stamp := inventoryStamp(entries, client.ID{})
	merged := map[volume.ObjectID]volume.ObjectRecord{}
	var boundary *volume.ObjectID
	for _, node := range s.inventoryOwners(b.ID()) {
		ownerStamp := inventoryStamp(entries, node)
		var records []volume.ObjectRecord
		var next volume.ObjectID
		var more bool
		var err error
		if node == s.control.ledger.node {
			err = b.inventoryAvailable(entries)
			if err == nil {
				records, next, more, err = b.scan(ctx, description, after, limit)
			}
		} else {
			e := client.Encoder{}
			e.U16(1)
			e.ID(client.ID(b.ID()))
			actor, actorErr := parseID(p.User.ID)
			if actorErr != nil {
				return nil, next, false, ErrBucketForbidden
			}
			e.ID(client.ID(actor))
			e.Text(p.User.Name)
			required := p.frontendPermission
			if required == 0 {
				required = BucketRead
			}
			e.U32(uint32(required))
			e.B = append(e.B, ownerStamp[:]...)
			e.Bool(after != nil)
			if after != nil {
				e.B = append(e.B, after[:]...)
			}
			e.U32(uint32(limit))
			body, callErr := (peerControlParticipants{runtime: s.control}).call(ctx, node, peerBucketInventory, e.B)
			if callErr != nil {
				// Normal pages use the already pushed map. Only a rejected/stale
				// owner or lost connection needs archive catch-up, not every page.
				if code := statusOf(callErr); code == 503 || code == 409 {
					_ = s.refreshInventoryMap(ctx, b.ID())
					return nil, next, false, fail(503, "inventory owner is not ready")
				}
				return nil, next, false, callErr
			}
			d := client.Decoder{B: body}
			n := d.Count(1000)
			if n > limit {
				return nil, next, false, client.ErrProtocol
			}
			records = make([]volume.ObjectRecord, n)
			var previous *volume.ObjectID = after
			for i := range records {
				r := &records[i]
				r.Key.Bucket = b.ID()
				copy(r.Key.Object[:], d.Take(32))
				r.Info.Version, r.Info.Length, r.Info.Compression = d.U64(), d.U64(), volume.Compression(d.U8())
				if previous != nil && bytes.Compare(r.Key.Object[:], previous[:]) <= 0 || r.Info.Version == 0 || r.Info.Compression > volume.XZ {
					d.Err = client.ErrProtocol
				}
				previous = &r.Key.Object
			}
			more = d.Bool()
			if more {
				copy(next[:], d.Take(32))
				if after != nil && bytes.Compare(next[:], after[:]) <= 0 || previous != nil && bytes.Compare(next[:], previous[:]) < 0 {
					d.Err = client.ErrProtocol
				}
			}
			err = d.Done()
		}
		if err != nil {
			return nil, next, false, err
		}
		for _, record := range records {
			merged[record.Key.Object] = record
		}
		// A page needs only the smallest limit+1 keys seen so far, regardless
		// of membership size. Larger keys will be fetched after its cursor.
		if len(merged) > limit+1 {
			keys := make([]volume.ObjectID, 0, len(merged))
			for id := range merged {
				keys = append(keys, id)
			}
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
			for _, id := range keys[limit+1:] {
				delete(merged, id)
			}
		}
		if more && (boundary == nil || bytes.Compare(next[:], boundary[:]) < 0) {
			cursor := next
			boundary = &cursor
		}
	}
	if inventoryStamp(s.placement.Load().bucketEntries(client.ID(b.ID())), client.ID{}) != stamp {
		return nil, volume.ObjectID{}, false, fail(503, "inventory placement changed")
	}
	ids := make([]volume.ObjectID, 0, len(merged))
	for id := range merged {
		if boundary == nil || bytes.Compare(id[:], boundary[:]) <= 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	if len(ids) > limit {
		ids = ids[:limit]
		cursor := ids[len(ids)-1]
		boundary = &cursor
	}
	out := make([]volume.ObjectRecord, 0, len(ids))
	for _, id := range ids {
		out = append(out, merged[id])
	}
	if boundary != nil {
		return out, *boundary, true, nil
	}
	return out, volume.ObjectID{}, false, nil
}

func (b *Bucket) inventoryAvailable(entries []client.RangeEntry) error {
	s := b.server
	for _, entry := range entries {
		for _, loc := range entry.Locations {
			if loc.Role == 0 && loc.Node == s.control.ledger.node && entry.Runtime != (client.ID{}) && !s.scopes.ramAvailable(b.ID(), volume.ObjectID(entry.Start), entry) {
				return fail(503, "RAM inventory waits for runtime recovery")
			}
		}
	}
	return nil
}

// Only authenticated, admitted peers can delegate this read-only operation.
// The target resolves the current user and ACL again; no claimed admin role,
// cleartext password, system-object access or mutation is accepted here.
func (s *Server) receiveBucketInventory(ctx context.Context, body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema, bucket, user, name, required := d.U16(), volume.ID(d.ID()), d.ID(), d.Text(), BucketPermissions(d.U32())
	var stamp [32]byte
	copy(stamp[:], d.Take(32))
	var after *volume.ObjectID
	if d.Bool() {
		var id volume.ObjectID
		copy(id[:], d.Take(32))
		after = &id
	}
	limit := d.U32()
	if schema != 1 || d.Done() != nil || bucket == systemBucket || limit < 1 || limit > 1000 || required != BucketRead && required != BucketWrite && required != BucketAppend && required != BucketDelete {
		return nil, client.ErrProtocol
	}
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, ErrBucketForbidden
	}
	account, err := s.findAccount(name)
	if err != nil || account.ID != user.String() || account.Disabled {
		return nil, ErrBucketForbidden
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	description, err := b.authorize(principal{User: account, frontendPermission: required}, BucketRead)
	if err != nil {
		return nil, err
	}
	entries := s.placement.Load().bucketEntries(client.ID(bucket))
	if inventoryStamp(entries, s.control.ledger.node) != stamp {
		return nil, fail(503, "inventory owner map differs")
	}
	if err := b.inventoryAvailable(entries); err != nil {
		return nil, err
	}
	records, next, more, err := b.scan(ctx, description, after, int(limit))
	if err != nil {
		return nil, err
	}
	if inventoryStamp(s.placement.Load().bucketEntries(client.ID(bucket)), s.control.ledger.node) != stamp {
		return nil, fail(503, "inventory owner changed")
	}
	e := client.Encoder{}
	e.V(uint64(len(records)))
	for _, record := range records {
		e.B = append(e.B, record.Key.Object[:]...)
		e.U64(record.Info.Version)
		e.U64(record.Info.Length)
		e.U8(uint8(record.Info.Compression))
	}
	e.Bool(more)
	if more {
		e.B = append(e.B, next[:]...)
	}
	return e.B, nil
}
