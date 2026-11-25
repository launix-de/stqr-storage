// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
)

// FSCheckResult counts live bucket objects, not volume tombstones or retry
// receipts. DeletedBytes is removed logical object payload, not OS file shrink.
type FSCheckResult struct {
	Scanned, Reachable, Deleted         uint64
	DeletedBytes                        uint64
	WhiteoutsRemoved, DirectoriesPruned uint64
}

// CheckFilesystem acquires exclusive maintenance on this bucket, validates the
// complete root-reachable graph, then deletes all unreachable live objects.
// Use a dedicated connection and Close it afterward to release maintenance.
// No reconnect/replay is permitted after acquiring maintenance; rerun after a
// disconnect. The bucket must contain only this filesystem, with all mounts closed.
func (c *Client) CheckFilesystem(ctx context.Context) (FSCheckResult, error) {
	if err := c.beginMaintenance(ctx); err != nil {
		return FSCheckResult{}, err
	}
	result, err := checkFilesystem(ctx, c)
	if err == nil {
		err = c.ReclaimBucketMetadata(ctx)
	}
	return result, err
}

func (c *Client) beginMaintenance(ctx context.Context) error {
	return c.beginMaintenanceCommand(ctx, BucketMaintenance, false)
}

type fsCheckObject = ObjectEntry
type fsCheckStore interface {
	FilesystemStore
	Delete(context.Context, ObjectID, DeleteOptions) error
	scanFilesystemObjects(context.Context, *ObjectID, uint32) ([]fsCheckObject, *ObjectID, error)
}

func (c *Client) scanFilesystemObjects(ctx context.Context, after *ObjectID, limit uint32) ([]fsCheckObject, *ObjectID, error) {
	return c.scanObjectEntries(ctx, after, limit, BucketScan)
}

func checkFilesystem(ctx context.Context, store fsCheckStore, opaqueRoots ...ObjectID) (FSCheckResult, error) {
	var result FSCheckResult
	if reset, ok := store.(interface{ ResetAbandonedObjectReferences(context.Context) error }); ok {
		if err := reset.ResetAbandonedObjectReferences(ctx); err != nil {
			return result, err
		}
	}
	if raw, _, err := store.Get(ctx, FilesystemRootID()); err == nil && len(raw) >= 4 && string(raw[:4]) == "STOR" {
		return result, ErrOverlayNeedsLower
	}
	f := Filesystem{store: store, directoryMemoryLimit: 64 << 20}
	if err := f.recoverNamespace(ctx); err != nil {
		return result, fmt.Errorf("fsck namespace recovery; nothing deleted: %w", err)
	}
	if err := prepareDependencyOrphans(ctx, &f, store); err != nil {
		return result, fmt.Errorf("fsck dependency orphan recovery; nothing deleted: %w", err)
	}
	inventory, err := filesystemInventory(ctx, store)
	if err != nil {
		return result, err
	}
	result.Scanned = uint64(len(inventory))
	mark := newFSCheckMarker(store, inventory)
	if err := mark.inode(ctx, FilesystemRootID(), true); err != nil {
		return result, fmt.Errorf("fsck validation; nothing deleted: %w", err)
	}
	if err := protectFilesystemDependencies(ctx, store, inventory, mark); err != nil {
		return result, fmt.Errorf("fsck dependency validation; nothing deleted: %w", err)
	}
	for id, record := range mark.links {
		refs := mark.references[id]
		if record.kind == fsDirectory {
			expected := uint64(1)
			if refs == 0 && mark.dependencies[id] {
				expected = 0
			}
			if refs != expected {
				return result, fmt.Errorf("directory has multiple parents: %w", ErrFSFormat)
			}
		} else if refs != uint64(record.count) {
			return result, fmt.Errorf("inode %s link count does not match namespace; nothing deleted: %w", id, ErrFSFormat)
		}
	}
	for _, id := range opaqueRoots {
		if _, ok := inventory[id]; !ok {
			return result, fmt.Errorf("missing opaque root %s; nothing deleted: %w", id, ErrNotFound)
		}
		if role := mark.roles[id]; role != 0 && role != 4 {
			return result, ErrFSFormat
		}
		mark.roles[id] = 4
	}
	return filesystemSweep(ctx, store, inventory, mark.roles)
}

// Dependency pins are live application roots, even when their owning upper
// bucket or COW snapshot is offline. Unlike crashed open references, they must
// survive ordinary fsck; releasing them requires global graph knowledge.
func protectFilesystemDependencies(ctx context.Context, store fsCheckStore, inventory map[ObjectID]ObjectInfo, mark *fsCheckMarker) error {
	reader, ok := store.(interface {
		ReadObjectDependencies(context.Context) ([]ObjectID, error)
	})
	if !ok {
		return nil
	}
	ids, err := reader.ReadObjectDependencies(ctx)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if _, exists := inventory[ObjectReferenceRootID()]; exists {
		mark.roles[ObjectReferenceRootID()] = 4
	}
	for _, id := range ids {
		if id == ObjectReferenceRootID() {
			return ErrFSFormat
		}
		if mark.dependencies == nil {
			mark.dependencies = map[ObjectID]bool{}
		}
		mark.dependencies[id] = true
		if err := mark.inode(ctx, id, false); err != nil {
			return err
		}
		// The dependency is a lifetime root, not an extra POSIX hard link.
		mark.references[id]--
	}
	if _, exists := inventory[filesystemOrphansID]; exists {
		if err := mark.inode(ctx, filesystemOrphansID, false); err != nil {
			return err
		}
	}
	return nil
}

// Keep a bounded queue of dependency-protected zero-link inodes. Once an upper
// graph releases its last anchor, a cooperating lower writer can finish normal
// cleanup rather than losing discoverability in an earlier offline sweep.
func prepareDependencyOrphans(ctx context.Context, f *Filesystem, store fsCheckStore) error {
	reader, ok := store.(interface {
		ReadObjectDependencies(context.Context) ([]ObjectID, error)
	})
	if !ok {
		return nil
	}
	dependencies, err := reader.ReadObjectDependencies(ctx)
	if err != nil || len(dependencies) == 0 {
		return err
	}
	var keep []ObjectID
	for _, id := range dependencies {
		n, _, err := f.load(ctx, id)
		if err != nil {
			return err
		}
		if fsLinks(n) == 0 {
			keep = append(keep, id)
		}
	}
	sort.Slice(keep, func(i, j int) bool { return bytes.Compare(keep[i][:], keep[j][:]) < 0 })
	s, old, err := f.orphanSnapshot(ctx, len(keep) > 0)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(old) == len(keep) {
		equal := true
		for i := range old {
			equal = equal && old[i] == keep[i]
		}
		if equal {
			return nil
		}
	}
	n := s.node
	body := encodeFSOrphans(keep)
	if err := f.stableBody(ctx, &n, body); err != nil {
		return err
	}
	n.blob.digest = sha256.Sum256(body)
	committed, err := f.transact(ctx, []fsTxMember{fsMember(s, n)})
	if err != nil {
		return err
	}
	if !committed {
		return ErrPrecondition
	}
	return nil
}

func filesystemInventory(ctx context.Context, store fsCheckStore) (map[ObjectID]ObjectInfo, error) {
	inventory := map[ObjectID]ObjectInfo{}
	var after *ObjectID
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, next, err := store.scanFilesystemObjects(ctx, after, 512)
		if err != nil {
			return nil, fmt.Errorf("fsck inventory: %w", err)
		}
		for _, object := range page {
			if _, exists := inventory[object.ID]; exists {
				return nil, ErrProtocol
			}
			inventory[object.ID] = object.Info
		}
		if next == nil {
			break
		}
		if after != nil && bytes.Compare(next[:], after[:]) <= 0 {
			return nil, ErrProtocol
		}
		after = next
	}
	return inventory, nil
}
func newFSCheckMarker(store fsCheckStore, inventory map[ObjectID]ObjectInfo) *fsCheckMarker {
	return &fsCheckMarker{references: map[ObjectID]uint64{}, links: map[ObjectID]fsCheckLinks{}, store: store, inventory: inventory, roles: map[ObjectID]uint8{}, indexes: map[ObjectID]fsCheckIndex{}, visiting: map[ObjectID]bool{}, lowerSources: map[ObjectID]fsCheckLower{}}
}
func filesystemSweep(ctx context.Context, store fsCheckStore, inventory map[ObjectID]ObjectInfo, roles map[ObjectID]uint8) (FSCheckResult, error) {
	result := FSCheckResult{Scanned: uint64(len(inventory)), Reachable: uint64(len(roles))}
	garbage := make([]ObjectID, 0, len(inventory)-len(roles))
	for id := range inventory {
		if roles[id] == 0 {
			garbage = append(garbage, id)
		}
	}
	sort.Slice(garbage, func(i, j int) bool { return bytes.Compare(garbage[i][:], garbage[j][:]) < 0 })
	for _, id := range garbage {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		info := inventory[id]
		if err := store.Delete(ctx, id, DeleteOptions{RequestID: NewRequestID()}); err != nil {
			return result, fmt.Errorf("fsck delete %s (rerun fsck to finish): %w", id, err)
		}
		result.Deleted++
		result.DeletedBytes += info.Length
	}
	return result, nil
}

const (
	fsCheckInode     uint8 = 1
	fsCheckData      uint8 = 2
	fsCheckIndexRole uint8 = 3
)

type fsCheckIndex struct {
	length uint64
	depth  uint8
}
type fsCheckLinks struct {
	kind  uint8
	count uint32
}
type fsCheckMarker struct {
	dependencies map[ObjectID]bool
	references   map[ObjectID]uint64
	links        map[ObjectID]fsCheckLinks
	store        fsCheckStore
	inventory    map[ObjectID]ObjectInfo
	roles        map[ObjectID]uint8
	indexes      map[ObjectID]fsCheckIndex
	visiting     map[ObjectID]bool
	lower        *Filesystem
	lowerSources map[ObjectID]fsCheckLower
}

func (m *fsCheckMarker) object(ctx context.Context, id ObjectID, role uint8) ([]byte, error) {
	info, exists := m.inventory[id]
	if !exists {
		return nil, fmt.Errorf("missing referenced object %s: %w", id, ErrNotFound)
	}
	if old := m.roles[id]; old != 0 && old != role {
		return nil, ErrFSFormat
	}
	data, actual, err := m.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if actual.Length != info.Length || uint64(len(data)) != info.Length || len(data) > FSObjectSize {
		return nil, ErrFSFormat
	}
	m.roles[id] = role
	return data, nil
}

func (m *fsCheckMarker) inode(ctx context.Context, id ObjectID, root bool) error {
	if m.references == nil {
		m.references = map[ObjectID]uint64{}
		m.links = map[ObjectID]fsCheckLinks{}
	}
	m.references[id]++
	if len(m.visiting) >= 1024 {
		return fmt.Errorf("directory nesting exceeds fsck limit: %w", ErrFSFormat)
	}
	if m.visiting[id] {
		return fmt.Errorf("directory cycle at %s: %w", id, ErrFSFormat)
	}
	if m.roles[id] == fsCheckInode {
		return nil
	}
	data, err := m.object(ctx, id, fsCheckInode)
	if err != nil {
		return err
	}
	node, err := decodeFSNode(data)
	if err != nil {
		return err
	}
	m.links[id] = fsCheckLinks{kind: node.kind, count: fsLinks(node)}
	if root && node.kind != fsDirectory {
		return ErrFSFormat
	}
	if node.blob.lowerOwner != (ObjectID{}) {
		f := Filesystem{lower: m.lower}
		if err := f.checkLowerAnchor(ctx, node.blob); err != nil {
			return err
		}
	}
	if node.blob.root != (ObjectID{}) {
		if err = m.segment(context.WithValue(ctx, fsSparsePageKey{}, node.blob.mutable), fsSegment{id: node.blob.root, length: node.blob.length, lower: node.blob.lower}, node.blob.depth); err != nil {
			return err
		}
		if err != nil {
			return err
		}
	}
	if node.kind == fsDirectory {
		m.visiting[id] = true
		defer delete(m.visiting, id)
		f := Filesystem{store: m.store, directoryMemoryLimit: 64 << 20}
		if node.removed && m.dependencies[id] {
			node.removed = false
		}
		entries, err := f.directory(ctx, node)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = m.inode(ctx, entry.ID, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *fsCheckMarker) segment(ctx context.Context, segment fsSegment, depth uint8) error {
	if segment.id == (ObjectID{}) {
		return nil
	} // sparse span
	if segment.lower {
		return m.lowerSource(ctx, segment.id, segment.offset, segment.length, depth)
	}
	if depth == 0 {
		if segment.lower {
			return m.lowerSource(ctx, segment.id, segment.offset, segment.length, 0)
		}
		info, ok := m.inventory[segment.id]
		sparse, _ := ctx.Value(fsSparsePageKey{}).(bool)
		if !ok || segment.offset > FSObjectSize || segment.length > FSObjectSize-segment.offset || (!sparse && (segment.offset > info.Length || segment.length > info.Length-segment.offset)) {
			return fmt.Errorf("invalid data span %s offset=%d length=%d stored=%d mutable=%t: %w", segment.id, segment.offset, segment.length, info.Length, sparse, ErrFSFormat)
		}
		if m.roles[segment.id] == fsCheckData {
			return nil
		}
		_, err := m.object(ctx, segment.id, fsCheckData)
		return err
	}
	shape := fsCheckIndex{segment.length, depth}
	if old, ok := m.indexes[segment.id]; ok {
		if old != shape {
			return ErrFSFormat
		}
		return nil
	}
	data, err := m.object(ctx, segment.id, fsCheckIndexRole)
	if err != nil {
		return err
	}
	children, err := decodeFSIndex(data, segment.length, depth)
	if err != nil {
		return err
	}
	m.indexes[segment.id] = shape
	for _, child := range children {
		if err = m.segment(ctx, child, depth-1); err != nil {
			return err
		}
	}
	return nil
}

// BeginMaintenance excludes other bucket clients until Close. It is intended for
// offline tools and exclusive block-device exports. It never reconnects after a
// lost session, and fails if distributed exclusion is unavailable.
func (c *Client) BeginMaintenance(ctx context.Context) error { return c.beginMaintenance(ctx) }

// CheckFilesystemWithOpaqueRoots validates the filesystem graph while retaining
// application-owned opaque objects. Call BeginMaintenance successfully first,
// then read/validate the application's root manifest under that same exclusion.
// A dedicated connection must be closed afterward. Missing roots abort the sweep.
func (c *Client) CheckFilesystemWithOpaqueRoots(ctx context.Context, roots []ObjectID) (FSCheckResult, error) {
	result, err := checkFilesystem(ctx, c, roots...)
	if err == nil {
		err = c.ReclaimBucketMetadata(ctx)
	}
	return result, err
}
