// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"fmt"
	"syscall"
)

type fsCheckLower struct{ version, length uint64 }

func (m *fsCheckMarker) lowerSource(ctx context.Context, id ObjectID, version, offset, length uint64) error {
	if m.lower == nil {
		return ErrOverlayNeedsLower
	}
	if old, ok := m.lowerSources[id]; ok {
		if old.version != version {
			return syscall.ESTALE
		}
		if offset > old.length || length > old.length-offset {
			return ErrFSFormat
		}
		return nil
	}
	n, info, err := m.lower.load(ctx, id)
	if err != nil {
		return fmt.Errorf("referenced lower inode %s is unavailable: %w", id, err)
	}
	if info.Version != version {
		return fmt.Errorf("referenced lower inode %s changed from version %d to %d: %w", id, version, info.Version, syscall.ESTALE)
	}
	if n.kind != fsFile || offset > n.blob.length || length > n.blob.length-offset {
		return ErrFSFormat
	}
	if n.blob.source != (ObjectID{}) {
		return ErrFSFormat
	}
	seen := map[ObjectID]uint64{}
	var validate func(fsSegment, uint8) error
	validate = func(s fsSegment, depth uint8) error {
		if s.id == (ObjectID{}) {
			return nil
		}
		if s.lowerVersion != 0 {
			return ErrFSFormat
		}
		if depth == 0 {
			if length, ok := seen[s.id]; ok {
				if s.offset > length || s.length > length-s.offset {
					return ErrFSFormat
				}
				return nil
			}
			data, _, e := m.lower.store.Get(ctx, s.id)
			if e != nil {
				return e
			}
			if len(data) > FSObjectSize || s.offset > uint64(len(data)) || s.length > uint64(len(data))-s.offset {
				return ErrFSFormat
			}
			seen[s.id] = uint64(len(data))
			return nil
		}
		children, e := m.lower.index(ctx, s, depth)
		if e != nil {
			return e
		}
		for _, child := range children {
			if e = validate(child, depth-1); e != nil {
				return e
			}
		}
		return nil
	}
	if n.blob.root != (ObjectID{}) {
		if err = validate(fsSegment{id: n.blob.root, length: n.blob.length}, n.blob.depth); err != nil {
			return err
		}
	}
	m.lowerSources[id] = fsCheckLower{version, n.blob.length}
	return nil
}

// CheckOverlayFilesystem owns only the upper maintenance session. The lower
// connection remains read-only and prevents a simultaneous lower fsck while
// references are being verified. Never deletes objects in the lower bucket.
func (c *Client) CheckOverlayFilesystem(ctx context.Context, lower FilesystemStore, lowerID BucketID) (FSCheckResult, error) {
	if err := c.beginMaintenance(ctx); err != nil {
		return FSCheckResult{}, err
	}
	o, err := OpenOverlayFilesystem(ctx, lower, c, OverlayOptions{LowerBucketID: lowerID})
	if err != nil {
		return FSCheckResult{}, err
	}
	return checkOverlayFilesystem(ctx, o, c)
}
func checkOverlayFilesystem(ctx context.Context, o *OverlayFilesystem, store fsCheckStore) (FSCheckResult, error) {
	s, info, blob, err := o.state(ctx)
	if err == nil {
		s = cloneOverlayState(s)
	}
	if err != nil {
		return FSCheckResult{}, err
	}
	inventory, err := filesystemInventory(ctx, store)
	if err != nil {
		return FSCheckResult{}, err
	}
	mark, dirs, err := markOverlay(ctx, o, store, s, info.Version, blob, inventory)
	if err != nil {
		return FSCheckResult{Scanned: uint64(len(inventory))}, fmt.Errorf("overlay fsck validation; nothing deleted: %w", err)
	}
	whiteouts, pruned, err := pruneOverlayMetadata(ctx, o, s, dirs)
	if err != nil {
		return FSCheckResult{Scanned: uint64(len(inventory))}, fmt.Errorf("overlay fsck reconciliation; nothing deleted: %w", err)
	}
	confirmed := FSCheckResult{Scanned: uint64(len(inventory))}
	if whiteouts != 0 || pruned != 0 {
		if err = o.publish(ctx, s, info.Version); err != nil {
			return confirmed, err
		}
		confirmed.WhiteoutsRemoved, confirmed.DirectoriesPruned = whiteouts, pruned
		s, info, blob, err = o.state(ctx)
		if err != nil {
			return confirmed, err
		}
		inventory, err = filesystemInventory(ctx, store)
		if err != nil {
			return confirmed, err
		}
		mark, _, err = markOverlay(ctx, o, store, s, info.Version, blob, inventory)
		if err != nil {
			return confirmed, err
		}
	}
	result, err := filesystemSweep(ctx, store, inventory, mark.roles)
	result.WhiteoutsRemoved, result.DirectoriesPruned = confirmed.WhiteoutsRemoved, confirmed.DirectoriesPruned
	return result, err
}

// Lower maintenance must be quiescent during this reconciliation. A whiteout
// is redundant only when that name is currently absent in its lower directory.
// Removing it intentionally permits a later lower re-creation to become visible.
func pruneOverlayMetadata(ctx context.Context, o *OverlayFilesystem, s *overlayState, reachable map[ObjectID]bool) (uint64, uint64, error) {
	// Ordinary upper operations mark removed directories explicitly. A live
	// record becoming unreachable instead signals changed lower ancestry (or
	// corruption). Its local children/attributes must never be swept as garbage.
	for id, dir := range s.dirs {
		if !reachable[id] && !dir.node.removed {
			return 0, 0, fmt.Errorf("live upper directory %s became unreachable after lower namespace changes; local entries retained, manual reconciliation required: %w", id, syscall.ESTALE)
		}
	}
	var whiteouts, pruned uint64
	for id, dir := range s.dirs {
		if !reachable[id] {
			delete(s.dirs, id)
			pruned++
			continue
		}
		lowerNames := map[string]bool{}
		needsLower := false
		for _, child := range dir.entries {
			if child == (ObjectID{}) {
				needsLower = true
				break
			}
		}
		if needsLower && !dir.opaque {
			entries, err := o.lower.ReadDir(ctx, dir.lower)
			if err != nil {
				return 0, 0, fmt.Errorf("cannot verify lower directory %s: %w", dir.lower, err)
			}
			for _, entry := range entries {
				lowerNames[entry.Name] = true
			}
		}
		for name, child := range dir.entries {
			if child == (ObjectID{}) && !lowerNames[name] {
				delete(dir.entries, name)
				whiteouts++
			}
		}
	}
	return whiteouts, pruned, nil
}
func markOverlay(ctx context.Context, o *OverlayFilesystem, store fsCheckStore, s *overlayState, version uint64, blob fsBlob, inventory map[ObjectID]ObjectInfo) (*fsCheckMarker, map[ObjectID]bool, error) {
	mark := newFSCheckMarker(store, inventory)
	mark.lower = o.lower
	if _, err := mark.object(ctx, FilesystemRootID(), 4); err != nil {
		return nil, nil, err
	}
	if blob.root != (ObjectID{}) {
		if err := mark.segment(ctx, fsSegment{id: blob.root, length: blob.length}, blob.depth); err != nil {
			return nil, nil, err
		}
	}
	seen := map[ObjectID]bool{}
	dirs := map[ObjectID]bool{}
	var walk func(ObjectID, int) error
	walk = func(id ObjectID, depth int) error {
		if depth > 1024 {
			return ErrFSFormat
		}
		info, err := o.stat(ctx, s, version, id)
		if err != nil {
			return err
		}
		if seen[id] {
			if info.Directory {
				return ErrFSFormat
			}
			if _, upper := inventory[id]; upper {
				return ErrFSFormat
			}
			return nil
		}
		seen[id] = true
		if info.Removed {
			return ErrFSFormat
		}
		if info.Directory {
			if s.dirs[id] != nil {
				dirs[id] = true
			}
			entries, err := o.entries(ctx, s, id)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if err = walk(entry.ID, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		if _, ok := inventory[id]; ok {
			return mark.inode(ctx, id, false)
		}
		// A missing upper object is normal only for an untouched lower inode.
		if _, _, err = o.lower.load(ctx, id); err != nil {
			return err
		}
		return nil
	}
	if err := walk(FilesystemRootID(), 0); err != nil {
		return nil, nil, err
	}
	// Every explicit dentry must resolve in its recorded bucket. Otherwise a
	// missing upper-created inode could accidentally look like a lower fallback.
	for id := range dirs {
		for _, child := range s.dirs[id].entries {
			if child == (ObjectID{}) {
				continue
			}
			if s.dirs[child] == nil {
				if _, ok := inventory[child]; !ok {
					return nil, nil, errors.New("overlay has a missing upper inode")
				}
			}
		}
	}
	return mark, dirs, nil
}
