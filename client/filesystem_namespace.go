// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"time"
)

// Rmdir removes an empty directory and fences its inode atomically against
// creates through stale handles. Space is reclaimed by the offline fsck.
func (f *Filesystem) Rmdir(ctx context.Context, parent ObjectID, name string) error {
	if f.readOnly {
		return syscall.EROFS
	}
	if !validFSName(name) {
		return syscall.EINVAL
	}
	for attempt := 0; attempt < 128; attempt++ {
		p, err := f.snapshot(ctx, parent)
		if err != nil {
			return err
		}
		entries, err := f.directory(ctx, p.node)
		if err != nil {
			return err
		}
		i := findFSEntry(entries, name)
		if i < 0 {
			return syscall.ENOENT
		}
		child, err := f.snapshot(ctx, entries[i].ID)
		if err != nil {
			return err
		}
		if child.id == FilesystemRootID() {
			return syscall.EBUSY
		}
		contents, err := f.directory(ctx, child.node)
		if err != nil {
			return err
		}
		if len(contents) != 0 {
			return syscall.ENOTEMPTY
		}
		next, err := f.directoryImage(ctx, p.node, append(entries[:i], entries[i+1:]...))
		if err != nil {
			return err
		}
		removed := child.node
		removed.removed = true
		committed, err := f.transact(ctx, []fsTxMember{fsMember(p, next), fsMember(child, removed)})
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return syscall.EAGAIN
}

func (f *Filesystem) directoryImage(ctx context.Context, n fsNode, entries []FSEntry) (next fsNode, err error) {
	defer func() {
		if err != nil && errors.Is(f.checkNodeView(ctx, n), ErrPrecondition) {
			err = ErrPrecondition
		}
	}()
	data, err := encodeFSDirectory(entries)
	if err != nil {
		return n, err
	}
	if uint64(len(data)) > f.directoryMemoryLimit {
		return n, syscall.ENOMEM
	}
	if err = f.directoryLinks(ctx, &n, entries); err != nil {
		return n, err
	}
	err = f.stableBody(ctx, &n, data)
	n.mtime = uint64(time.Now().UnixNano())
	n.ctime = n.mtime
	return n, err
}

const (
	FSRenameNoReplace uint32 = 1
	FSRenameExchange  uint32 = 2
)

// RenameWithFlags supports POSIX replacement, NOREPLACE and EXCHANGE. Parent
// changes and replacement-directory removal share one durable decision.
func (f *Filesystem) RenameWithFlags(ctx context.Context, parent ObjectID, name string, newParent ObjectID, newName string, flags uint32) error {
	if f.readOnly {
		return syscall.EROFS
	}
	if !validFSName(name) || !validFSName(newName) {
		return syscall.EINVAL
	}
	if flags&^(FSRenameNoReplace|FSRenameExchange) != 0 {
		return syscall.ENOTSUP
	}
	if flags == (FSRenameNoReplace | FSRenameExchange) {
		return syscall.EINVAL
	}
	for attempt := 0; attempt < 128; attempt++ {
		members, noop, err := f.planRename(ctx, parent, name, newParent, newName, flags)
		if err != nil {
			return err
		}
		if noop {
			return nil
		}
		if members == nil {
			continue
		} // conflicting snapshots; read a fresh plan
		committed, err := f.transact(ctx, members)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return syscall.EAGAIN
}

func (f *Filesystem) planRename(ctx context.Context, parent ObjectID, name string, newParent ObjectID, newName string, flags uint32) ([]fsTxMember, bool, error) {
	p, err := f.snapshot(ctx, parent)
	if err != nil {
		return nil, false, err
	}
	entries, err := f.directory(ctx, p.node)
	if err != nil {
		return nil, false, err
	}
	i := findFSEntry(entries, name)
	if i < 0 {
		return nil, false, syscall.ENOENT
	}
	q := p
	dest := entries
	if parent != newParent {
		q, err = f.snapshot(ctx, newParent)
		if err != nil {
			return nil, false, err
		}
		dest, err = f.directory(ctx, q.node)
		if err != nil {
			return nil, false, err
		}
	}
	j := findFSEntry(dest, newName)
	if flags == FSRenameNoReplace && j >= 0 {
		return nil, false, syscall.EEXIST
	}
	if flags == FSRenameExchange && j < 0 {
		return nil, false, syscall.ENOENT
	}
	if parent == newParent && name == newName {
		return nil, true, nil
	}
	source, err := f.snapshot(ctx, entries[i].ID)
	if err != nil {
		return nil, false, err
	}
	if source.node.removed {
		return nil, false, syscall.ENOENT
	}
	var target fsSnapshot
	if j >= 0 {
		target, err = f.snapshot(ctx, dest[j].ID)
		if err != nil {
			return nil, false, err
		}
		if target.node.removed {
			return nil, false, syscall.ENOENT
		}
		if target.id == source.id {
			return nil, true, nil
		}
		if flags != FSRenameExchange {
			if source.node.kind == fsDirectory && target.node.kind != fsDirectory {
				return nil, false, syscall.ENOTDIR
			}
			if source.node.kind != fsDirectory && target.node.kind == fsDirectory {
				return nil, false, syscall.EISDIR
			}
			if target.node.kind == fsDirectory {
				contents, e := f.directory(ctx, target.node)
				if e != nil {
					return nil, false, e
				}
				if len(contents) != 0 {
					return nil, false, syscall.ENOTEMPTY
				}
			}
		}
	}
	checks := map[ObjectID]fsSnapshot{}
	consistent := true
	add := func(s fsSnapshot) {
		if old, ok := checks[s.id]; ok && (old.info.Version != s.info.Version || !bytes.Equal(old.raw, s.raw)) {
			consistent = false
		}
		checks[s.id] = s
	}
	add(p)
	add(q)
	// Lock the destination ancestor path for directory moves. Another rename
	// cannot move that path underneath us between the cycle check and commit.
	// This is an optimistic namespace transaction; ordinary file writes and
	// changes to unrelated directories do not take a global writer lock.
	if source.node.kind == fsDirectory {
		path, e := f.directoryPath(ctx, newParent)
		if e != nil {
			return nil, false, e
		}
		for _, s := range path {
			if s.id == source.id {
				return nil, false, syscall.EINVAL
			}
			add(s)
		}
		add(source)
	}
	if flags == FSRenameExchange && target.node.kind == fsDirectory {
		path, e := f.directoryPath(ctx, parent)
		if e != nil {
			return nil, false, e
		}
		for _, s := range path {
			if s.id == target.id {
				return nil, false, syscall.EINVAL
			}
			add(s)
		}
		add(target)
	}
	if !consistent {
		return nil, false, nil
	}
	remove := func(list []FSEntry, key string) []FSEntry {
		out := make([]FSEntry, 0, len(list))
		for _, entry := range list {
			if entry.Name != key {
				out = append(out, entry)
			}
		}
		return out
	}
	nextP := remove(entries, name)
	nextQ := remove(dest, newName)
	if parent == newParent {
		nextP = remove(nextP, newName)
		nextQ = nextP
	}
	nextQ = append(nextQ, FSEntry{Name: newName, ID: source.id})
	if flags == FSRenameExchange {
		if parent == newParent {
			nextQ = append(nextQ, FSEntry{Name: name, ID: target.id})
		} else {
			nextP = append(nextP, FSEntry{Name: name, ID: target.id})
		}
	}
	replacements := map[ObjectID]fsNode{}
	if parent == newParent {
		n, e := f.directoryImage(ctx, p.node, nextQ)
		if e != nil {
			return nil, false, e
		}
		replacements[parent] = n
	} else {
		n, e := f.directoryImage(ctx, p.node, nextP)
		if e != nil {
			return nil, false, e
		}
		replacements[parent] = n
		n, e = f.directoryImage(ctx, q.node, nextQ)
		if e != nil {
			return nil, false, e
		}
		replacements[newParent] = n
	}
	if j >= 0 && flags != FSRenameExchange {
		add(target)
		n := target.node
		if n.kind == fsDirectory {
			n.removed = true
		} else {
			if fsLinks(n) == 0 {
				return nil, false, nil
			}
			setFSLinks(&n, fsLinks(n)-1)
		}
		n.ctime = uint64(time.Now().UnixNano())
		replacements[target.id] = n
	}
	if !consistent {
		return nil, false, nil
	}
	members := make([]fsTxMember, 0, len(checks))
	for id, s := range checks {
		n, ok := replacements[id]
		if !ok {
			n = s.node
		}
		members = append(members, fsMember(s, n))
	}
	return members, false, nil
}

func (f *Filesystem) directoryPath(ctx context.Context, target ObjectID) ([]fsSnapshot, error) {
	seen := map[ObjectID]bool{}
	var walk func(ObjectID, int) ([]fsSnapshot, error)
	walk = func(id ObjectID, depth int) ([]fsSnapshot, error) {
		if depth > 1024 || seen[id] {
			return nil, ErrFSFormat
		}
		seen[id] = true
		s, err := f.snapshot(ctx, id)
		if err != nil {
			return nil, err
		}
		if s.node.kind != fsDirectory {
			return nil, nil
		}
		if s.node.removed {
			return nil, syscall.ENOENT
		}
		if id == target {
			return []fsSnapshot{s}, nil
		}
		entries, err := f.directory(ctx, s.node)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			path, e := walk(entry.ID, depth+1)
			if e != nil {
				return nil, e
			}
			if path != nil {
				return append([]fsSnapshot{s}, path...), nil
			}
		}
		return nil, nil
	}
	path, err := walk(FilesystemRootID(), 0)
	if err == nil && path == nil {
		err = syscall.ENOENT
	}
	return path, err
}
