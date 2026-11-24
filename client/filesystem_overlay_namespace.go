// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"time"
)

func (o *OverlayFilesystem) destination(ctx context.Context, s *overlayState, parent ObjectID, name string, directory bool) (string, error) {
	if !validFSName(name) {
		return "", syscall.EINVAL
	}
	if _, err := o.entries(ctx, s, parent); err != nil {
		return "", err
	}
	p, err := o.path(ctx, s, 0, parent)
	if err != nil {
		return "", err
	}
	p = overlayChildPath(p, name)
	if !overlayAllowed(s.policy, p, directory) {
		return "", syscall.EROFS
	}
	return p, nil
}
func (o *OverlayFilesystem) Create(ctx context.Context, parent ObjectID, name string, mode, uid, gid uint32, directory bool) (FSInfo, error) {
	if !validFSName(name) {
		return FSInfo{}, syscall.EINVAL
	}
	var id ObjectID
	err := o.edit(ctx, func(s *overlayState) error {
		p, e := o.destination(ctx, s, parent, name, directory)
		if e != nil {
			return e
		}
		if _, e = o.child(ctx, s, parent, name); e == nil {
			return syscall.EEXIST
		} else if !errors.Is(e, syscall.ENOENT) {
			return e
		}
		n := fsNode{kind: fsFile, mode: mode & 07777, uid: uid, gid: gid, mtime: uint64(time.Now().UnixNano()), writePath: p}
		if directory {
			n.kind = fsDirectory
			n.writePath = ""
			id = newFSID()
			s.dirs[id] = &overlayDirectory{opaque: true, metadata: true, node: n, entries: map[string]ObjectID{}}
		} else {
			if id == (ObjectID{}) {
				var e error
				id, e = o.upper.putImmutable(ctx, encodeFSNode(n))
				if e != nil {
					return e
				}
			}
		}
		dir, e := o.directory(ctx, s, parent)
		if e != nil {
			return e
		}
		dir.entries[name] = id
		dir.node.mtime = n.mtime
		dir.metadata = true
		return nil
	})
	if err != nil {
		return FSInfo{}, err
	}
	return o.Stat(ctx, id)
}

func (o *OverlayFilesystem) remove(ctx context.Context, parent ObjectID, name string, directory bool) error {
	if !validFSName(name) {
		return syscall.EINVAL
	}
	return o.edit(ctx, func(s *overlayState) error {
		id, err := o.child(ctx, s, parent, name)
		if err != nil {
			return err
		}
		info, err := o.stat(ctx, s, 0, id)
		if err != nil {
			return err
		}
		if directory && !info.Directory {
			return syscall.ENOTDIR
		}
		if !directory && info.Directory {
			return syscall.EISDIR
		}
		if _, err = o.destination(ctx, s, parent, name, directory); err != nil {
			return err
		}
		if directory {
			entries, e := o.entries(ctx, s, id)
			if e != nil {
				return e
			}
			if len(entries) != 0 {
				return syscall.ENOTEMPTY
			}
			child, e := o.directory(ctx, s, id)
			if e != nil {
				return e
			}
			child.node.removed = true
			child.metadata = true
		}
		dir, err := o.directory(ctx, s, parent)
		if err != nil {
			return err
		}
		dir.entries[name] = ObjectID{}
		dir.node.mtime = uint64(time.Now().UnixNano())
		dir.metadata = true
		return nil
	})
}
func (o *OverlayFilesystem) Unlink(ctx context.Context, parent ObjectID, name string) error {
	return o.remove(ctx, parent, name, false)
}
func (o *OverlayFilesystem) Rmdir(ctx context.Context, parent ObjectID, name string) error {
	return o.remove(ctx, parent, name, true)
}
func (o *OverlayFilesystem) Rename(ctx context.Context, parent ObjectID, name string, newParent ObjectID, newName string) error {
	return o.RenameWithFlags(ctx, parent, name, newParent, newName, 0)
}
func (o *OverlayFilesystem) RenameWithFlags(ctx context.Context, parent ObjectID, name string, newParent ObjectID, newName string, flags uint32) error {
	if !validFSName(name) || !validFSName(newName) {
		return syscall.EINVAL
	}
	if flags&^(FSRenameNoReplace|FSRenameExchange) != 0 {
		return syscall.ENOTSUP
	}
	if flags == (FSRenameNoReplace | FSRenameExchange) {
		return syscall.EINVAL
	}
	return o.edit(ctx, func(s *overlayState) error {
		id, err := o.child(ctx, s, parent, name)
		if err != nil {
			return err
		}
		source, err := o.stat(ctx, s, 0, id)
		if err != nil {
			return err
		}
		oldPath, err := o.destination(ctx, s, parent, name, source.Directory)
		if err != nil {
			return err
		}
		newPath, err := o.destination(ctx, s, newParent, newName, source.Directory)
		if err != nil {
			return err
		}
		other, existsErr := o.child(ctx, s, newParent, newName)
		exists := existsErr == nil
		if existsErr != nil && !errors.Is(existsErr, syscall.ENOENT) {
			return existsErr
		}
		if flags == FSRenameNoReplace && exists {
			return syscall.EEXIST
		}
		if flags == FSRenameExchange && !exists {
			return syscall.ENOENT
		}
		if parent == newParent && name == newName {
			return errOverlayUnchanged
		}
		if source.Directory && (newPath == oldPath || strings.HasPrefix(newPath, oldPath+"/")) {
			return syscall.EINVAL
		}
		var target FSInfo
		if exists {
			target, err = o.stat(ctx, s, 0, other)
			if err != nil {
				return err
			}
			if flags == FSRenameExchange {
				// Both names must be writable for the actual incoming node type.
				if _, err = o.destination(ctx, s, parent, name, target.Directory); err != nil {
					return err
				}
				if target.Directory && strings.HasPrefix(oldPath, newPath+"/") {
					return syscall.EINVAL
				}
			} else {
				if source.Directory && !target.Directory {
					return syscall.ENOTDIR
				}
				if !source.Directory && target.Directory {
					return syscall.EISDIR
				}
				if target.Directory {
					children, e := o.entries(ctx, s, other)
					if e != nil {
						return e
					}
					if len(children) != 0 {
						return syscall.ENOTEMPTY
					}
				}
			}
		}
		// Lower file moves must get a stable upper inode first, otherwise the
		// lower name disappearing later would also lose the moved content.
		for _, item := range []struct {
			id   ObjectID
			info FSInfo
			p    string
		}{{id, source, oldPath}, {other, target, newPath}} {
			if item.id == (ObjectID{}) {
				continue
			}
			if item.info.Directory {
				if _, err = o.directory(ctx, s, item.id); err != nil {
					return err
				}
				continue
			}
			if _, _, e := o.upper.store.Get(ctx, item.id); errors.Is(e, ErrNotFound) {
				n, base, e := o.lower.load(ctx, item.id)
				if e != nil {
					return e
				}
				if n.kind == fsFile {
					if fsLinks(n) > 1 {
						return syscall.EOPNOTSUPP
					}
					n.blob = fsBlob{length: n.blob.length, source: item.id, sourceVersion: base.Version}
				}
				n.writePath = item.p
				_, e = o.upper.store.CAS(ctx, item.id, 0, nil, encodeFSNode(n), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
				if e != nil && !errors.Is(e, ErrPrecondition) {
					return e
				}
			} else if e != nil {
				return e
			}
		}
		left, err := o.directory(ctx, s, parent)
		if err != nil {
			return err
		}
		right, err := o.directory(ctx, s, newParent)
		if err != nil {
			return err
		}
		left.entries[name] = ObjectID{}
		right.entries[newName] = id
		if flags == FSRenameExchange {
			left.entries[name] = other
		} else if exists && target.Directory {
			dir, err := o.directory(ctx, s, other)
			if err != nil {
				return err
			}
			dir.node.removed = true
			dir.metadata = true
		}
		now := uint64(time.Now().UnixNano())
		left.metadata, right.metadata = true, true
		left.node.mtime = now
		right.node.mtime = now
		return nil
	})
}
