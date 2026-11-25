// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"sort"
	"sync"
	"syscall"
	"time"
)

// FilesystemStore is the native object subset needed by the filesystem. Every
// successful CAS must already meet the backend's durability contract. Immutable
// objects and mutable inode heads are both ordinary bucket objects.
type FilesystemStore interface {
	Get(context.Context, ObjectID) ([]byte, ObjectInfo, error)
	CAS(context.Context, ObjectID, uint64, []byte, []byte, CASOptions) (ObjectInfo, error)
}

var _ FilesystemStore = (*Client)(nil)

// Filesystem is a bucket-local filesystem with a fixed root inode. It has no
// global writer lock: inode writes use CAS, namespace transactions use durable
// per-inode intents. Mutable data/index blocks keep their identities; replaced
// blocks and intents are retired after publication on reference-capable stores.
// Cooperative opens pin unlinked inodes until last close; crashed holders and
// legacy shared/orphan objects require exclusive offline fsck.
type Filesystem struct {
	orphanMu             sync.Mutex
	orphanRunning        bool
	orphanCollector      func(context.Context) error
	directoryCache       *filesystemDirectoryCache
	store                FilesystemStore
	directoryMemoryLimit uint64
	readOnly             bool
	lower                *Filesystem
}
type FSOptions struct {
	Create         bool
	Mode, UID, GID uint32
	ReadOnly       bool
	// DirectoryMemoryLimit bounds materialized directory content. Zero selects
	// 64 MiB. Storage remains segmented; larger directories need a future
	// incremental directory editor or an explicitly increased memory budget.
	DirectoryMemoryLimit uint64
}

// OpenFilesystem opens a root, creating it only when explicitly requested.
// Existing malformed roots are never reformatted.
func OpenFilesystem(ctx context.Context, store FilesystemStore, opt FSOptions) (*Filesystem, error) {
	if store == nil {
		return nil, syscall.EINVAL
	}
	limit := opt.DirectoryMemoryLimit
	if limit == 0 {
		limit = 64 << 20
	}
	if opt.Create && opt.ReadOnly {
		return nil, syscall.EROFS
	}
	f := &Filesystem{directoryCache: newFilesystemDirectoryCache(), store: store, directoryMemoryLimit: limit, readOnly: opt.ReadOnly}
	if raw, _, err := store.Get(ctx, FilesystemRootID()); err == nil && len(raw) >= 4 && string(raw[:4]) == "STOR" {
		return nil, ErrOverlayNeedsLower
	}
	_, _, err := f.load(ctx, FilesystemRootID())
	if errors.Is(err, ErrNotFound) && opt.Create {
		mode := opt.Mode
		if mode == 0 {
			mode = 0755
		}
		body, _ := encodeFSDirectory(nil)
		node := fsNode{extended: true, links: 2, kind: fsDirectory, mode: mode & 07777, uid: opt.UID, gid: opt.GID, mtime: uint64(time.Now().UnixNano()), blob: fsBlob{length: uint64(len(body)), inline: body}}
		node.atime = node.mtime
		node.ctime = node.mtime
		_, err = store.CAS(ctx, FilesystemRootID(), 0, nil, encodeFSNode(node), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
		if err != nil && !errors.Is(err, ErrPrecondition) {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	n, _, err := f.load(ctx, FilesystemRootID())
	if err != nil {
		return nil, err
	}
	if n.kind != fsDirectory {
		return nil, ErrFSFormat
	}
	if _, managed := f.managedReferences(); managed {
		f.scheduleOrphanCleanup()
	}
	return f, nil
}
func (f *Filesystem) load(ctx context.Context, id ObjectID) (fsNode, ObjectInfo, error) {
	if f.readOnly {
		return f.readLockedSnapshot(ctx, id)
	}
	s, err := f.snapshot(ctx, id)
	if errors.Is(err, ErrUnauthorized) {
		return f.readLockedSnapshot(ctx, id)
	}
	return s.node, s.info, err
}
func fsInfo(id ObjectID, n fsNode, info ObjectInfo) FSInfo {
	return FSInfo{Kind: FSKind(n.kind), Nlink: fsLinks(n), ID: id, Proof: info.Proof, Size: n.blob.length, Mode: n.mode, UID: n.uid, GID: n.gid, MtimeNS: n.mtime, AtimeNS: n.atime, CtimeNS: n.ctime, Directory: n.kind == fsDirectory, Removed: n.removed}
}
func (f *Filesystem) Stat(ctx context.Context, id ObjectID) (FSInfo, error) {
	n, i, err := f.load(ctx, id)
	return fsInfo(id, n, i), err
}
func (f *Filesystem) mutate(ctx context.Context, id ObjectID, fn func(*fsNode) error) (FSInfo, error) {
	if f.readOnly {
		return FSInfo{}, syscall.EROFS
	}
	for attempt := 0; attempt < 64; attempt++ {
		if err := ctx.Err(); err != nil {
			return FSInfo{}, err
		}
		s, err := f.snapshot(ctx, id)
		if err != nil {
			return FSInfo{}, err
		}
		n := s.node
		if n.blob.source != (ObjectID{}) {
			if f.lower == nil {
				return FSInfo{}, ErrOverlayNeedsLower
			}
			lower, _, e := f.lower.load(ctx, n.blob.source)
			if e != nil {
				return FSInfo{}, e
			}
			if lower.kind != fsFile || lower.blob.length < n.blob.length {
				return FSInfo{}, ErrFSFormat
			}
			priorLength := n.blob.length
			n.blob = lower.blob
			n.blob.length = priorLength
			n.blob.source = ObjectID{}
			n.blob.legacyVersion = 0
			n.blob.lower = n.blob.root != (ObjectID{})
		}
		n.ctime = uint64(time.Now().UnixNano())
		if err = fn(&n); err != nil {
			if errors.Is(err, ErrPrecondition) || errors.Is(f.checkNodeView(ctx, s.node), ErrPrecondition) {
				continue
			}
			return FSInfo{}, err
		}
		_, managed := f.managedReferences()
		if len(n.edits) > 0 || managed {
			committed, e := f.transact(ctx, []fsTxMember{fsMember(s, n)})
			if e != nil {
				return FSInfo{}, e
			}
			if !committed {
				continue
			}
			return f.Stat(ctx, id)
		}
		result, err := f.store.CAS(ctx, id, 0, s.raw, encodeFSNode(n), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return fsInfo(id, n, result), err
	}
	return FSInfo{}, syscall.EAGAIN
}
func (f *Filesystem) directory(ctx context.Context, n fsNode) ([]FSEntry, error) {
	entries, err := f.directoryView(ctx, n)
	return append([]FSEntry(nil), entries...), err
}
func (f *Filesystem) directoryView(ctx context.Context, n fsNode) ([]FSEntry, error) {
	for attempt := 0; attempt < 128; attempt++ {
		entries, err := f.readDirectoryView(ctx, n)
		if !errors.Is(err, ErrPrecondition) {
			return entries, err
		}
		if n.identity == (ObjectID{}) {
			return nil, err
		}
		n, _, err = f.load(ctx, n.identity)
		if err != nil {
			return nil, err
		}
	}
	return nil, syscall.EAGAIN
}
func (f *Filesystem) readDirectoryView(ctx context.Context, n fsNode) ([]FSEntry, error) {
	if n.kind != fsDirectory {
		return nil, syscall.ENOTDIR
	}
	if n.removed {
		return nil, syscall.ENOENT
	}
	// This prototype materializes directory entries for CAS editing; their storage
	// is segmented, but directory edit RAM still scales with the entry count.
	if n.blob.length > f.directoryMemoryLimit || n.blob.length > uint64(int(^uint(0)>>1)) {
		return nil, syscall.ENOMEM
	}
	key, cacheable := directoryKey(n.blob)
	if cacheable {
		if entries, ok := f.directoryCache.get(key); ok {
			if err := f.checkNodeView(ctx, n); err != nil {
				return nil, err
			}
			return entries, nil
		}
	}
	data := make([]byte, int(n.blob.length))
	if len(data) > 0 {
		if _, err := f.readBlob(ctx, n.blob, data, 0); err != nil {
			return nil, err
		}
	}
	entries, err := decodeFSDirectory(data)
	if e := f.checkNodeView(ctx, n); e != nil {
		return nil, e
	}
	if err == nil && cacheable {
		f.directoryCache.put(key, entries)
	}
	return entries, err
}
func (f *Filesystem) ReadDir(ctx context.Context, id ObjectID) ([]FSEntry, error) {
	for attempt := 0; attempt < 128; attempt++ {
		n, _, err := f.load(ctx, id)
		if err != nil {
			return nil, err
		}
		out, err := f.directory(ctx, n)
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return out, err
	}
	return nil, syscall.EAGAIN
}
func findFSEntry(entries []FSEntry, name string) int {
	i := sort.Search(len(entries), func(i int) bool { return entries[i].Name >= name })
	if i == len(entries) || entries[i].Name != name {
		return -1
	}
	return i
}
func (f *Filesystem) Lookup(ctx context.Context, parent ObjectID, name string) (FSInfo, error) {
	if !validFSName(name) {
		return FSInfo{}, syscall.EINVAL
	}
	entries, err := f.ReadDir(ctx, parent)
	if err != nil {
		return FSInfo{}, err
	}
	i := findFSEntry(entries, name)
	if i < 0 {
		return FSInfo{}, syscall.ENOENT
	}
	info, err := f.Stat(ctx, entries[i].ID)
	if err == nil && info.Removed {
		err = syscall.ENOENT
	}
	return info, err
}
func (f *Filesystem) editDirectory(ctx context.Context, id ObjectID, fn func([]FSEntry) ([]FSEntry, error)) error {
	_, err := f.mutate(ctx, id, func(n *fsNode) error {
		entries, err := f.directory(ctx, *n)
		if err != nil {
			return err
		}
		entries, err = fn(entries)
		if err != nil {
			return err
		}
		data, err := encodeFSDirectory(entries)
		if err != nil {
			return err
		}
		if uint64(len(data)) > f.directoryMemoryLimit {
			return syscall.ENOMEM
		}
		if err = f.directoryLinks(ctx, n, entries); err != nil {
			return err
		}
		err = f.stableBody(ctx, n, data)
		n.mtime = n.ctime
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return syscall.ENOENT
	}
	return err
}

// Create makes a regular file or directory. It publishes the initialized inode
// through a parent CAS; failed publication can leave an unreachable object.
func (f *Filesystem) Create(ctx context.Context, parent ObjectID, name string, mode, uid, gid uint32, directory bool) (FSInfo, error) {
	if f.readOnly {
		return FSInfo{}, syscall.EROFS
	}
	if !validFSName(name) {
		return FSInfo{}, syscall.EINVAL
	}
	kind := uint8(fsFile)
	if directory {
		kind = fsDirectory
	}
	return f.createNode(ctx, parent, name, mode, uid, gid, kind, nil)
}
func (f *Filesystem) createNode(ctx context.Context, parent ObjectID, name string, mode, uid, gid uint32, kind uint8, content []byte) (FSInfo, error) {
	if f.readOnly {
		return FSInfo{}, syscall.EROFS
	}
	if !validFSName(name) {
		return FSInfo{}, syscall.EINVAL
	}
	n := fsNode{kind: kind, mode: mode & 07777, uid: uid, gid: gid, mtime: uint64(time.Now().UnixNano())}
	n.atime = n.mtime
	n.ctime = n.mtime
	if kind == fsDirectory {
		n.extended = true
		n.links = 2
		content, _ = encodeFSDirectory(nil)
	}
	if kind > fsDirectory {
		n.extended = true
		n.links = 1
	}
	n.blob = fsBlob{length: uint64(len(content)), inline: append([]byte(nil), content...)}
	id, err := f.putImmutable(ctx, encodeFSNode(n))
	if err != nil {
		return FSInfo{}, err
	}
	err = f.editDirectory(ctx, parent, func(entries []FSEntry) ([]FSEntry, error) {
		if findFSEntry(entries, name) >= 0 {
			return nil, syscall.EEXIST
		}
		return append(entries, FSEntry{Name: name, ID: id}), nil
	})
	if err != nil {
		return FSInfo{}, err
	}
	return f.Stat(ctx, id)
}
func (f *Filesystem) ReadAt(ctx context.Context, id ObjectID, dst []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	if len(dst) == 0 {
		return 0, nil
	}
	for attempt := 0; attempt < 128; attempt++ {
		n, _, err := f.load(ctx, id)
		if err != nil {
			return 0, err
		}
		if n.kind != fsFile {
			return 0, syscall.EISDIR
		}
		count, readErr := f.readBlob(ctx, n.blob, dst, uint64(offset))
		if err = f.checkNodeView(ctx, n); errors.Is(err, ErrPrecondition) {
			continue
		} else if err != nil {
			return 0, err
		}
		return count, readErr
	}
	return 0, syscall.EAGAIN
}

// writeNode builds a new extent view; the caller publishes it by inode CAS.
func (f *Filesystem) writeNode(ctx context.Context, n *fsNode, data []byte, start uint64) error {
	return f.stableWriteNode(ctx, n, data, start)
}
func (f *Filesystem) WriteAt(ctx context.Context, id ObjectID, data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	_, err := f.mutate(ctx, id, func(n *fsNode) error { return f.writeNode(ctx, n, data, uint64(offset)) })
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// Append chooses EOF inside each CAS attempt, so simultaneous O_APPEND writers
// on independent mounts cannot overwrite one another at a stale EOF.
func (f *Filesystem) Append(ctx context.Context, id ObjectID, data []byte) (int, error) {
	_, err := f.mutate(ctx, id, func(n *fsNode) error { return f.writeNode(ctx, n, data, n.blob.length) })
	if err != nil {
		return 0, err
	}
	return len(data), nil
}
func (f *Filesystem) resize(ctx context.Context, n *fsNode, length uint64) error {
	return f.stableResize(ctx, n, length)
}
func (f *Filesystem) Truncate(ctx context.Context, id ObjectID, length uint64) (FSInfo, error) {
	return f.mutate(ctx, id, func(n *fsNode) error { return f.resize(ctx, n, length) })
}

// FSAttributes updates the supported attributes atomically at the inode head.
type FSAttributes struct {
	Size           *uint64
	Mode, UID, GID *uint32
	MtimeNS        *uint64
	AtimeNS        *uint64
}

func (f *Filesystem) SetAttributes(ctx context.Context, id ObjectID, a FSAttributes) (FSInfo, error) {
	return f.mutate(ctx, id, func(n *fsNode) error {
		if a.Size != nil {
			if err := f.resize(ctx, n, *a.Size); err != nil {
				return err
			}
		}
		if a.Mode != nil {
			n.mode = *a.Mode & 07777
		}
		if a.UID != nil {
			n.uid = *a.UID
		}
		if a.GID != nil {
			n.gid = *a.GID
		}
		if a.MtimeNS != nil {
			n.mtime = *a.MtimeNS
		}
		if a.AtimeNS != nil {
			n.atime = *a.AtimeNS
		}
		return nil
	})
}

// Unlink removes a file name while retaining its inode for existing handles.
// Removed files remain readable through open handles until offline collection.
func (f *Filesystem) Unlink(ctx context.Context, parent ObjectID, name string) error {
	return f.unlinkNode(ctx, parent, name)
}

// Rename atomically moves a name, replacing a compatible destination if present.
func (f *Filesystem) Rename(ctx context.Context, parent ObjectID, name string, newParent ObjectID, newName string) error {
	return f.RenameWithFlags(ctx, parent, name, newParent, newName, 0)
}

// Sync checkpoints all observed writes, including dependent immutable objects.
// Stores without deferred durability already give durable mutation ACKs.
func (f *Filesystem) Sync(ctx context.Context, id ObjectID) error {
	if _, err := f.Stat(ctx, id); err != nil {
		return err
	}
	if s, ok := f.store.(interface{ Sync(context.Context) error }); ok {
		return s.Sync(ctx)
	}
	return nil
}

func (f *Filesystem) CheckWrite(ctx context.Context, id ObjectID) error {
	if f.readOnly {
		return syscall.EROFS
	}
	return ctx.Err()
}
