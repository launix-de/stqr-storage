// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"math"
	"sort"
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
// global writer lock: each mutation publishes through the target inode's CAS.
// Data/index objects are immutable. Orphans (including unlinked open inodes) are
// retained until a future reachability collector; no unsafe eager GC is performed.
type Filesystem struct {
	store                FilesystemStore
	directoryMemoryLimit uint64
}
type FSOptions struct {
	Create         bool
	Mode, UID, GID uint32
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
	f := &Filesystem{store: store, directoryMemoryLimit: limit}
	_, _, err := f.load(ctx, FilesystemRootID())
	if errors.Is(err, ErrNotFound) && opt.Create {
		mode := opt.Mode
		if mode == 0 {
			mode = 0755
		}
		body, _ := encodeFSDirectory(nil)
		node := fsNode{kind: fsDirectory, mode: mode & 07777, uid: opt.UID, gid: opt.GID, mtime: uint64(time.Now().UnixNano()), blob: fsBlob{length: uint64(len(body)), inline: body}}
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
	return f, nil
}
func (f *Filesystem) load(ctx context.Context, id ObjectID) (fsNode, ObjectInfo, error) {
	data, info, err := f.store.Get(ctx, id)
	if err != nil {
		return fsNode{}, info, err
	}
	n, err := decodeFSNode(data)
	return n, info, err
}
func fsInfo(id ObjectID, n fsNode, info ObjectInfo) FSInfo {
	return FSInfo{ID: id, Version: info.Version, Size: n.blob.length, Mode: n.mode, UID: n.uid, GID: n.gid, MtimeNS: n.mtime, Directory: n.kind == fsDirectory}
}
func (f *Filesystem) Stat(ctx context.Context, id ObjectID) (FSInfo, error) {
	n, i, err := f.load(ctx, id)
	return fsInfo(id, n, i), err
}
func (f *Filesystem) mutate(ctx context.Context, id ObjectID, fn func(*fsNode) error) (FSInfo, error) {
	for attempt := 0; attempt < 64; attempt++ {
		if err := ctx.Err(); err != nil {
			return FSInfo{}, err
		}
		n, i, err := f.load(ctx, id)
		if err != nil {
			return FSInfo{}, err
		}
		n.mtime = uint64(time.Now().UnixNano())
		if err = fn(&n); err != nil {
			return FSInfo{}, err
		}
		result, err := f.store.CAS(ctx, id, 0, nil, encodeFSNode(n), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), ExpectedVersion: &i.Version}})
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return fsInfo(id, n, result), err
	}
	return FSInfo{}, syscall.EAGAIN
}
func (f *Filesystem) directory(ctx context.Context, n fsNode) ([]FSEntry, error) {
	if n.kind != fsDirectory {
		return nil, syscall.ENOTDIR
	}
	// This prototype materializes directory entries for CAS editing; their storage
	// is segmented, but directory edit RAM still scales with the entry count.
	if n.blob.length > f.directoryMemoryLimit || n.blob.length > uint64(int(^uint(0)>>1)) {
		return nil, syscall.ENOMEM
	}
	data := make([]byte, int(n.blob.length))
	if len(data) > 0 {
		if _, err := f.readBlob(ctx, n.blob, data, 0); err != nil {
			return nil, err
		}
	}
	return decodeFSDirectory(data)
}
func (f *Filesystem) ReadDir(ctx context.Context, id ObjectID) ([]FSEntry, error) {
	n, _, err := f.load(ctx, id)
	if err != nil {
		return nil, err
	}
	return f.directory(ctx, n)
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
	return f.Stat(ctx, entries[i].ID)
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
		n.blob, err = f.blobFromBytes(ctx, data)
		return err
	})
	return err
}

// Create makes a regular file or directory. It publishes the initialized inode
// through a parent CAS; failed publication can leave an unreachable object.
func (f *Filesystem) Create(ctx context.Context, parent ObjectID, name string, mode, uid, gid uint32, directory bool) (FSInfo, error) {
	if !validFSName(name) {
		return FSInfo{}, syscall.EINVAL
	}
	n := fsNode{kind: fsFile, mode: mode & 07777, uid: uid, gid: gid, mtime: uint64(time.Now().UnixNano())}
	if directory {
		n.kind = fsDirectory
		body, _ := encodeFSDirectory(nil)
		n.blob = fsBlob{length: uint64(len(body)), inline: body}
	}
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
	n, _, err := f.load(ctx, id)
	if err != nil {
		return 0, err
	}
	if n.kind != fsFile {
		return 0, syscall.EISDIR
	}
	if len(dst) == 0 {
		return 0, nil
	}
	return f.readBlob(ctx, n.blob, dst, uint64(offset))
}

// writeNode builds a new extent view; the caller publishes it by inode CAS.
func (f *Filesystem) writeNode(ctx context.Context, n *fsNode, data []byte, start uint64) error {
	if n.kind != fsFile {
		return syscall.EISDIR
	}
	if uint64(len(data)) > uint64(math.MaxInt64)-start {
		return syscall.EFBIG
	}
	if len(data) == 0 {
		return nil
	}
	end := start + uint64(len(data))
	length := max(n.blob.length, end)
	if length <= fsInlineSize {
		inline := make([]byte, int(length))
		if n.blob.length != 0 {
			if _, err := f.readBlob(ctx, n.blob, inline[:int(n.blob.length)], 0); err != nil {
				return err
			}
		}
		copy(inline[int(start):], data)
		n.blob = fsBlob{length: length, inline: inline}
		return nil
	}
	segments, err := f.segments(ctx, n.blob)
	if err != nil {
		return err
	}
	next := sliceFSSegments(segments, 0, min(start, n.blob.length))
	if start > n.blob.length {
		next = append(next, fsSegment{length: start - n.blob.length})
	}
	written, err := f.dataSegments(ctx, data)
	if err != nil {
		return err
	}
	next = append(next, written...)
	next = append(next, sliceFSSegments(segments, end, n.blob.length)...)
	n.blob, err = f.buildBlob(ctx, next, length)
	return err
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
	if n.kind != fsFile {
		return syscall.EISDIR
	}
	if length > math.MaxInt64 {
		return syscall.EFBIG
	}
	if length == n.blob.length {
		return nil
	}
	if length <= fsInlineSize {
		inline := make([]byte, int(length))
		count := min(length, n.blob.length)
		if count != 0 {
			if _, err := f.readBlob(ctx, n.blob, inline[:int(count)], 0); err != nil {
				return err
			}
		}
		n.blob = fsBlob{length: length, inline: inline}
		return nil
	}
	segments, err := f.segments(ctx, n.blob)
	if err != nil {
		return err
	}
	segments = sliceFSSegments(segments, 0, min(length, n.blob.length))
	if length > n.blob.length {
		segments = append(segments, fsSegment{length: length - n.blob.length})
	}
	n.blob, err = f.buildBlob(ctx, segments, length)
	return err
}
func (f *Filesystem) Truncate(ctx context.Context, id ObjectID, length uint64) (FSInfo, error) {
	return f.mutate(ctx, id, func(n *fsNode) error { return f.resize(ctx, n, length) })
}

// FSAttributes updates the supported attributes atomically at the inode head.
type FSAttributes struct {
	Size           *uint64
	Mode, UID, GID *uint32
	MtimeNS        *uint64
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
		return nil
	})
}

// Unlink removes a file name while retaining its inode for existing handles.
// Rmdir and cross-directory rename require an intent protocol and are not yet
// exposed: an emptiness check followed by a parent CAS would race another mount.
func (f *Filesystem) Unlink(ctx context.Context, parent ObjectID, name string) error {
	if !validFSName(name) {
		return syscall.EINVAL
	}
	return f.editDirectory(ctx, parent, func(entries []FSEntry) ([]FSEntry, error) {
		i := findFSEntry(entries, name)
		if i < 0 {
			return nil, syscall.ENOENT
		}
		n, _, err := f.load(ctx, entries[i].ID)
		if err != nil {
			return nil, err
		}
		if n.kind == fsDirectory {
			return nil, syscall.EISDIR
		}
		return append(entries[:i], entries[i+1:]...), nil
	})
}

// Rename currently supports a same-directory rename to an unused name. Other
// cases return explicit errors until replacement/intent semantics are defined.
func (f *Filesystem) Rename(ctx context.Context, parent ObjectID, name string, newParent ObjectID, newName string) error {
	if !validFSName(name) || !validFSName(newName) {
		return syscall.EINVAL
	}
	if parent != newParent {
		return syscall.EXDEV
	}
	return f.editDirectory(ctx, parent, func(entries []FSEntry) ([]FSEntry, error) {
		i := findFSEntry(entries, name)
		if i < 0 {
			return nil, syscall.ENOENT
		}
		if name == newName {
			return entries, nil
		}
		if findFSEntry(entries, newName) >= 0 {
			return nil, syscall.EEXIST
		}
		entries[i].Name = newName
		return entries, nil
	})
}

// Sync performs no additional flush: no dirty data is buffered by this frontend.
// All prior completed mutations already received the store's durable CAS ACK.
func (f *Filesystem) Sync(ctx context.Context, id ObjectID) error {
	_, err := f.Stat(ctx, id)
	return err
}
