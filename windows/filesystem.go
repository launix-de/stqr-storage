// SPDX-License-Identifier: GPL-3.0-only
// Package winfs shares STQR's inode format with the Linux filesystem clients.
package winfs

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/launix-de/stqr-storage/client"
)

type OpenOptions struct{ Read, Write, Create, Exclusive, Truncate, Append, Directory bool }
type Entry struct {
	Name string
	Info client.FSInfo
}
type handle struct {
	path    string
	id      client.ObjectID
	options OpenOptions
	entries []Entry
	active  *sync.RWMutex
	token   client.RequestID
}

// FilesystemView keeps ordinary and native-overlay mounts on the shared FS core.
type FilesystemView interface {
	client.FilesystemView
	CollectOrphans(context.Context) error
	Space(context.Context) (client.BucketSpace, error)
	Retain(context.Context, client.ObjectID) (client.RequestID, error)
	Release(context.Context, client.ObjectID, client.RequestID) error
}

type Filesystem struct {
	Context  context.Context
	Volume   FilesystemView
	ReadOnly bool
	// Owner for newly created POSIX inodes; the CLI inherits the bucket root.
	UID, GID    uint32
	mu          sync.Mutex
	next        uint64
	handles     map[uint64]handle
	directories map[string][]Entry
}

func New(ctx context.Context, f FilesystemView, readonly bool) *Filesystem {
	return &Filesystem{Context: ctx, Volume: f, ReadOnly: readonly, handles: map[uint64]handle{}, directories: map[string][]Entry{}}
}
func clean(p string) (string, error) {
	if strings.ContainsAny(p, "\x00\\") || !strings.HasPrefix(p, "/") {
		return "", syscall.EINVAL
	}
	return path.Clean(p), nil
}
func (f *Filesystem) Lookup(p string) (client.FSInfo, error) {
	p, e := clean(p)
	if e != nil {
		return client.FSInfo{}, e
	}
	info, e := f.Volume.Stat(f.Context, client.FilesystemRootID())
	if e != nil {
		return info, e
	}
	if p == "/" {
		return info, nil
	}
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		info, e = f.Volume.Lookup(f.Context, info.ID, name)
		if e != nil {
			return info, e
		}
	}
	return info, nil
}
func (f *Filesystem) parent(p string) (client.ObjectID, string, error) {
	p, e := clean(p)
	if e != nil {
		return client.ObjectID{}, "", e
	}
	if p == "/" {
		return client.ObjectID{}, "", syscall.EBUSY
	}
	i, e := f.Lookup(path.Dir(p))
	if e != nil {
		return i.ID, "", e
	}
	if !i.Directory {
		return i.ID, "", syscall.ENOTDIR
	}
	return i.ID, path.Base(p), nil
}
func (f *Filesystem) writable() error {
	if f.ReadOnly {
		return syscall.EROFS
	}
	return nil
}

func (f *Filesystem) creationOwner(parent client.ObjectID, mode uint32, directory bool) (uint32, uint32, error) {
	info, err := f.Volume.Stat(f.Context, parent)
	if err != nil {
		return 0, 0, err
	}
	gid := f.GID
	if info.Mode&syscall.S_ISGID != 0 {
		gid = info.GID
		if directory {
			mode |= syscall.S_ISGID
		}
	}
	return gid, mode, nil
}
func (f *Filesystem) Open(p string, o OpenOptions, mode uint32) (uint64, error) {
	if o.Write || o.Create || o.Truncate {
		if e := f.writable(); e != nil {
			return 0, e
		}
	}
	if o.Truncate && !o.Write {
		return 0, syscall.EACCES
	}
	info, e := f.Lookup(p)
	if o.Create && errors.Is(e, syscall.ENOENT) || o.Create && errors.Is(e, client.ErrNotFound) {
		parent, name, err := f.parent(p)
		if err != nil {
			return 0, err
		}
		gid, inheritedMode, err := f.creationOwner(parent, mode, false)
		if err != nil {
			return 0, err
		}
		info, e = f.Volume.Create(f.Context, parent, name, inheritedMode, f.UID, gid, false)
		if errors.Is(e, syscall.EEXIST) && !o.Exclusive {
			info, e = f.Lookup(p)
		}
	} else if o.Create && o.Exclusive && e == nil {
		return 0, syscall.EEXIST
	}
	if e != nil {
		return 0, e
	}
	if o.Directory && !info.Directory {
		return 0, syscall.ENOTDIR
	}
	if !o.Directory && info.Directory {
		return 0, syscall.EISDIR
	}
	if !info.Directory && info.Kind != client.FSRegular {
		return 0, syscall.EOPNOTSUPP
	}
	token, e := f.Volume.Retain(f.Context, info.ID)
	if e != nil {
		return 0, e
	}
	registered := false
	defer func() {
		if !registered {
			_ = f.Volume.Release(f.Context, info.ID, token)
		}
	}()
	if o.Write {
		if e = f.Volume.PrepareWrite(f.Context, info.ID); e != nil {
			return 0, e
		}
	}
	if o.Truncate {
		if !o.Write {
			return 0, syscall.EACCES
		}
		if _, e = f.Volume.Truncate(f.Context, info.ID, 0); e != nil {
			return 0, e
		}
	}
	h := handle{path: path.Clean(p), id: info.ID, options: o, active: &sync.RWMutex{}, token: token}
	if o.Directory {
		h.entries = []Entry{{".", info}}
		parent, err := f.Lookup(path.Dir(p))
		if err != nil {
			return 0, err
		}
		h.entries = append(h.entries, Entry{"..", parent})
		entries, err := f.Volume.ReadDir(f.Context, info.ID)
		if err != nil {
			return 0, err
		}
		for _, entry := range entries {
			i, err := f.Volume.Stat(f.Context, entry.ID)
			if errors.Is(err, client.ErrNotFound) || errors.Is(err, syscall.ENOENT) {
				continue
			}
			if err != nil {
				return 0, err
			}
			h.entries = append(h.entries, Entry{entry.Name, i})
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	f.handles[f.next] = h
	if o.Directory {
		f.rememberDirectoryLocked(h.path, h.entries)
	}
	registered = true
	return f.next, nil
}
func (f *Filesystem) get(h uint64) (handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.handles[h]
	if !ok {
		return v, syscall.EBADF
	}
	return v, nil
}

// acquire keeps a registered handle alive for an operation. Close first removes
// it from the table, then waits for these readers before releasing its inode.
func (f *Filesystem) acquire(h uint64) (handle, func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.handles[h]
	if !ok {
		return handle{}, nil, syscall.EBADF
	}
	v.active.RLock()
	return v, v.active.RUnlock, nil
}

func (f *Filesystem) Close(h uint64) error {
	f.mu.Lock()
	v, ok := f.handles[h]
	if !ok {
		f.mu.Unlock()
		return syscall.EBADF
	}
	delete(f.handles, h)
	f.mu.Unlock()
	v.active.Lock()
	defer v.active.Unlock()
	return f.Volume.Release(f.Context, v.id, v.token)
}
func (f *Filesystem) Stat(p string, h uint64) (client.FSInfo, error) {
	if h == 0 || h == ^uint64(0) {
		return f.Lookup(p)
	}
	v, release, e := f.acquire(h)
	if e != nil {
		return client.FSInfo{}, e
	}
	defer release()
	return f.Volume.Stat(f.Context, v.id)
}
func (f *Filesystem) Read(h uint64, data []byte, off int64) (int, error) {
	v, release, e := f.acquire(h)
	if e != nil {
		return 0, e
	}
	defer release()
	if !v.options.Read {
		return 0, syscall.EBADF
	}
	return f.Volume.ReadAt(f.Context, v.id, data, off)
}
func (f *Filesystem) Write(h uint64, data []byte, off int64) (int, error) {
	if e := f.writable(); e != nil {
		return 0, e
	}
	v, release, e := f.acquire(h)
	if e != nil {
		return 0, e
	}
	defer release()
	if !v.options.Write {
		return 0, syscall.EBADF
	}
	if v.options.Append {
		return f.Volume.Append(f.Context, v.id, data)
	}
	return f.Volume.WriteAt(f.Context, v.id, data, off)
}
func (f *Filesystem) Setattr(p string, h uint64, a client.FSAttributes) error {
	if e := f.writable(); e != nil {
		return e
	}
	if h != 0 && h != ^uint64(0) {
		v, release, e := f.acquire(h)
		if e != nil {
			return e
		}
		defer release()
		if a.Size != nil && !v.options.Write {
			return syscall.EBADF
		}
		_, e = f.Volume.SetAttributes(f.Context, v.id, a)
		return e
	}
	info, e := f.Stat(p, h)
	if e != nil {
		return e
	}
	_, e = f.Volume.SetAttributes(f.Context, info.ID, a)
	return e
}
func (f *Filesystem) Entries(h uint64) ([]Entry, error) {
	v, release, e := f.acquire(h)
	if e != nil {
		return nil, e
	}
	defer release()
	if !v.options.Directory {
		return nil, syscall.ENOTDIR
	}
	return v.entries, nil
}
func (f *Filesystem) Sync(p string, h uint64) error {
	if h != 0 && h != ^uint64(0) {
		v, release, e := f.acquire(h)
		if e != nil {
			return e
		}
		defer release()
		return f.Volume.Sync(f.Context, v.id)
	}
	info, e := f.Stat(p, h)
	if e != nil {
		return e
	}
	return f.Volume.Sync(f.Context, info.ID)
}
func (f *Filesystem) Mkdir(p string, mode uint32) error {
	if e := f.writable(); e != nil {
		return e
	}
	parent, name, e := f.parent(p)
	if e != nil {
		return e
	}
	gid, mode, e := f.creationOwner(parent, mode, true)
	if e != nil {
		return e
	}
	_, e = f.Volume.Create(f.Context, parent, name, mode, f.UID, gid, true)
	return e
}
func (f *Filesystem) Remove(p string, directory bool) error {
	if e := f.writable(); e != nil {
		return e
	}
	parent, name, e := f.parent(p)
	if e != nil {
		return e
	}
	if directory {
		return f.Volume.Rmdir(f.Context, parent, name)
	}
	return f.Volume.Unlink(f.Context, parent, name)
}
func (f *Filesystem) Rename(from, to string, flags uint32) error {
	if e := f.writable(); e != nil {
		return e
	}
	p, n, e := f.parent(from)
	if e != nil {
		return e
	}
	q, m, e := f.parent(to)
	if e != nil {
		return e
	}
	if e = f.Volume.RenameWithFlags(f.Context, p, n, q, m, flags); e != nil {
		return e
	}
	from, to = path.Clean(from), path.Clean(to)
	remap := func(current string) string {
		if current == from || strings.HasPrefix(current, from+"/") {
			return to + strings.TrimPrefix(current, from)
		}
		if flags&2 != 0 && (current == to || strings.HasPrefix(current, to+"/")) {
			return from + strings.TrimPrefix(current, to)
		}
		return current
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for h, current := range f.handles {
		current.path = remap(current.path)
		f.handles[h] = current
	}
	// Keep the old directory views until Changes diffs their parents, but move
	// enumerations within the renamed subtree to their new path.
	dirs := make(map[string][]Entry, len(f.directories))
	for p, entries := range f.directories {
		dirs[remap(p)] = entries
	}
	f.directories = dirs
	return nil
}
func (f *Filesystem) Link(from, to string) error {
	if e := f.writable(); e != nil {
		return e
	}
	info, e := f.Lookup(from)
	if e != nil {
		return e
	}
	parent, name, e := f.parent(to)
	if e != nil {
		return e
	}
	links, ok := f.Volume.(interface {
		Link(context.Context, client.ObjectID, string, client.ObjectID) (client.FSInfo, error)
	})
	if !ok {
		return syscall.EOPNOTSUPP
	}
	_, e = links.Link(f.Context, parent, name, info.ID)
	return e
}
func (f *Filesystem) Symlink(target, p string) error {
	if e := f.writable(); e != nil {
		return e
	}
	parent, name, e := f.parent(p)
	if e != nil {
		return e
	}
	gid, _, e := f.creationOwner(parent, 0777, false)
	if e != nil {
		return e
	}
	links, ok := f.Volume.(interface {
		Symlink(context.Context, client.ObjectID, string, string, uint32, uint32) (client.FSInfo, error)
	})
	if !ok {
		return syscall.EOPNOTSUPP
	}
	_, e = links.Symlink(f.Context, parent, name, target, f.UID, gid)
	return e
}
