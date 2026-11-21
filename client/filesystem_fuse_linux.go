// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package client

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// FSNode is the Linux FUSE adapter. It also allows direct callback tests without
// /dev/fuse. A mount-local table assigns stable inode numbers to complete OIDs;
// OIDs are never truncated to 64 bits to synthesize kernel inode numbers.
type FSNode struct {
	fusefs.Inode
	filesystem *Filesystem
	numbers    *fsInodeNumbers
	id         ObjectID
}

type fsInodeNumbers struct {
	mu   sync.Mutex
	next uint64
	ids  map[ObjectID]uint64
}

func (s *fsInodeNumbers) number(id ObjectID) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ino, ok := s.ids[id]; ok {
		return ino
	}
	ino := s.next
	s.next++
	s.ids[id] = ino
	return ino
}
func (f *Filesystem) FUSERoot() *FSNode {
	return &FSNode{filesystem: f, id: FilesystemRootID(), numbers: &fsInodeNumbers{next: 2, ids: map[ObjectID]uint64{FilesystemRootID(): 1}}}
}

type fsOpenFile struct{ append bool }

// Mount serves this filesystem through go-fuse. No daemon/CLI is started.
// The dry memory backend loses all data when its process ends. Other backends
// must honor FilesystemStore's synchronous commit contract.
func (f *Filesystem) Mount(mountpoint string, debug bool) (*fuse.Server, error) {
	zero := time.Duration(0)
	return fusefs.Mount(mountpoint, f.FUSERoot(), &fusefs.Options{
		MountOptions: fuse.MountOptions{FsName: "stqr", Name: "stqr", Debug: debug, Options: []string{"default_permissions"}},
		EntryTimeout: &zero, AttrTimeout: &zero, NegativeTimeout: &zero, NullPermissions: true,
	})
}
func fsErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, ErrPrecondition):
		return syscall.EAGAIN
	case errors.Is(err, ErrFull):
		return syscall.ENOSPC
	case errors.Is(err, ErrWouldBlock):
		return syscall.EAGAIN
	case errors.Is(err, ErrUnauthorized):
		return syscall.EACCES
	case errors.Is(err, ErrRange):
		return syscall.EINVAL
	case errors.Is(err, ErrNotImplemented):
		return syscall.ENOSYS
	case errors.Is(err, context.Canceled):
		return syscall.EINTR
	case errors.Is(err, context.DeadlineExceeded):
		return syscall.ETIMEDOUT
	}
	return syscall.EIO
}
func fsMode(info FSInfo) uint32 {
	if info.Directory {
		return syscall.S_IFDIR | info.Mode
	}
	return syscall.S_IFREG | info.Mode
}
func fillFSAttr(out *fuse.Attr, info FSInfo) {
	out.Mode = fsMode(info)
	out.Size = info.Size
	out.Owner = fuse.Owner{Uid: info.UID, Gid: info.GID}
	out.Mtime = info.MtimeNS / 1e9
	out.Mtimensec = uint32(info.MtimeNS % 1e9)
	out.Ctime = out.Mtime
	out.Ctimensec = out.Mtimensec
	out.Nlink = 1
	if info.Directory {
		out.Nlink = 2
	}
	out.Blksize = 4096
}
func (n *FSNode) child(ctx context.Context, info FSInfo, out *fuse.EntryOut) *fusefs.Inode {
	fillFSAttr(&out.Attr, info)
	return n.NewInode(ctx, &FSNode{filesystem: n.filesystem, id: info.ID, numbers: n.numbers}, fusefs.StableAttr{Mode: fsMode(info) & syscall.S_IFMT, Ino: n.numbers.number(info.ID)})
}
func (n *FSNode) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	info, err := n.filesystem.Stat(ctx, n.id)
	if err != nil {
		return fsErrno(err)
	}
	fillFSAttr(&out.Attr, info)
	return 0
}
func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	info, err := n.filesystem.Lookup(ctx, n.id, name)
	if err != nil {
		return nil, fsErrno(err)
	}
	return n.child(ctx, info, out), 0
}
func (n *FSNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	entries, err := n.filesystem.ReadDir(ctx, n.id)
	if err != nil {
		return nil, fsErrno(err)
	}
	list := make([]fuse.DirEntry, 0, len(entries))
	for _, entry := range entries {
		info, err := n.filesystem.Stat(ctx, entry.ID)
		if err != nil {
			return nil, fsErrno(err)
		}
		list = append(list, fuse.DirEntry{Name: entry.Name, Mode: fsMode(info)})
	}
	return fusefs.NewListDirStream(list), 0
}
func fsCaller(ctx context.Context) (uint32, uint32) {
	if c, ok := fuse.FromContext(ctx); ok {
		return c.Uid, c.Gid
	}
	return uint32(os.Getuid()), uint32(os.Getgid())
}
func (n *FSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	uid, gid := fsCaller(ctx)
	info, err := n.filesystem.Create(ctx, n.id, name, mode, uid, gid, true)
	if err != nil {
		return nil, fsErrno(err)
	}
	return n.child(ctx, info, out), 0
}
func (n *FSNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fusefs.Inode, fusefs.FileHandle, uint32, syscall.Errno) {
	uid, gid := fsCaller(ctx)
	info, err := n.filesystem.Create(ctx, n.id, name, mode, uid, gid, false)
	if errors.Is(err, syscall.EEXIST) && flags&syscall.O_EXCL == 0 {
		info, err = n.filesystem.Lookup(ctx, n.id, name)
		if err == nil && info.Directory {
			return nil, nil, 0, syscall.EISDIR
		}
		if err == nil && flags&syscall.O_TRUNC != 0 {
			info, err = n.filesystem.Truncate(ctx, info.ID, 0)
		}
	}
	if err != nil {
		return nil, nil, 0, fsErrno(err)
	}
	return n.child(ctx, info, out), &fsOpenFile{append: flags&syscall.O_APPEND != 0}, fuse.FOPEN_DIRECT_IO, 0
}
func (n *FSNode) Open(ctx context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	info, err := n.filesystem.Stat(ctx, n.id)
	if err != nil {
		return nil, 0, fsErrno(err)
	}
	if info.Directory {
		return nil, 0, syscall.EISDIR
	}
	if flags&syscall.O_TRUNC != 0 {
		if _, err = n.filesystem.Truncate(ctx, n.id, 0); err != nil {
			return nil, 0, fsErrno(err)
		}
	}
	return &fsOpenFile{append: flags&syscall.O_APPEND != 0}, fuse.FOPEN_DIRECT_IO, 0
}
func (n *FSNode) Read(ctx context.Context, fh fusefs.FileHandle, dst []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	count, err := n.filesystem.ReadAt(ctx, n.id, dst, offset)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if err != nil {
		return nil, fsErrno(err)
	}
	return fuse.ReadResultData(dst[:count]), 0
}
func (n *FSNode) Write(ctx context.Context, fh fusefs.FileHandle, data []byte, offset int64) (uint32, syscall.Errno) {
	var count int
	var err error
	if h, ok := fh.(*fsOpenFile); ok && h.append {
		count, err = n.filesystem.Append(ctx, n.id, data)
	} else {
		count, err = n.filesystem.WriteAt(ctx, n.id, data, offset)
	}
	return uint32(count), fsErrno(err)
}
func (n *FSNode) Fsync(ctx context.Context, fh fusefs.FileHandle, flags uint32) syscall.Errno {
	return fsErrno(n.filesystem.Sync(ctx, n.id))
}
func (n *FSNode) Flush(ctx context.Context, fh fusefs.FileHandle) syscall.Errno {
	return fsErrno(n.filesystem.Sync(ctx, n.id))
}
func (n *FSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return fsErrno(n.filesystem.Unlink(ctx, n.id, name))
}
func (n *FSNode) Rmdir(ctx context.Context, name string) syscall.Errno { return syscall.ENOTSUP }
func (n *FSNode) Rename(ctx context.Context, name string, newParent fusefs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags & ^uint32(1) != 0 {
		return syscall.ENOTSUP
	}
	other, ok := newParent.(*FSNode)
	if !ok || other.filesystem != n.filesystem {
		return syscall.EXDEV
	}
	return fsErrno(n.filesystem.Rename(ctx, n.id, name, other.id, newName))
}
func (n *FSNode) Setattr(ctx context.Context, fh fusefs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// Atime/ctime are not stored separately in this prototype. Reject explicit
	// atime changes rather than silently claiming to persist them.
	if _, ok := in.GetATime(); ok {
		return syscall.ENOTSUP
	}
	a := FSAttributes{}
	if x, ok := in.GetSize(); ok {
		a.Size = &x
	}
	if x, ok := in.GetMode(); ok {
		a.Mode = &x
	}
	if x, ok := in.GetUID(); ok {
		a.UID = &x
	}
	if x, ok := in.GetGID(); ok {
		a.GID = &x
	}
	if x, ok := in.GetMTime(); ok {
		v := uint64(x.UnixNano())
		a.MtimeNS = &v
	}
	info, err := n.filesystem.SetAttributes(ctx, n.id, a)
	if err != nil {
		return fsErrno(err)
	}
	fillFSAttr(&out.Attr, info)
	return 0
}

var (
	_ fusefs.NodeLookuper  = (*FSNode)(nil)
	_ fusefs.NodeReaddirer = (*FSNode)(nil)
	_ fusefs.NodeGetattrer = (*FSNode)(nil)
	_ fusefs.NodeSetattrer = (*FSNode)(nil)
	_ fusefs.NodeCreater   = (*FSNode)(nil)
	_ fusefs.NodeMkdirer   = (*FSNode)(nil)
	_ fusefs.NodeOpener    = (*FSNode)(nil)
	_ fusefs.NodeReader    = (*FSNode)(nil)
	_ fusefs.NodeWriter    = (*FSNode)(nil)
	_ fusefs.NodeFsyncer   = (*FSNode)(nil)
	_ fusefs.NodeRenamer   = (*FSNode)(nil)
	_ fusefs.NodeUnlinker  = (*FSNode)(nil)
	_ fusefs.NodeRmdirer   = (*FSNode)(nil)
)
