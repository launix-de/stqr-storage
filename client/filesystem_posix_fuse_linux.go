//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"strings"
	"syscall"
)

func (n *FSNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	f, ok := n.filesystem.(*Filesystem)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	uid, gid, _, err := n.creationOwner(ctx, 0777, false)
	if err != nil {
		return nil, fsErrno(err)
	}
	info, err := f.Symlink(ctx, n.id, name, target, uid, gid)
	if err != nil {
		return nil, fsErrno(err)
	}
	return n.child(ctx, info, out), 0
}
func (n *FSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	f, ok := n.filesystem.(interface {
		Readlink(context.Context, ObjectID) ([]byte, error)
	})
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	data, err := f.Readlink(ctx, n.id)
	return data, fsErrno(err)
}
func (n *FSNode) Link(ctx context.Context, target fusefs.InodeEmbedder, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	f, ok := n.filesystem.(*Filesystem)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	node, ok := target.(*FSNode)
	if !ok || node.numbers != n.numbers {
		return nil, syscall.EXDEV
	}
	info, err := f.Link(ctx, n.id, name, node.id)
	if err != nil {
		return nil, fsErrno(err)
	}
	return n.child(ctx, info, out), 0
}
func (n *FSNode) Mknod(ctx context.Context, name string, mode, dev uint32, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	f, ok := n.filesystem.(*Filesystem)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	kind := FSRegular
	switch mode & syscall.S_IFMT {
	case syscall.S_IFREG:
	case syscall.S_IFIFO:
		kind = FSFIFO
	case syscall.S_IFSOCK:
		kind = FSSocket
	default:
		return nil, syscall.EOPNOTSUPP
	}
	if dev != 0 {
		return nil, syscall.EOPNOTSUPP
	}
	uid, gid, mode, err := n.creationOwner(ctx, mode, false)
	if err != nil {
		return nil, fsErrno(err)
	}
	info, err := f.Mknod(ctx, n.id, name, kind, mode, uid, gid)
	if err != nil {
		return nil, fsErrno(err)
	}
	return n.child(ctx, info, out), 0
}
func xattrCaller(ctx context.Context, name string) syscall.Errno {
	uid, _ := fsCaller(ctx)
	if strings.HasPrefix(name, "trusted.") && uid != 0 {
		return syscall.EPERM
	}
	return 0
}
func (n *FSNode) Getxattr(ctx context.Context, name string, dest []byte) (uint32, syscall.Errno) {
	if err := xattrCaller(ctx, name); err != 0 {
		return 0, err
	}
	f, ok := n.filesystem.(interface {
		GetXattr(context.Context, ObjectID, string) ([]byte, error)
	})
	if !ok {
		return 0, syscall.EOPNOTSUPP
	}
	data, err := f.GetXattr(ctx, n.id, name)
	if err != nil {
		return 0, fsErrno(err)
	}
	if len(dest) != 0 && len(dest) < len(data) {
		return uint32(len(data)), syscall.ERANGE
	}
	if len(dest) != 0 {
		copy(dest, data)
	}
	return uint32(len(data)), 0
}
func (n *FSNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	f, ok := n.filesystem.(interface {
		ListXattr(context.Context, ObjectID) ([]string, error)
	})
	if !ok {
		return 0, 0
	}
	names, err := f.ListXattr(ctx, n.id)
	if err != nil {
		return 0, fsErrno(err)
	}
	var data []byte
	for _, name := range names {
		if xattrCaller(ctx, name) == 0 {
			data = append(data, []byte(name)...)
			data = append(data, 0)
		}
	}
	if len(dest) != 0 && len(dest) < len(data) {
		return uint32(len(data)), syscall.ERANGE
	}
	if len(dest) != 0 {
		copy(dest, data)
	}
	return uint32(len(data)), 0
}
func (n *FSNode) Setxattr(ctx context.Context, name string, data []byte, flags uint32) syscall.Errno {
	if err := xattrCaller(ctx, name); err != 0 {
		return err
	}
	f, ok := n.filesystem.(*Filesystem)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return fsErrno(f.SetXattr(ctx, n.id, name, data, flags))
}
func (n *FSNode) Removexattr(ctx context.Context, name string) syscall.Errno {
	if err := xattrCaller(ctx, name); err != 0 {
		return err
	}
	f, ok := n.filesystem.(*Filesystem)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return fsErrno(f.RemoveXattr(ctx, n.id, name))
}

var _ fusefs.NodeSymlinker = (*FSNode)(nil)
var _ fusefs.NodeReadlinker = (*FSNode)(nil)
var _ fusefs.NodeLinker = (*FSNode)(nil)
var _ fusefs.NodeMknoder = (*FSNode)(nil)
var _ fusefs.NodeGetxattrer = (*FSNode)(nil)
var _ fusefs.NodeSetxattrer = (*FSNode)(nil)
