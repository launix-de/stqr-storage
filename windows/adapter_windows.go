//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"syscall"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/winfsp/cgofuse/fuse"
)

type Adapter struct {
	fuse.FileSystemBase
	FS *Filesystem
}

func errno(e error) int {
	if e == nil {
		return 0
	}
	for _, pair := range []struct {
		err  error
		code int
	}{{syscall.ENOENT, fuse.ENOENT}, {syscall.EEXIST, fuse.EEXIST}, {syscall.EPERM, fuse.EPERM}, {syscall.EACCES, fuse.EACCES}, {syscall.EROFS, fuse.EROFS}, {syscall.ENOTDIR, fuse.ENOTDIR}, {syscall.EISDIR, fuse.EISDIR}, {syscall.ENOTEMPTY, fuse.ENOTEMPTY}, {syscall.EBADF, fuse.EBADF}, {syscall.EBUSY, fuse.EBUSY}, {syscall.EINVAL, fuse.EINVAL}, {syscall.EAGAIN, fuse.EAGAIN}, {syscall.EOPNOTSUPP, fuse.EOPNOTSUPP}, {syscall.ENOSPC, fuse.ENOSPC}, {syscall.EFBIG, fuse.EFBIG}, {syscall.EMLINK, fuse.EMLINK}, {syscall.ESTALE, fuse.EIO}, {client.ErrNotFound, fuse.ENOENT}, {client.ErrPrecondition, fuse.EAGAIN}, {client.ErrFull, fuse.ENOSPC}, {client.ErrUnauthorized, fuse.EACCES}, {client.ErrRange, fuse.EINVAL}, {client.ErrNotImplemented, fuse.ENOSYS}, {context.Canceled, fuse.EINTR}, {context.DeadlineExceeded, fuse.ETIMEDOUT}} {
		if errors.Is(e, pair.err) {
			return -pair.code
		}
	}
	return -fuse.EIO
}
func attr(i client.FSInfo) *fuse.Stat_t {
	kind := uint32(fuse.S_IFREG)
	switch i.Kind {
	case client.FSDirectory:
		kind = fuse.S_IFDIR
	case client.FSSymlink:
		kind = fuse.S_IFLNK
	case client.FSFIFO:
		kind = fuse.S_IFIFO
	case client.FSSocket:
		kind = fuse.S_IFSOCK
	}
	ino := binary.LittleEndian.Uint64(i.ID[:8])
	if ino == 0 {
		ino = 1
	}
	ts := func(v uint64) fuse.Timespec { return fuse.NewTimespec(time.Unix(int64(v/1e9), int64(v%1e9))) }
	return &fuse.Stat_t{Ino: ino, Mode: kind | i.Mode, Nlink: i.Nlink, Uid: i.UID, Gid: i.GID, Size: int64(i.Size), Atim: ts(i.AtimeNS), Mtim: ts(i.MtimeNS), Ctim: ts(i.CtimeNS), Birthtim: ts(i.CtimeNS), Blksize: 1 << 20, Blocks: int64((i.Size + 511) / 512)}
}
func (a *Adapter) Getattr(p string, s *fuse.Stat_t, h uint64) int {
	i, e := a.FS.Stat(p, h)
	if e != nil {
		return errno(e)
	}
	*s = *attr(i)
	return 0
}
func (a *Adapter) open(p string, flags int, mode uint32, create bool) (int, uint64) {
	access := flags & fuse.O_ACCMODE
	h, e := a.FS.Open(p, OpenOptions{Read: access != fuse.O_WRONLY, Write: access != fuse.O_RDONLY, Create: create, Exclusive: flags&fuse.O_EXCL != 0, Truncate: flags&fuse.O_TRUNC != 0, Append: flags&fuse.O_APPEND != 0}, mode)
	if e != nil {
		return errno(e), ^uint64(0)
	}
	return 0, h
}
func (a *Adapter) Open(p string, flags int) (int, uint64) { return a.open(p, flags, 0, false) }
func (a *Adapter) Create(p string, flags int, mode uint32) (int, uint64) {
	return a.open(p, flags, mode, true)
}
func (a *Adapter) Read(_ string, data []byte, off int64, h uint64) int {
	n, e := a.FS.Read(h, data, off)
	if n != 0 || errors.Is(e, io.EOF) {
		return n
	}
	if e != nil {
		return errno(e)
	}
	return n
}
func (a *Adapter) Write(_ string, data []byte, off int64, h uint64) int {
	n, e := a.FS.Write(h, data, off)
	if e != nil {
		return errno(e)
	}
	return n
}
func (a *Adapter) Release(_ string, h uint64) int       { return errno(a.FS.Close(h)) }
func (a *Adapter) Flush(_ string, h uint64) int         { _, e := a.FS.get(h); return errno(e) }
func (a *Adapter) Fsync(p string, _ bool, h uint64) int { return errno(a.FS.Sync(p, h)) }
func (a *Adapter) Truncate(p string, size int64, h uint64) int {
	if size < 0 {
		return -fuse.EINVAL
	}
	v := uint64(size)
	return errno(a.FS.Setattr(p, h, client.FSAttributes{Size: &v}))
}
func (a *Adapter) Mkdir(p string, mode uint32) int { return errno(a.FS.Mkdir(p, mode)) }
func (a *Adapter) Unlink(p string) int             { return errno(a.FS.Remove(p, false)) }
func (a *Adapter) Rmdir(p string) int              { return errno(a.FS.Remove(p, true)) }
func (a *Adapter) Rename(from, to string) int      { return errno(a.FS.Rename(from, to, 0)) }
func (a *Adapter) Rename3(from, to string, flags uint32) int {
	return errno(a.FS.Rename(from, to, flags))
}

// WinFsp currently does not expose creation of hardlinks through Windows APIs.
func (a *Adapter) Link(from, to string) int     { return -fuse.EOPNOTSUPP }
func (a *Adapter) Symlink(target, p string) int { return errno(a.FS.Symlink(target, p)) }
func (a *Adapter) Readlink(p string) (int, string) {
	i, e := a.FS.Lookup(p)
	if e != nil {
		return errno(e), ""
	}
	target, e := a.FS.Volume.Readlink(a.FS.Context, i.ID)
	return errno(e), string(target)
}
func (a *Adapter) Chmod(p string, mode uint32) int {
	return errno(a.FS.Setattr(p, 0, client.FSAttributes{Mode: &mode}))
}
func (a *Adapter) Chown(p string, uid, gid uint32) int {
	v := client.FSAttributes{}
	if uid != ^uint32(0) {
		v.UID = &uid
	}
	if gid != ^uint32(0) {
		v.GID = &gid
	}
	if v.UID == nil && v.GID == nil {
		return 0
	}
	return errno(a.FS.Setattr(p, 0, v))
}
func (a *Adapter) Utimens(p string, ts []fuse.Timespec) int {
	if len(ts) != 2 {
		return -fuse.EINVAL
	}
	v := client.FSAttributes{}
	for i, t := range ts {
		if t.Nsec == fuse.UTIME_OMIT {
			continue
		}
		if t.Nsec == fuse.UTIME_NOW {
			t = fuse.Now()
		}
		if t.Nsec < 0 || t.Nsec >= 1e9 {
			return -fuse.EINVAL
		}
		n := t.Time().UnixNano()
		if n < 0 {
			return -fuse.EINVAL
		}
		value := uint64(n)
		if i == 0 {
			v.AtimeNS = &value
		} else {
			v.MtimeNS = &value
		}
	}
	if v.AtimeNS == nil && v.MtimeNS == nil {
		return 0
	}
	return errno(a.FS.Setattr(p, 0, v))
}
func (a *Adapter) Access(p string, mask uint32) int {
	if _, e := a.FS.Lookup(p); e != nil {
		return errno(e)
	}
	if mask&fuse.W_OK != 0 {
		return errno(a.FS.writable())
	}
	return 0
}
func (a *Adapter) Opendir(p string) (int, uint64) {
	h, e := a.FS.Open(p, OpenOptions{Read: true, Directory: true}, 0)
	if e != nil {
		return errno(e), ^uint64(0)
	}
	return 0, h
}
func (a *Adapter) Readdir(_ string, fill func(string, *fuse.Stat_t, int64) bool, offset int64, h uint64) int {
	entries, e := a.FS.Entries(h)
	if e != nil {
		return errno(e)
	}
	if offset < 0 || offset > int64(len(entries)) {
		return -fuse.EINVAL
	}
	for i := offset; i < int64(len(entries)); i++ {
		entry := entries[i]
		if !fill(entry.Name, attr(entry.Info), i+1) {
			break
		}
	}
	return 0
}
func (a *Adapter) Releasedir(p string, h uint64) int          { return a.Release(p, h) }
func (a *Adapter) Fsyncdir(p string, data bool, h uint64) int { return a.Fsync(p, data, h) }
func (a *Adapter) Statfs(_ string, s *fuse.Statfs_t) int {
	space, e := a.FS.Volume.Space(a.FS.Context)
	if e != nil {
		return errno(e)
	}
	*s = fuse.Statfs_t{Bsize: 4096, Frsize: 4096, Blocks: space.Total / 4096, Bfree: space.Free / 4096, Bavail: space.Available / 4096, Namemax: 255}
	return 0
}

var _ fuse.FileSystemInterface = (*Adapter)(nil)
