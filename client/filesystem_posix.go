// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	fsXattrCount = 128
	fsXattrValue = 65536
	fsXattrTotal = 262144
)

func fsLinks(n fsNode) uint32 {
	if n.removed {
		return 0
	}
	if n.extended {
		return n.links
	}
	return 1
}
func setFSLinks(n *fsNode, count uint32) { n.links = count; n.extended = true }
func (f *Filesystem) directoryLinks(ctx context.Context, n *fsNode, entries []FSEntry) error {
	count := uint32(2)
	for _, entry := range entries {
		child, _, err := f.load(ctx, entry.ID)
		if err != nil {
			return err
		}
		if child.kind == fsDirectory {
			if count == math.MaxUint32 {
				return syscall.EMLINK
			}
			count++
		}
	}
	setFSLinks(n, count)
	return nil
}
func (f *Filesystem) Symlink(ctx context.Context, parent ObjectID, name, target string, uid, gid uint32) (FSInfo, error) {
	if len(target) == 0 || len(target) > 4095 || strings.ContainsRune(target, 0) {
		return FSInfo{}, syscall.EINVAL
	}
	return f.createNode(ctx, parent, name, 0777, uid, gid, fsSymlink, []byte(target))
}
func (f *Filesystem) Readlink(ctx context.Context, id ObjectID) ([]byte, error) {
	n, _, err := f.load(ctx, id)
	if err != nil {
		return nil, err
	}
	if n.kind != fsSymlink {
		return nil, syscall.EINVAL
	}
	return append([]byte(nil), n.blob.inline...), nil
}
func (f *Filesystem) Mknod(ctx context.Context, parent ObjectID, name string, kind FSKind, mode, uid, gid uint32) (FSInfo, error) {
	if kind != FSFIFO && kind != FSSocket && kind != FSRegular {
		return FSInfo{}, syscall.EOPNOTSUPP
	}
	return f.createNode(ctx, parent, name, mode, uid, gid, uint8(kind), nil)
}
func (f *Filesystem) Link(ctx context.Context, parent ObjectID, name string, target ObjectID) (FSInfo, error) {
	if f.readOnly {
		return FSInfo{}, syscall.EROFS
	}
	if !validFSName(name) {
		return FSInfo{}, syscall.EINVAL
	}
	for attempt := 0; attempt < 128; attempt++ {
		p, err := f.snapshot(ctx, parent)
		if err != nil {
			return FSInfo{}, err
		}
		entries, err := f.directory(ctx, p.node)
		if err != nil {
			return FSInfo{}, err
		}
		if findFSEntry(entries, name) >= 0 {
			return FSInfo{}, syscall.EEXIST
		}
		child, err := f.snapshot(ctx, target)
		if err != nil {
			return FSInfo{}, err
		}
		if child.node.kind == fsDirectory {
			return FSInfo{}, syscall.EPERM
		}
		count := fsLinks(child.node)
		if count == 0 {
			return FSInfo{}, syscall.ENOENT
		}
		if count == math.MaxUint32 {
			return FSInfo{}, syscall.EMLINK
		}
		nextP, err := f.directoryImage(ctx, p.node, append(entries, FSEntry{Name: name, ID: target}))
		if err != nil {
			return FSInfo{}, err
		}
		next := child.node
		setFSLinks(&next, count+1)
		next.ctime = uint64(time.Now().UnixNano())
		committed, err := f.transact(ctx, []fsTxMember{fsMember(p, nextP), fsMember(child, next)})
		if err != nil {
			return FSInfo{}, err
		}
		if committed {
			return f.Stat(ctx, target)
		}
	}
	return FSInfo{}, syscall.EAGAIN
}
func (f *Filesystem) unlinkNode(ctx context.Context, parent ObjectID, name string) error {
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
		at := findFSEntry(entries, name)
		if at < 0 {
			return syscall.ENOENT
		}
		child, err := f.snapshot(ctx, entries[at].ID)
		if err != nil {
			return err
		}
		if child.node.kind == fsDirectory {
			return syscall.EISDIR
		}
		count := fsLinks(child.node)
		if count == 0 {
			fresh, e := f.snapshot(ctx, parent)
			if e != nil {
				return e
			}
			if fresh.info.Version != p.info.Version {
				continue
			}
			return ErrFSFormat
		}
		nextP, err := f.directoryImage(ctx, p.node, append(entries[:at], entries[at+1:]...))
		if err != nil {
			return err
		}
		next := child.node
		setFSLinks(&next, count-1)
		next.ctime = uint64(time.Now().UnixNano())
		committed, err := f.transact(ctx, []fsTxMember{fsMember(p, nextP), fsMember(child, next)})
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return syscall.EAGAIN
}
func validXattrName(name string) bool {
	return len(name) > 0 && len(name) <= 255 && utf8.ValidString(name) && !strings.ContainsAny(name, "\x00/") && strings.Contains(name, ".") && !strings.HasPrefix(name, ".") && !strings.HasSuffix(name, ".")
}
func xattrName(name string) error {
	if !validXattrName(name) {
		return syscall.EINVAL
	}
	if !strings.HasPrefix(name, "user.") && !strings.HasPrefix(name, "trusted.") {
		return syscall.EOPNOTSUPP
	}
	return nil
}
func (f *Filesystem) GetXattr(ctx context.Context, id ObjectID, name string) ([]byte, error) {
	if err := xattrName(name); err != nil {
		return nil, err
	}
	n, _, err := f.load(ctx, id)
	if err != nil {
		return nil, err
	}
	value, ok := n.xattrs[name]
	if !ok {
		return nil, syscall.ENODATA
	}
	return append([]byte(nil), value...), nil
}
func (f *Filesystem) ListXattr(ctx context.Context, id ObjectID) ([]string, error) {
	n, _, err := f.load(ctx, id)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(n.xattrs))
	for name := range n.xattrs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
func (f *Filesystem) SetXattr(ctx context.Context, id ObjectID, name string, value []byte, flags uint32) error {
	if err := xattrName(name); err != nil {
		return err
	}
	if flags > 2 {
		return syscall.EINVAL
	}
	if len(value) > fsXattrValue {
		return syscall.E2BIG
	}
	_, err := f.mutate(ctx, id, func(n *fsNode) error {
		if n.kind != fsFile && n.kind != fsDirectory {
			return syscall.EPERM
		}
		_, exists := n.xattrs[name]
		if flags == 1 && exists {
			return syscall.EEXIST
		}
		if flags == 2 && !exists {
			return syscall.ENODATA
		}
		setFSLinks(n, fsLinks(*n))
		if n.xattrs == nil {
			n.xattrs = make(map[string][]byte)
		}
		n.xattrs[name] = append([]byte(nil), value...)
		if len(n.xattrs) > fsXattrCount {
			return syscall.ENOSPC
		}
		total := 4
		for k, v := range n.xattrs {
			total += len(k) + len(v) + 20
		}
		if total > fsXattrTotal {
			return syscall.ENOSPC
		}
		return nil
	})
	return err
}
func (f *Filesystem) RemoveXattr(ctx context.Context, id ObjectID, name string) error {
	if err := xattrName(name); err != nil {
		return err
	}
	_, err := f.mutate(ctx, id, func(n *fsNode) error {
		if _, ok := n.xattrs[name]; !ok {
			return syscall.ENODATA
		}
		delete(n.xattrs, name)
		return nil
	})
	return err
}

// Overlay reads follow the existing upper-reference selection, including its
// missing-upper guard. Mutation support remains limited to the normal view.
func (o *OverlayFilesystem) posixSource(ctx context.Context, id ObjectID) (*Filesystem, ObjectID, error) {
	s, _, _, err := o.state(ctx)
	if err != nil {
		return nil, id, err
	}
	if dir := s.dirs[id]; dir != nil {
		if dir.node.removed {
			return nil, id, syscall.ENOENT
		}
		if dir.opaque {
			return nil, id, nil
		}
		return o.lower, dir.lower, nil
	}
	_, _, err = o.upper.load(ctx, id)
	if err == nil {
		return o.upper, id, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, id, err
	}
	if overlayUpperReference(s, id) {
		return nil, id, ErrNotFound
	}
	return o.lower, id, nil
}
func (o *OverlayFilesystem) Readlink(ctx context.Context, id ObjectID) ([]byte, error) {
	f, source, err := o.posixSource(ctx, id)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, syscall.EINVAL
	}
	return f.Readlink(ctx, source)
}
func (o *OverlayFilesystem) GetXattr(ctx context.Context, id ObjectID, name string) ([]byte, error) {
	if err := xattrName(name); err != nil {
		return nil, err
	}
	s, _, _, err := o.state(ctx)
	if err != nil {
		return nil, err
	}
	if d := s.dirs[id]; d != nil && (d.metadata || d.opaque) {
		if d.node.removed {
			return nil, syscall.ENOENT
		}
		v, ok := d.node.xattrs[name]
		if !ok {
			return nil, syscall.ENODATA
		}
		return append([]byte(nil), v...), nil
	}

	f, source, err := o.posixSource(ctx, id)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, syscall.ENODATA
	}
	return f.GetXattr(ctx, source, name)
}
func (o *OverlayFilesystem) ListXattr(ctx context.Context, id ObjectID) ([]string, error) {
	s, _, _, err := o.state(ctx)
	if err != nil {
		return nil, err
	}
	if d := s.dirs[id]; d != nil && (d.metadata || d.opaque) {
		if d.node.removed {
			return nil, syscall.ENOENT
		}
		names := make([]string, 0, len(d.node.xattrs))
		for name := range d.node.xattrs {
			names = append(names, name)
		}
		sort.Strings(names)
		return names, nil
	}

	f, source, err := o.posixSource(ctx, id)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, nil
	}
	return f.ListXattr(ctx, source)
}
