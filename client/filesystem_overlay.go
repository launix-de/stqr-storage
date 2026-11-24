// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type OverlayOptions struct {
	Create        bool
	LowerBucketID BucketID
	// nil adopts an existing upper's policy. Directory rules end in '/'.
	Whitelist []string
}
type OverlayFilesystem struct {
	absentMu     sync.Mutex
	absentInfo   ObjectInfo
	absentRoot   string
	absentUpper  map[ObjectID]bool
	stateMu      sync.Mutex
	cachedState  *overlayState
	cachedRoot   []byte
	cachedInfo   ObjectInfo
	upper, lower *Filesystem
	lowerID      BucketID
	mu           sync.Mutex
	cacheRoot    string
	paths        map[ObjectID]string
}

var errOverlayUnchanged = errors.New("overlay unchanged")

func OpenOverlayFilesystem(ctx context.Context, lowerStore, upperStore FilesystemStore, opt OverlayOptions) (*OverlayFilesystem, error) {
	if lowerStore == nil || upperStore == nil || opt.LowerBucketID == (BucketID{}) {
		return nil, syscall.EINVAL
	}
	if bucket, known := filesystemStoreBucket(lowerStore); known && bucket != opt.LowerBucketID {
		return nil, syscall.EINVAL
	}
	if bucket, known := filesystemStoreBucket(upperStore); known && (bucket == (BucketID{}) || bucket == opt.LowerBucketID) {
		return nil, syscall.EINVAL
	}
	lower, err := OpenFilesystem(ctx, lowerStore, FSOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	policy, err := canonicalWhitelist(opt.Whitelist)
	if err != nil {
		return nil, err
	}
	// Existing lower directories also accept the convenient "foldername"
	// spelling. A trailing slash explicitly declares a directory not yet there.
	for i, p := range policy {
		if strings.HasSuffix(p, "/") {
			continue
		}
		id := FilesystemRootID()
		var info FSInfo
		for _, name := range strings.Split(p, "/") {
			info, err = lower.Lookup(ctx, id, name)
			if err != nil {
				break
			}
			id = info.ID
		}
		if err == nil && info.Directory {
			policy[i] = p + "/"
		} else if err != nil && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	policy, err = canonicalWhitelist(policy)
	if err != nil {
		return nil, err
	}
	o := &OverlayFilesystem{lower: lower, lowerID: opt.LowerBucketID, paths: map[ObjectID]string{}}
	o.upper = &Filesystem{directoryCache: newFilesystemDirectoryCache(), store: upperStore, lower: lower, directoryMemoryLimit: 64 << 20}
	_, _, _, err = o.state(ctx)
	if errors.Is(err, ErrNotFound) && opt.Create {
		n, _, e := lower.load(ctx, FilesystemRootID())
		if e != nil {
			return nil, e
		}
		n.blob = fsBlob{}
		n.removed = false
		s := &overlayState{policy: policy, dirs: map[ObjectID]*overlayDirectory{FilesystemRootID(): {lower: FilesystemRootID(), node: n, entries: map[string]ObjectID{}}}}
		body, e := encodeOverlayState(s)
		if e != nil {
			return nil, e
		}
		blob, e := o.upper.blobFromBytes(ctx, body)
		if e != nil {
			return nil, e
		}
		_, err = upperStore.CAS(ctx, FilesystemRootID(), 0, nil, encodeOverlayRoot(o.lowerID, blob), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
		if err != nil && !errors.Is(err, ErrPrecondition) {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	s, _, _, err := o.state(ctx)
	if err != nil {
		return nil, err
	}
	if opt.Whitelist != nil && !equalStrings(policy, s.policy) {
		return nil, errors.New("client: whitelist differs from persisted overlay policy; use a separate upper bucket")
	}
	return o, nil
}
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func (o *OverlayFilesystem) state(ctx context.Context) (*overlayState, ObjectInfo, fsBlob, error) {
	for attempt := 0; attempt < 128; attempt++ {
		s, info, blob, err := o.readState(ctx)
		if !errors.Is(err, ErrPrecondition) {
			return s, info, blob, err
		}
	}
	return nil, ObjectInfo{}, fsBlob{}, syscall.EAGAIN
}
func (o *OverlayFilesystem) readState(ctx context.Context) (*overlayState, ObjectInfo, fsBlob, error) {
	raw, info, guard, err := o.upper.overlayRootImage(ctx)
	if err != nil {
		return nil, info, fsBlob{}, err
	}
	o.resetAbsent(info, raw)
	o.stateMu.Lock()
	if o.cachedState != nil && o.cachedInfo.RuntimeID == info.RuntimeID && bytes.Equal(o.cachedRoot, raw) {
		s := o.cachedState
		o.stateMu.Unlock()
		_, blob, e := decodeOverlayRoot(raw)
		return s, info, blob, e
	}
	o.stateMu.Unlock()
	id, blob, err := decodeOverlayRoot(raw)
	if err != nil {
		return nil, info, blob, err
	}
	if id != o.lowerID {
		return nil, info, blob, errors.New("client: upper belongs to a different readonly bucket")
	}
	if blob.length > o.upper.directoryMemoryLimit {
		return nil, info, blob, syscall.ENOMEM
	}
	blob.view = guard.blob.view
	data := make([]byte, int(blob.length))
	if _, err = o.upper.readBlob(ctx, blob, data, 0); err != nil && !(len(data) == 0 && errors.Is(err, io.EOF)) {
		return nil, info, blob, err
	}
	s, err := decodeOverlayState(data)
	if err == nil {
		s.rootRaw = bytes.Clone(raw)
	}
	if e := o.upper.checkNodeView(ctx, guard); e != nil {
		return nil, info, blob, e
	}
	if err == nil && len(data) <= 1<<20 {
		o.stateMu.Lock()
		o.cachedState = s
		o.cachedRoot = append([]byte(nil), raw...)
		o.cachedInfo = info
		o.stateMu.Unlock()
	}
	return s, info, blob, err
}
func (o *OverlayFilesystem) publish(ctx context.Context, s *overlayState) error {
	body, err := encodeOverlayState(s)
	if err != nil {
		return err
	}
	if uint64(len(body)) > o.upper.directoryMemoryLimit {
		return syscall.ENOMEM
	}
	raw, _, guard, err := o.upper.overlayRootImage(ctx)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, s.rootRaw) {
		return ErrPrecondition
	}
	_, old, err := decodeOverlayRoot(raw)
	if err != nil {
		return err
	}
	n := fsNode{identity: FilesystemRootID(), head: bytes.Clone(raw), kind: fsDirectory, blob: old}
	if err = o.upper.stableBody(ctx, &n, body); err != nil {
		return err
	}
	if err = o.upper.checkNodeView(ctx, guard); err != nil {
		return err
	}
	n.blob.digest = sha256.Sum256(body)
	after := encodeOverlayRoot(o.lowerID, n.blob)
	if len(n.edits) > 0 {
		committed, e := o.upper.transact(ctx, []fsTxMember{{id: FilesystemRootID(), before: raw, after: after, writes: n.edits}})
		if e != nil {
			return e
		}
		if !committed {
			return ErrPrecondition
		}
		return nil
	}
	_, err = o.upper.store.CAS(ctx, FilesystemRootID(), 0, raw, after, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
	return err
}
func (o *OverlayFilesystem) edit(ctx context.Context, fn func(*overlayState) error) error {
	for attempt := 0; attempt < 128; attempt++ {
		s, _, _, err := o.state(ctx)
		if err != nil {
			return err
		}
		s = cloneOverlayState(s)
		if err = fn(s); errors.Is(err, errOverlayUnchanged) {
			return nil
		} else if err != nil {
			return err
		}
		err = o.publish(ctx, s)
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return err
	}
	return syscall.EAGAIN
}
func (o *OverlayFilesystem) stat(ctx context.Context, s *overlayState, rootKey string, id ObjectID) (FSInfo, error) {
	if dir := s.dirs[id]; dir != nil {
		n := dir.node
		if !dir.metadata && !dir.opaque && !n.removed {
			base, _, err := o.lower.load(ctx, dir.lower)
			if err != nil {
				return FSInfo{}, err
			}
			n.mode, n.uid, n.gid, n.mtime = base.mode, base.uid, base.gid, base.mtime
			n.atime, n.ctime = base.atime, base.ctime
		}
		return fsInfo(id, n, ObjectInfo{}), nil
	}
	if !overlayUpperReference(s, id) && o.upperMissing(id, rootKey, false) {
		return o.lower.Stat(ctx, id)
	}
	n, info, err := o.upper.load(ctx, id)
	if errors.Is(err, ErrNotFound) {
		if overlayUpperReference(s, id) {
			return FSInfo{}, ErrNotFound
		}
		o.upperMissing(id, rootKey, true)
		return o.lower.Stat(ctx, id)
	}
	if err != nil {
		return FSInfo{}, err
	}
	if n.kind == fsDirectory {
		return FSInfo{}, ErrFSFormat
	}
	return fsInfo(id, n, info), nil
}

func overlayUpperReference(s *overlayState, id ObjectID) bool {
	for _, dir := range s.dirs {
		for _, child := range dir.entries {
			if child == id && child != (ObjectID{}) {
				return true
			}
		}
	}
	return false
}
func (o *OverlayFilesystem) entries(ctx context.Context, s *overlayState, id ObjectID) ([]FSEntry, error) {
	dir := s.dirs[id]
	lower := id
	merged := map[string]ObjectID{}
	if dir != nil {
		if dir.node.removed {
			return nil, syscall.ENOENT
		}
		lower = dir.lower
	}
	if dir == nil || !dir.opaque {
		entries, err := o.lower.ReadDir(ctx, lower)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			// An inherited alias must not resolve to an upper inode published
			// for another name: that would bypass path-based write policies.
			shadowed := false
			if dir != nil {
				_, shadowed = dir.entries[entry.Name]
			}
			if !shadowed && overlayUpperReference(s, entry.ID) {
				return nil, syscall.EOPNOTSUPP
			}
			merged[entry.Name] = entry.ID
		}
	}
	if dir != nil {
		for name, child := range dir.entries {
			if child == (ObjectID{}) {
				delete(merged, name)
			} else {
				merged[name] = child
			}
		}
	}
	out := make([]FSEntry, 0, len(merged))
	for name, child := range merged {
		out = append(out, FSEntry{Name: name, ID: child})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (o *OverlayFilesystem) child(ctx context.Context, s *overlayState, parent ObjectID, name string) (ObjectID, error) {
	if !validFSName(name) {
		return ObjectID{}, syscall.EINVAL
	}
	lower := parent
	if dir := s.dirs[parent]; dir != nil {
		if dir.node.removed {
			return ObjectID{}, syscall.ENOENT
		}
		if id, ok := dir.entries[name]; ok {
			if id == (ObjectID{}) {
				return ObjectID{}, syscall.ENOENT
			}
			return id, nil
		}
		if dir.opaque {
			return ObjectID{}, syscall.ENOENT
		}
		lower = dir.lower
	}
	info, err := o.lower.Lookup(ctx, lower, name)
	// Match readDir's inherited-alias check on the direct lookup fast path.
	if err == nil && overlayUpperReference(s, info.ID) {
		return ObjectID{}, syscall.EOPNOTSUPP
	}
	return info.ID, err
}
func (o *OverlayFilesystem) directory(ctx context.Context, s *overlayState, id ObjectID) (*overlayDirectory, error) {
	if dir := s.dirs[id]; dir != nil {
		if dir.node.removed {
			return nil, syscall.ENOENT
		}
		return dir, nil
	}
	n, _, err := o.lower.load(ctx, id)
	if err != nil {
		return nil, err
	}
	if n.kind != fsDirectory {
		return nil, syscall.ENOTDIR
	}
	if n.removed {
		return nil, syscall.ENOENT
	}
	n.blob = fsBlob{}
	dir := &overlayDirectory{lower: id, node: n, entries: map[string]ObjectID{}}
	s.dirs[id] = dir
	return dir, nil
}
func (o *OverlayFilesystem) pathID(ctx context.Context, s *overlayState, p string) (ObjectID, error) {
	id := FilesystemRootID()
	if p == "" {
		return id, nil
	}
	for _, name := range strings.Split(p, "/") {
		next, err := o.child(ctx, s, id, name)
		if err != nil {
			return ObjectID{}, err
		}
		id = next
	}
	return id, nil
}
func (o *OverlayFilesystem) path(ctx context.Context, s *overlayState, rootKey string, target ObjectID) (string, error) {
	if target == FilesystemRootID() {
		return "", nil
	}
	o.mu.Lock()
	cached, ok := o.paths[target]
	if o.cacheRoot != rootKey {
		ok = false
	}
	o.mu.Unlock()
	if ok {
		if id, err := o.pathID(ctx, s, cached); err == nil && id == target {
			return cached, nil
		}
	}
	paths := map[ObjectID]string{}
	seen := map[ObjectID]bool{}
	var walk func(ObjectID, string, int) error
	walk = func(id ObjectID, p string, depth int) error {
		if depth > 1024 {
			return ErrFSFormat
		}
		info, err := o.stat(ctx, s, rootKey, id)
		if err != nil {
			return err
		}
		if seen[id] {
			if info.Directory {
				return ErrFSFormat
			}
			return nil
		}
		seen[id] = true
		paths[id] = p
		if !info.Directory {
			return nil
		}
		entries, err := o.entries(ctx, s, id)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			next := entry.Name
			if p != "" {
				next = p + "/" + entry.Name
			}
			if err = walk(entry.ID, next, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(FilesystemRootID(), "", 0); err != nil {
		return "", err
	}
	o.mu.Lock()
	o.cacheRoot = rootKey
	o.paths = paths
	o.mu.Unlock()
	p, ok := paths[target]
	if !ok {
		return "", syscall.ENOENT
	}
	return p, nil
}
func overlayChildPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
func (o *OverlayFilesystem) writable(ctx context.Context, s *overlayState, rootKey string, id ObjectID) (string, error) {
	info, err := o.stat(ctx, s, rootKey, id)
	if err != nil {
		return "", err
	}
	if info.Removed {
		return "", syscall.ENOENT
	}
	p, err := o.path(ctx, s, rootKey, id)
	if errors.Is(err, syscall.ENOENT) && !info.Directory {
		n, _, e := o.upper.load(ctx, id)
		if e == nil && n.writePath != "" && overlayAllowed(s.policy, n.writePath, false) {
			return n.writePath, nil
		}
	}
	if err != nil {
		return "", err
	}
	if !overlayAllowed(s.policy, p, info.Directory) {
		return "", syscall.EROFS
	}
	return p, nil
}
func (o *OverlayFilesystem) CheckWrite(ctx context.Context, id ObjectID) error {
	s, _, _, err := o.state(ctx)
	if err != nil {
		return err
	}
	_, err = o.writable(ctx, s, string(s.rootRaw), id)
	return err
}
func (o *OverlayFilesystem) Stat(ctx context.Context, id ObjectID) (FSInfo, error) {
	s, _, _, err := o.state(ctx)
	if err != nil {
		return FSInfo{}, err
	}
	return o.stat(ctx, s, string(s.rootRaw), id)
}
func (o *OverlayFilesystem) ReadDir(ctx context.Context, id ObjectID) ([]FSEntry, error) {
	s, _, _, err := o.state(ctx)
	if err != nil {
		return nil, err
	}
	return o.entries(ctx, s, id)
}
func (o *OverlayFilesystem) Lookup(ctx context.Context, parent ObjectID, name string) (FSInfo, error) {
	s, _, _, err := o.state(ctx)
	if err != nil {
		return FSInfo{}, err
	}
	id, err := o.child(ctx, s, parent, name)
	if err != nil {
		return FSInfo{}, err
	}
	return o.stat(ctx, s, string(s.rootRaw), id)
}
func (o *OverlayFilesystem) ReadAt(ctx context.Context, id ObjectID, dst []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	if id == FilesystemRootID() {
		return 0, syscall.EISDIR
	}
	var state *overlayState
	if _, ok := o.upper.store.(filesystemChangeStore); ok {
		var err error
		state, _, _, err = o.state(ctx)
		if err != nil {
			return 0, err
		}
		if !overlayUpperReference(state, id) && o.upperMissing(id, string(state.rootRaw), false) {
			return o.lower.ReadAt(ctx, id, dst, offset)
		}
	}
	raw, _, err := o.upper.store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		s := state
		if s == nil {
			var e error
			s, _, _, e = o.state(ctx)
			if e != nil {
				return 0, e
			}
		}
		if overlayUpperReference(s, id) {
			return 0, ErrNotFound
		}
		o.upperMissing(id, string(s.rootRaw), true)
		return o.lower.ReadAt(ctx, id, dst, offset)
	}
	if err != nil {
		return 0, err
	}
	n, err := decodeFSNode(raw)
	if err != nil {
		return 0, err
	}
	if n.kind != fsFile {
		return 0, syscall.EISDIR
	}
	if len(dst) == 0 {
		return 0, nil
	}
	return o.upper.readBlob(ctx, n.blob, dst, uint64(offset))
}
func (o *OverlayFilesystem) Sync(ctx context.Context, id ObjectID) error {
	if _, err := o.Stat(ctx, id); err != nil {
		return err
	}
	for _, f := range []*Filesystem{o.lower, o.upper} {
		if s, ok := f.store.(interface{ Sync(context.Context) error }); ok {
			if err := s.Sync(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *OverlayFilesystem) copyUp(ctx context.Context, id ObjectID) error {
	s, _, _, err := o.state(ctx)
	if err != nil {
		return err
	}
	p, err := o.writable(ctx, s, string(s.rootRaw), id)
	if err != nil {
		return err
	}
	visible, err := o.stat(ctx, s, string(s.rootRaw), id)
	if err != nil {
		return err
	}
	if visible.Directory {
		return syscall.EISDIR
	}
	if visible.Kind != FSRegular || visible.Nlink > 1 {
		return syscall.EOPNOTSUPP
	}
	parentPath, name := "", p
	if i := strings.LastIndex(p, "/"); i >= 0 {
		parentPath, name = p[:i], p[i+1:]
	}
	_, _, err = o.upper.store.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		n, _, err := o.lower.load(ctx, id)
		if err != nil {
			return err
		}
		if n.kind != fsFile {
			return syscall.EISDIR
		}
		if syncer, ok := o.lower.store.(interface{ Sync(context.Context) error }); ok {
			if err := syncer.Sync(ctx); err != nil {
				return err
			}
		}
		n.blob.lower = n.blob.root != (ObjectID{})
		if n.blob.lower {
			n.blob.lowerOwner = id
			n.blob.lowerDigest = lowerDataFingerprint(n)
		}
		n.writePath = p
		_, err = o.upper.store.CAS(ctx, id, 0, nil, encodeFSNode(n), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
		if err != nil && !errors.Is(err, ErrPrecondition) {
			return err
		}
	} else if err != nil {
		return err
	} else {
		parent, e := o.pathID(ctx, s, parentPath)
		if errors.Is(e, syscall.ENOENT) {
			return nil
		}
		if e != nil {
			return e
		}
		if dir := s.dirs[parent]; dir != nil && dir.entries[name] == id {
			return nil
		}
		if child, e := o.child(ctx, s, parent, name); errors.Is(e, syscall.ENOENT) || e == nil && child != id {
			return nil
		} else if e != nil {
			return e
		}
	}
	// Publish the dentry before changing the copied inode. A concurrent unlink
	// may leave an open, unlinked upper inode; its persisted authorized path
	// keeps that handle writable without making any protected name writable.
	return o.edit(ctx, func(s *overlayState) error {
		parent, e := o.pathID(ctx, s, parentPath)
		if errors.Is(e, syscall.ENOENT) {
			return errOverlayUnchanged
		}
		if e != nil {
			return e
		}
		current, e := o.child(ctx, s, parent, name)
		if errors.Is(e, syscall.ENOENT) {
			return errOverlayUnchanged
		}
		if e != nil {
			return e
		}
		if current != id {
			return errOverlayUnchanged
		}
		dir, e := o.directory(ctx, s, parent)
		if e != nil {
			return e
		}
		dir.entries[name] = id
		return nil
	})
}
func (o *OverlayFilesystem) WriteAt(ctx context.Context, id ObjectID, data []byte, offset int64) (int, error) {
	if err := o.copyUp(ctx, id); err != nil {
		return 0, err
	}
	return o.upper.WriteAt(ctx, id, data, offset)
}
func (o *OverlayFilesystem) Append(ctx context.Context, id ObjectID, data []byte) (int, error) {
	if err := o.copyUp(ctx, id); err != nil {
		return 0, err
	}
	return o.upper.Append(ctx, id, data)
}
func (o *OverlayFilesystem) Truncate(ctx context.Context, id ObjectID, size uint64) (FSInfo, error) {
	if err := o.copyUp(ctx, id); err != nil {
		return FSInfo{}, err
	}
	return o.upper.Truncate(ctx, id, size)
}
func overlaySetAttrs(n *fsNode, a FSAttributes) {
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
	n.ctime = uint64(time.Now().UnixNano())
}
func (o *OverlayFilesystem) SetAttributes(ctx context.Context, id ObjectID, a FSAttributes) (FSInfo, error) {
	info, err := o.Stat(ctx, id)
	if err != nil {
		return info, err
	}
	if !info.Directory {
		if err = o.copyUp(ctx, id); err != nil {
			return FSInfo{}, err
		}
		return o.upper.SetAttributes(ctx, id, a)
	}
	if a.Size != nil {
		return FSInfo{}, syscall.EISDIR
	}
	err = o.edit(ctx, func(s *overlayState) error {
		if _, e := o.writable(ctx, s, string(s.rootRaw), id); e != nil {
			return e
		}
		dir, e := o.directory(ctx, s, id)
		if e != nil {
			return e
		}
		if !dir.metadata && !dir.opaque {
			base, _, e := o.lower.load(ctx, dir.lower)
			if e != nil {
				return e
			}
			dir.node.mode, dir.node.uid, dir.node.gid, dir.node.mtime = base.mode, base.uid, base.gid, base.mtime
			dir.node.atime, dir.node.ctime = base.atime, base.ctime
		}
		overlaySetAttrs(&dir.node, a)
		dir.metadata = true
		return nil
	})
	if err != nil {
		return FSInfo{}, err
	}
	return o.Stat(ctx, id)
}

func cloneOverlayState(s *overlayState) *overlayState {
	next := &overlayState{rootRaw: append([]byte(nil), s.rootRaw...), policy: append([]string(nil), s.policy...), dirs: make(map[ObjectID]*overlayDirectory, len(s.dirs))}
	for id, d := range s.dirs {
		copyDir := *d
		copyDir.entries = make(map[string]ObjectID, len(d.entries))
		for name, child := range d.entries {
			copyDir.entries[name] = child
		}
		next.dirs[id] = &copyDir
	}
	return next
}
func filesystemStoreBucket(store FilesystemStore) (BucketID, bool) {
	switch c := store.(type) {
	case *Client:
		if c == nil {
			return BucketID{}, true
		}
		return c.bucket, true
	case *SubscriptionStore:
		if c == nil || c.client == nil {
			return BucketID{}, true
		}
		return c.client.bucket, true
	}
	return BucketID{}, false
}

// A missing upper inode can be reused only while the subscribed namespace head
// is unchanged. Copy-up publishes its dentry before changing content. Orphaned
// upper inodes remain readable: successful upper probes are never cached absent.
func (o *OverlayFilesystem) resetAbsent(info ObjectInfo, raw []byte) {
	o.absentMu.Lock()
	defer o.absentMu.Unlock()
	if o.absentInfo.Proof != info.Proof || o.absentInfo.RuntimeID != info.RuntimeID || o.absentRoot != string(raw) {
		o.absentInfo = info
		o.absentRoot = string(raw)
		o.absentUpper = map[ObjectID]bool{}
	}
}
func (o *OverlayFilesystem) upperMissing(id ObjectID, rootKey string, record bool) bool {
	o.absentMu.Lock()
	defer o.absentMu.Unlock()
	if o.absentInfo.Proof.Subscription == (ID{}) || o.absentRoot != rootKey {
		return false
	}
	if record {
		if len(o.absentUpper) >= 4096 {
			o.absentUpper = map[ObjectID]bool{}
		}
		if o.absentUpper == nil {
			o.absentUpper = map[ObjectID]bool{}
		}
		o.absentUpper[id] = true
	}
	return o.absentUpper[id]
}
