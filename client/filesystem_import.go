// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// Used only by immutable blob/index builders, which consume the assigned ID
// and ignore ObjectInfo. Publication waits for the queue before writing heads.
type importImmutableStore struct {
	FilesystemStore
	enqueue func(ObjectID, []byte) error
}

func (s importImmutableStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, data []byte, opt CASOptions) (ObjectInfo, error) {
	if off != 0 || len(expected) != 0 || !opt.Absent || opt.Scope != CASObject {
		return ObjectInfo{}, ErrProtocol
	}
	return ObjectInfo{}, s.enqueue(id, data)
}

type importInode struct {
	id      ObjectID
	node    fsNode
	entries map[string]*importInode
}

// ImportFilesystemTar publishes a new native filesystem tree. It never changes
// an existing root. Paths are interpreted inside the object namespace, never
// extracted through host symlinks. Failed imports leave unreachable objects for
// collection; callers should import into a fresh release bucket.
func ImportFilesystemTar(ctx context.Context, store FilesystemStore, source io.Reader) (result error) {
	if _, _, err := store.Get(ctx, FilesystemRootID()); !errors.Is(err, ErrNotFound) {
		if err == nil {
			return syscall.EEXIST
		}
		return err
	}

	// Payloads have fresh immutable IDs, so bounded independent writes may run
	// concurrently. No inode/root references are published until all finish.
	type payload struct {
		id   ObjectID
		data []byte
	}
	payloadCtx, payloadCancel := context.WithCancel(ctx)
	payloads := make(chan payload, 8)
	var payloadWorkers sync.WaitGroup
	var payloadOnce sync.Once
	var payloadError error
	payloadsClosed := false
	for i := 0; i < 8; i++ {
		payloadWorkers.Add(1)
		go func() {
			defer payloadWorkers.Done()
			for p := range payloads {
				_, err := store.CAS(payloadCtx, p.id, 0, nil, p.data, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true, RequestID: NewRequestID()}})
				if err != nil {
					payloadOnce.Do(func() { payloadError = err; payloadCancel() })
					return
				}
			}
		}()
	}
	defer func() {
		payloadCancel()
		if !payloadsClosed {
			close(payloads)
		}
		payloadWorkers.Wait()
		if payloadError != nil && errors.Is(result, context.Canceled) {
			result = payloadError
		}
	}()

	enqueue := func(id ObjectID, data []byte) error {
		select {
		case payloads <- payload{id, append([]byte(nil), data...)}:
			return nil
		case <-payloadCtx.Done():
			return payloadCtx.Err()
		}
	}
	f := &Filesystem{store: importImmutableStore{store, enqueue}, directoryMemoryLimit: 64 << 20}

	root := &importInode{id: FilesystemRootID(), node: fsNode{kind: fsDirectory, mode: 0755, extended: true, links: 2}, entries: map[string]*importInode{}}
	nodes := map[string]*importInode{".": root}
	links := map[string]string{}
	clean := func(name string) (string, error) {
		if strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) {
			return "", ErrFSFormat
		}
		for _, part := range strings.Split(name, "/") {
			if part == ".." {
				return "", ErrFSFormat
			}
		}
		name = path.Clean(name)
		if name != "." {
			for _, part := range strings.Split(name, "/") {
				if !validFSName(part) {
					return "", ErrFSFormat
				}
			}
		}
		return name, nil
	}
	var directory func(string) (*importInode, error)
	directory = func(name string) (*importInode, error) {
		if _, ok := links[name]; ok {
			return nil, syscall.ENOTDIR
		}
		if n := nodes[name]; n != nil {
			if n.node.kind != fsDirectory {
				return nil, syscall.ENOTDIR
			}
			return n, nil
		}
		parent, err := directory(path.Dir(name))
		if err != nil {
			return nil, err
		}
		n := &importInode{node: fsNode{kind: fsDirectory, mode: 0755, extended: true, links: 2}, entries: map[string]*importInode{}}
		if err = randomFSID(&n.id); err != nil {
			return nil, err
		}
		nodes[name] = n
		parent.entries[path.Base(name)] = n
		return n, nil
	}
	reader := tar.NewReader(source)
	for {
		if err := payloadCtx.Err(); err != nil {
			return err
		}
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name, err := clean(h.Name)
		if err != nil {
			return fmt.Errorf("tar path %q: %w", h.Name, err)
		}
		if h.Uid < 0 || h.Gid < 0 || uint64(h.Uid) > math.MaxUint32 || uint64(h.Gid) > math.MaxUint32 {
			return ErrFSFormat
		}
		if h.Typeflag == tar.TypeLink {
			if name == "." || nodes[name] != nil || links[name] != "" {
				return ErrFSFormat
			}
			target, err := clean(h.Linkname)
			if err != nil {
				return err
			}
			links[name] = target
			continue
		}
		if _, exists := links[name]; exists {
			return ErrFSFormat
		}
		var n *importInode
		if h.Typeflag == tar.TypeDir {
			n, err = directory(name)
		} else {
			if name == "." || nodes[name] != nil {
				return ErrFSFormat
			}
			parent, e := directory(path.Dir(name))
			if e != nil {
				return e
			}
			n = &importInode{}
			err = randomFSID(&n.id)
			nodes[name] = n
			parent.entries[path.Base(name)] = n
		}
		if err != nil {
			return err
		}
		n.node.mode = uint32(h.Mode) & 07777
		n.node.uid = uint32(h.Uid)
		n.node.gid = uint32(h.Gid)
		if h.ModTime.UnixNano() < 0 {
			return ErrFSFormat
		}
		n.node.mtime = uint64(h.ModTime.UnixNano())
		n.node.atime = n.node.mtime
		n.node.ctime = n.node.mtime
		if !h.AccessTime.IsZero() && h.AccessTime.UnixNano() >= 0 {
			n.node.atime = uint64(h.AccessTime.UnixNano())
		}
		if !h.ChangeTime.IsZero() && h.ChangeTime.UnixNano() >= 0 {
			n.node.ctime = uint64(h.ChangeTime.UnixNano())
		}
		n.node.extended = true
		n.node.links = 1
		switch h.Typeflag {
		case tar.TypeDir:
			n.node.kind = fsDirectory
			n.node.links = 2
		case tar.TypeReg, tar.TypeRegA:
			n.node.kind = fsFile
			if h.Size < 0 {
				return ErrFSFormat
			}
			if h.Size <= fsInlineSize {
				data, err := io.ReadAll(reader)
				if err != nil {
					return err
				}
				n.node.blob = fsBlob{length: uint64(len(data)), inline: data}
			} else {
				var segments []fsSegment
				remaining := uint64(h.Size)
				buffer := make([]byte, FSObjectSize)
				zeros := make([]byte, FSObjectSize)
				for remaining > 0 {
					length := min(remaining, uint64(len(buffer)))
					if _, err = io.ReadFull(reader, buffer[:length]); err != nil {
						return err
					}
					seg := fsSegment{length: length}
					if !bytes.Equal(buffer[:length], zeros[:length]) {

						seg.id = newFSID()
						select {
						case payloads <- payload{seg.id, append([]byte(nil), buffer[:length]...)}:
						case <-payloadCtx.Done():
							return payloadCtx.Err()
						}

					}
					if seg.id == (ObjectID{}) && len(segments) > 0 && segments[len(segments)-1].id == (ObjectID{}) {
						segments[len(segments)-1].length += length
					} else {
						segments = append(segments, seg)
					}
					remaining -= length
				}
				n.node.blob, err = f.buildBlob(ctx, segments, uint64(h.Size))
				if err != nil {
					return err
				}
			}
		case tar.TypeSymlink:
			if h.Linkname == "" || len(h.Linkname) > 4095 || strings.ContainsRune(h.Linkname, 0) {
				return ErrFSFormat
			}
			n.node.kind = fsSymlink
			n.node.blob = fsBlob{length: uint64(len(h.Linkname)), inline: []byte(h.Linkname)}
		case tar.TypeFifo:
			n.node.kind = fsFIFO
		default:
			return fmt.Errorf("unsupported tar node %q (type %d)", name, h.Typeflag)
		}
		total := 0
		for key, value := range h.PAXRecords {
			if strings.HasPrefix(key, "SCHILY.acl.") && value != "" {
				return fmt.Errorf("POSIX ACL import for %q requires ACL support", name)
			}
			if !strings.HasPrefix(key, "SCHILY.xattr.") {
				continue
			}
			key = strings.TrimPrefix(key, "SCHILY.xattr.")
			if !validXattrName(key) || len(value) > fsXattrValue {
				return ErrFSFormat
			}
			total += len(key) + len(value) + 16
			if n.node.xattrs == nil {
				n.node.xattrs = map[string][]byte{}
			}
			n.node.xattrs[key] = []byte(value)
		}
		if len(n.node.xattrs) > fsXattrCount || total > fsXattrTotal {
			return ErrFSFormat
		}
	}
	// Tar readers stop at the trailer. Drain the transport as well so a
	// streaming source can finish its whole-artifact checksum before publication.
	if _, err := io.Copy(io.Discard, source); err != nil {
		return err
	}

	// Resolve hardlinks only after all headers, including forward links, are read.
	var resolve func(string, map[string]bool) (*importInode, error)
	resolve = func(name string, visiting map[string]bool) (*importInode, error) {
		if n := nodes[name]; n != nil {
			return n, nil
		}
		target, ok := links[name]
		if !ok || visiting[name] {
			return nil, ErrFSFormat
		}
		visiting[name] = true
		n, err := resolve(target, visiting)
		delete(visiting, name)
		if err != nil {
			return nil, err
		}
		if n.node.kind == fsDirectory || n.node.links == math.MaxUint32 {
			return nil, ErrFSFormat
		}
		parent, err := directory(path.Dir(name))
		if err != nil {
			return nil, err
		}
		n.node.links++
		nodes[name] = n
		parent.entries[path.Base(name)] = n
		return n, nil
	}
	for name := range links {
		if _, err := resolve(name, map[string]bool{}); err != nil {
			return err
		}
	}
	unique := map[*importInode]bool{}
	for _, n := range nodes {
		if unique[n] {
			continue
		}
		unique[n] = true
		if n.node.kind == fsDirectory {
			entries := make([]FSEntry, 0, len(n.entries))
			n.node.links = 2
			for name, child := range n.entries {
				entries = append(entries, FSEntry{Name: name, ID: child.id})
				if child.node.kind == fsDirectory {
					n.node.links++
				}
			}
			data, err := encodeFSDirectory(entries)
			if err != nil {
				return err
			}
			n.node.blob, err = f.blobFromBytes(ctx, data)
			if err != nil {
				return err
			}
		}
	}
	close(payloads)
	payloadsClosed = true
	payloadWorkers.Wait()
	if payloadError != nil {
		return payloadError
	}
	// A bounded queue permits independent inode publications without repeatedly
	// rewriting every ancestor directory for each extracted file.
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan *importInode)
	var workers sync.WaitGroup
	var once sync.Once
	var failure error
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for n := range jobs {
				_, err := store.CAS(workCtx, n.id, 0, nil, encodeFSNode(n.node), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true, RequestID: NewRequestID()}})
				if err != nil {
					once.Do(func() { failure = err; cancel() })
					return
				}
			}
		}()
	}
	ordered := make([]string, 0, len(nodes))
	for name := range nodes {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	queued := map[*importInode]bool{}
send:
	for _, name := range ordered {
		n := nodes[name]
		if n == root || queued[n] {
			continue
		}
		queued[n] = true
		select {
		case jobs <- n:
		case <-workCtx.Done():
			break send
		}
	}
	close(jobs)
	workers.Wait()
	if failure != nil {
		return failure
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	syncStore, canSync := store.(interface{ Sync(context.Context) error })
	if canSync {
		if err := syncStore.Sync(ctx); err != nil {
			return err
		}
	}
	if _, err := store.CAS(ctx, root.id, 0, nil, encodeFSNode(root.node), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true, RequestID: NewRequestID()}}); err != nil {
		return err
	}
	if canSync {
		return syncStore.Sync(ctx)
	}
	return nil
}
func randomFSID(out *ObjectID) error {
	a, b := NewRequestID(), NewRequestID()
	copy(out[:16], a[:])
	copy(out[16:], b[:])
	return nil
}
