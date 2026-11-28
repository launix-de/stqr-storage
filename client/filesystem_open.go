// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"syscall"
	"time"
)

// fsReferenceStore is ordinary, format-agnostic object coordination. Tokens are
// valid across reconnects; explicit Sync checkpoints them for crash recovery.
type fsReferenceStore interface {
	PinObject(context.Context, ObjectID, ID) error
	UnpinObject(context.Context, ObjectID, ID) error
	ClaimObjectRetirement(context.Context, ObjectID, ID) error
	ReleaseObjectRetirement(context.Context, ObjectID, ID) error
}

var filesystemOrphansID = ObjectID(sha256.Sum256([]byte("STQR filesystem orphans v1")))

const fsLowerReferenceXattr = "trusted.stqr.lower-reference"

type fsDependencyStore interface {
	PinObjectDependency(context.Context, ObjectID, ID) error
	UnpinObjectDependency(context.Context, ObjectID, ID) error
}

func (f *Filesystem) managedReferences() (fsReferenceStore, bool) {
	if support, ok := f.store.(interface{ ObjectReferencesSupported() bool }); ok && !support.ObjectReferencesSupported() {
		return nil, false
	}
	refs, ok := f.store.(fsReferenceStore)
	_, deletes := f.store.(interface {
		Delete(context.Context, ObjectID, DeleteOptions) error
	})
	return refs, ok && deletes
}

// Only owner-derived blocks can be retired online. Legacy immutable blocks may
// be shared by several inodes and require the complete offline reachability graph.
func (f *Filesystem) ownedBlockIDs(ctx context.Context, owner ObjectID, b fsBlob) ([]ObjectID, error) {
	parts, err := f.segmentsReadOnly(ctx, b)
	if err != nil {
		return nil, err
	}
	var out []ObjectID
	var logical uint64
	for _, p := range parts {
		if !p.lower && p.legacyVersion == 0 && p.id != (ObjectID{}) && (p.id == fsStableID(owner, 0, 0, logical/FSObjectSize) || p.id == fsStableID(owner, 0, 0, logical/4096) || p.id == fsStableID(owner, 2, 0, logical/4096)) {
			out = append(out, p.id)
		}
		logical += p.length
	}
	counts := map[uint8]uint64{}
	var walk func(ObjectID, uint64, uint8) error
	walk = func(id ObjectID, length uint64, depth uint8) error {
		if b.lower || b.source != (ObjectID{}) || depth == 0 || id == (ObjectID{}) {
			return nil
		}
		slot := counts[depth]
		counts[depth]++
		if id == fsStableID(owner, 1, depth, slot) {
			out = append(out, id)
		}
		children, err := f.index(ctx, fsSegment{id: id, length: length}, depth)
		if err != nil {
			return err
		}
		for _, child := range children {
			if err := walk(child.id, child.length, depth-1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(b.root, b.length, b.depth); err != nil {
		return nil, err
	}
	return out, nil
}

func fsReferenceWait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

// Every helper pins the decision object until it stops issuing REDO writes.
// Only an exclusive finalizer may remove inode markers. This drains delayed
// helpers before a subsequent mutation can start, without permanent PUT/CAS
// request receipts or historical file versions.
func (f *Filesystem) helpPinnedTransaction(ctx context.Context, id ObjectID, depth int, pin ID) (bool, error) {
	if depth >= 128 {
		return false, syscall.EAGAIN
	}
	refs, _ := f.managedReferences()
	if pin == (ID{}) {
		pin = NewRequestID()
		for {
			err := refs.PinObject(ctx, id, pin)
			if !errors.Is(err, ErrWouldBlock) {
				if err != nil {
					return false, err
				}
				break
			}
			if err := fsReferenceWait(ctx); err != nil {
				return false, err
			}
		}
	}
	pinned := true
	defer func() {
		if pinned {
			fsDetachedCleanup(ctx, func(clean context.Context) error { return refs.UnpinObject(clean, id, pin) })
		}
	}()
	marker := fsLock(id)
	for round := 0; round < 256; round++ {
		tx, _, raw, err := f.loadIntent(ctx, id)
		if err != nil {
			return false, err
		}
		if tx.state != fsTxPending {
			committed := tx.state == fsTxCommitted
			// Originators already hold a pre-publication pin: no other helper
			// can retire this decision before its result has been observed here.
			if err := refs.UnpinObject(ctx, id, pin); err != nil {
				return false, err
			}
			pinned = false
			claim := NewRequestID()
			for {
				err := refs.ClaimObjectRetirement(ctx, id, claim)
				if !errors.Is(err, ErrWouldBlock) {
					if err != nil {
						return false, err
					}
					break
				}
				if err := fsReferenceWait(ctx); err != nil {
					return false, err
				}
			}
			// The claimed decision now owns admitted in-place writes. Caller
			// cancellation cannot prove that a submitted REDO did not execute:
			// releasing here would let a later writer race its delayed completion.
			// Keep this goroutine and its context values until the critical phase
			// settles. Cancellation before the claim remains interruptible; this
			// completion requests visibility only, never an implicit Sync.
			ctx = context.WithoutCancel(ctx)
			defer fsDetachedCleanup(ctx, func(clean context.Context) error { return refs.ReleaseObjectRetirement(clean, id, claim) })
			current, _, decisionRaw, err := f.loadIntent(ctx, id)
			if errors.Is(err, ErrNotFound) {
				return committed, nil
			}
			if err != nil {
				return false, err
			}
			if current.state == fsTxPending {
				return false, ErrFSFormat
			}
			index := uint64(0)
			var external []fsExternalWrite
			for _, m := range current.members {
				head, _, err := f.store.Get(ctx, m.id)
				if errors.Is(err, ErrNotFound) && current.state == fsTxAborted {
					continue
				}
				if err != nil {
					return false, err
				}
				owns := bytes.Equal(head, marker)
				for _, w := range m.writes {
					if owns && current.state == fsTxCommitted {
						external = append(external, fsExternalWrite{m.id, index, w})
					}
					index++
				}
			}
			if err := f.applyExternalWrites(ctx, id, external, true); err != nil {
				return false, err
			}
			for _, m := range current.members {
				for {
					head, _, err := f.store.Get(ctx, m.id)
					if errors.Is(err, ErrNotFound) && current.state == fsTxAborted {
						break
					}
					if err != nil {
						return false, err
					}
					if !bytes.Equal(head, marker) {
						break
					}
					next := m.before
					if current.state == fsTxCommitted {
						next = m.after
					}
					_, err = f.store.CAS(ctx, m.id, 0, marker, next, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
					if errors.Is(err, ErrPrecondition) {
						continue
					}
					if err != nil {
						return false, err
					}
					break
				}
			}
			f.retirePinnedIntent(ctx, id, current, decisionRaw)
			return current.state == fsTxCommitted, nil
		}
		decision := uint8(fsTxCommitted)
		for _, m := range tx.members {
			head, _, err := f.store.Get(ctx, m.id)
			if errors.Is(err, ErrNotFound) {
				decision = fsTxAborted
				break
			}
			if err != nil {
				return false, err
			}
			if bytes.Equal(head, marker) {
				continue
			}
			other, locked, err := fsLockID(head)
			if err != nil {
				return false, err
			}
			if locked {
				if _, err := f.helpTransaction(ctx, other, depth+1); err != nil && !errors.Is(err, ErrNotFound) {
					return false, err
				}
				decision = fsTxPending
				break
			}
			if !bytes.Equal(head, m.before) {
				decision = fsTxAborted
				break
			}
			_, err = f.store.CAS(ctx, m.id, 0, head, marker, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
			if errors.Is(err, ErrPrecondition) {
				decision = fsTxPending
				break
			}
			if err != nil {
				return false, err
			}
		}
		if decision == fsTxPending {
			continue
		}
		next := bytes.Clone(raw)
		next[6] = decision
		_, err = f.store.CAS(ctx, id, 0, raw, next, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
		if err != nil && !errors.Is(err, ErrPrecondition) {
			return false, err
		}
	}
	return false, syscall.EAGAIN
}

func (f *Filesystem) applyPinnedWrite(ctx context.Context, txID ObjectID, index uint64, w fsBlockWrite) error {
	err := f.applyFSWrite(ctx, txID, index, w)
	if !errors.Is(err, ErrPrecondition) {
		return err
	}
	// An interrupted exclusive finalizer may have written this image already.
	// The inode remains marked, so no newer mutation can have changed the page.
	raw, _, readErr := f.store.Get(ctx, w.id)
	if w.remove && errors.Is(readErr, ErrNotFound) {
		return nil
	}
	if readErr != nil {
		return err
	}
	if w.full && bytes.Equal(raw, w.data) {
		return nil
	}
	if !w.full && !w.remove && w.offset <= uint64(len(raw)) && uint64(len(w.data)) <= uint64(len(raw))-w.offset && bytes.Equal(raw[w.offset:w.offset+uint64(len(w.data))], w.data) {
		return nil
	}
	return err
}

func (f *Filesystem) retirePinnedIntent(ctx context.Context, id ObjectID, tx fsTransaction, raw []byte) {
	del := f.store.(interface {
		Delete(context.Context, ObjectID, DeleteOptions) error
	})
	if err := del.Delete(ctx, id, DeleteOptions{RequestID: NewRequestID()}); err != nil {
		// A WRITE-only filesystem can compact a decision, but cannot bypass
		// DELETE policy. An authorized later fsck reclaims the remainder.
		f.retireIntentPayload(ctx, id, tx, raw, ObjectInfo{})
		return
	}
	if len(raw) >= 15 && string(raw[:4]) == "STFJ" {
		d := Decoder{B: raw}
		d.Take(11)
		count := d.U32()
		for i := uint32(0); i < count; i++ {
			var part ObjectID
			copy(part[:], d.Take(32))
			if d.Err != nil {
				return
			}
			f.removeRedoObject(ctx, part)
		}
	}
}

// Retain registers a visible open reference before exposing a handle. A linked
// inode is checked after pinning, so unlink cannot race the lifetime acquisition.
// Minimal custom stores keep the older conservative, offline-only lifecycle.
func (f *Filesystem) Retain(ctx context.Context, id ObjectID) (RequestID, error) {
	if support, ok := f.store.(interface{ ObjectReferencesSupported() bool }); ok && !support.ObjectReferencesSupported() {
		return RequestID{}, nil
	}
	s, ok := f.store.(fsReferenceStore)
	if !ok {
		return RequestID{}, nil
	}
	token := NewRequestID()
	if err := s.PinObject(ctx, id, token); err != nil {
		return RequestID{}, err
	}
	n, _, err := f.load(ctx, id)
	if err == nil && fsLinks(n) == 0 {
		err = syscall.ENOENT
	}
	if err != nil {
		fsDetachedCleanup(ctx, func(clean context.Context) error { return s.UnpinObject(clean, id, token) })
		return RequestID{}, err
	}
	return token, nil
}

// Release is called only after the last in-flight operation on this handle.
// A read-only caller releases its pin but leaves cleanup to a cooperating writer.
func (f *Filesystem) Release(ctx context.Context, id ObjectID, token RequestID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if token == (RequestID{}) {
		return nil
	}
	s, ok := f.store.(fsReferenceStore)
	if !ok {
		return ErrNotImplemented
	}
	if err := s.UnpinObject(ctx, id, token); err != nil {
		return err
	}
	if !f.readOnly {
		err := f.CollectOrphans(ctx)
		if !errors.Is(err, ErrUnauthorized) {
			return err
		}
	}
	return nil
}

func (o *OverlayFilesystem) Retain(ctx context.Context, id ObjectID) (RequestID, error) {
	token := NewRequestID()
	var held []fsReferenceStore
	for _, f := range []*Filesystem{o.upper, o.lower} {
		if support, known := f.store.(interface{ ObjectReferencesSupported() bool }); known && !support.ObjectReferencesSupported() {
			continue
		}
		if refs, ok := f.store.(fsReferenceStore); ok {
			if err := refs.PinObject(ctx, id, token); err != nil {
				for _, old := range held {
					fsDetachedCleanup(ctx, func(clean context.Context) error { return old.UnpinObject(clean, id, token) })
				}
				return RequestID{}, err
			}
			held = append(held, refs)
		}
	}
	info, err := o.Stat(ctx, id)
	if err == nil && (info.Removed || info.Nlink == 0) {
		err = syscall.ENOENT
	}
	if err != nil {
		for _, old := range held {
			fsDetachedCleanup(ctx, func(clean context.Context) error { return old.UnpinObject(clean, id, token) })
		}
		return RequestID{}, err
	}
	if len(held) == 0 {
		return RequestID{}, nil
	}
	return token, nil
}

func (o *OverlayFilesystem) Release(ctx context.Context, id ObjectID, token RequestID) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if token == (RequestID{}) {
		return nil
	}
	var first error
	for _, f := range []*Filesystem{o.upper, o.lower} {
		if support, known := f.store.(interface{ ObjectReferencesSupported() bool }); known && !support.ObjectReferencesSupported() {
			continue
		}
		if refs, ok := f.store.(fsReferenceStore); ok {
			if err := refs.UnpinObject(ctx, id, token); err != nil && first == nil {
				first = err
			}
		}
	}
	if first != nil {
		return first
	}
	err := o.CollectOrphans(ctx)
	if errors.Is(err, ErrUnauthorized) {
		return nil
	}
	return err
}

func (o *OverlayFilesystem) markUpperOrphan(ctx context.Context, s *overlayState, id ObjectID) error {
	if _, ok := o.upper.managedReferences(); !ok {
		return nil
	}
	child, err := o.upper.snapshot(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil
	} // a whiteout never owns lower data
	if err != nil {
		return err
	}
	n := child.node
	setFSLinks(&n, 0)
	s.orphaned = append(s.orphaned, id)
	s.orphanMembers = append(s.orphanMembers, fsMember(child, n))
	return nil
}

// encodeFSOrphans is stored as the body of a normal STFS file, never an inode
// extension. All implementations share its fixed OID and canonical sorted IDs.
func encodeFSOrphans(ids []ObjectID) []byte {
	e := Encoder{B: []byte("STOL")}
	e.U16(1)
	e.U32(uint32(len(ids)))
	for _, id := range ids {
		e.B = append(e.B, id[:]...)
	}
	return e.B
}

func (f *Filesystem) orphanSnapshot(ctx context.Context, create bool) (fsSnapshot, []ObjectID, error) {
	ctx = context.WithValue(ctx, fsFreshReadKey{}, true)
	for attempt := 0; attempt < 128; attempt++ {
		s, err := f.snapshot(ctx, filesystemOrphansID)
		if errors.Is(err, ErrNotFound) && create {
			body := encodeFSOrphans(nil)
			n := fsNode{kind: fsFile, extended: true, links: 1, mode: 0600, blob: fsBlob{inline: body, length: uint64(len(body)), digest: sha256.Sum256(body)}}
			_, err = f.store.CAS(ctx, filesystemOrphansID, 0, nil, encodeFSNode(n), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
			if err == nil || errors.Is(err, ErrPrecondition) {
				continue
			}
		}
		if err != nil {
			return s, nil, err
		}
		if s.node.kind != fsFile || s.node.blob.length > f.directoryMemoryLimit || s.node.blob.length > uint64(int(^uint(0)>>1)) {
			return s, nil, ErrFSFormat
		}
		raw := make([]byte, int(s.node.blob.length))
		if _, err = f.readBlob(ctx, s.node.blob, raw, 0); err != nil {
			return s, nil, err
		}
		if err = f.checkNodeView(ctx, s.node); errors.Is(err, ErrPrecondition) {
			continue
		} else if err != nil {
			return s, nil, err
		}
		d := Decoder{B: raw}
		if string(d.Take(4)) != "STOL" || d.U16() != 1 {
			return s, nil, ErrFSFormat
		}
		count := d.U32()
		if uint64(count)*32 != uint64(len(d.B)) {
			return s, nil, ErrFSFormat
		}
		ids := make([]ObjectID, count)
		for i := range ids {
			copy(ids[i][:], d.Take(32))
			if ids[i] == (ObjectID{}) || ids[i] == filesystemOrphansID || i > 0 && bytes.Compare(ids[i-1][:], ids[i][:]) >= 0 {
				return s, nil, ErrFSFormat
			}
		}
		return s, ids, d.Done()
	}
	return fsSnapshot{}, nil, ErrWouldBlock
}

func (f *Filesystem) withOrphan(ctx context.Context, members []fsTxMember, id ObjectID) ([]fsTxMember, error) {
	if _, ok := f.managedReferences(); !ok {
		return members, nil
	}
	if _, ok := f.store.(interface {
		Delete(context.Context, ObjectID, DeleteOptions) error
	}); !ok {
		return members, nil
	}
	f.scheduleOrphanCleanup()
	s, ids, err := f.orphanSnapshot(ctx, true)
	if err != nil {
		return nil, err
	}
	for _, existing := range ids {
		if existing == id {
			return append(members, fsMember(s, s.node)), nil
		}
	}
	ids = append(ids, id)
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	n := s.node
	body := encodeFSOrphans(ids)
	if err = f.stableBody(ctx, &n, body); err != nil {
		return nil, err
	}
	n.blob.digest = sha256.Sum256(body)
	n.mtime = uint64(time.Now().UnixNano())
	n.ctime = n.mtime
	return append(members, fsMember(s, n)), nil
}

// CollectOrphans retires only zero-link inodes while holding a generic exclusive
// claim that excludes every open pin. No namespace sweep occurs online. Failure
// during deletion leaves unreachable objects for the explicit offline fsck.
func (f *Filesystem) CollectOrphans(ctx context.Context) error {
	return f.collectOrphans(ctx, nil)
}

// Overlay directory inodes live inside STOM, so their last-reference cleanup
// retires only the explicitly orphan-listed directory record under its ID claim.
func (o *OverlayFilesystem) CollectOrphans(ctx context.Context) error {
	return o.upper.collectOrphans(ctx, func(ctx context.Context, id ObjectID) error {
		return o.edit(ctx, func(s *overlayState) error {
			dir := s.dirs[id]
			if dir == nil || !dir.node.removed {
				return errOverlayUnchanged
			}
			delete(s.dirs, id)
			return nil
		})
	})
}

func (f *Filesystem) collectOrphans(ctx context.Context, virtual func(context.Context, ObjectID) error) error {
	refs, ok := f.managedReferences()
	if !ok || f.readOnly {
		return nil
	}
	del, ok := f.store.(interface {
		Delete(context.Context, ObjectID, DeleteOptions) error
	})
	if !ok {
		return nil
	}
	_, ids, err := f.orphanSnapshot(ctx, false)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, id := range ids {
		token := NewRequestID()
		if err := refs.ClaimObjectRetirement(ctx, id, token); errors.Is(err, ErrWouldBlock) {
			continue
		} else if err != nil {
			return err
		}
		err := func() error {
			defer fsDetachedCleanup(ctx, func(clean context.Context) error { return refs.ReleaseObjectRetirement(clean, id, token) })
			n, _, err := f.load(ctx, id)
			if errors.Is(err, ErrNotFound) {
				if virtual != nil {
					if err := virtual(ctx, id); err != nil {
						return err
					}
				}
				return f.removeOrphan(ctx, id)
			}
			if err != nil {
				return err
			}
			if fsLinks(n) != 0 {
				return f.removeOrphan(ctx, id)
			}
			// Kernel special-file handles bypass the filesystem open callback.
			// Their heads remain for the explicit offline collector across all
			// cooperating implementations, rather than treating absent pins as
			// proof that these handles have closed.
			if n.kind != fsFile && n.kind != fsDirectory {
				return nil
			}
			blocks, err := f.ownedBlockIDs(ctx, id, n.blob)
			if err != nil {
				return err
			}
			owned := map[ObjectID]bool{id: true}
			for _, block := range blocks {
				owned[block] = true
			}
			// Remove the inode before its payload so no new reference can be
			// acquired after this claim is released, even on a partial failure.
			if err = del.Delete(ctx, id, DeleteOptions{RequestID: NewRequestID()}); err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			delete(owned, id)
			for object := range owned {
				if err = del.Delete(ctx, object, DeleteOptions{RequestID: NewRequestID()}); err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
			}
			return f.removeOrphan(ctx, id)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Filesystem) removeOrphan(ctx context.Context, id ObjectID) error {
	for attempt := 0; attempt < 128; attempt++ {
		s, ids, err := f.orphanSnapshot(ctx, false)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		next := make([]ObjectID, 0, len(ids))
		for _, existing := range ids {
			if existing != id {
				next = append(next, existing)
			}
		}
		if len(next) == len(ids) {
			return nil
		}
		n := s.node
		body := encodeFSOrphans(next)
		if err = f.stableBody(ctx, &n, body); err != nil {
			return err
		}
		n.blob.digest = sha256.Sum256(body)
		n.mtime = uint64(time.Now().UnixNano())
		n.ctime = n.mtime
		committed, err := f.transact(ctx, []fsTxMember{fsMember(s, n)})
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}
	return ErrWouldBlock
}

func fsDetachedCleanup(ctx context.Context, fn func(context.Context) error) {
	clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = fn(clean)
}

// A surviving authorized writer retries queued cleanup after a read-only holder
// drops its final pin. Workers are native-client-lifetime bound and stop as soon
// as the queue is empty; newly committed orphans restart them without idle polls.
func (f *Filesystem) scheduleOrphanCleanup() {
	var life context.Context
	var native *Client
	switch store := f.store.(type) {
	case *filesystemNativeStore:
		native = store.Client
		store.mu.Lock()
		offline := store.maintenance != nil
		store.mu.Unlock()
		if offline {
			return
		}
		life = store.life
	case *Client:
		native = store
		store.mu.Lock()
		offline := store.maintenance != nil
		store.mu.Unlock()
		if offline {
			return
		}
		life = store.life
	case *SubscriptionStore:
		native = store.client
		store.client.mu.Lock()
		offline := store.client.maintenance != nil
		store.client.mu.Unlock()
		if offline {
			return
		}
		life = store.life
	}
	if life == nil || f.readOnly {
		return
	}
	f.orphanMu.Lock()
	if f.orphanRunning {
		f.orphanMu.Unlock()
		return
	}
	f.orphanRunning = true
	f.orphanMu.Unlock()
	go func() {
		active := true
		defer func() {
			if active {
				f.orphanMu.Lock()
				f.orphanRunning = false
				f.orphanMu.Unlock()
			}
		}()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-life.Done():
				return
			case <-ticker.C:
				native.mu.Lock()
				offline := native.maintenance != nil
				native.mu.Unlock()
				if offline {
					return
				}
				ctx, cancel := context.WithTimeout(life, 5*time.Second)
				collect := f.orphanCollector
				if collect == nil {
					collect = f.CollectOrphans
				}
				err := collect(ctx)
				if errors.Is(err, ErrUnauthorized) {
					cancel()
					return
				}
				// Hold the startup mutex across the authoritative empty check. A later
				// namespace publication either appears here or starts a fresh worker.
				f.orphanMu.Lock()
				_, ids, peekErr := f.orphanSnapshot(ctx, false)
				if (peekErr == nil || errors.Is(peekErr, ErrNotFound)) && len(ids) == 0 {
					f.orphanRunning = false
					active = false
					f.orphanMu.Unlock()
					cancel()
					return
				}
				f.orphanMu.Unlock()
				cancel()
			}
		}
	}()
}
