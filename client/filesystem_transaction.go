// SPDX-License-Identifier: GPL-3.0-only
package client

// Namespace transactions use ordinary bucket objects. A durable descriptor
// lists expected inode images and replacements; small inode markers
// refer to it. Readers help finish interrupted transactions before using an
// inode. All participants are acquired in OID order, avoiding lock cycles.

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"syscall"
)

const (
	fsTxPending uint8 = iota
	fsTxCommitted
	fsTxAborted
)

type fsSnapshot struct {
	id   ObjectID
	node fsNode
	info ObjectInfo
	raw  []byte
}
type fsTxMember struct {
	id            ObjectID
	before, after []byte
	writes        []fsBlockWrite
}
type fsTransaction struct {
	state   uint8
	members []fsTxMember
}

func fsLock(id ObjectID) []byte {
	e := Encoder{B: []byte("STFL")}
	e.U16(1)
	e.B = append(e.B, id[:]...)
	return e.B
}
func fsLockID(raw []byte) (ObjectID, bool, error) {
	var id ObjectID
	if len(raw) < 4 || string(raw[:4]) != "STFL" {
		return id, false, nil
	}
	d := Decoder{B: raw}
	d.Take(4)
	if d.U16() != 1 {
		return id, true, ErrFSFormat
	}
	copy(id[:], d.Take(32))
	if d.Done() != nil || id == (ObjectID{}) {
		return id, true, ErrFSFormat
	}
	return id, true, nil
}
func encodeFSTransaction(tx fsTransaction) []byte {
	e := Encoder{B: []byte("STFT")}
	version := uint16(3)
	e.U16(version)
	e.U8(tx.state)
	e.U32(uint32(len(tx.members)))
	for _, m := range tx.members {
		e.B = append(e.B, m.id[:]...)
		e.Blob(m.before)
		e.Blob(m.after)
		if version >= 2 {
			encodeFSWrites(&e, m.writes)
		}
	}
	return e.B
}
func decodeFSTransaction(raw []byte, id ObjectID) (fsTransaction, error) {
	var tx fsTransaction
	d := Decoder{B: raw}
	magic := string(d.Take(4))
	version := d.U16()
	if len(raw) > fsMaxIntentBytes || magic != "STFT" || (version != 1 && version != 2 && version != 3) {
		return tx, ErrFSFormat
	}
	tx.state = d.U8()
	count := d.U32()
	if tx.state > fsTxAborted || count == 0 || count > 1024 || uint64(count) > uint64(len(d.B))/42 {
		return tx, ErrFSFormat
	}
	for i := uint32(0); i < count; i++ {
		var m fsTxMember
		copy(m.id[:], d.Take(32))
		if version < 3 {
			d.U64()
		} // obsolete per-object precondition, never used
		m.before = d.Blob()
		m.after = d.Blob()
		if version >= 2 {
			var err error
			m.writes, err = decodeFSWrites(&d, version < 3)
			if err != nil {
				return tx, err
			}
		}
		if d.Err != nil || m.id == (ObjectID{}) || m.id == id || i > 0 && bytes.Compare(tx.members[i-1].id[:], m.id[:]) >= 0 {
			return tx, ErrFSFormat
		}
		if len(m.before) >= 4 && string(m.before[:4]) == "STOR" {
			if m.id != FilesystemRootID() {
				return tx, ErrFSFormat
			}
			before, _, e := decodeOverlayRoot(m.before)
			if e != nil {
				return tx, e
			}
			after, _, e := decodeOverlayRoot(m.after)
			if e != nil || before != after {
				return tx, ErrFSFormat
			}
		} else {
			if _, err := decodeFSNode(m.before); err != nil {
				return tx, err
			}
			if _, err := decodeFSNode(m.after); err != nil {
				return tx, err
			}
		}
		tx.members = append(tx.members, m)
	}
	if d.Done() != nil {
		return tx, ErrFSFormat
	}
	return tx, nil
}

func (f *Filesystem) snapshot(ctx context.Context, id ObjectID) (fsSnapshot, error) {
	for attempt := 0; attempt < 128; attempt++ {
		raw, info, err := f.store.Get(ctx, id)
		if err != nil {
			return fsSnapshot{}, err
		}
		txID, locked, err := fsLockID(raw)
		if err != nil {
			return fsSnapshot{}, err
		}
		if locked {
			if _, err = f.helpTransaction(ctx, txID, 0); err != nil {
				if errors.Is(err, ErrNotFound) || errors.Is(err, ErrWouldBlock) {
					continue
				}
				return fsSnapshot{}, err
			}
			continue
		}
		node, err := decodeFSNode(raw)
		node.identity = id
		node.head = bytes.Clone(raw)
		return fsSnapshot{id, node, info, raw}, err
	}
	return fsSnapshot{}, syscall.EAGAIN
}

// A reader with no CAS grant can still observe a marker's logical snapshot.
// PENDING exposes the old committed image; COMMITTED exposes the new image.
// Mutators always use snapshot(), so a virtual inode image cannot overwrite a lock.
func (f *Filesystem) readLockedSnapshot(ctx context.Context, id ObjectID) (fsNode, ObjectInfo, error) {
	for attempt := 0; attempt < 128; attempt++ {
		n, info, err := f.readLockedSnapshotOnce(ctx, id)
		if !errors.Is(err, ErrPrecondition) {
			return n, info, err
		}
	}
	return fsNode{}, ObjectInfo{}, syscall.EAGAIN
}
func (f *Filesystem) readLockedSnapshotOnce(ctx context.Context, id ObjectID) (fsNode, ObjectInfo, error) {
	raw, info, err := f.store.Get(ctx, id)
	if err != nil {
		return fsNode{}, info, err
	}
	txID, locked, err := fsLockID(raw)
	if err != nil {
		return fsNode{}, info, err
	}
	if !locked {
		n, e := decodeFSNode(raw)
		n.identity = id
		n.head = bytes.Clone(raw)
		return n, info, e
	}
	tx, _, txRaw, err := f.loadIntent(ctx, txID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			err = ErrPrecondition
		}
		return fsNode{}, info, err
	}
	if len(tx.members) == 0 {
		return fsNode{}, info, ErrPrecondition
	}
	for _, m := range tx.members {
		if m.id == id {
			image := m.before
			if tx.state == fsTxCommitted {
				image = m.after
			}
			n, e := decodeFSNode(image)
			n.identity = id
			n.head = bytes.Clone(raw)
			n.guardTx = txID
			n.guardRaw = bytes.Clone(txRaw)
			if tx.state == fsTxCommitted {
				n.blob.view = fsWriteView(m.writes)
			}
			info.Length = uint64(len(image))
			return n, info, e
		}
	}
	return fsNode{}, info, ErrFSFormat
}

func (f *Filesystem) transact(ctx context.Context, members []fsTxMember) (bool, error) {
	ctx = context.WithValue(ctx, fsDurableKey{}, true)
	if len(members) == 0 || len(members) > 1024 {
		return false, syscall.E2BIG
	}
	sort.Slice(members, func(i, j int) bool { return bytes.Compare(members[i].id[:], members[j].id[:]) < 0 })
	for i, m := range members {
		if m.id == (ObjectID{}) || i > 0 && m.id == members[i-1].id {
			return false, ErrFSFormat
		}
	}
	raw := encodeFSTransaction(fsTransaction{members: members})
	if len(raw) > fsMaxIntentBytes {
		return false, syscall.E2BIG
	}
	if refs, ok := f.managedReferences(); ok {
		id, pin := newFSID(), NewRequestID()
		if err := refs.PinObject(ctx, id, pin); err != nil {
			return false, err
		}
		if _, err := f.storeIntentAt(ctx, id, raw); err != nil {
			fsDetachedCleanup(ctx, func(clean context.Context) error { return refs.UnpinObject(clean, id, pin) })
			return false, err
		}
		return f.helpPinnedTransaction(ctx, id, 0, pin)
	}
	id, err := f.storeIntent(ctx, raw)
	if err != nil {
		return false, err
	}
	return f.helpTransaction(ctx, id, 0)
}

// Recovery under exclusive fsck may complete intents and change inode images.
// Reach a stable namespace before taking the sweep inventory; old descriptors
// become ordinary garbage after every reachable marker has been resolved.
func (f *Filesystem) recoverNamespace(ctx context.Context) error {
	for {
		recovered := false
		seen := map[ObjectID]bool{}
		visiting := map[ObjectID]bool{}
		var walk func(ObjectID, int) error
		walk = func(id ObjectID, depth int) error {
			if depth > 1024 || visiting[id] {
				return ErrFSFormat
			}
			if seen[id] {
				return nil
			}
			seen[id] = true
			raw, _, err := f.store.Get(ctx, id)
			if err != nil {
				return err
			}
			_, locked, err := fsLockID(raw)
			if err != nil {
				return err
			}
			s, err := f.snapshot(ctx, id)
			if err != nil {
				return err
			}
			if locked {
				recovered = true
			}
			n := s.node
			if id == FilesystemRootID() && n.kind != fsDirectory {
				return ErrFSFormat
			}
			if n.kind == fsDirectory {
				entries, err := f.directory(ctx, n)
				if err != nil {
					return err
				}
				visiting[id] = true
				defer delete(visiting, id)
				for _, entry := range entries {
					if err = walk(entry.ID, depth+1); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if err := walk(FilesystemRootID(), 0); err != nil {
			if recovered && (errors.Is(err, syscall.ENOENT) || errors.Is(err, ErrNotFound)) {
				continue
			}
			return err
		}
		if !recovered {
			return nil
		}
	}
}

func (f *Filesystem) helpTransaction(ctx context.Context, id ObjectID, depth int) (bool, error) {
	if _, ok := f.managedReferences(); ok {
		return f.helpPinnedTransaction(ctx, id, depth, ID{})
	}
	ctx = context.WithValue(ctx, fsDurableKey{}, true)
	if depth >= 128 {
		return false, syscall.EAGAIN
	}
	marker := fsLock(id)
	for round := 0; round < 256; round++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		tx, _, raw, err := f.loadIntent(ctx, id)
		if err != nil {
			return false, err
		}
		if len(raw) >= 7 && string(raw[:4]) == "STFD" {
			return raw[6] == fsTxCommitted, nil
		}
		if tx.state != fsTxPending {
			writeIndex := uint64(0)
			if tx.state == fsTxCommitted {
				for _, m := range tx.members {
					current, _, e := f.store.Get(ctx, m.id)
					if e != nil {
						return false, e
					}
					owns := bytes.Equal(current, marker)
					for _, w := range m.writes {
						if owns {
							if e = f.applyFSWrite(ctx, id, writeIndex, w); e != nil {
								// A delayed helper cannot modify a newer inode.
								head, _, readErr := f.store.Get(ctx, m.id)
								if readErr != nil || bytes.Equal(head, marker) {
									return false, e
								}
							}
						}
						writeIndex++
					}
				}
			}
			// Final state is durable before removing any marker. A concurrent
			// writer may already have changed an inode after another helper
			// removed its marker; only our own markers can be replaced here.
			for _, m := range tx.members {
				for {
					current, _, e := f.store.Get(ctx, m.id)
					if e != nil {
						return false, e
					}
					if !bytes.Equal(current, marker) {
						break
					}
					next := m.before
					if tx.state == fsTxCommitted {
						next = m.after
					}
					_, e = f.store.CAS(ctx, m.id, 0, current, next, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
					if errors.Is(e, ErrPrecondition) {
						continue
					}
					if e != nil {
						return false, e
					}
					break
				}
			}
			f.retireIntentPayload(ctx, id, tx, raw, ObjectInfo{})
			return tx.state == fsTxCommitted, nil
		}
		decision := uint8(fsTxCommitted)
		for _, m := range tx.members {
			current, _, e := f.store.Get(ctx, m.id)
			if e != nil {
				return false, e
			}
			if bytes.Equal(current, marker) {
				continue
			}
			other, locked, e := fsLockID(current)
			if e != nil {
				return false, e
			}
			if locked {
				if _, e = f.helpTransaction(ctx, other, depth+1); e != nil {
					return false, e
				}
				decision = fsTxPending
				break // reload our decision and this inode
			}
			if !bytes.Equal(current, m.before) {
				decision = fsTxAborted
				break
			}
			// Compare the complete observed inode under the storage lock. A late
			// marker still refers to this durable transaction descriptor and is
			// resolved using its committed/aborted decision.
			_, e = f.store.CAS(ctx, m.id, 0, current, marker, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
			if errors.Is(e, ErrPrecondition) {
				decision = fsTxPending
				break
			}
			if e != nil {
				return false, e
			}
		}
		if decision == fsTxPending {
			continue
		}
		tx.state = decision
		nextRaw := bytes.Clone(raw)
		nextRaw[6] = decision
		_, err = f.store.CAS(ctx, id, 0, raw, nextRaw, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
		if err != nil && !errors.Is(err, ErrPrecondition) {
			return false, err
		}
	}
	return false, syscall.EAGAIN
}

func fsMember(s fsSnapshot, next fsNode) fsTxMember {
	return fsTxMember{id: s.id, before: s.raw, after: encodeFSNode(next), writes: next.edits}
}
