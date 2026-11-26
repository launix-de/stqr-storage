// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
)

const fsMaxIntentBytes = 128 << 20

type fsDurableKey struct{}
type fsAcceptedBurstKey struct{}
type fsExternalWrite struct {
	owner ObjectID
	index uint64
	write fsBlockWrite
}

func encodeFSWrites(e *Encoder, writes []fsBlockWrite) {
	e.U32(uint32(len(writes)))
	for _, w := range writes {
		e.B = append(e.B, w.id[:]...)
		e.Bool(w.absent)
		e.Bool(w.full)
		e.Bool(w.remove)
		e.U64(w.offset)
		e.Blob(w.data)
		e.Blob(w.before)
	}
}
func decodeFSWrites(d *Decoder, legacy bool) ([]fsBlockWrite, error) {
	count := d.U32()
	if count > 1<<20 || uint64(count) > uint64(len(d.B))/44 {
		return nil, ErrFSFormat
	}
	writes := make([]fsBlockWrite, 0, count)
	seen := map[ObjectID]bool{}
	for i := uint32(0); i < count; i++ {
		var w fsBlockWrite
		copy(w.id[:], d.Take(32))
		if legacy {
			d.U64()
		}
		w.absent = d.Bool()
		w.full = d.Bool()
		w.remove = d.Bool()
		w.offset = d.U64()
		w.data = bytes.Clone(d.Blob())
		if !legacy {
			w.before = bytes.Clone(d.Blob())
		}
		if d.Err != nil || w.id == (ObjectID{}) || seen[w.id] || w.offset > FSObjectSize || uint64(len(w.data)) > FSObjectSize-w.offset || w.full && w.offset != 0 || w.remove && (w.absent || w.full || w.offset != 0 || len(w.data) != 0) {
			return nil, ErrFSFormat
		}
		seen[w.id] = true
		writes = append(writes, w)
	}
	return writes, nil
}

func (f *Filesystem) storeIntent(ctx context.Context, raw []byte) (ObjectID, error) {
	return f.storeIntentAt(ctx, newFSID(), raw)
}
func (f *Filesystem) storeIntentAt(ctx context.Context, id ObjectID, raw []byte) (ObjectID, error) {
	if len(raw) > fsMaxIntentBytes {
		return ObjectID{}, ErrFSFormat
	}
	if len(raw) <= 64<<10 {
		_, err := f.store.CAS(ctx, id, 0, nil, raw, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
		return id, err
	}
	// One small mutable decision object, bounded immutable REDO pieces only for
	// the duration of this operation. These are not versions of filesystem blocks.
	e := Encoder{B: []byte("STFJ")}
	e.U16(1)
	e.U8(fsTxPending)
	e.U32(uint32(len(raw)))
	e.U32(uint32((len(raw) + FSObjectSize - 1) / FSObjectSize))
	parts := make([]ObjectID, (len(raw)+FSObjectSize-1)/FSObjectSize)
	if err := f.objectBurst(ctx, len(parts), func(ctx context.Context, i int) error {
		at := i * FSObjectSize
		var err error
		parts[i], err = f.putImmutable(ctx, raw[at:min(at+FSObjectSize, len(raw))])
		return err
	}); err != nil {
		return ObjectID{}, err
	}
	// Only the final descriptor publishes these independent, already durable
	// pieces. Their completion order has no bearing on the encoded chunk order.
	for _, part := range parts {
		e.B = append(e.B, part[:]...)
	}
	_, err := f.store.CAS(ctx, id, 0, nil, e.B, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID(), Absent: true}})
	return id, err
}
func (f *Filesystem) loadIntent(ctx context.Context, id ObjectID) (fsTransaction, ObjectInfo, []byte, error) {
	raw, info, err := f.store.Get(ctx, id)
	if err != nil {
		return fsTransaction{}, info, nil, err
	}
	if len(raw) == 7 && string(raw[:4]) == "STFD" && raw[4] == 1 && raw[5] == 0 && (raw[6] == fsTxCommitted || raw[6] == fsTxAborted) {
		return fsTransaction{state: raw[6]}, info, raw, nil
	}
	if len(raw) < 4 || string(raw[:4]) != "STFJ" {
		tx, e := decodeFSTransaction(raw, id)
		return tx, info, raw, e
	}
	d := Decoder{B: raw}
	d.Take(4)
	if d.U16() != 1 {
		return fsTransaction{}, info, nil, ErrFSFormat
	}
	state := d.U8()
	length, count := d.U32(), d.U32()
	if state > fsTxAborted || length <= 64<<10 || length > fsMaxIntentBytes || count != (length+FSObjectSize-1)/FSObjectSize || uint64(count)*32 != uint64(len(d.B)) {
		return fsTransaction{}, info, nil, ErrFSFormat
	}
	parts := make([]ObjectID, int(count))
	for i := range parts {
		copy(parts[i][:], d.Take(32))
		if parts[i] == id || parts[i] == (ObjectID{}) {
			return fsTransaction{}, info, nil, ErrFSFormat
		}
	}
	assembled := make([]byte, int(length))
	loadErr := f.objectBurst(ctx, len(parts), func(ctx context.Context, i int) error {
		data, _, err := f.store.Get(ctx, parts[i])
		want := min(FSObjectSize, int(length)-i*FSObjectSize)
		if err != nil {
			return err
		}
		if len(data) != want {
			return ErrFSFormat
		}
		copy(assembled[i*FSObjectSize:], data)
		return nil
	})
	if loadErr != nil {
		if errors.Is(loadErr, ErrNotFound) || errors.Is(loadErr, ErrFSFormat) {
			done, doneInfo, re := f.store.Get(ctx, id)
			if re == nil && len(done) == 7 && string(done[:6]) == "STFD\x01\x00" && (done[6] == fsTxCommitted || done[6] == fsTxAborted) {
				return fsTransaction{state: done[6]}, doneInfo, done, nil
			}
		}
		return fsTransaction{}, info, nil, loadErr
	}
	tx, e := decodeFSTransaction(assembled, id)
	tx.state = state
	return tx, info, raw, e
}
func fsRedoRequest(id ObjectID, index uint64) RequestID {
	e := Encoder{B: append([]byte("STQR filesystem REDO request v1\x00"), id[:]...)}
	e.U64(index)
	sum := sha256.Sum256(e.B)
	var out RequestID
	copy(out[:], sum[:16])
	return out
}
func (f *Filesystem) applyFSWrite(ctx context.Context, txID ObjectID, index uint64, w fsBlockWrite) error {
	accepted, _ := ctx.Value(fsAcceptedBurstKey{}).(bool)
	opt := MutationOptions{RequestID: fsRedoRequest(txID, index), Accepted: accepted}
	if w.absent {
		opt.Absent = true
	}
	if w.remove {
		if d, ok := f.store.(interface {
			Delete(context.Context, ObjectID, DeleteOptions) error
		}); ok {
			err := d.Delete(ctx, w.id, DeleteOptions{RequestID: opt.RequestID, Accepted: accepted})
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
		_, err := f.casOwnedBlock(ctx, w.id, w.before, nil, opt)
		return err
	}
	if w.full {
		_, err := f.casOwnedBlock(ctx, w.id, w.before, w.data, opt)
		return err
	}
	if r, ok := f.store.(interface {
		PutRange(context.Context, ObjectID, uint64, []byte, PutOptions) (ObjectInfo, error)
	}); ok {
		_, err := r.PutRange(ctx, w.id, w.offset, w.data, opt)
		return err
	}
	// Minimal custom backends can implement the same contract through whole-
	// object CAS. Native stores always use the small ranged mutation above.
	raw, _, err := f.store.Get(ctx, w.id)
	before := bytes.Clone(w.before)
	if w.absent && errors.Is(err, ErrNotFound) {
		err = nil
		raw = nil
	}
	if err != nil {
		return err
	}
	end := w.offset + uint64(len(w.data))
	if end > uint64(len(raw)) {
		raw = append(raw, make([]byte, end-uint64(len(raw)))...)
	}
	copy(raw[w.offset:], w.data)
	_, err = f.store.CAS(ctx, w.id, 0, before, raw, CASOptions{Scope: CASObject, MutationOptions: opt})
	return err
}

// Retire redo payload only after every inode marker is durably gone. A tiny
// completed descriptor remains until ordinary fsck so delayed helpers can still
// recognize completion without finding a dangling marker. Payloads do not grow
// with the number of successful overwrites.
func (f *Filesystem) retireIntentPayload(ctx context.Context, id ObjectID, tx fsTransaction, raw []byte, info ObjectInfo) {
	// Keep original namespace-only v1 descriptors for compatibility with existing
	// mounts. Mutable-block intents use the compact terminal form below.
	hasWrites := false
	for _, m := range tx.members {
		hasWrites = hasWrites || len(m.writes) > 0
	}
	if !hasWrites {
		return
	}
	done := Encoder{B: []byte("STFD")}
	done.U16(1)
	done.U8(tx.state)
	if _, err := f.store.CAS(ctx, id, 0, raw, done.B, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: fsRedoRequest(id, ^uint64(0))}}); err != nil {
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
func (f *Filesystem) removeRedoObject(ctx context.Context, id ObjectID) {
	raw, _, err := f.store.Get(ctx, id)
	if err != nil {
		return
	}
	if d, ok := f.store.(interface {
		Delete(context.Context, ObjectID, DeleteOptions) error
	}); ok {
		_ = d.Delete(ctx, id, DeleteOptions{RequestID: NewRequestID()})
		return
	}
	_, _ = f.store.CAS(ctx, id, 0, raw, nil, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
}

// The overlay namespace root uses the same inode intent protocol. It remains
// an ordinary bucket object; no frontend-specific server command is required.
func (f *Filesystem) overlayRootImage(ctx context.Context) ([]byte, ObjectInfo, fsNode, error) {
	root := FilesystemRootID()
	for attempt := 0; attempt < 128; attempt++ {
		raw, info, err := f.store.Get(ctx, root)
		if err != nil {
			return nil, info, fsNode{}, err
		}
		txID, locked, err := fsLockID(raw)
		if err != nil {
			return nil, info, fsNode{}, err
		}
		guard := fsNode{identity: root, head: bytes.Clone(raw)}
		if !locked {
			return raw, info, guard, nil
		}
		if !f.readOnly {
			if _, err = f.helpTransaction(ctx, txID, 0); err == nil || errors.Is(err, ErrNotFound) {
				continue
			}
			if !errors.Is(err, ErrUnauthorized) {
				return nil, info, guard, err
			}
		}
		tx, _, txRaw, err := f.loadIntent(ctx, txID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, info, guard, err
		}
		if len(tx.members) == 0 {
			continue
		}
		for _, member := range tx.members {
			if member.id == root {
				image := member.before
				guard.guardTx = txID
				guard.guardRaw = bytes.Clone(txRaw)
				if tx.state == fsTxCommitted {
					image = member.after
					guard.blob.view = fsWriteView(member.writes)
				}
				return bytes.Clone(image), info, guard, nil
			}
		}
		return nil, info, guard, ErrFSFormat
	}
	return nil, ObjectInfo{}, fsNode{}, ErrWouldBlock
}

// The recoverable inode intent owns this block. Actual bytes are still compared
// for minimal CAS-only stores; native PUT remains an ordinary in-place write.
func (f *Filesystem) casOwnedBlock(ctx context.Context, id ObjectID, before, data []byte, opt MutationOptions) (ObjectInfo, error) {
	if p, ok := f.store.(interface {
		Put(context.Context, ObjectID, []byte, PutOptions) (ObjectInfo, error)
	}); ok {
		return p.Put(ctx, id, data, opt)
	}
	return f.store.CAS(ctx, id, 0, before, data, CASOptions{Scope: CASObject, MutationOptions: opt})
}

// objectBurst overlaps independent immutable object I/O. Custom stores retain
// their previous serial contract unless they explicitly opt into concurrency.
// Dependent inode markers and the final decision never move across this join.
func (f *Filesystem) objectBurst(ctx context.Context, count int, run func(context.Context, int) error) error {
	parallel, ok := f.store.(interface{ ConcurrentObjectRequests() bool })
	if !ok || !parallel.ConcurrentObjectRequests() {
		for i := 0; i < count; i++ {
			if err := run(ctx, i); err != nil {
				return err
			}
		}
		return nil
	}
	ctx = context.WithValue(ctx, filesystemWaitKey{}, true)
	errs := make([]error, count)
	var work sync.WaitGroup
	for i := 0; i < count; i++ {
		work.Add(1)
		go func(i int) { defer work.Done(); errs[i] = run(ctx, i) }(i)
	}
	work.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// applyExternalWrites runs only after the durable REDO decision and marker
// ownership checks. Distinct objects are independent; a repeated object starts
// another wave. Every accepted wave is fenced before a later wave or any inode
// can publish its final image, so helpers can recover an interrupted burst.
func (f *Filesystem) applyExternalWrites(ctx context.Context, txID ObjectID, writes []fsExternalWrite, pinned bool) error {
	parallel, ok := f.store.(interface{ ConcurrentObjectRequests() bool })
	syncer, canSync := f.store.(interface{ Sync(context.Context) error })
	accepted := ok && parallel.ConcurrentObjectRequests() && canSync
	if accepted {
		ctx = context.WithValue(ctx, fsAcceptedBurstKey{}, true)
	}
	for len(writes) > 0 {
		seen := map[ObjectID]bool{}
		end := 0
		for end < len(writes) && !seen[writes[end].write.id] {
			seen[writes[end].write.id] = true
			end++
		}
		wave := writes[:end]
		err := f.objectBurst(ctx, len(wave), func(ctx context.Context, i int) error {
			item := wave[i]
			if pinned {
				return f.applyPinnedWrite(ctx, txID, item.index, item.write)
			}
			err := f.applyFSWrite(ctx, txID, item.index, item.write)
			if err != nil {
				head, _, readErr := f.store.Get(ctx, item.owner)
				if readErr == nil && !bytes.Equal(head, fsLock(txID)) {
					return nil
				}
			}
			return err
		})
		// Checkpoint successes even when another independent operation failed. Keep
		// all inode markers and REDO payload on either operation or fence failure.
		if accepted {
			if fenceErr := syncer.Sync(ctx); err == nil {
				err = fenceErr
			}
		}
		if err != nil {
			return err
		}
		writes = writes[end:]
	}
	return nil
}
