// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"io"
)

// ReplicaBinding fences a write against a different range incarnation or replica
// set. SetHash covers the ordered set, its policy version and RAM boot evidence.
// Sequence is assigned by the primary; a timeout never changes this binding.
type ReplicaBinding struct {
	Epoch, Sequence uint64
	SetHash         [32]byte
}

type ReplicaState uint8

// ReplicaAbortError is a retained failed outcome, validated against the
// original request fingerprint before it is returned, across policy changes.
type ReplicaAbortError struct{ Status uint16 }

func (e *ReplicaAbortError) Error() string { return "replica write was aborted before commit" }
func (e *ReplicaAbortError) Unwrap() error { return ErrPrecondition }

const (
	ReplicaPrepared ReplicaState = iota + 1
	ReplicaDecided
	ReplicaPublished
	ReplicaAborted
)

// ReplicaRecord is durable protocol evidence, not a visible object version.
type ReplicaRecord struct {
	Request               RequestRecord
	Binding               ReplicaBinding
	State                 ReplicaState
	AbortStatus           uint16
	PreviousLength        uint64
	operation             uint8
	whole                 bool
	offset, length        uint64
	base, object, receipt uint64
}

// IsSnapshot identifies physical snapshot installation, whose publication does
// not represent an application-level content mutation.
func (r ReplicaRecord) IsSnapshot() bool { return r.operation == opSnapshot }
func (r ReplicaRecord) IsAppend() bool   { return r.operation == opAppend }

const replicaBytes = 256

func replicaKey(id ID) indexKey   { k := requestKey(id); k[0] = 2; return k }
func pendingKey(key Key) indexKey { k := objectKey(key); k[0] = 3; return k }

func (v *Volume) loadReplica(id ID) (ReplicaRecord, bool, error) {
	ptr, err := v.lookup(replicaKey(id))
	if err != nil || ptr == 0 {
		return ReplicaRecord{}, false, err
	}
	r, err := v.replicaAt(ptr)
	if err == nil && r.Request.ID != id {
		err = ErrCorrupt
	}
	return r, err == nil, err
}
func (v *Volume) replicaAt(ptr uint64) (ReplicaRecord, error) {
	r := ReplicaRecord{base: ptr}
	b := make([]byte, replicaBytes)
	if err := v.readBytes(b, ptr); err != nil {
		return r, err
	}
	if string(b[:4]) != "RPL4" || checksum(b[:252]) != diskOrder.Uint32(b[252:]) {
		return r, ErrCorrupt
	}
	r.State = ReplicaState(b[4])
	r.operation, r.whole = b[5], b[6] == 1
	r.offset, r.length = diskOrder.Uint64(b[104:]), diskOrder.Uint64(b[136:])
	r.AbortStatus = diskOrder.Uint16(b[224:])
	copy(r.Request.ID[:], b[8:24])
	copy(r.Request.Key.Bucket[:], b[24:40])
	copy(r.Request.Key.Object[:], b[40:72])
	copy(r.Request.Fingerprint[:], b[72:104])
	r.Request.Result = Result{Info: Info{Length: diskOrder.Uint64(b[112:]), Compression: Compression(b[128]), Deleted: b[129] == 1}, Offset: diskOrder.Uint64(b[120:])}
	r.Binding.Epoch, r.Binding.Sequence = diskOrder.Uint64(b[144:]), diskOrder.Uint64(b[152:])
	copy(r.Binding.SetHash[:], b[160:192])
	r.object = diskOrder.Uint64(b[200:])
	r.receipt = diskOrder.Uint64(b[208:])
	r.PreviousLength = diskOrder.Uint64(b[216:])
	if r.State < ReplicaPrepared || r.State > ReplicaAborted || r.Request.ID == (ID{}) || !r.Request.Result.Info.Compression.valid() || (r.State >= ReplicaPublished) != (r.object == 0) || r.Binding.Epoch == 0 || r.Binding.Sequence == 0 || r.Binding.SetHash == ([32]byte{}) {
		return r, ErrCorrupt
	}
	return r, nil
}
func (t *transaction) saveReplica(r *ReplicaRecord) error {
	if r.base == 0 {
		ptr, err := t.alloc(replicaBytes, true)
		if err != nil {
			return err
		}
		r.base = ptr
	}
	b := make([]byte, replicaBytes)
	copy(b, "RPL4")
	b[4] = byte(r.State)
	b[5] = r.operation
	if r.whole {
		b[6] = 1
	}
	diskOrder.PutUint64(b[104:], r.offset)
	diskOrder.PutUint64(b[136:], r.length)
	copy(b[8:24], r.Request.ID[:])
	copy(b[24:40], r.Request.Key.Bucket[:])
	copy(b[40:72], r.Request.Key.Object[:])
	copy(b[72:104], r.Request.Fingerprint[:])
	diskOrder.PutUint64(b[112:], r.Request.Result.Info.Length)
	diskOrder.PutUint64(b[120:], r.Request.Result.Offset)
	b[128] = byte(r.Request.Result.Info.Compression)
	if r.Request.Result.Info.Deleted {
		b[129] = 1
	}
	diskOrder.PutUint64(b[144:], r.Binding.Epoch)
	diskOrder.PutUint64(b[152:], r.Binding.Sequence)
	copy(b[160:192], r.Binding.SetHash[:])
	diskOrder.PutUint64(b[200:], r.object)
	diskOrder.PutUint64(b[208:], r.receipt)
	diskOrder.PutUint64(b[216:], r.PreviousLength)
	diskOrder.PutUint16(b[224:], r.AbortStatus)
	diskOrder.PutUint32(b[252:], checksum(b[:252]))
	if err := t.patch(r.base, b); err != nil {
		return err
	}
	return t.set(&t.sb.ObjectRoot, replicaKey(r.Request.ID), r.base)
}

func (v *Volume) pending(k Key) (bool, error) {
	ptr, err := v.lookup(pendingKey(k))
	return ptr != 0, err
}

// PrepareMutation supports the six sequenced mutation operations (1=PUT,
// 2=PUT_RANGE, 3=APPEND, 4=TRUNC, 5=DELETE, 6=CAS). It reserves the complete
// replacement and retry evidence in one transaction, without changing readers.
// Payload is streamed and sparse intervals remain sparse. No handle is retained
// while the caller waits for other nodes.
func (h *Handle) PrepareMutation(op uint8, k Key, offset, length uint64, src io.Reader, expected []byte, opts Options, binding ReplicaBinding) (Result, error) {
	return withHandle(h, true, func(v *Volume) (Result, error) {
		if op < opPut || op > opCAS || opts.RequestID == (ID{}) || binding.Epoch == 0 || binding.Sequence == 0 || binding.SetHash == ([32]byte{}) {
			return Result{}, ErrRange
		}
		if op == opCAS {
			if length > 1<<20 || (!opts.WholeObject && uint64(len(expected)) != length) || opts.WholeObject && offset != 0 {
				return Result{}, ErrRange
			}
		}
		opts.prepare = &binding
		opts.Accepted = false // ALL disk prepare is durable even for an accepted client request.
		return v.mutateWithRetryLocked(op, k, offset, length, src, expected, opts)
	})
}

// HasPendingReplica performs one exact-key index lookup. It neither scans
// replica records nor treats a probabilistic membership hint as authority.
func (h *Handle) HasPendingReplica(key Key) (bool, error) {
	return withHandle(h, false, func(v *Volume) (bool, error) { return v.pending(key) })
}

func (h *Handle) ReplicaStatus(id ID) (ReplicaRecord, bool, error) {
	type answer struct {
		r     ReplicaRecord
		found bool
	}
	a, err := withHandle(h, false, func(v *Volume) (answer, error) { r, ok, e := v.loadReplica(id); return answer{r, ok}, e })
	return a.r, a.found, err
}
func (h *Handle) ScanReplicas(fn func(ReplicaRecord) error) error {
	return h.do(false, func(v *Volume) error {
		return v.walkIndexBounds(indexKey{2}, indexKey{3}, func(x indexEntry) error {
			r, e := v.replicaAt(x.Value)
			if e != nil {
				return e
			}
			return fn(r)
		})
	})
}

// RestoreReplicaAbort transfers a terminal failure after a migration fence.
// It cannot import private prepared data or overwrite a published operation.
func (h *Handle) RestoreReplicaAbort(record ReplicaRecord, opts Options) error {
	return h.do(true, func(v *Volume) error {
		if err := v.flushDeferred(); err != nil {
			return err
		}
		if record.State != ReplicaAborted || record.AbortStatus < 400 || record.AbortStatus > 599 || record.Request.ID == (ID{}) || !record.Request.Result.Info.Compression.valid() || record.Binding.Epoch == 0 || record.Binding.Sequence == 0 || record.Binding.SetHash == ([32]byte{}) {
			return ErrRange
		}
		if _, found, err := v.loadDedupe(record.Request.ID); err != nil {
			return err
		} else if found {
			return ErrRequestID
		}
		prior, found, err := v.loadReplica(record.Request.ID)
		if err != nil {
			return err
		}
		if found {
			if prior.State != ReplicaAborted || prior.Request != record.Request || prior.Binding != record.Binding {
				return ErrRequestID
			}
			if prior.AbortStatus == record.AbortStatus {
				return nil
			}
		}
		if pending, err := v.pending(record.Request.Key); err != nil {
			return err
		} else if pending {
			return ErrBusy
		}
		record.base, record.object, record.receipt = prior.base, 0, 0
		t := v.transaction()
		defer t.close()
		if err := t.saveReplica(&record); err != nil {
			return err
		}
		return t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance, opts.GuaranteedAllowance)
	})
}

func (h *Handle) ScanPendingReplicas(fn func(ReplicaRecord) error) error {
	return h.do(false, func(v *Volume) error {
		return v.walkIndexBounds(indexKey{3}, indexKey{4}, func(x indexEntry) error {
			r, err := v.replicaAt(x.Value)
			if err != nil {
				return err
			}
			if r.State == ReplicaPublished {
				return ErrCorrupt
			}
			return fn(r)
		})
	})
}

// DecideReplica records an irreversible commit decision. PublishReplica is a
// separate step: the primary calls it only after every required follower has
// durably published. Repeating either call is safe after lost replies/restarts.
func (h *Handle) DecideReplica(id ID, binding ReplicaBinding) error {
	return h.do(true, func(v *Volume) error {
		r, ok, err := v.loadReplica(id)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		if r.Binding != binding {
			return ErrPrecondition
		}
		if r.State == ReplicaAborted {
			return ErrPrecondition
		}
		if r.State >= ReplicaDecided {
			return nil
		}
		r.State = ReplicaDecided
		t := v.transaction()
		defer t.close()
		if err = t.saveReplica(&r); err != nil {
			return err
		}
		return t.commit(true)
	})
}
func (h *Handle) PublishReplica(id ID, binding ReplicaBinding) (Result, error) {
	return withHandle(h, true, func(v *Volume) (Result, error) {
		if err := v.flushDeferred(); err != nil {
			return Result{}, err
		}
		r, ok, err := v.loadReplica(id)
		if err != nil {
			return Result{}, err
		}
		if !ok {
			return Result{}, ErrNotFound
		}
		if r.Binding != binding || r.State < ReplicaDecided || r.State == ReplicaAborted {
			return Result{}, ErrPrecondition
		}
		if r.State == ReplicaPublished {
			return r.Request.Result, nil
		}
		old, err := v.loadObject(r.Request.Key)
		if errors.Is(err, ErrNotFound) {
			old = nil
		} else if err != nil {
			return Result{}, err
		}
		pending, err := v.lookup(pendingKey(r.Request.Key))
		if err != nil {
			return Result{}, err
		}
		if pending != r.base {
			return Result{}, ErrPrecondition
		}
		o, err := v.loadObjectAt(r.object)
		if err != nil {
			return Result{}, err
		}
		t := v.transaction()
		defer t.close()
		// Prepared payload remains durable until this redo transaction commits.
		// Apply addressed bytes to existing allocations under the volume lock.
		if reusableObject(old, o.Info.Compression) && (r.operation == opPut || r.operation == opPutRange || r.operation == opCAS) {
			staged := o
			next := cloneObject(old)
			next.Base, next.Size = 0, 0
			source := &objectReader{v: v, o: staged, off: r.offset, left: r.length}
			if err = t.writeRange(next, r.offset, r.length, source, o.Info.Compression); err != nil {
				return Result{}, err
			}
			if r.operation == opPut || r.whole {
				next.Spans, err = v.retainSpans(next, o.Info.Length, 0, 0)
				if err != nil {
					return Result{}, err
				}
				for i := range next.Spans {
					if next.Spans[i].Direct || next.Spans[i].Codec != Raw {
						continue
					}
					if err = t.trimRawAllocation(&next.Spans[i]); err != nil {
						return Result{}, err
					}
				}
			}
			next.Info = staged.Info
			next.Segmented = len(next.Spans) > 1
			next.ListBlocks = nil
			next.ListHead, next.ListTail = 0, 0
			if err = t.writeObject(next); err != nil {
				return Result{}, err
			}
			combined := cloneObject(old)
			combined.Spans = append(combined.Spans, staged.Spans...)
			combined.ListBlocks = append(combined.ListBlocks, staged.ListBlocks...)
			releaseUnsharedData(t, combined, next)
			t.release(staged.Base, staged.Size, true)
			o = next
		} else {
			releaseUnsharedData(t, old, o)
		}
		if old != nil {
			t.release(old.Base, old.Size, true)
			if !old.Info.Deleted {
				t.sb.Objects--
				t.sb.LogicalBytes -= old.Info.Length
			}
		}
		if !o.Info.Deleted {
			t.sb.Objects++
			t.sb.LogicalBytes, err = add(t.sb.LogicalBytes, o.Info.Length)
			if err != nil {
				return Result{}, err
			}
		}
		t.sb.ArenaUnused = arenaAfter(t.sb.ArenaUnused, old, o)
		if err = t.set(&t.sb.ObjectRoot, objectKey(r.Request.Key), o.Base); err != nil {
			return Result{}, err
		}
		if err = t.set(&t.sb.ObjectRoot, requestKey(id), r.receipt); err != nil {
			return Result{}, err
		}
		r.State, r.object, r.receipt = ReplicaPublished, 0, 0
		if err = t.saveReplica(&r); err != nil {
			return Result{}, err
		}
		if err = t.remove(&t.sb.ObjectRoot, pendingKey(r.Request.Key)); err != nil {
			return Result{}, err
		}
		if err = t.commit(true); err != nil {
			return Result{}, err
		}
		v.cache.remove(r.Request.Key)
		return r.Request.Result, nil
	})
}

// AbortReplica requires an explicit primary abort. Absence of a commit response
// or expiry of a timeout is never authorization to call it.
func (h *Handle) AbortReplica(id ID, binding ReplicaBinding) error {
	return h.abortReplica(id, binding, false, 412)
}

// AbortReplicaPending retains an outbox reference until ALL peers have learned
// the primary's durable abort. Status is the retained original failure code.
func (h *Handle) AbortReplicaPending(id ID, binding ReplicaBinding, status uint16) error {
	if status < 400 || status > 599 {
		return ErrRange
	}
	return h.abortReplica(id, binding, true, status)
}
func (h *Handle) abortReplica(id ID, binding ReplicaBinding, hold bool, status uint16) error {
	return h.do(true, func(v *Volume) error {
		r, ok, err := v.loadReplica(id)
		if err != nil || !ok {
			return err
		}
		if r.Binding == binding && r.State == ReplicaAborted {
			return nil
		}
		if r.Binding != binding || r.State != ReplicaPrepared {
			return ErrPrecondition
		}
		o, err := v.loadObjectAt(r.object)
		if err != nil {
			return err
		}
		t := v.transaction()
		defer t.close()
		old, err := v.loadObject(r.Request.Key)
		if errors.Is(err, ErrNotFound) {
			old = nil
		} else if err != nil {
			return err
		}
		releaseUnsharedData(t, o, old)
		t.release(o.Base, o.Size, true)
		t.release(r.receipt, receiptBytes, true)
		r.State, r.object, r.receipt = ReplicaAborted, 0, 0
		r.AbortStatus = status
		if err = t.saveReplica(&r); err != nil {
			return err
		}
		if !hold {
			if err = t.remove(&t.sb.ObjectRoot, pendingKey(r.Request.Key)); err != nil {
				return err
			}
		}
		return t.commit(true)
	})
}

func (h *Handle) FinishReplicaAbort(id ID, binding ReplicaBinding) error {
	return h.do(true, func(v *Volume) error {
		r, ok, err := v.loadReplica(id)
		if err != nil {
			return err
		}
		if !ok || r.Binding != binding || r.State != ReplicaAborted {
			return ErrPrecondition
		}
		ptr, err := v.lookup(pendingKey(r.Request.Key))
		if err != nil || ptr == 0 {
			return err
		}
		if ptr != r.base {
			return ErrPrecondition
		}
		t := v.transaction()
		defer t.close()
		if err = t.remove(&t.sb.ObjectRoot, pendingKey(r.Request.Key)); err != nil {
			return err
		}
		return t.commit(true)
	})
}
