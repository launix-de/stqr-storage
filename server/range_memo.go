// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"errors"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Transfer progress is advisory, indexed on disk instead of retained once per
// object in either the control log or RAM. Canonical data is flushed first;
// READY still certifies the entire snapshot through the durable phase journal.
// A missing memo merely causes another copy, never an ownership decision.
var rangeMemoBucket = func() volume.ID {
	sum := sha256.Sum256([]byte("STQR private range transfer memos v1"))
	var id volume.ID
	copy(id[:], sum[:16])
	return id
}()

func rangeMemoKey(plan [32]byte, object volume.ObjectID) volume.Key {
	e := client.Encoder{}
	e.B = append(e.B, []byte("STQR transfer memo v1\x00")...)
	e.B = append(e.B, plan[:]...)
	e.B = append(e.B, object[:]...)
	return volume.Key{Bucket: rangeMemoBucket, Object: volume.ObjectID(sha256.Sum256(e.B))}
}

func (r *rangeManager) objectMemo(plan [32]byte, key volume.Key) (rangeObjectMemo, bool, error) {
	h, err := r.journal.store.Open(false)
	if err != nil {
		return rangeObjectMemo{}, false, err
	}
	data, _, err := h.Get(rangeMemoKey(plan, key.Object))
	h.Close()
	if errors.Is(err, volume.ErrNotFound) {
		return rangeObjectMemo{}, false, nil
	}
	if err != nil {
		return rangeObjectMemo{}, false, err
	}
	d := client.Decoder{B: data}
	got := d.Take(32)
	memo := rangeObjectMemo{}
	memo.Key, memo.Info = decodeRangeInfo(&d)
	memo.Codec = volume.Compression(d.U8())
	copy(memo.Digest[:], d.Take(32))
	if d.Done() != nil || string(got) != string(plan[:]) || memo.Key != key || memo.Digest == ([32]byte{}) {
		return rangeObjectMemo{}, false, volume.ErrCorrupt
	}
	return memo, true, nil
}

func (r *rangeManager) storeObjectMemo(plan [32]byte, memo rangeObjectMemo) error {
	e := client.Encoder{}
	e.B = append(e.B, plan[:]...)
	encodeRangeInfo(&e, memo.Key, memo.Info)
	e.U8(uint8(memo.Codec))
	e.B = append(e.B, memo.Digest[:]...)
	return r.journal.writeObject(rangeMemoKey(plan, memo.Key.Object), e.B)
}

// writeObject shares the private journal's growth/close serialization, while
// leaving its append sequence and hash chain untouched.
func (j *controlJournal) writeObject(key volume.Key, data []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	for {
		h, err := j.store.Open(true)
		if err != nil {
			return err
		}
		_, err = h.Put(key, data, volume.Options{})
		h.Close()
		if !errors.Is(err, volume.ErrFull) || errors.Is(err, volume.ErrRecovery) {
			return err
		}
		size := j.store.Size()
		if size > uint64(^uint64(0)>>1)/2 {
			return volume.ErrFull
		}
		if err := j.store.Grow(int64(size * 2)); err != nil {
			return err
		}
	}
}

// collectMemos reclaims one bounded page after completed handoffs. It never
// deletes active progress and does not put a per-object fsync on the DONE path.
// The cursor is advisory too: a restart can scan the private index from zero.
func (r *rangeManager) collectMemos() error {
	r.mu.Lock()
	cursor := r.memoCursor
	r.mu.Unlock()
	h, err := r.journal.store.Open(false)
	if err != nil {
		return err
	}
	records, err := h.ScanObjects(cursor, 128)
	if err != nil {
		h.Close()
		return err
	}
	var candidates []struct {
		record volume.ObjectRecord
		id     [32]byte
	}
	for _, record := range records {
		if record.Key.Bucket != rangeMemoBucket {
			continue
		}
		data, _, err := h.Get(record.Key)
		if err != nil {
			h.Close()
			return err
		}
		if len(data) < 32 {
			h.Close()
			return volume.ErrCorrupt
		}
		var id [32]byte
		copy(id[:], data[:32])
		candidates = append(candidates, struct {
			record volume.ObjectRecord
			id     [32]byte
		}{record, id})
	}
	h.Close()
	var obsolete []volume.ObjectRecord
	r.mu.Lock()
	for _, candidate := range candidates {
		txn := r.txns[candidate.id]
		if txn != nil && (txn.aborted || txn.targetDone.RecordSequence != 0 || txn.sourceDone.RecordSequence != 0) {
			obsolete = append(obsolete, candidate.record)
		}
	}
	r.mu.Unlock()
	if len(obsolete) > 0 {
		r.journal.mu.Lock()
		h, err = r.journal.store.Open(true)
		if err == nil {
			err = h.PurgeBatch(obsolete)
			h.Close()
		}
		r.journal.mu.Unlock()
		if err != nil {
			return err
		}
	}
	r.mu.Lock()
	if len(records) == 0 {
		r.memoCursor = nil
	} else {
		next := records[len(records)-1].Key
		r.memoCursor = &next
	}
	r.mu.Unlock()
	return nil
}
