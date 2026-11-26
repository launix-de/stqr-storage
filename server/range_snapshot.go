// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	peerRangeReserve uint16 = 0x1050 + iota
	peerRangeSnapshot
	peerRangeObject
	peerRangeChunk
	peerRangeObjectEnd
	peerRangeReceipts
	peerRangeSnapshotEnd
	peerRangeCommit
	peerRangeDone
	peerRangeObserve
	peerRangeArchive
	peerRangeProbe
	peerRangeAbort
	peerRangeState
	peerRangeRAMAbort
	peerRangeRAMLostCommit
)

type rangeSnapshot struct {
	mu          sync.Mutex
	final       bool
	logicalUsed uint64
	digest      hash.Hash
	object      *rangeIncomingObject
}
type rangeObjectMemo struct {
	Key    volume.Key
	Info   volume.Info
	Codec  volume.Compression
	Digest [32]byte
}
type rangeIncomingObject struct {
	transfer         client.ID
	key              volume.Key
	info             volume.Info
	header           []byte
	payload, extents *os.File
	hash             hash.Hash
	last             uint64
	baseDigest       [32]byte
	encoded          bool
}

func (o *rangeIncomingObject) close() {
	for _, file := range []*os.File{o.payload, o.extents} {
		if file != nil {
			name := file.Name()
			file.Close()
			os.Remove(name)
		}
	}
}
func (r *rangeManager) snapshot(id [32]byte) (*rangeTxn, *rangeSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.txns[id]
	if t == nil || t.aborted || t.forceRetired || t.plan.Target != r.node || t.decision != nil {
		return nil, nil, errControlConflict
	}
	return t, t.snapshot, nil
}
func encodeRangeInfo(e *client.Encoder, k volume.Key, info volume.Info) {
	e.ID(client.ID(k.Bucket))
	e.B = append(e.B, k.Object[:]...)
	e.U64(info.Length)
	e.U8(uint8(info.Compression))
	e.Bool(info.Deleted)
}
func decodeRangeInfo(d *client.Decoder) (volume.Key, volume.Info) {
	k := volume.Key{Bucket: volume.ID(d.ID())}
	copy(k.Object[:], d.Take(32))
	info := volume.Info{Length: d.U64(), Compression: volume.Compression(d.U8()), Deleted: d.Bool()}
	return k, info
}
func encodeRangeReceipt(x volume.RequestRecord) []byte {
	e := client.Encoder{}
	e.ID(client.ID(x.ID))
	encodeRangeInfo(&e, x.Key, x.Result.Info)
	e.B = append(e.B, x.Fingerprint[:]...)
	e.U64(x.Result.Offset)
	return e.B
}
func decodeRangeReceipt(d *client.Decoder) volume.RequestRecord {
	x := volume.RequestRecord{ID: volume.ID(d.ID())}
	x.Key, x.Result.Info = decodeRangeInfo(d)
	copy(x.Fingerprint[:], d.Take(32))
	x.Result.Offset = d.U64()
	return x
}

const rangeReceiptBytes = 114

func (r *rangeManager) receiveSnapshot(m client.Message, actor client.ID, force *controlClaim) ([]byte, error) {
	d := client.Decoder{B: m.Body}
	schema := d.U16()
	var id [32]byte
	copy(id[:], d.Take(32))
	if (schema != 1 && !(schema == 2 && (m.Opcode == peerRangeSnapshotEnd || m.Opcode == peerRangeObject || m.Opcode == peerRangeChunk) || schema == 3 && m.Opcode == peerRangeObject)) || d.Err != nil {
		return nil, client.ErrProtocol
	}
	t, snapshot, err := r.snapshot(id)
	if err != nil {
		return nil, err
	}
	if t.plan.Source != actor {
		return nil, fail(403, "range snapshot source identity mismatch")
	}
	if m.Opcode == peerRangeSnapshot {
		final := d.Bool()
		if d.Done() != nil {
			return nil, client.ErrProtocol
		}
		if snapshot != nil {
			snapshot.mu.Lock()
			if snapshot.object != nil {
				snapshot.object.close()
			}
			snapshot.mu.Unlock()
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if t.aborted || t.forceRetired {
			return nil, errControlConflict
		}
		t.snapshot = &rangeSnapshot{final: final, digest: sha256.New()}
		return nil, nil
	}
	if snapshot == nil {
		return nil, errControlConflict
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	r.mu.Lock()
	aborted := t.aborted || t.forceRetired
	r.mu.Unlock()
	if aborted {
		return nil, errControlConflict
	}
	switch m.Opcode {
	case peerRangeProbe:
		key, info := decodeRangeInfo(&d)
		headerEncoder := client.Encoder{}
		encodeRangeInfo(&headerEncoder, key, info)
		header := headerEncoder.B
		var expected [32]byte
		copy(expected[:], d.Take(32))
		if d.Done() != nil || key.Bucket != volume.ID(t.plan.Before.Bucket) || !t.plan.contains(key.Object) || snapshot.object != nil {
			return nil, client.ErrProtocol
		}
		memo, exists, err := r.objectMemo(id, key)
		if err != nil {
			return nil, err
		}
		available := exists && memo.Info == info && memo.Codec == t.plan.Compression && memo.Digest == expected
		if available {
			store, err := r.destination(t.plan)
			if err != nil {
				return nil, err
			}
			h, err := store.Open(false)
			if err != nil {
				return nil, err
			}
			current, err := h.RecordInfo(key)
			h.Close()
			available = err == nil && current.Length == info.Length && current.Deleted == info.Deleted && current.Compression == t.plan.Compression
		}
		e := client.Encoder{}
		e.Bool(available)
		if available {
			e.B = append(e.B, memo.Digest[:]...)
			snapshot.digest.Write([]byte{0})
			snapshot.digest.Write(header)
			snapshot.digest.Write(memo.Digest[:])
			if !info.Deleted {
				if info.Length > math.MaxUint64-snapshot.logicalUsed {
					return nil, volume.ErrRange
				}
				snapshot.logicalUsed += info.Length
			}
		} else if hashes, base, e2 := r.deltaHashes(t, key, info); e2 == nil && len(hashes) != 0 {
			e.B = append(e.B, base[:]...)
			e.U32(rangeDeltaPage)
			e.U32(uint32(len(hashes)))
			for _, sum := range hashes {
				e.B = append(e.B, sum[:]...)
			}
		}
		return e.B, nil
	case peerRangeObject:
		transfer := d.ID()
		header := append([]byte{}, d.B...)
		key, info := decodeRangeInfo(&d)
		var baseDigest [32]byte
		if schema == 2 {
			copy(baseDigest[:], d.Take(32))
			if d.Err != nil || len(header) < 32 {
				return nil, client.ErrProtocol
			}
			header = header[:len(header)-32]
		}
		if d.Done() != nil || schema == 2 && (baseDigest == ([32]byte{}) || info.Deleted || info.Compression != volume.Raw || t.plan.Compression != volume.Raw) || transfer == (client.ID{}) || key.Bucket != volume.ID(t.plan.Before.Bucket) || !t.plan.contains(key.Object) || info.Compression > volume.XZ || info.Deleted && info.Length != 0 {
			return nil, client.ErrProtocol
		}
		if snapshot.object != nil {
			return nil, errControlConflict
		}
		payload, err := os.CreateTemp("", "stqr-range-data-*")
		if err != nil {
			return nil, err
		}
		extents, err := os.CreateTemp("", "stqr-range-extents-*")
		if err != nil {
			payload.Close()
			os.Remove(payload.Name())
			return nil, err
		}
		o := &rangeIncomingObject{transfer: transfer, key: key, info: info, header: header, payload: payload, extents: extents, hash: sha256.New(), baseDigest: baseDigest, encoded: schema == 3}
		o.hash.Write(header)
		snapshot.object = o
		return nil, nil
	case peerRangeChunk:
		transfer := d.ID()
		var encoded volume.EncodedChunk
		var offset, logicalLength uint64
		if schema == 2 {
			encoded = decodeRangeEncoded(&d)
			offset = encoded.Offset
			logicalLength = encoded.Length
		} else {
			offset = d.U64()
		}
		data := d.Blob()
		if schema != 2 {
			logicalLength = uint64(len(data))
		}
		o := snapshot.object
		if d.Done() != nil || o == nil || o.transfer != transfer || len(data) == 0 || len(data) > 1<<20 || o.encoded != (schema == 2) || logicalLength == 0 || offset < o.last || offset > o.info.Length || logicalLength > o.info.Length-offset {
			return nil, client.ErrProtocol
		}
		if o.encoded {
			meta := client.Encoder{}
			encodeRangeEncoded(&meta, encoded)
			meta.U32(uint32(len(data)))
			if _, err := o.extents.Write(meta.B); err != nil {
				return nil, err
			}
		} else {
			var descriptor [16]byte
			binary.LittleEndian.PutUint64(descriptor[:8], offset)
			binary.LittleEndian.PutUint64(descriptor[8:], uint64(len(data)))
			if _, err := o.extents.Write(descriptor[:]); err != nil {
				return nil, err
			}
		}
		if _, err := o.payload.Write(data); err != nil {
			return nil, err
		}
		e := client.Encoder{}
		if o.encoded {
			encodeRangeEncoded(&e, encoded)
		} else {
			e.U64(offset)
		}
		e.Blob(data)
		o.hash.Write(e.B)
		o.last = offset + logicalLength
		return nil, nil
	case peerRangeObjectEnd:
		transfer := d.ID()
		digest := d.Take(32)
		o := snapshot.object
		if d.Done() != nil || o == nil || o.transfer != transfer || !bytes.Equal(digest, o.hash.Sum(nil)) {
			return nil, client.ErrProtocol
		}
		store, err := r.destination(t.plan)
		if err != nil {
			return nil, err
		}
		end, err := r.server.beginRebuildIO(r.server.ctx)
		if err != nil {
			return nil, err
		}
		defer end()
		r.server.opMu.Lock()
		reservedBefore := store.Stats().InFlightReservedPhysical
		allowance, err := r.allowance(t, force)
		if err != nil {
			r.server.opMu.Unlock()
			return nil, err
		}
		err = func() error {
			h, err := store.Open(true)
			if err != nil {
				return err
			}
			defer h.Close()
			info := o.info
			info.Compression = t.plan.Compression
			source := func(yield func(uint64, io.Reader, uint64) error) error {
				if _, err := o.extents.Seek(0, io.SeekStart); err != nil {
					return err
				}
				var at int64
				for {
					var descriptor [16]byte
					_, err := io.ReadFull(o.extents, descriptor[:])
					if err == io.EOF {
						return nil
					}
					if err != nil {
						return err
					}
					offset, length := binary.LittleEndian.Uint64(descriptor[:8]), binary.LittleEndian.Uint64(descriptor[8:])
					if length > uint64(^uint64(0)>>1) {
						return volume.ErrRange
					}
					reader := io.NewSectionReader(o.payload, at, int64(length))
					if err := yield(offset, reader, length); err != nil {
						return err
					}
					at += int64(length)
				}
			}
			if o.baseDigest != ([32]byte{}) {
				var patches []volume.RestorePatch
				e := source(func(offset uint64, reader io.Reader, length uint64) error {
					data := make([]byte, int(length))
					if _, e := io.ReadFull(reader, data); e != nil {
						return e
					}
					patches = append(patches, volume.RestorePatch{Offset: offset, Data: data})
					return nil
				})
				if e != nil {
					return e
				}
				return h.RestoreDelta(o.key, o.baseDigest, info, patches, volume.Options{InFlightAllowance: allowance, ImportShadow: true})
			}
			_, err = h.RecordInfo(o.key)
			if err == nil {
				// This copy is not authoritative yet. Reclaim it before replacing
				// it, so catch-up needs no second full object allocation (MVCC).
				if err := h.Purge(o.key); err != nil {
					return err
				}
			}
			if err != nil && !errors.Is(err, volume.ErrNotFound) {
				return err
			}
			if o.encoded {
				return h.RestoreEncoded(o.key, info, o.encodedSource(), volume.Options{InFlightAllowance: allowance, ImportShadow: true})
			}
			return h.RestoreSeekable(o.key, info, source, volume.Options{InFlightAllowance: allowance, ImportShadow: true})
		}()
		r.consumed(t, store, reservedBefore)
		r.server.opMu.Unlock()
		if err != nil {
			return nil, err
		}
		var sum [32]byte
		copy(sum[:], digest)
		err = r.storeObjectMemo(id, rangeObjectMemo{Key: o.key, Info: o.info, Codec: t.plan.Compression, Digest: sum})
		if err != nil {
			return nil, err
		}
		snapshot.digest.Write([]byte{0})
		snapshot.digest.Write(o.header)
		snapshot.digest.Write(digest)
		if !o.info.Deleted {
			if o.info.Length > math.MaxUint64-snapshot.logicalUsed {
				return nil, volume.ErrRange
			}
			snapshot.logicalUsed += o.info.Length
		}
		o.close()
		snapshot.object = nil
		return nil, nil
	case peerRangeReceipts:
		n := d.Count(64)
		if n == 0 || n > len(d.B)/rangeReceiptBytes {
			return nil, client.ErrProtocol
		}
		var records []volume.RequestRecord
		var raw [][]byte
		for i := 0; i < n; i++ {
			b := append([]byte{}, d.Take(rangeReceiptBytes)...)
			sub := client.Decoder{B: b}
			record := decodeRangeReceipt(&sub)
			if sub.Done() != nil || record.Key.Bucket != volume.ID(t.plan.Before.Bucket) || !t.plan.contains(record.Key.Object) {
				return nil, client.ErrProtocol
			}
			records = append(records, record)
			raw = append(raw, b)
		}
		if d.Done() != nil {
			return nil, client.ErrProtocol
		}
		store, err := r.destination(t.plan)
		if err != nil {
			return nil, err
		}
		r.server.opMu.Lock()
		err = func() error {
			for _, record := range records {
				if err := r.server.ctx.Err(); err != nil {
					return err
				}
				before := store.Stats().InFlightReservedPhysical
				allowance, err := r.allowance(t, force)
				if err != nil {
					return err
				}
				h, err := store.Open(true)
				if err != nil {
					return err
				}
				err = h.RestoreRequestWithOptions(record, volume.Options{InFlightAllowance: allowance, ImportShadow: true})
				h.Close()
				r.consumed(t, store, before)
				if err != nil {
					return err
				}
			}
			return nil
		}()
		r.server.opMu.Unlock()
		if err != nil {
			return nil, err
		}
		for _, b := range raw {
			snapshot.digest.Write([]byte{1})
			snapshot.digest.Write(b)
		}
		return nil, nil
	case peerRangeSnapshotEnd:
		digest := d.Take(32)
		quotaUsed := uint64(0)
		if schema == 2 {
			quotaUsed = d.U64()
		}
		if d.Done() != nil || snapshot.object != nil || !bytes.Equal(digest, snapshot.digest.Sum(nil)) {
			return nil, client.ErrProtocol
		}
		if err := r.checkPhysicalActivation(t, quotaUsed); err != nil {
			return nil, err
		}
		if !snapshot.final {
			return append([]byte{}, digest...), nil
		}
		var sum [32]byte
		copy(sum[:], digest)
		proof, err := r.readyWithForce(id, sum, false, force, snapshot.logicalUsed, quotaUsed)
		if err != nil {
			return nil, err
		}
		return proof.MarshalBinary()
	}
	return nil, client.ErrProtocol
}

func rangeMessage(op uint16, id [32]byte) client.Encoder {
	e := client.Encoder{}
	e.U16(1)
	e.B = append(e.B, id[:]...)
	return e
}
func (r *rangeManager) call(ctx context.Context, node client.ID, opcode uint16, body []byte) ([]byte, error) {
	if force := forceScopeContext(ctx); force != nil {
		wire, err := marshalControlClaim(*force)
		if err != nil {
			return nil, err
		}
		e := client.Encoder{}
		e.U16(1)
		e.Blob(wire)
		e.U16(opcode)
		e.Blob(body)
		if node == r.node {
			return r.server.receiveForceScopeEnvelope(e.B, r.node, r.server.boot)
		}
		return (peerControlParticipants{runtime: r.server.control}).call(ctx, node, peerForceScopeEnvelope, e.B)
	}

	if node == r.node {
		if opcode == peerReplicaAbortHistory {
			return r.server.receiveReplicaAbortHistory(client.Message{Kind: client.Request, Opcode: opcode, Body: body}, r.node)
		}
		if opcode == peerReplicaTopologyReady || opcode == peerReplicaTopologyCleanup {
			return r.server.replicaTopologyMessage(client.Message{Kind: client.Request, Opcode: opcode, Body: body}, r.node)
		}
		return r.server.rangePeerMessage(client.Message{Kind: client.Request, Opcode: opcode, Body: body}, r.node, r.server.boot)
	}
	return (peerControlParticipants{runtime: r.server.control}).call(ctx, node, opcode, body)
}

// sourceVolumes contains authoritative storage only; retained stale copies from
// an earlier handoff must never be re-exported over a newer range location.
func (r *rangeManager) sourceVolumes(p rangePlan) []*volume.Volume {
	b, err := r.server.Bucket(volume.ID(p.Before.Bucket))
	if err != nil {
		return nil
	}
	description := b.storageDescription()
	if description.DesiredPolicy != nil && p.Before.Entries[0].PolicyVersion == description.DesiredPolicy.Version {
		description.Policy = *description.DesiredPolicy
	}
	all := b.volumesFor(description)
	var out []*volume.Volume
	pin := volume.ID(p.Before.Entries[0].Locations[0].Volume)
	for _, v := range all {
		if !r.server.volumeAllowedForEntry(p.Before.Bucket, p.Before.Entries[0], v.Identity().VolumeID, r.server.currentSystem.Load()) {
			continue
		}
		if p.Source == p.Target && v.Identity().VolumeID == p.Volume {
			continue
		}
		if pin == (volume.ID{}) || v.Identity().VolumeID == pin {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Identity().VolumeID.String() < out[j].Identity().VolumeID.String() })
	return out
}

func (r *rangeManager) sendSnapshot(ctx context.Context, p rangePlan, final bool) ([32]byte, client.Proof, error) {
	id := p.id()
	e := rangeMessage(peerRangeSnapshot, id)
	e.Bool(final)
	if _, err := r.call(ctx, p.Target, peerRangeSnapshot, e.B); err != nil {
		return [32]byte{}, client.Proof{}, err
	}
	digest := sha256.New()
	var logicalUsed uint64
	for _, v := range r.sourceVolumes(p) {
		var after *volume.ObjectID
		for {
			h, err := v.Open(false)
			if err != nil {
				return [32]byte{}, client.Proof{}, err
			}
			records, err := p.scanObjects(h, after, 128)
			h.Close()
			if err != nil {
				return [32]byte{}, client.Proof{}, err
			}
			if len(records) == 0 {
				break
			}
			for _, record := range records {
				if err := r.server.waitForUserIO(ctx); err != nil {
					return [32]byte{}, client.Proof{}, err
				}
				if !p.contains(record.Key.Object) {
					continue
				}
				selected, err := r.selectSnapshotSource(p, record.Key)
				if err != nil {
					return [32]byte{}, client.Proof{}, err
				}
				if selected != v {
					continue
				}
				header, hash, err := r.sendObject(ctx, p, v, record.Key)
				if err != nil {
					return [32]byte{}, client.Proof{}, err
				}
				digest.Write([]byte{0})
				digest.Write(header)
				digest.Write(hash[:])
				d := client.Decoder{B: header}
				_, info := decodeRangeInfo(&d)
				if d.Done() != nil {
					return [32]byte{}, client.Proof{}, client.ErrProtocol
				}
				if !info.Deleted {
					if info.Length > math.MaxUint64-logicalUsed {
						return [32]byte{}, client.Proof{}, volume.ErrRange
					}
					logicalUsed += info.Length
				}
			}
			last := records[len(records)-1].Key.Object
			after = &last
		}
	}
	for _, v := range r.sourceVolumes(p) {
		file, err := os.CreateTemp("", "stqr-range-receipts-*")
		if err != nil {
			return [32]byte{}, client.Proof{}, err
		}
		err = func() error {
			defer func() { file.Close(); os.Remove(file.Name()) }()
			var cursor *volume.ID
			for {
				if err := r.server.waitForUserIO(ctx); err != nil {
					return err
				}
				h, err := v.Open(false)
				if err != nil {
					return err
				}
				records, err := h.ScanRequestRecords(cursor, 128)
				h.Close()
				if err != nil {
					return err
				}
				if len(records) == 0 {
					break
				}
				for _, record := range records {
					if record.Key.Bucket == volume.ID(p.Before.Bucket) && p.contains(record.Key.Object) {
						if _, err := file.Write(encodeRangeReceipt(record)); err != nil {
							return err
						}
					}
				}
				next := records[len(records)-1].ID
				cursor = &next
			}

			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return err
			}
			for {
				batch := make([]byte, 64*rangeReceiptBytes)
				n, err := io.ReadFull(file, batch)
				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					return err
				}
				if n == 0 {
					return nil
				}
				if n%rangeReceiptBytes != 0 {
					return volume.ErrCorrupt
				}
				e := rangeMessage(peerRangeReceipts, id)
				e.V(uint64(n / rangeReceiptBytes))
				e.B = append(e.B, batch[:n]...)
				if _, err := r.call(ctx, p.Target, peerRangeReceipts, e.B); err != nil {
					return err
				}
				for i := 0; i < n; i += rangeReceiptBytes {
					digest.Write([]byte{1})
					digest.Write(batch[i : i+rangeReceiptBytes])
				}
			}
		}()
		if err != nil {
			return [32]byte{}, client.Proof{}, err
		}
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	quotaUsed := uint64(0)
	if !final {
		var err error
		quotaUsed, err = r.physicalFootprint(p)
		if err != nil {
			return sum, client.Proof{}, err
		}
	}
	if final {
		var err error
		r.mu.Lock()
		ready := r.txns[id].sourceReady.RecordSequence != 0
		priorQuota := r.txns[id].quotaUsed
		r.mu.Unlock()
		if ready {
			quotaUsed = priorQuota
		} else {
			quotaUsed, err = r.physicalFootprint(p)
		}
		if err != nil {
			return sum, client.Proof{}, err
		}
		r.mu.Lock()
		r.txns[id].pendingQuota = quotaUsed
		r.txns[id].pendingUsed = logicalUsed
		r.mu.Unlock()
		if quotaUsed != 0 {
			if _, err := r.ready(id, sum, true, logicalUsed, quotaUsed); err != nil {
				return sum, client.Proof{}, err
			}
		}
	}
	e = rangeMessage(peerRangeSnapshotEnd, id)
	e.B = append(e.B, sum[:]...)
	if quotaUsed != 0 {
		e.B[0] = 2
		e.U64(quotaUsed)
	}
	body, err := r.call(ctx, p.Target, peerRangeSnapshotEnd, e.B)
	if err != nil {
		return sum, client.Proof{}, err
	}
	if !final {
		if !bytes.Equal(body, sum[:]) {
			return sum, client.Proof{}, client.ErrProtocol
		}
		return sum, client.Proof{}, nil
	}
	proof, err := client.DecodeProof(body)
	return sum, proof, err
}

func (r *rangeManager) sendObject(ctx context.Context, p rangePlan, v *volume.Volume, key volume.Key) ([]byte, [32]byte, error) {
	var probeInfo volume.Info
	var sourceDigest [32]byte
	err := func() error {
		end, err := r.server.beginRebuildIO(ctx)
		if err != nil {
			return err
		}
		defer end()
		h, err := v.Open(false)
		if err != nil {
			return err
		}
		defer h.Close()
		probeInfo, err = h.RecordInfo(key)
		if err == nil {
			sourceDigest, err = rangeObjectDigest(ctx, h, key, probeInfo)
		}
		return err
	}()
	if err != nil {
		return nil, [32]byte{}, err
	}
	probeHeader := client.Encoder{}
	encodeRangeInfo(&probeHeader, key, probeInfo)
	probe := rangeMessage(peerRangeProbe, p.id())
	probe.B = append(probe.B, probeHeader.B...)
	probe.B = append(probe.B, sourceDigest[:]...)
	probeBody, err := r.call(ctx, p.Target, peerRangeProbe, probe.B)
	if err != nil {
		return nil, [32]byte{}, err
	}
	decoder := client.Decoder{B: probeBody}
	have := decoder.Bool()
	var prior [32]byte
	if have {
		copy(prior[:], decoder.Take(32))
	}
	var deltaBase [32]byte
	var deltaHashes [][32]byte
	if !have && len(decoder.B) != 0 {
		copy(deltaBase[:], decoder.Take(32))
		page, count := decoder.U32(), decoder.U32()
		if page != rangeDeltaPage || count == 0 || count > rangeDeltaMaxPages || uint64(count)*32 != uint64(len(decoder.B)) || probeInfo.Length == 0 || uint64(count) != (probeInfo.Length+rangeDeltaPage-1)/rangeDeltaPage {
			return nil, [32]byte{}, client.ErrProtocol
		}
		deltaHashes = make([][32]byte, int(count))
		for i := range deltaHashes {
			copy(deltaHashes[i][:], decoder.Take(32))
		}
	}
	if decoder.Done() != nil {
		return nil, [32]byte{}, client.ErrProtocol
	}
	if have {
		return probeHeader.B, prior, nil
	}
	data, err := os.CreateTemp("", "stqr-range-source-*")
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer func() { data.Close(); os.Remove(data.Name()) }()
	meta, err := os.CreateTemp("", "stqr-range-source-extents-*")
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer func() { meta.Close(); os.Remove(meta.Name()) }()
	var info volume.Info
	encodedTransfer := false
	err = func() error {
		end, err := r.server.beginRebuildIO(ctx)
		if err != nil {
			return err
		}
		defer end()
		h, err := v.Open(false)
		if err != nil {
			return err
		}
		defer h.Close()
		info, err = h.RecordInfo(key)
		if err != nil {
			return err
		}
		if info.Deleted {
			return nil
		}
		if info.Compression != volume.Raw && info.Compression == p.Compression {
			encodedTransfer = true
			_, err := h.ExportEncoded(key, func(c volume.EncodedChunk) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				descriptor := client.Encoder{}
				encodeRangeEncoded(&descriptor, c)
				descriptor.U32(uint32(len(c.Data)))
				if _, err := meta.Write(descriptor.B); err != nil {
					return err
				}
				_, err := data.Write(c.Data)
				return err
			})
			return err
		}
		extents, err := h.Extents(key)
		if err != nil {
			return err
		}
		for _, extent := range extents {
			if extent.Zero {
				continue
			}
			for offset, left := extent.Offset, extent.Length; left > 0; {
				if err := ctx.Err(); err != nil {
					return err
				}
				length := min(left, 1<<20)
				buffer := make([]byte, int(length))
				n, err := h.ReadAt(key, buffer, offset)
				if err != nil && err != io.EOF {
					return err
				}
				if uint64(n) != length {
					return io.ErrUnexpectedEOF
				}
				if _, err := data.Write(buffer); err != nil {
					return err
				}
				var descriptor [16]byte
				binary.LittleEndian.PutUint64(descriptor[:8], offset)
				binary.LittleEndian.PutUint64(descriptor[8:], length)
				if _, err := meta.Write(descriptor[:]); err != nil {
					return err
				}
				offset += length
				left -= length
			}
		}
		return nil
	}()
	if err != nil {
		return nil, [32]byte{}, err
	}
	id := p.id()
	transfer := client.NewRequestID()
	header := client.Encoder{}
	encodeRangeInfo(&header, key, info)
	hash := sha256.New()
	hash.Write(header.B)
	// Only a dense snapshot with exactly the probed shape can use page deltas.
	useDelta := deltaBase != ([32]byte{}) && info.Length == probeInfo.Length && info.Compression == volume.Raw && p.Compression == volume.Raw && !info.Deleted
	if useDelta {
		extentData, e2 := data.Stat()
		useDelta = e2 == nil && extentData.Size() == int64(info.Length)
	}
	e := rangeMessage(peerRangeObject, id)
	if useDelta {
		binary.LittleEndian.PutUint16(e.B[:2], 2)
	}
	if encodedTransfer {
		binary.LittleEndian.PutUint16(e.B[:2], 3)
	}
	e.ID(transfer)
	e.B = append(e.B, header.B...)
	if useDelta {
		e.B = append(e.B, deltaBase[:]...)
	}
	if _, err := r.call(ctx, p.Target, peerRangeObject, e.B); err != nil {
		return nil, [32]byte{}, err
	}
	if _, err := data.Seek(0, io.SeekStart); err != nil {
		return nil, [32]byte{}, err
	}
	if _, err := meta.Seek(0, io.SeekStart); err != nil {
		return nil, [32]byte{}, err
	}
	for {
		if encodedTransfer {
			descriptor := make([]byte, rangeEncodedDescriptorSize+4)
			_, err := io.ReadFull(meta, descriptor)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, [32]byte{}, err
			}
			d := client.Decoder{B: descriptor}
			c := decodeRangeEncoded(&d)
			n := d.U32()
			if d.Done() != nil || n == 0 || n > 1<<20 {
				return nil, [32]byte{}, client.ErrProtocol
			}
			c.Data = make([]byte, int(n))
			if _, err = io.ReadFull(data, c.Data); err != nil {
				return nil, [32]byte{}, err
			}
			chunk := client.Encoder{}
			encodeRangeEncoded(&chunk, c)
			chunk.Blob(c.Data)
			hash.Write(chunk.B)
			e := rangeMessage(peerRangeChunk, id)
			binary.LittleEndian.PutUint16(e.B[:2], 2)
			e.ID(transfer)
			e.B = append(e.B, chunk.B...)
			if _, err = r.call(ctx, p.Target, peerRangeChunk, e.B); err != nil {
				return nil, [32]byte{}, err
			}
			continue
		}
		var descriptor [16]byte
		_, err := io.ReadFull(meta, descriptor[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, [32]byte{}, err
		}
		offset, length := binary.LittleEndian.Uint64(descriptor[:8]), binary.LittleEndian.Uint64(descriptor[8:])
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(data, payload); err != nil {
			return nil, [32]byte{}, err
		}
		unchanged := func(at, stop int) bool {
			pos := offset + uint64(at)
			return useDelta && pos%rangeDeltaPage == 0 && pos/rangeDeltaPage < uint64(len(deltaHashes)) && sha256.Sum256(payload[at:stop]) == deltaHashes[pos/rangeDeltaPage]
		}
		for at := 0; at < len(payload); {
			start := at
			stop := len(payload)
			if useDelta {
				stop = min(at+rangeDeltaPage, len(payload))
				if unchanged(at, stop) {
					at = stop
					continue
				}
				// Adjacent changed pages share one request, up to the normal
				// 1-MiB message bound; scattered updates keep their gaps.
				for stop < len(payload) {
					next := min(stop+rangeDeltaPage, len(payload))
					if unchanged(stop, next) {
						break
					}
					stop = next
				}
			}
			piece := payload[start:stop]
			pos := offset + uint64(start)
			at = stop
			chunk := client.Encoder{}
			chunk.U64(pos)
			chunk.Blob(piece)
			hash.Write(chunk.B)
			e := rangeMessage(peerRangeChunk, id)
			e.ID(transfer)
			e.B = append(e.B, chunk.B...)
			if _, err := r.call(ctx, p.Target, peerRangeChunk, e.B); err != nil {
				return nil, [32]byte{}, err
			}
		}
	}
	var sum [32]byte
	copy(sum[:], hash.Sum(nil))
	e = rangeMessage(peerRangeObjectEnd, id)
	e.ID(transfer)
	e.B = append(e.B, sum[:]...)
	if _, err := r.call(ctx, p.Target, peerRangeObjectEnd, e.B); err != nil {
		return nil, [32]byte{}, err
	}
	return header.B, sum, nil
}

// Compare actual bytes when reusing transfer progress. Equal lengths and codecs
// do not establish that a source object remained unchanged between snapshots.
func rangeObjectDigest(ctx context.Context, h *volume.Handle, key volume.Key, info volume.Info) ([32]byte, error) {
	e := client.Encoder{}
	encodeRangeInfo(&e, key, info)
	hash := sha256.New()
	hash.Write(e.B)
	var sum [32]byte
	if !info.Deleted {
		extents, err := h.Extents(key)
		if err != nil {
			return sum, err
		}
		for _, extent := range extents {
			if extent.Zero {
				continue
			}
			for offset, left := extent.Offset, extent.Length; left > 0; {
				if err := ctx.Err(); err != nil {
					return sum, err
				}
				length := min(left, 1<<20)
				data := make([]byte, int(length))
				n, err := h.ReadAt(key, data, offset)
				if err != nil && err != io.EOF {
					return sum, err
				}
				if uint64(n) != length {
					return sum, io.ErrUnexpectedEOF
				}
				chunk := client.Encoder{}
				chunk.U64(offset)
				chunk.Blob(data)
				hash.Write(chunk.B)
				offset += length
				left -= length
			}
		}
	}
	copy(sum[:], hash.Sum(nil))
	return sum, nil
}
