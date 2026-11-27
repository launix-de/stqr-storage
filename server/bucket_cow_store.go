// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type cowCaptureRangesKey struct{}
type cowProofKey struct{}
type cowLeaseKey struct{}
type cowLocalOwnerKey struct{}
type cowRestoreKey struct{}
type cowAcceptedBucketKey struct{}
type cowRestore struct {
	info   volume.Info
	source volume.ID
	proof  [32]byte
	lease  client.ID
}
type cowServerObjects struct {
	server      *Server
	metadata    bool
	readReplica bool
}

func (store cowServerObjects) capture(ctx context.Context, bucket volume.ID, object volume.ObjectID) (*cowSnapshot, error) {
	s := store.server
	owner, err := s.cowOwner(bucket, object)
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.ID(client.ID(bucket))
	e.B = append(e.B, object[:]...)
	e.Bool(store.metadata)
	replicaFlag := len(e.B)
	e.Bool(false)
	if ranges, ok := ctx.Value(cowCaptureRangesKey{}).([]volume.Extent); ok && !store.metadata {
		e.V(uint64(len(ranges)))
		for _, r := range ranges {
			e.U64(r.Offset)
			e.U64(r.Length)
		}
	}
	body, err := s.cowCall(ctx, owner, peerCowCapture, e.B)
	if err != nil && store.readReplica && s.controller != nil && ctx.Err() == nil {
		entry, known := s.placement.Load().lookup(client.ID(bucket), client.ObjectID(object))
		if known {
			e.B[replicaFlag] = 1
			for _, location := range entry.Locations {
				if location.Role != 1 || location.Node == owner {
					continue
				}
				alternate, e2 := s.cowCall(ctx, location.Node, peerCowCapture, e.B)
				if e2 == nil {
					body, err, owner = alternate, nil, location.Node
					break
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}
	d := client.Decoder{B: body}
	if !d.Bool() {
		if d.Done() != nil {
			return nil, client.ErrProtocol
		}
		return nil, nil
	}
	id := d.ID()
	info := readCowInfo(&d)
	var extents []volume.Extent
	var end uint64
	for n := d.Count(1 << 20); n > 0; n-- {
		off, length := d.U64(), d.U64()
		if length == 0 || off < end || off > info.Length || length > info.Length-off {
			return nil, client.ErrProtocol
		}
		extents = append(extents, volume.Extent{Offset: off, Length: length})
		end = off + length
	}
	if d.Done() != nil || !store.metadata && id == (client.ID{}) || info.Compression > volume.XZ || info.Deleted && info.Length != 0 {
		return nil, client.ErrProtocol
	}
	if store.metadata {
		return &cowSnapshot{bucket: bucket, node: owner, Info: info}, nil
	}
	return &cowSnapshot{bucket: bucket, node: owner, transfer: id, Info: info, Extents: extents, Data: &cowRemoteReader{ctx: ctx, server: s, node: owner, transfer: id, length: info.Length}, Close: func() error {
		cleanup, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		defer cancel()
		x := client.Encoder{}
		x.ID(id)
		_, err := s.cowCall(cleanup, owner, peerCowClose, x.B)
		return err
	}}, nil
}
func (store cowServerObjects) present(ctx context.Context, bucket volume.ID, object volume.ObjectID) (bool, error) {
	snapshot, err := (cowServerObjects{server: store.server, metadata: true}).capture(ctx, bucket, object)
	if err != nil {
		return false, err
	}
	present := snapshot != nil
	snapshot.close()
	return present, nil
}
func (store cowServerObjects) materialize(ctx context.Context, bucket volume.ID, object volume.ObjectID, snapshot *cowSnapshot) error {
	s := store.server
	owner, err := s.cowOwner(bucket, object)
	if err != nil {
		return err
	}
	proof, ok := ctx.Value(cowProofKey{}).([32]byte)
	if !ok {
		return errors.New("clone: missing graph publication proof")
	}
	e := client.Encoder{}
	e.ID(client.ID(bucket))
	e.B = append(e.B, object[:]...)
	e.B = append(e.B, proof[:]...)
	lease, _ := ctx.Value(cowLeaseKey{}).(client.ID)
	e.ID(lease)
	e.ID(client.ID(snapshot.bucket))
	e.ID(snapshot.node)
	e.ID(snapshot.transfer)
	writeCowInfo(&e, snapshot.Info)
	e.V(uint64(len(snapshot.Extents)))
	for _, extent := range snapshot.Extents {
		e.U64(extent.Offset)
		e.U64(extent.Length)
	}
	_, err = s.cowCall(ctx, owner, peerCowMaterialize, e.B)
	return err
}

// A seekable view of packed extent descriptors and their bytes. The replica
// protocol transfers the physical stream length, not the sparse logical length.
type cowStreamSpan struct {
	offset uint64
	extent volume.Extent
}
type cowStream struct {
	data             io.ReaderAt
	spans            []cowStreamSpan
	length, position int64
}

func newCowStream(snapshot *cowSnapshot) (*cowStream, error) {
	r := &cowStream{data: snapshot.Data}
	var offset, end uint64
	for _, e := range snapshot.Extents {
		if e.Zero {
			continue
		}
		if e.Length == 0 || e.Offset < end || e.Offset > snapshot.Info.Length || e.Length > snapshot.Info.Length-e.Offset || e.Length > math.MaxInt64-16-offset {
			return nil, volume.ErrRange
		}
		r.spans = append(r.spans, cowStreamSpan{offset, e})
		offset += 16 + e.Length
		end = e.Offset + e.Length
	}
	r.length = int64(offset)
	return r, nil
}
func (r *cowStream) Seek(offset int64, whence int) (int64, error) {
	base := int64(0)
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.position
	case io.SeekEnd:
		base = r.length
	default:
		return r.position, volume.ErrRange
	}
	if offset < -base || offset > math.MaxInt64-base {
		return r.position, volume.ErrRange
	}
	r.position = base + offset
	return r.position, nil
}
func (r *cowStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.position >= r.length {
		return 0, io.EOF
	}
	total := 0
	for len(p) > 0 && r.position < r.length {
		at := sort.Search(len(r.spans), func(i int) bool { return r.spans[i].offset+16+r.spans[i].extent.Length > uint64(r.position) })
		if at == len(r.spans) {
			return total, io.ErrUnexpectedEOF
		}
		s := r.spans[at]
		within := uint64(r.position) - s.offset
		if within < 16 {
			var h [16]byte
			binary.LittleEndian.PutUint64(h[:8], s.extent.Offset)
			binary.LittleEndian.PutUint64(h[8:], s.extent.Length)
			n := copy(p, h[within:])
			p = p[n:]
			total += n
			r.position += int64(n)
			continue
		}
		n := int(min(uint64(len(p)), s.extent.Length-(within-16)))
		got, err := r.data.ReadAt(p[:n], int64(s.extent.Offset+within-16))
		p = p[got:]
		total += got
		r.position += int64(got)
		if err != nil && err != io.EOF {
			return total, err
		}
		if got != n {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}
func (s *Server) cowPeerMutation(ctx context.Context, m client.Message, bucket volume.ID, object volume.ObjectID, d *client.Decoder) ([]byte, error) {
	if bucket == systemBucket {
		return nil, ErrBucketForbidden
	}
	if m.Opcode == peerCowSync {
		if d.Done() != nil {
			return nil, client.ErrProtocol
		}
		b, err := s.Bucket(bucket)
		if err != nil {
			return nil, err
		}
		for _, v := range b.allVolumes() {
			if err := v.Sync(); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	if m.Opcode == peerCowInventory {
		hasAfter := d.Bool()
		limit := d.U32()
		if d.Done() != nil || limit == 0 || limit > 1000 {
			return nil, client.ErrProtocol
		}
		b, err := s.Bucket(bucket)
		if err != nil {
			return nil, err
		}
		var after *volume.ObjectID
		if hasAfter {
			after = &object
		}
		seen := map[volume.ObjectID]bool{}
		for _, v := range b.allVolumes() {
			h, err := v.Open(false)
			if err != nil {
				return nil, err
			}
			page, err := h.ScanBucketObjects(bucket, after, int(limit)+1)
			h.Close()
			if err != nil {
				return nil, err
			}
			for _, r := range page {
				seen[r.Key.Object] = true
			}
		}
		ids := make([]volume.ObjectID, 0, len(seen))
		for id := range seen {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
		more := len(ids) > int(limit)
		if more {
			ids = ids[:limit]
		}
		e := client.Encoder{}
		e.Bool(more)
		e.V(uint64(len(ids)))
		for _, id := range ids {
			e.B = append(e.B, id[:]...)
		}
		return e.B, nil
	}
	if m.Opcode != peerCowMaterialize {
		return nil, client.ErrProtocol
	}
	var proof [32]byte
	copy(proof[:], d.Take(32))
	lease := d.ID()
	source := volume.ID(d.ID())
	node, transfer := d.ID(), d.ID()
	info := readCowInfo(d)
	var extents []volume.Extent
	for n := d.Count(1 << 20); n > 0; n-- {
		extents = append(extents, volume.Extent{Offset: d.U64(), Length: d.U64()})
	}
	if d.Done() != nil || info.Compression > volume.XZ || info.Deleted && info.Length != 0 || !info.Deleted && transfer == (client.ID{}) {
		return nil, client.ErrProtocol
	}
	owner, err := s.cowOwner(bucket, object)
	if err != nil {
		return nil, err
	}
	if !s.cowLocal(owner) {
		return nil, fail(503, "clone materialization owner changed")
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	existing, err := s.cowCaptureLocal(ctx, bucket, object, true)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		existing.close()
		return nil, nil
	}
	snapshot := &cowSnapshot{Info: info, Extents: extents, Data: &cowRemoteReader{ctx: ctx, server: s, node: node, transfer: transfer, length: info.Length}}
	stream, err := newCowStream(snapshot)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, objectBindingKey{}, struct{}{}) // a preservation RPC binds its own namespace
	ctx = context.WithValue(ctx, cowRestoreKey{}, cowRestore{info: info, source: source, proof: proof, lease: lease})
	request, err := b.cowMaterializationRequest(object, proof)
	if err != nil {
		return nil, err
	}
	// Copying an inherited preimage into the writing bucket is part of that
	// same owner's accepted mutation. Its generation is tracked by the usual
	// bucket/connection fence, so it must not introduce a physical checkpoint
	// before the caller can publish its change. The context is deliberately
	// local: preserving a DIFFERENT linked namespace still requires the existing
	// durability barrier, including when that namespace has a remote owner.
	// Removing that barrier requires cross-owner preservation dependencies at
	// every volume checkpoint, not merely a flag on this RPC.
	acceptedBucket, accepted := ctx.Value(cowAcceptedBucketKey{}).(volume.ID)
	opt := volume.Options{RequestID: request, Accepted: accepted && acceptedBucket == bucket}
	_, err = b.mutate(ctx, principal{cowInternal: true}, object, cowRestoreOpcode, 0, uint64(stream.length), stream, nil, opt)
	return nil, err
}

func (s *Server) cowPrepareWrite(ctx context.Context, b *Bucket, id volume.ObjectID, p principal, accepted bool) ([32]byte, bool, error) {
	if p.cowInternal || bucketLocalReferenceRoot(id) || len(b.storageDescription().ClonePeers) == 0 {
		return [32]byte{}, false, nil
	}
	g, err := s.cowGraph()
	if err != nil {
		return [32]byte{}, false, err
	}
	proof, err := s.cowGraphDigest(g, b.ID())
	if err != nil {
		return proof, false, err
	}
	ctx = context.WithValue(ctx, cowProofKey{}, proof)
	if accepted {
		ctx = context.WithValue(ctx, cowAcceptedBucketKey{}, b.ID())
	}
	if p.connection != nil {
		ctx = context.WithValue(ctx, cowLocalOwnerKey{}, p.connection)
		if lease := p.connection.lease.Load(); lease != nil {
			ctx = context.WithValue(ctx, cowLeaseKey{}, lease.Token)
		}
	}
	err = g.prepareWrite(ctx, cowServerObjects{server: s}, b.ID(), id)
	return proof, true, err
}
func (s *Server) cowValidateRestore(bucket volume.ID, restore cowRestore) error {
	g, err := s.cowGraph()
	if err != nil {
		return err
	}
	proof, err := s.cowGraphDigest(g, bucket)
	if err != nil {
		return err
	}
	if proof != restore.proof {
		return fail(503, "clone connections changed during materialization")
	}
	if restore.source == (volume.ID{}) && restore.info.Deleted {
		return nil
	}
	for _, peer := range g.component(bucket) {
		if peer == restore.source {
			return nil
		}
	}
	return fail(403, "clone source is outside the connected namespaces")
}

func (b *Bucket) cowMaintenanceAllowed(ctx context.Context, p principal) bool {
	if !p.cowInternal {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.maintenance == nil {
		return true
	}
	if b.maintenance.objectLease.Load() {
		return true
	}
	if owner, _ := ctx.Value(cowLocalOwnerKey{}).(*connectionAuthorization); owner != nil && owner == b.maintenance {
		return true
	}
	restore, ok := ctx.Value(cowRestoreKey{}).(cowRestore)
	return ok && b.lease != nil && !b.lease.Released && restore.lease != (client.ID{}) && restore.lease == b.lease.Token
}

// A retried preservation must resume its durable PREPARE, not create a second
// operation which blocks behind the first one. Terminal aborts advance to a
// fresh request ID; this is protocol retry identity, never an object revision.
func (b *Bucket) cowMaterializationRequest(object volume.ObjectID, proof [32]byte) (volume.ID, error) {
	e := client.Encoder{B: []byte("STQR COW materialization request v1\x00")}
	e.ID(client.ID(b.ID()))
	e.B = append(e.B, object[:]...)
	e.B = append(e.B, proof[:]...)
	digest := sha256.Sum256(e.B)
	id := volume.ID(digest[:16])
	for range 128 {
		aborted := false
		for _, v := range b.allVolumes() {
			h, err := v.Open(false)
			if err != nil {
				return volume.ID{}, err
			}
			record, found, err := h.ReplicaStatus(id)
			h.Close()
			if err != nil {
				return volume.ID{}, err
			}
			if found {
				if record.Request.Key != (volume.Key{Bucket: b.ID(), Object: object}) {
					return volume.ID{}, volume.ErrRequestID
				}
				if record.State != volume.ReplicaAborted {
					return id, nil
				}
				aborted = true
			}
		}
		if !aborted {
			return id, nil
		}
		next := sha256.Sum256(append([]byte("STQR COW retry after confirmed abort\x00"), id[:]...))
		id = volume.ID(next[:16])
	}
	return volume.ID{}, fail(503, "clone materialization has too many retained aborts")
}
