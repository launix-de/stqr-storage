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
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	peerCowCapture uint16 = 0x1090 + iota
	peerCowRead
	peerCowClose
	peerCowMaterialize
	peerCowInventory
	peerCowSync
)
const cowRestoreOpcode uint16 = 0xf001 // internal only; never a client opcode

type cowTransfer struct {
	snapshot *cowSnapshot
	touched  time.Time
}
type cowTransfers struct {
	once     sync.Once
	mu       sync.Mutex
	entries  map[client.ID]*cowTransfer
	detaches map[volume.ID]cowDetachProgress
}
type cowPackedExtent struct{ logical, physical, length uint64 }
type cowPackedReader struct {
	file   io.ReaderAt
	spans  []cowPackedExtent
	length uint64
}

func (r *cowPackedReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, volume.ErrRange
	}
	start := uint64(off)
	if start >= r.length {
		return 0, io.EOF
	}
	n := int(min(uint64(len(p)), r.length-start))
	clear(p[:n])
	end := start + uint64(n)
	at := sort.Search(len(r.spans), func(i int) bool { return r.spans[i].logical+r.spans[i].length > start })
	for ; at < len(r.spans) && r.spans[at].logical < end; at++ {
		s := r.spans[at]
		a, b := max(start, s.logical), min(end, s.logical+s.length)
		count, err := r.file.ReadAt(p[a-start:b-start], int64(s.physical+a-s.logical))
		if err != nil {
			return int(a-start) + count, err
		}
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (s *Server) cowGraph() (cowGraph, error) { return cowGraphDescriptions(s.bucketSnapshots()) }
func cowGraphDescriptions(descriptions []BucketDescription) (cowGraph, error) {
	g := cowGraph{}
	links := map[volume.ID]map[volume.ID]string{}
	for _, d := range descriptions {
		if d.DeletionComplete {
			continue
		}
		id, err := bucketID(d.ID)
		if err != nil {
			return nil, err
		}
		for _, p := range d.ClonePeers {
			peer, err := bucketID(p.Bucket)
			if err != nil {
				return nil, err
			}
			g[id] = append(g[id], peer)
			if links[id] == nil {
				links[id] = map[volume.ID]string{}
			}
			links[id][peer] = p.Link
		}
	}
	for id, peers := range g {
		for _, peer := range peers {
			found := false
			for _, back := range g[peer] {
				found = found || back == id && links[id][peer] == links[peer][id]
			}
			if !found {
				return nil, fail(503, "clone graph publication is incomplete")
			}
		}
	}
	return g, nil
}
func (s *Server) cowOwner(bucket volume.ID, object volume.ObjectID) (client.ID, error) {
	if s.controller == nil {
		return client.ID{}, nil
	}
	entry, ok := s.placement.Load().lookup(client.ID(bucket), client.ObjectID(object))
	if !ok || len(entry.Locations) == 0 || entry.State == 4 || entry.State == 5 {
		return client.ID{}, fail(503, "clone object placement unavailable")
	}
	return entry.Locations[0].Node, nil
}
func (s *Server) cowLocal(node client.ID) bool {
	return node == (client.ID{}) || s.control != nil && node == s.control.ledger.node
}
func (s *Server) cowCall(ctx context.Context, node client.ID, op uint16, body []byte) ([]byte, error) {
	if s.cowLocal(node) {
		return s.cowPeerMessage(ctx, client.Message{Kind: client.Request, Opcode: op, Body: body})
	}
	if s.control == nil {
		return nil, fail(503, "clone peer transport unavailable")
	}
	return (peerControlParticipants{runtime: s.control}).call(ctx, node, op, body)
}
func (s *Server) cowStartTransfers() {
	s.cow.once.Do(func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.workers.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.workers.Done()
			timer := time.NewTicker(15 * time.Second)
			defer timer.Stop()
			defer s.cowExpire(true)
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-timer.C:
					s.cowExpire(false)
				}
			}
		}()
	})
}
func (s *Server) cowExpire(all bool) {
	s.cow.mu.Lock()
	defer s.cow.mu.Unlock()
	for id, t := range s.cow.entries {
		if all || time.Since(t.touched) > time.Minute {
			delete(s.cow.entries, id)
			t.snapshot.close()
		}
	}
}
func (s *Server) cowCaptureLocal(ctx context.Context, bucket volume.ID, object volume.ObjectID, metadata bool, replicaRead ...bool) (out *cowSnapshot, err error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	owner, err := s.cowOwner(bucket, object)
	if err != nil {
		return nil, err
	}
	if !s.cowLocal(owner) {
		allowed := false
		if len(replicaRead) > 0 && replicaRead[0] && s.control != nil {
			entry, ok := s.placement.Load().lookup(client.ID(bucket), client.ObjectID(object))
			if ok {
				for _, loc := range entry.Locations {
					if loc.Role == 1 && loc.Node == s.control.ledger.node {
						allowed = true
					}
				}
			}
		}
		if !allowed {
			return nil, fail(503, "clone object owner changed")
		}
	}
	if s.scopes != nil {
		release, e := s.scopes.guard(bucket, object)
		if e != nil {
			return nil, e
		}
		defer release()
	}
	ctx = context.WithValue(ctx, objectBindingKey{}, struct{}{})
	key := volume.Key{Bucket: bucket, Object: object}
	v, _, found, err := b.selectVolume(b.storageDescription(), key, volume.ID{}, false)
	if errors.Is(err, volume.ErrNotFound) || !found && err == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	h, err := v.Open(false)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	if err := s.checkObjectBinding(ctx, bucket, object, len(replicaRead) > 0 && replicaRead[0]); err != nil {
		return nil, err
	}
	info, err := h.RecordInfo(key)
	if err != nil {
		return nil, err
	}
	if info.Deleted || metadata {
		return &cowSnapshot{Info: info}, nil
	}
	file, err := newReplicaSpool(v.Identity().Class == volume.RAM)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			file.Close()
		}
	}()
	extents, err := h.Extents(key)
	if err != nil {
		return nil, err
	}
	packed := &cowPackedReader{file: file, length: info.Length}
	var kept []volume.Extent
	var physical uint64
	for _, extent := range extents {
		if extent.Zero {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if extent.Length > math.MaxInt64-physical {
			return nil, volume.ErrRange
		}
		if _, err = h.CopyTo(key, extent.Offset, extent.Length, file); err != nil {
			return nil, err
		}
		packed.spans = append(packed.spans, cowPackedExtent{extent.Offset, physical, extent.Length})
		kept = append(kept, extent)
		physical += extent.Length
	}
	success = true
	return &cowSnapshot{Info: info, Data: packed, Extents: kept, Close: func() error { file.Close(); return nil }}, nil
}
func writeCowInfo(e *client.Encoder, info volume.Info) {
	e.U64(info.Length)
	e.U8(uint8(info.Compression))
	e.Bool(info.Deleted)
}
func readCowInfo(d *client.Decoder) volume.Info {
	return volume.Info{Length: d.U64(), Compression: volume.Compression(d.U8()), Deleted: d.Bool()}
}
func (s *Server) cowRegister(snapshot *cowSnapshot) (client.ID, error) {
	s.cowStartTransfers()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.ctx.Err() != nil {
		return client.ID{}, fail(503, "server is stopping")
	}
	s.cow.mu.Lock()
	defer s.cow.mu.Unlock()
	if len(s.cow.entries) >= maxObjectStagingTransfers {
		return client.ID{}, fail(503, "clone snapshot slots exhausted")
	}
	if s.cow.entries == nil {
		s.cow.entries = map[client.ID]*cowTransfer{}
	}
	id := client.NewRequestID()
	s.cow.entries[id] = &cowTransfer{snapshot: snapshot, touched: time.Now()}
	return id, nil
}
func (s *Server) cowPeerMessage(ctx context.Context, m client.Message) ([]byte, error) {
	if m.Kind != client.Request {
		return nil, client.ErrProtocol
	}
	d := client.Decoder{B: m.Body}
	e := client.Encoder{}
	if m.Opcode == peerCowRead || m.Opcode == peerCowClose {
		id := d.ID()
		var off, n uint64
		if m.Opcode == peerCowRead {
			off, n = d.U64(), d.U64()
		}
		if d.Done() != nil || n > 1<<20 || off > math.MaxInt64 {
			return nil, client.ErrProtocol
		}
		s.cow.mu.Lock()
		defer s.cow.mu.Unlock()
		t := s.cow.entries[id]
		if t == nil {
			return nil, fail(503, "clone read snapshot expired")
		}
		t.touched = time.Now()
		if m.Opcode == peerCowClose {
			delete(s.cow.entries, id)
			t.snapshot.close()
			return nil, nil
		}
		if t.snapshot.Info.Deleted || off > t.snapshot.Info.Length || n > t.snapshot.Info.Length-off {
			return nil, volume.ErrRange
		}
		data := make([]byte, int(n))
		if n > 0 {
			actual, err := t.snapshot.Data.ReadAt(data, int64(off))
			if err != nil && err != io.EOF {
				return nil, err
			}
			if actual != len(data) {
				return nil, io.ErrUnexpectedEOF
			}
		}
		e.Blob(data)
		return e.B, nil
	}
	bucket := volume.ID(d.ID())
	var object volume.ObjectID
	copy(object[:], d.Take(32))
	if m.Opcode == peerCowCapture {
		metadata := d.Bool()
		replicaRead := d.Bool()
		if d.Done() != nil || bucket == systemBucket {
			return nil, client.ErrProtocol
		}
		snapshot, err := s.cowCaptureLocal(ctx, bucket, object, metadata, replicaRead)
		if err != nil {
			return nil, err
		}
		e.Bool(snapshot != nil)
		if snapshot == nil {
			return e.B, nil
		}
		if metadata {
			e.ID(client.ID{})
			writeCowInfo(&e, snapshot.Info)
			e.V(0)
			snapshot.close()
			return e.B, nil
		}
		id, err := s.cowRegister(snapshot)
		if err != nil {
			snapshot.close()
			return nil, err
		}
		e.ID(id)
		writeCowInfo(&e, snapshot.Info)
		e.V(uint64(len(snapshot.Extents)))
		for _, x := range snapshot.Extents {
			e.U64(x.Offset)
			e.U64(x.Length)
		}
		if len(e.B) > client.MaxMessage {
			_, _ = s.cowCall(ctx, client.ID{}, peerCowClose, func() []byte { x := client.Encoder{}; x.ID(id); return x.B }())
			return nil, fail(503, "clone extent map exceeds transfer metadata budget")
		}
		return e.B, nil
	}
	return s.cowPeerMutation(ctx, m, bucket, object, &d)
}

type cowRemoteReader struct {
	ctx            context.Context
	server         *Server
	node, transfer client.ID
	length         uint64
}

func (r *cowRemoteReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, volume.ErrRange
	}
	if uint64(off) >= r.length {
		return 0, io.EOF
	}
	n := int(min(uint64(len(p)), r.length-uint64(off)))
	total := 0
	for total < n {
		part := min(n-total, 1<<20)
		e := client.Encoder{}
		e.ID(r.transfer)
		e.U64(uint64(off) + uint64(total))
		e.U64(uint64(part))
		body, err := r.server.cowCall(r.ctx, r.node, peerCowRead, e.B)
		if err != nil {
			return total, err
		}
		d := client.Decoder{B: body}
		data := d.Blob()
		if d.Done() != nil || len(data) != part {
			return total, client.ErrProtocol
		}
		copy(p[total:], data)
		total += part
	}
	if total < len(p) {
		return total, io.EOF
	}
	return total, nil
}

// Extent streams contain only allocated bytes, independent of logical length.
// They let a clone preserve a multi-terabyte sparse object without transferring
// or allocating its holes, using the native replica decision machinery.
func cowExtentSource(src io.Reader) volume.ExtentSource {
	return func(yield func(uint64, io.Reader, uint64) error) error {
		for {
			var header [16]byte
			_, err := io.ReadFull(src, header[:])
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			off, n := binary.LittleEndian.Uint64(header[:8]), binary.LittleEndian.Uint64(header[8:])
			if n == 0 || n > math.MaxInt64 {
				return volume.ErrRange
			}
			part := &io.LimitedReader{R: src, N: int64(n)}
			if err := yield(off, part, n); err != nil {
				return err
			}
			if part.N != 0 {
				return io.ErrUnexpectedEOF
			}
		}
	}
}

func (s *Server) cowGraphDigest(g cowGraph, bucket volume.ID) ([32]byte, error) {
	e := client.Encoder{}
	for _, id := range g.component(bucket) {
		b, err := s.Bucket(id)
		if err != nil {
			return [32]byte{}, err
		}
		e.ID(client.ID(id))
		for _, edge := range b.storageDescription().ClonePeers {
			peer, err := bucketID(edge.Bucket)
			if err != nil {
				return [32]byte{}, err
			}
			link, err := bucketID(edge.Link)
			if err != nil {
				return [32]byte{}, err
			}
			other, err := s.Bucket(peer)
			if err != nil {
				return [32]byte{}, err
			}
			symmetric := false
			for _, back := range other.storageDescription().ClonePeers {
				if back.Bucket == id.String() && back.Link == edge.Link {
					symmetric = true
					break
				}
			}
			if !symmetric {
				return [32]byte{}, fail(503, "clone connection is being published")
			}
			e.ID(client.ID(peer))
			e.ID(client.ID(link))
		}
		e.ID(client.ID{})
	}
	return sha256.Sum256(e.B), nil
}
