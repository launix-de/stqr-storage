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
	directions := map[volume.ID]map[volume.ID]string{}
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
				directions[id] = map[volume.ID]string{}
			}
			links[id][peer] = p.Link
			directions[id][peer] = p.Direction
		}
	}
	for id, peers := range g {
		for _, peer := range peers {
			found := false
			for _, back := range g[peer] {
				found = found || back == id && links[id][peer] == links[peer][id] && cowDirectionsMatch(directions[id][peer], directions[peer][id])
			}
			if !found {
				return nil, fail(503, "clone graph publication is incomplete")
			}
		}
	}
	if err := cowValidateDirectedGraph(directions); err != nil {
		return nil, err
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
	// Copy-before-write captures belong to their query. No peer wait occurs here.
	s.ioPriority.users.Add(1)
	defer s.ioPriority.users.Add(-1)
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
		generation, err := h.Generation()
		if err != nil {
			return nil, err
		}
		return &cowSnapshot{Info: info, store: v, generation: generation}, nil
	}
	extents, err := h.Extents(key)
	if err != nil {
		return nil, err
	}
	if ranges, ok := ctx.Value(cowCaptureRangesKey{}).([]volume.Extent); ok {
		extents = cowReadExtents(extents, ranges)
	}
	var captureBytes uint64
	for _, extent := range extents {
		if !extent.Zero {
			if extent.Length > math.MaxInt64-captureBytes {
				return nil, volume.ErrRange
			}
			captureBytes += extent.Length
		}
	}
	// Native filesystem/block clients use at most 1-MiB objects. Their COW
	// read-through captures must not introduce a hidden disk spool underneath
	// an otherwise RAM-only subscription. Keep these bounded captures in RAM;
	// large general-purpose preservation transfers retain their disk path.
	memory := v.Identity().Class == volume.RAM || captureBytes <= maxSubscriptionEntryBytes
	reserved := false
	if memory && captureBytes <= maxObjectStagingBytes {
		if err = s.objectSubscriptions.reserveSnapshot(captureBytes); err != nil {
			return nil, err
		}
		reserved = true
	}
	file, err := newReplicaSpool(memory)
	if err != nil {
		if reserved {
			s.objectSubscriptions.releaseSnapshot(captureBytes)
		}
		return nil, err
	}
	if reserved {
		file.memory = make([]byte, 0, int(captureBytes))
	}
	var closeOnce sync.Once
	closeSnapshot := func() error {
		closeOnce.Do(func() {
			file.Close()
			if reserved {
				s.objectSubscriptions.releaseSnapshot(captureBytes)
			}
		})
		return nil
	}
	success := false
	defer func() {
		if !success {
			closeSnapshot()
		}
	}()
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
	return &cowSnapshot{Info: info, Data: packed, Extents: kept, Close: closeSnapshot}, nil
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
		if len(d.B) != 0 {
			var ranges []volume.Extent
			for n := d.Count(2); n > 0; n-- {
				ranges = append(ranges, volume.Extent{Offset: d.U64(), Length: d.U64()})
			}
			if metadata || len(ranges) == 0 {
				return nil, client.ErrProtocol
			}
			ctx = context.WithValue(ctx, cowCaptureRangesKey{}, ranges)
		}
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
				if back.Bucket == id.String() && back.Link == edge.Link && cowDirectionsMatch(edge.Direction, back.Direction) {
					symmetric = true
					break
				}
			}
			if !symmetric {
				return [32]byte{}, fail(503, "clone connection is being published")
			}
			e.ID(client.ID(peer))
			e.ID(client.ID(link))
			e.Text(edge.Direction)
		}
		e.ID(client.ID{})
	}
	return sha256.Sum256(e.B), nil
}

// cowReadExtents clips the snapshot to a read and its optional byte condition.
// Zero holes remain implicit; intersecting observations are copied only once.
func cowReadExtents(extents, ranges []volume.Extent) []volume.Extent {
	var out []volume.Extent
	for _, extent := range extents {
		if extent.Zero {
			continue
		}
		for _, r := range ranges {
			start := max(extent.Offset, r.Offset)
			if start < extent.Offset || start-extent.Offset >= extent.Length || start < r.Offset || start-r.Offset >= r.Length {
				continue
			}
			n := min(extent.Length-(start-extent.Offset), r.Length-(start-r.Offset))
			out = append(out, volume.Extent{Offset: start, Length: n})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
	merged := out[:0]
	for _, e := range out {
		if len(merged) > 0 {
			last := &merged[len(merged)-1]
			if e.Offset <= last.Offset+last.Length {
				last.Length = max(last.Length, e.Offset-last.Offset+e.Length)
				continue
			}
		}
		merged = append(merged, e)
	}
	return merged
}

func cowDirectionsMatch(a, b string) bool {
	return a == "" && b == "" || a == "parent" && b == "child" || a == "child" && b == "parent"
}

// Legacy symmetric components are valid, including their historical cycles.
// Directed parent links between those components must form a DAG: otherwise a
// missing record could be incorrectly resolved as absence after a read cycle.
func cowValidateDirectedGraph(edges map[volume.ID]map[volume.ID]string) error {
	directed := false
	for _, peers := range edges {
		for _, direction := range peers {
			if direction != "" {
				directed = true
				break
			}
		}
		if directed {
			break
		}
	}
	if !directed {
		return nil
	}
	parent := map[volume.ID]volume.ID{}
	find := func(id volume.ID) volume.ID {
		root := id
		for parent[root] != root {
			root = parent[root]
		}
		for parent[id] != id {
			next := parent[id]
			parent[id] = root
			id = next
		}
		return root
	}
	for id, peers := range edges {
		parent[id] = id
		for peer := range peers {
			parent[peer] = peer
		}
	}
	for id, peers := range edges {
		for peer, direction := range peers {
			if direction == "" {
				a, b := find(id), find(peer)
				parent[a] = b
			}
		}
	}
	indegree := map[volume.ID]int{}
	forward := map[volume.ID][]volume.ID{}
	for id, peers := range edges {
		parents := 0
		for peer, direction := range peers {
			if direction != "parent" {
				continue
			}
			parents++
			if parents > 1 {
				return fail(503, "clone namespace has multiple parents")
			}
			a, b := find(id), find(peer)
			if a == b {
				return fail(503, "clone inheritance cycle")
			}
			if _, ok := indegree[a]; !ok {
				indegree[a] = 0
			}
			indegree[b]++
			forward[a] = append(forward[a], b)
		}
	}
	var ready []volume.ID
	for id, n := range indegree {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	visited := 0
	for len(ready) > 0 {
		id := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		visited++
		for _, peer := range forward[id] {
			indegree[peer]--
			if indegree[peer] == 0 {
				ready = append(ready, peer)
			}
		}
	}
	if visited != len(indegree) {
		return fail(503, "clone inheritance cycle")
	}
	return nil
}
