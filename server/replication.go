// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	peerReplicaPrepare         uint16 = 0x101c
	peerReplicaReady           uint16 = 0x101d
	peerReplicaCommit          uint16 = 0x101e
	peerReplicaCommitted       uint16 = 0x101f
	peerReplicaAbort           uint16 = 0x1020
	peerReplicaChunk           uint16 = 0x1074
	peerReplicaTopologyReady   uint16 = 0x1075
	peerReplicaTopologyCleanup uint16 = 0x1076
)

type replicaLock struct {
	sync.Mutex
	refs int
}
type replicaManager struct {
	server   *Server
	mu       sync.Mutex
	locks    map[volume.Key]*replicaLock
	uploads  map[volume.ID]*replicaUpload
	sequence atomic.Uint64
}
type replicaHeader struct {
	Schema          uint16
	Volume          volume.ID
	Request         volume.RequestRecord
	Binding         volume.ReplicaBinding
	PreviousVersion uint64
	Opcode          uint8
	Offset, Length  uint64
	Expected        []byte
	Options         volume.Options
}
type replicaUpload struct {
	actor   client.ID
	header  replicaHeader
	spool   *replicaSpool
	updated time.Time
}

// Persistent payload staging uses a bounded-memory temporary file; RAM payload
// never enters a file, including during the transfer preceding durable READY.
type replicaSpool struct {
	file     *os.File
	memory   []byte
	position int64
}

func newReplicaSpool(ram bool) (*replicaSpool, error) {
	s := &replicaSpool{}
	if !ram {
		f, err := os.CreateTemp("", "stqr-replica-*")
		if err != nil {
			return nil, err
		}
		s.file = f
	}
	return s, nil
}
func (s *replicaSpool) Write(b []byte) (int, error) {
	if s.file != nil {
		return s.file.Write(b)
	}
	s.memory = append(s.memory, b...)
	s.position = int64(len(s.memory))
	return len(b), nil
}
func (s *replicaSpool) Read(b []byte) (int, error) {
	if s.file != nil {
		return s.file.Read(b)
	}
	if s.position >= int64(len(s.memory)) {
		return 0, io.EOF
	}
	n := copy(b, s.memory[s.position:])
	s.position += int64(n)
	return n, nil
}
func (s *replicaSpool) Seek(off int64, whence int) (int64, error) {
	if s.file != nil {
		return s.file.Seek(off, whence)
	}
	switch whence {
	case io.SeekStart:
		s.position = off
	case io.SeekCurrent:
		s.position += off
	case io.SeekEnd:
		s.position = int64(len(s.memory)) + off
	default:
		return 0, volume.ErrRange
	}
	if s.position < 0 {
		return 0, volume.ErrRange
	}
	return s.position, nil
}
func (s *replicaSpool) Close() {
	if s.file != nil {
		name := s.file.Name()
		s.file.Close()
		os.Remove(name)
		s.file = nil
	}
	s.memory = nil
}

func (s *Server) initReplicas() error {
	r := &replicaManager{server: s, locks: make(map[volume.Key]*replicaLock), uploads: make(map[volume.ID]*replicaUpload)}
	for _, v := range s.quotaVolumes() {
		h, err := v.Open(false)
		if err != nil {
			return err
		}
		err = h.ScanReplicas(func(x volume.ReplicaRecord) error {
			if x.Binding.Sequence > r.sequence.Load() {
				r.sequence.Store(x.Binding.Sequence)
			}
			return nil
		})
		h.Close()
		if err != nil {
			return err
		}
	}
	s.replicas = r
	return nil
}

// Caller holds opMu. A handoff/control activation must not strand an earlier
// durable PREPARE or COMMIT under a different owner epoch or replica-set hash.
func (s *Server) checkPendingReplicas(include func(volume.Key) bool) error {
	for _, v := range s.quotaVolumes() {
		h, err := v.Open(false)
		if err != nil {
			return err
		}
		err = h.ScanPendingReplicas(func(x volume.ReplicaRecord) error {
			if include == nil || include(x.Request.Key) {
				return fail(503, "replica transaction must finish before placement change")
			}
			return nil
		})
		h.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) pendingReplicaGrowth(bucket volume.ID) (uint64, error) {
	var growth uint64
	for _, v := range s.quotaVolumes() {
		h, err := v.Open(false)
		if err != nil {
			return 0, err
		}
		err = h.ScanPendingReplicas(func(x volume.ReplicaRecord) error {
			if x.Request.Key.Bucket != bucket || x.State == volume.ReplicaAborted {
				return nil
			}
			entry, ok := s.placement.Load().lookup(client.ID(bucket), client.ObjectID(x.Request.Key.Object))
			if !ok || len(entry.Locations) == 0 || entry.Locations[0].Node != s.control.ledger.node {
				return nil
			}
			if x.Request.Result.Info.Length > x.PreviousLength {
				growth = addCapacity(growth, x.Request.Result.Info.Length-x.PreviousLength)
			}
			return nil
		})
		h.Close()
		if err != nil {
			return 0, err
		}
	}
	return growth, nil
}
func (r *replicaManager) lock(k volume.Key) func() {
	r.mu.Lock()
	l := r.locks[k]
	if l == nil {
		l = &replicaLock{}
		r.locks[k] = l
	}
	l.refs++
	r.mu.Unlock()
	l.Lock()
	return func() {
		l.Unlock()
		r.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(r.locks, k)
		}
		r.mu.Unlock()
	}
}
func replicaSetHash(bucket volume.ID, entry client.RangeEntry) [32]byte {
	e := client.Encoder{}
	e.ID(client.ID(bucket))
	e.B = append(e.B, entry.Start[:]...)
	e.U16(entry.PrefixBits)
	e.U64(entry.OwnerEpoch)
	e.U64(entry.PolicyVersion)
	e.ID(entry.Runtime)
	for _, loc := range entry.Locations {
		e.ID(loc.Node)
		e.ID(loc.Volume)
		e.U8(loc.Role)
		e.U8(loc.Class)
	}
	return sha256.Sum256(e.B)
}
func replicaOpcode(op uint16) uint8 {
	switch op {
	case 0x0102:
		return 1
	case 0x0103:
		return 2
	case 0x0104:
		return 3
	case 0x0105:
		return 4
	case 0x0106:
		return 5
	case 0x0108:
		return 6
	}
	return 0
}

func (b *Bucket) prepareReplicated(v *volume.Volume, key volume.Key, entry client.RangeEntry, opcode uint16, offset, length uint64, src io.Reader, expected []byte, opts volume.Options) (volume.Result, error) {
	h, err := v.Open(true)
	if err != nil {
		return volume.Result{}, err
	}
	defer h.Close()
	prior, known, err := h.ReplicaStatus(opts.RequestID)
	if err != nil {
		return volume.Result{}, err
	}
	binding := volume.ReplicaBinding{Epoch: entry.OwnerEpoch, SetHash: replicaSetHash(key.Bucket, entry)}
	if known {
		binding.Sequence = prior.Binding.Sequence
	} else {
		binding.Sequence = b.server.replicas.sequence.Add(1)
	}
	result, err := h.PrepareMutation(replicaOpcode(opcode), key, offset, length, src, expected, opts, binding)
	if err != nil && known && prior.State == volume.ReplicaAborted && prior.AbortStatus >= 400 && errors.Is(err, volume.ErrPrecondition) {
		return result, fail(int(prior.AbortStatus), "replica write was aborted before commit")
	}
	var quotaFull *physicalQuotaFull
	if errors.As(err, &quotaFull) && quotaFull.Waiting {
		return result, fail(503, "waiting for physical replica growth credits")
	}
	if errors.Is(err, volume.ErrBusy) {
		err = fail(503, "an earlier replica transaction is still pending")
	}
	return result, err
}

func (r *replicaManager) validate(header replicaHeader, actor client.ID) (*volume.Volume, error) {
	s := r.server
	entry, ok := s.placement.Load().lookup(client.ID(header.Request.Key.Bucket), client.ObjectID(header.Request.Key.Object))
	if !ok || len(entry.Locations) < 2 || entry.State == 4 || entry.State == 5 || entry.Locations[0].Node != actor || header.Binding.Epoch != entry.OwnerEpoch || header.Binding.SetHash != replicaSetHash(header.Request.Key.Bucket, entry) {
		return nil, fail(409, "replica authority changed")
	}
	local := false
	for _, loc := range entry.Locations {
		if loc.Role == 1 && loc.Node == s.control.ledger.node && loc.Volume == client.ID(header.Volume) {
			local = true
		}
	}
	if !local {
		return nil, fail(403, "node is outside required replica set")
	}
	s.mu.RLock()
	m := s.volumes[header.Volume.String()]
	s.mu.RUnlock()
	if m == nil {
		return nil, fail(503, "required replica volume unavailable")
	}
	if entry.Runtime != (client.ID{}) {
		return nil, fail(503, "RAM replica boot proof is not active")
	}
	return m.store, nil
}

func (s *Server) replicaPeerMessage(m client.Message, actor client.ID) ([]byte, error) {
	if m.Kind != client.Request {
		return nil, fail(400, "replica transition requires a request")
	}
	if m.Opcode == peerReplicaTopologyReady || m.Opcode == peerReplicaTopologyCleanup {
		return s.replicaTopologyMessage(m, actor)
	}
	if s.replicas == nil {
		return nil, fail(503, "replication unavailable")
	}
	r := s.replicas
	d := client.Decoder{B: m.Body}
	var header replicaHeader
	if err := json.Unmarshal(d.Blob(), &header); err != nil {
		return nil, client.ErrProtocol
	}
	if header.Schema != 1 {
		return nil, client.ErrProtocol
	}
	if header.Options.RequestID != header.Request.ID || header.Request.ID == (volume.ID{}) || header.Length > maxObjectStagingBytes || len(header.Expected) > 1<<20 {
		return nil, client.ErrProtocol
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v, err := r.validate(header, actor)
	if err != nil {
		return nil, err
	}
	if m.Opcode == peerReplicaPrepare {
		if d.Done() != nil {
			return nil, client.ErrProtocol
		}
		h, err := v.Open(false)
		if err != nil {
			return nil, err
		}
		prior, ok, err := h.ReplicaStatus(header.Request.ID)
		h.Close()
		if err != nil {
			return nil, err
		}
		if ok {
			if prior.Binding != header.Binding || prior.Request != header.Request {
				return nil, volume.ErrRequestID
			}
			if prior.State == volume.ReplicaAborted {
				return nil, volume.ErrPrecondition
			}
			return []byte{1}, nil
		}
		if old := r.uploads[header.Request.ID]; old != nil {
			old.spool.Close()
			delete(r.uploads, header.Request.ID)
		}
		if len(r.uploads) >= maxObjectStagingTransfers {
			return nil, fail(503, "replica upload slots exhausted")
		}
		spool, err := newReplicaSpool(v.Identity().Class == volume.RAM)
		if err != nil {
			return nil, err
		}
		r.uploads[header.Request.ID] = &replicaUpload{actor: actor, header: header, spool: spool, updated: time.Now()}
		return []byte{0}, nil
	}
	if m.Opcode == peerReplicaChunk {
		offset, data := d.U64(), d.Blob()
		if d.Done() != nil || len(data) > 1<<20 {
			return nil, client.ErrProtocol
		}
		u := r.uploads[header.Request.ID]
		if u == nil || u.actor != actor || !sameReplicaHeader(u.header, header) {
			return nil, volume.ErrPrecondition
		}
		position, err := u.spool.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if offset != uint64(position) || offset > header.Length || uint64(len(data)) > header.Length-offset {
			return nil, client.ErrProtocol
		}
		_, err = u.spool.Write(data)
		u.updated = time.Now()
		return nil, err
	}
	if d.Done() != nil {
		return nil, client.ErrProtocol
	}
	// Local admission and publication are serialized with other local writes;
	// neither volume handles nor opMu are held while sending any peer request.
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if _, err = r.validate(header, actor); err != nil {
		return nil, err
	}
	if err = s.refreshQuotaProtectionLocked(); err != nil {
		return nil, err
	}
	if err = s.ensureVolumeAuthorityLocked(); err != nil {
		return nil, err
	}
	h, err := v.Open(true)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	switch m.Opcode {
	case peerReplicaReady:
		u := r.uploads[header.Request.ID]
		if u == nil || u.actor != actor || !sameReplicaHeader(u.header, header) {
			return nil, volume.ErrPrecondition
		}
		defer func() { u.spool.Close(); delete(r.uploads, header.Request.ID) }()
		position, err := u.spool.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		payload := header.Opcode != 4 && header.Opcode != 5
		if payload && uint64(position) != header.Length {
			return nil, io.ErrUnexpectedEOF
		}
		if _, err = u.spool.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		old, e := h.RecordInfo(header.Request.Key)
		if e != nil && !errors.Is(e, volume.ErrNotFound) {
			return nil, e
		}
		if old.Version != header.PreviousVersion {
			return nil, volume.ErrPrecondition
		}
		_, err = h.PrepareMutation(header.Opcode, header.Request.Key, header.Offset, header.Length, u.spool, header.Expected, header.Options, header.Binding)
		if err != nil {
			var quotaFull *physicalQuotaFull
			if errors.As(err, &quotaFull) && quotaFull.Waiting {
				return nil, fail(503, "waiting for physical replica growth credits")
			}
			return nil, err
		}
		record, ok, err := h.ReplicaStatus(header.Request.ID)
		if err != nil {
			return nil, err
		}
		if !ok || record.Request != header.Request {
			_ = h.AbortReplica(header.Request.ID, header.Binding)
			return nil, volume.ErrPrecondition
		}
		return nil, nil
	case peerReplicaCommit:
		record, ok, e := h.ReplicaStatus(header.Request.ID)
		if e != nil {
			return nil, e
		}
		if !ok || record.Request != header.Request {
			return nil, volume.ErrPrecondition
		}
		if record.State == volume.ReplicaPublished {
			if record.Binding != header.Binding {
				return nil, volume.ErrPrecondition
			}
			return nil, nil
		}
		if err = h.DecideReplica(header.Request.ID, header.Binding); err != nil {
			return nil, err
		}
		old, _ := h.RecordInfo(header.Request.Key)
		result, err := h.PublishReplica(header.Request.ID, header.Binding)
		if err == nil && result.Info.Version != old.Version && s.objectSubscriptions.watched(header.Request.Key) {
			s.objectSubscriptions.publish(header.Request.Key, old.Version, result.Info, h)
		}
		return nil, err
	case peerReplicaCommitted:
		record, ok, err := h.ReplicaStatus(header.Request.ID)
		if err != nil {
			return nil, err
		}
		if !ok || record.Binding != header.Binding || record.Request != header.Request {
			return nil, volume.ErrPrecondition
		}
		return []byte{byte(record.State)}, nil
	case peerReplicaAbort:
		return nil, h.AbortReplica(header.Request.ID, header.Binding)
	}
	return nil, client.ErrProtocol
}
func sameReplicaHeader(a, b replicaHeader) bool {
	aw, _ := json.Marshal(a)
	bw, _ := json.Marshal(b)
	return bytes.Equal(aw, bw)
}
func replicaWire(h replicaHeader) client.Encoder {
	h.Schema = 1
	wire, _ := json.Marshal(h)
	e := client.Encoder{}
	e.Blob(wire)
	return e
}
func (r *replicaManager) call(ctx context.Context, node client.ID, op uint16, body []byte) ([]byte, error) {
	return (peerControlParticipants{runtime: r.server.control}).call(ctx, node, op, body)
}

func (r *replicaManager) preparePeers(ctx context.Context, entry client.RangeEntry, header replicaHeader, src io.ReadSeeker) error {
	for _, loc := range entry.Locations {
		if loc.Role != 1 {
			continue
		}
		header.Volume = volume.ID(loc.Volume)
		e := replicaWire(header)
		body, err := r.call(ctx, loc.Node, peerReplicaPrepare, e.B)
		if err != nil {
			return err
		}
		if bytes.Equal(body, []byte{1}) {
			continue
		}
		if !bytes.Equal(body, []byte{0}) {
			return client.ErrProtocol
		}
		if header.Opcode != 4 && header.Opcode != 5 {
			if _, err = src.Seek(0, io.SeekStart); err != nil {
				return err
			}
			buf := make([]byte, 1<<20)
			for offset := uint64(0); offset < header.Length; {
				n := min(uint64(len(buf)), header.Length-offset)
				if _, err = io.ReadFull(src, buf[:n]); err != nil {
					return err
				}
				chunk := replicaWire(header)
				chunk.U64(offset)
				chunk.Blob(buf[:n])
				if _, err = r.call(ctx, loc.Node, peerReplicaChunk, chunk.B); err != nil {
					return err
				}
				offset += n
			}
		}
		if _, err = r.call(ctx, loc.Node, peerReplicaReady, e.B); err != nil {
			return err
		}
	}
	return nil
}
func (r *replicaManager) commitPeers(ctx context.Context, entry client.RangeEntry, header replicaHeader) error {
	return r.decisionPeers(ctx, entry, header, peerReplicaCommit)
}
func (r *replicaManager) decisionPeers(ctx context.Context, entry client.RangeEntry, header replicaHeader, opcode uint16) error {
	for _, loc := range entry.Locations {
		if loc.Role != 1 {
			continue
		}
		header.Volume = volume.ID(loc.Volume)
		e := replicaWire(header)
		if _, err := r.call(ctx, loc.Node, opcode, e.B); err != nil {
			return err
		}
	}
	return nil
}

func (r *replicaManager) abortPrepared(ctx context.Context, v *volume.Volume, entry client.RangeEntry, record volume.ReplicaRecord, status uint16) error {
	s := r.server
	s.opMu.Lock()
	h, err := v.Open(true)
	if err == nil {
		err = h.AbortReplicaPending(record.Request.ID, record.Binding, status)
		h.Close()
	}
	s.opMu.Unlock()
	if err != nil {
		return err
	}
	header := replicaHeader{Request: record.Request, Binding: record.Binding, Options: volume.Options{RequestID: record.Request.ID}}
	if err = r.decisionPeers(ctx, entry, header, peerReplicaAbort); err != nil {
		return fail(503, "abort is waiting for required replicas: "+err.Error())
	}
	s.opMu.Lock()
	h, err = v.Open(true)
	if err == nil {
		err = h.FinishReplicaAbort(record.Request.ID, record.Binding)
		h.Close()
	}
	s.opMu.Unlock()
	if err != nil {
		return err
	}
	return fail(int(status), "replica preparation rejected; write was aborted before commit")
}

func (r *replicaManager) start() {
	s := r.server
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		defer func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			for _, u := range r.uploads {
				u.spool.Close()
			}
			r.uploads = nil
		}()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
			}
			r.mu.Lock()
			for id, u := range r.uploads {
				if time.Since(u.updated) > time.Minute {
					u.spool.Close()
					delete(r.uploads, id)
				}
			}
			r.mu.Unlock()
			for _, v := range s.quotaVolumes() {
				var pending []volume.ReplicaRecord
				h, err := v.Open(false)
				if err != nil {
					continue
				}
				_ = h.ScanPendingReplicas(func(x volume.ReplicaRecord) error {
					if x.State == volume.ReplicaDecided || x.State == volume.ReplicaAborted {
						pending = append(pending, x)
					}
					return nil
				})
				h.Close()
				for _, record := range pending {
					entry, ok := s.placement.Load().lookup(client.ID(record.Request.Key.Bucket), client.ObjectID(record.Request.Key.Object))
					if !ok || len(entry.Locations) < 2 || entry.Locations[0].Node != s.control.ledger.node || replicaSetHash(record.Request.Key.Bucket, entry) != record.Binding.SetHash {
						continue
					}
					ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
					header := replicaHeader{Request: record.Request, Binding: record.Binding, Options: volume.Options{RequestID: record.Request.ID}}
					if record.State == volume.ReplicaAborted {
						err = r.decisionPeers(ctx, entry, header, peerReplicaAbort)
					} else {
						err = r.commitPeers(ctx, entry, header)
					}
					cancel()
					if err != nil {
						continue
					}
					s.opMu.Lock()
					h, err := v.Open(true)
					if err == nil {
						published := false
						current, known, e := h.ReplicaStatus(record.Request.ID)
						if e == nil && known {
							if record.State == volume.ReplicaAborted {
								_ = h.FinishReplicaAbort(record.Request.ID, record.Binding)
							} else if current.State == volume.ReplicaDecided {
								old, _ := h.RecordInfo(record.Request.Key)
								result, e := h.PublishReplica(record.Request.ID, record.Binding)
								if e == nil {
									published = true
									s.objectSubscriptions.publish(record.Request.Key, old.Version, result.Info, h)
								}
							}
						}
						h.Close()
						if published {
							if b, e := s.Bucket(record.Request.Key.Bucket); e == nil {
								b.usage.mu.Lock()
								b.usage.valid = false
								b.usage.mu.Unlock()
							}
						}
					}
					s.opMu.Unlock()
				}
			}
		}
	}()
}
