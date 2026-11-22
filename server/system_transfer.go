// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerControlInfo uint16 = 0x1046
const peerSystemChunk uint16 = 0x1047

type controlNodeInfo struct {
	node  client.ID
	root  volume.ID
	class volume.Class
	base  controlBase
}

func (s *Server) systemPeerMessage(m client.Message) ([]byte, error) {
	if m.Kind != client.Request {
		return nil, fail(400, "system transfer requires a request")
	}
	if s.control == nil {
		return nil, fail(503, "cluster controller is not active")
	}
	if m.Opcode == peerControlInfo {
		d := client.Decoder{B: m.Body}
		if d.U16() != 1 || d.Done() != nil {
			return nil, client.ErrProtocol
		}
		l := s.control.ledger
		l.mu.Lock()
		base := cloneControlBase(l.current)
		l.mu.Unlock()
		wire, err := (client.NodeHelloAck{Membership: base.Membership, ControlSequence: base.Sequence, Generation: base.Generation, MapRevision: base.MapRevision}).MarshalBinary()
		if err != nil {
			return nil, err
		}
		root, err := s.metadataStore()
		if err != nil {
			return nil, err
		}
		e := client.Encoder{}
		e.U16(1)
		e.ID(l.node)
		e.ID(client.ID(root.Identity().VolumeID))
		e.U8(uint8(root.Identity().Class))
		e.Blob(wire)
		e.B = append(e.B, base.Head[:]...)
		e.B = append(e.B, base.SystemImage[:]...)
		return e.B, nil
	}
	d := client.Decoder{B: m.Body}
	schema := d.U16()
	var id volume.ObjectID
	copy(id[:], d.Take(32))
	offset, total := d.U64(), d.U64()
	data := d.Blob()
	final := d.Bool()
	if schema != 1 || d.Done() != nil || len(data) > 1<<20 || offset > total || uint64(len(data)) > total-offset || final != (offset+uint64(len(data)) == total) {
		return nil, client.ErrProtocol
	}
	if err := s.putSystemChunk(id, offset, total, data, final); err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U64(offset + uint64(len(data)))
	return e.B, nil
}

func (c *clusterController) info(ctx context.Context, node client.ID) (controlNodeInfo, error) {
	e := client.Encoder{}
	e.U16(1)
	body, err := (peerControlParticipants{runtime: c.server.control}).call(ctx, node, peerControlInfo, e.B)
	if err != nil {
		return controlNodeInfo{}, err
	}
	d := client.Decoder{B: body}
	schema := d.U16()
	info := controlNodeInfo{node: d.ID(), root: volume.ID(d.ID()), class: volume.Class(d.U8())}
	ack, err := client.DecodeNodeHelloAck(d.Blob())
	info.base = controlBase{Membership: ack.Membership, Sequence: ack.ControlSequence, Generation: ack.Generation, MapRevision: ack.MapRevision}
	copy(info.base.Head[:], d.Take(32))
	copy(info.base.SystemImage[:], d.Take(32))
	if err != nil || d.Done() != nil || schema != 1 || info.node != node || info.root == (volume.ID{}) || info.class > volume.SSD {
		return controlNodeInfo{}, client.ErrProtocol
	}
	return info, nil
}

func (c *clusterController) sendBundle(ctx context.Context, node client.ID, bundle *systemBundle) error {
	for id, data := range bundle.Objects {
		for offset := 0; offset < len(data) || offset == 0; {
			end := min(offset+1<<20, len(data))
			e := client.Encoder{}
			e.U16(1)
			e.B = append(e.B, id[:]...)
			e.U64(uint64(offset))
			e.U64(uint64(len(data)))
			e.Blob(data[offset:end])
			e.Bool(end == len(data))
			body, err := (peerControlParticipants{runtime: c.server.control}).call(ctx, node, peerSystemChunk, e.B)
			if err != nil {
				return err
			}
			d := client.Decoder{B: body}
			if d.U64() != uint64(end) || d.Done() != nil {
				return client.ErrProtocol
			}
			if end == len(data) {
				break
			}
			offset = end
		}
	}
	return nil
}

// Chunk staging is durable and idempotent across connections/restarts. Final
// publication verifies the complete object hash and claims one contiguous PUT
// allocation. Only the referenced graph/head can activate these ordinary bytes.
func (s *Server) putSystemChunk(id volume.ObjectID, offset, total uint64, data []byte, final bool) error {
	tmpID := volume.ObjectID(sha256.Sum256(append([]byte("STQR staged system object v1\x00"), id[:]...)))
	return s.metadataWrite(func(h *volume.Handle) error {
		key := volume.Key{Bucket: systemBucket, Object: id}
		if existing, err := h.Stat(key); err == nil {
			if existing.Length != total {
				return volume.ErrCorrupt
			}
			return nil
		} else if !errors.Is(err, volume.ErrNotFound) {
			return err
		}
		tmp := volume.Key{Bucket: systemBucket, Object: tmpID}
		info, err := h.Stat(tmp)
		if err != nil && !errors.Is(err, volume.ErrNotFound) {
			return err
		}
		if offset > info.Length {
			return fail(409, "missing earlier system chunk")
		}
		if offset < info.Length {
			prior, _, err := h.GetRange(tmp, offset, uint64(len(data)))
			if err != nil {
				return err
			}
			if !bytes.Equal(prior, data) {
				return volume.ErrCorrupt
			}
		} else {
			if _, err := h.Append(tmp, data, volume.Options{ExpectedLength: &offset}); err != nil {
				return err
			}
		}
		if !final {
			return nil
		}
		info, err = h.Stat(tmp)
		if err != nil {
			return err
		}
		if info.Length != total {
			return volume.ErrCorrupt
		}
		file, err := os.CreateTemp("", "stqr-system-content-*")
		if err != nil {
			return err
		}
		defer func() { file.Close(); os.Remove(file.Name()) }()
		hash := sha256.New()
		if _, err := h.CopyTo(tmp, 0, total, io.MultiWriter(file, hash)); err != nil {
			return err
		}
		if !bytes.Equal(hash.Sum(nil), id[:]) {
			return volume.ErrCorrupt
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := h.PutReader(key, file, total, volume.Options{CreateOnly: true}); err != nil {
			return err
		}
		return h.Delete(tmp, volume.Options{})
	})
}
