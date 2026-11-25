// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"math"
	"os"
)

const transferChunk = 1 << 20

var errEarlyReply = errors.New("client: upload stopped by terminal reply")

type incomingTransfer struct {
	file               *os.File
	memory             *bytes.Buffer
	length, next, part uint64
	digest             [32]byte
	hash               hash.Hash
	ended              bool
}

func (t *incomingTransfer) close() {
	t.memory = nil
	if t.file != nil {
		name := t.file.Name()
		t.file.Close()
		os.Remove(name)
		t.file = nil
	}
}
func (t *objectTransport) streamMessage(m Message) error {
	d := Decoder{B: m.Body}
	id := d.ID()
	if d.Err != nil {
		return ErrProtocol
	}
	t.mu.Lock()
	p := t.pending[id]
	t.mu.Unlock()
	if p == nil {
		return nil
	}
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	if p.canceled {
		return nil
	}
	if p.opcode != objectGet && p.opcode != objectGetRange && p.opcode != ObjectSubscribe {
		return ErrProtocol
	}
	switch m.Opcode {
	case 13:
		if p.transfer != nil {
			return ErrProtocol
		}
		total := d.U64()
		var digest [32]byte
		copy(digest[:], d.Take(32))
		if d.Bool() {
			readObjectInfo(&d)
		}
		if d.Bool() {
			d.U64()
			d.U64()
			d.U64()
		}
		if d.Done() != nil || total > math.MaxInt64 {
			return ErrProtocol
		}
		s := &incomingTransfer{length: total, digest: digest, hash: sha256.New()}
		if p.opcode == ObjectSubscribe {
			// The buffered subscription API already has a 16-MiB limit. Its
			// snapshot is cache state, so never spill it into a filesystem.
			if total > 16<<20 {
				return ErrRange
			}
			s.memory = bytes.NewBuffer(make([]byte, 0, int(total)))
		} else {
			f, e := os.CreateTemp("", "stqr-client-transfer-*")
			if e != nil {
				return e
			}
			s.file = f
		}
		p.transfer = s
	case 5:
		part, offset := d.U64(), d.U64()
		data := d.Blob()
		s := p.transfer
		if d.Done() != nil || s == nil || s.ended || len(data) > transferChunk || part != s.part || offset != s.next || uint64(len(data)) > s.length-s.next {
			return ErrProtocol
		}
		var dst io.Writer = s.file
		if s.memory != nil {
			dst = s.memory
		}
		if _, e := dst.Write(data); e != nil {
			return e
		}
		s.hash.Write(data)
		s.next += uint64(len(data))
		s.part++
	case 6:
		length := d.U64()
		digest := d.Take(32)
		s := p.transfer
		if d.Done() != nil || s == nil || s.ended || length != s.length || s.next != s.length || !equalBytes(digest, s.digest[:]) || !equalBytes(s.hash.Sum(nil), s.digest[:]) {
			return ErrProtocol
		}
		if s.file != nil {
			if _, e := s.file.Seek(0, io.SeekStart); e != nil {
				return e
			}
		}
		s.ended = true
	default:
		return ErrProtocol
	}
	return nil
}
func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(b)
}

type preparedPayload struct {
	length uint64
	digest [32]byte
	inline []byte
	file   *os.File
}

func (p *preparedPayload) close() {
	if p.file != nil {
		name := p.file.Name()
		p.file.Close()
		os.Remove(name)
	}
}
func preparePayload(ctx context.Context, src io.Reader, length uint64) (*preparedPayload, error) {
	if length > math.MaxInt64 || length > 0 && src == nil {
		return nil, ErrRange
	}
	p := &preparedPayload{length: length}
	if length <= transferChunk {
		p.inline = make([]byte, int(length))
		if length > 0 {
			if _, e := io.ReadFull(contextReader{ctx, src}, p.inline); e != nil {
				return nil, e
			}
		}
		p.digest = sha256.Sum256(p.inline)
		return p, nil
	}
	f, e := os.CreateTemp("", "stqr-client-upload-*")
	if e != nil {
		return nil, e
	}
	p.file = f
	h := sha256.New()
	_, e = io.CopyBuffer(io.MultiWriter(f, h), io.LimitReader(contextReader{ctx, src}, int64(length)), make([]byte, 64<<10))
	if e != nil {
		p.close()
		return nil, e
	}
	pos, e := f.Seek(0, io.SeekCurrent)
	if e != nil || uint64(pos) != length {
		p.close()
		if e != nil {
			return nil, e
		}
		return nil, io.ErrUnexpectedEOF
	}
	copy(p.digest[:], h.Sum(nil))
	return p, nil
}
func (p *preparedPayload) encode(e *Encoder) {
	if p.file != nil {
		e.U8(1)
	} else {
		e.U8(0)
	}
	e.U64(p.length)
	e.B = append(e.B, p.digest[:]...)
	e.Blob(p.inline)
}
func (p *preparedPayload) uploader() uploadFunc {
	if p.file == nil {
		return nil
	}
	return func(ctx context.Context, t *objectTransport, pending *pendingObject, id ID) error {
		if _, e := p.file.Seek(0, io.SeekStart); e != nil {
			return e
		}
		buffer := make([]byte, transferChunk)
		var off, part uint64
		for off < p.length {
			if pending.completed.Load() {
				return errEarlyReply
			}
			n := min(uint64(len(buffer)), p.length-off)
			if _, e := io.ReadFull(contextReader{ctx, p.file}, buffer[:n]); e != nil {
				return e
			}
			e := Encoder{}
			e.ID(id)
			e.U64(part)
			e.U64(off)
			e.Blob(buffer[:n])
			if err := t.enqueue(ctx, outboundObject{message: Message{Kind: FireAndForget, Opcode: 5, ID: NewRequestID(), Body: e.B}, parent: id}, true); err != nil {
				select {
				case <-t.done:
					return errEarlyReply
				default:
					return err
				}
			}
			off += n
			part++
		}
		end := Encoder{}
		end.ID(id)
		end.U64(p.length)
		end.B = append(end.B, p.digest[:]...)
		return t.enqueue(ctx, outboundObject{message: Message{Kind: FireAndForget, Opcode: 6, ID: NewRequestID(), Body: end.B}, parent: id}, true)
	}
}
func (c *Client) writePayload(ctx context.Context, opcode uint16, id ObjectID, off uint64, src io.Reader, length uint64, opt MutationOptions) (AppendResult, error) {
	if e := c.ready(ctx); e != nil {
		return AppendResult{}, e
	}
	if opcode == objectPutRange && length > ^uint64(0)-off {
		return AppendResult{}, ErrRange
	}
	e := Encoder{}
	c.objectRef(&e, id)
	if opcode == objectPutRange {
		e.U64(off)
	}
	if err := encodePrecondition(&e, opt); err != nil {
		return AppendResult{}, err
	}
	p, err := preparePayload(ctx, src, length)
	if err != nil {
		return AppendResult{}, err
	}
	defer p.close()
	p.encode(&e)
	r, err := c.do(withAccepted(ctx, opt.Accepted), opcode, e.B, opt.RequestID, true, p.uploader())
	if err != nil {
		return AppendResult{}, err
	}
	defer r.close()
	d := Decoder{B: r.result}
	out := AppendResult{ObjectInfo: readObjectInfo(&d)}
	switch opcode {
	case objectAppend:
		out.StartOffset = d.U64()
	case objectPutRange:
		if offset, n, total := d.U64(), d.U64(), d.U64(); offset != off || n != length || total != out.Length {
			d.Err = ErrProtocol
		}
	}
	if d.Done() != nil {
		return AppendResult{}, ErrProtocol
	}
	if err := c.acceptObjectInfo(out.ObjectInfo); err != nil {
		return AppendResult{}, err
	}
	return out, nil
}
