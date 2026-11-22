// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
)

// ObjectStore is already bound to one bucket by Dial. Object IDs are supplied
// directly; no operation accepts another bucket or a server-side filename.
// Frontends (FUSE/S3/blockdevice) keep their metadata in ordinary objects.
type ObjectStore interface {
	Get(context.Context, ObjectID) ([]byte, ObjectInfo, error)
	GetRange(context.Context, ObjectID, uint64, uint64) ([]byte, ObjectInfo, error)
	Put(context.Context, ObjectID, []byte, PutOptions) (ObjectInfo, error)
	PutRange(context.Context, ObjectID, uint64, []byte, PutOptions) (ObjectInfo, error)
	Append(context.Context, ObjectID, []byte, AppendOptions) (AppendResult, error)
	Trunc(context.Context, ObjectID, uint64, TruncOptions) (ObjectInfo, error)
	Delete(context.Context, ObjectID, DeleteOptions) error
	Stat(context.Context, ObjectID) (ObjectInfo, error)
	CAS(context.Context, ObjectID, uint64, []byte, []byte, CASOptions) (ObjectInfo, error)
}

type StreamingObjectStore interface {
	GetTo(context.Context, ObjectID, io.Writer) (ObjectInfo, error)
	GetRangeTo(context.Context, ObjectID, uint64, uint64, io.Writer) (ObjectInfo, error)
	PutFrom(context.Context, ObjectID, io.Reader, uint64, PutOptions) (ObjectInfo, error)
	PutRangeFrom(context.Context, ObjectID, uint64, io.Reader, uint64, PutOptions) (ObjectInfo, error)
	AppendFrom(context.Context, ObjectID, io.Reader, uint64, AppendOptions) (AppendResult, error)
}

var _ ObjectStore = (*Client)(nil)
var _ StreamingObjectStore = (*Client)(nil)

const maxBufferedObject = 16 << 20
const objectGet uint16 = 0x0100
const objectGetRange uint16 = 0x0101
const objectPut uint16 = 0x0102
const objectPutRange uint16 = 0x0103
const objectAppend uint16 = 0x0104
const objectTrunc uint16 = 0x0105
const objectDelete uint16 = 0x0106
const objectStat uint16 = 0x0107
const objectCAS uint16 = 0x0108

func (c *Client) objectRef(e *Encoder, id ObjectID) {
	e.ID(c.bucket)
	e.B = append(e.B, id[:]...)
	e.U64(0)
	e.U64(0)
	c.mu.Lock()
	runtime := c.runtime
	c.mu.Unlock()
	e.ID(runtime) // TODO(multinode): map revision/epoch and per-range runtime.
}
func encodePrecondition(e *Encoder, o MutationOptions) error {
	if o.Absent && o.ExpectedVersion != nil {
		return ErrRange
	}
	e.Bool(o.ExpectedVersion != nil)
	if o.ExpectedVersion != nil {
		e.U64(*o.ExpectedVersion)
	}
	e.Bool(o.ExpectedLength != nil)
	if o.ExpectedLength != nil {
		e.U64(*o.ExpectedLength)
	}
	e.Bool(o.Absent)
	return nil
}
func readObjectInfo(d *Decoder) ObjectInfo {
	o := ObjectInfo{Version: d.U64(), Length: d.U64()}
	if d.Bool() {
		var hash [32]byte
		copy(hash[:], d.Take(32))
		o.PayloadDigest = &hash
	}
	o.Flags = d.U32()
	o.RuntimeID = d.ID()
	o.Compression = d.U8()
	o.LayoutVersion = d.U16()
	if o.Compression > 3 {
		d.Err = ErrProtocol
	}
	return o
}
func (c *Client) Get(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	return c.getBuffered(ctx, id, 0, 0, false)
}
func (c *Client) GetRange(ctx context.Context, id ObjectID, offset, length uint64) ([]byte, ObjectInfo, error) {
	return c.getBuffered(ctx, id, offset, length, true)
}

type objectBuffer struct{ bytes.Buffer }

func (b *objectBuffer) Write(p []byte) (int, error) {
	if len(p) > maxBufferedObject-b.Len() {
		return 0, ErrTooLarge
	}
	return b.Buffer.Write(p)
}
func (c *Client) getBuffered(ctx context.Context, id ObjectID, off, length uint64, ranged bool) ([]byte, ObjectInfo, error) {
	var b objectBuffer
	info, e := c.getTo(ctx, id, off, length, ranged, &b)
	if e != nil {
		return nil, info, e
	}
	return b.Bytes(), info, nil
}
func (c *Client) GetTo(ctx context.Context, id ObjectID, dst io.Writer) (ObjectInfo, error) {
	return c.getTo(ctx, id, 0, 0, false, dst)
}
func (c *Client) GetRangeTo(ctx context.Context, id ObjectID, off, length uint64, dst io.Writer) (ObjectInfo, error) {
	return c.getTo(ctx, id, off, length, true, dst)
}

// GetRangeToVersion checks the response snapshot version before writing dst.
// It uses the existing GET_RANGE wire operation, not a racy preliminary Stat.
func (c *Client) GetRangeToVersion(ctx context.Context, id ObjectID, off, length, version uint64, dst io.Writer) (ObjectInfo, error) {
	return c.getToChecked(ctx, id, off, length, true, dst, &version)
}
func (c *Client) getTo(ctx context.Context, id ObjectID, off, length uint64, ranged bool, dst io.Writer) (ObjectInfo, error) {
	return c.getToChecked(ctx, id, off, length, ranged, dst, nil)
}
func (c *Client) getToChecked(ctx context.Context, id ObjectID, off, length uint64, ranged bool, dst io.Writer, version *uint64) (ObjectInfo, error) {
	if e := c.ready(ctx); e != nil {
		return ObjectInfo{}, e
	}
	if dst == nil || ranged && length > ^uint64(0)-off {
		return ObjectInfo{}, ErrRange
	}
	e := Encoder{}
	c.objectRef(&e, id)
	op := objectGet
	if ranged {
		op = objectGetRange
		e.U64(off)
		e.U64(length)
	}
	r, err := c.do(ctx, op, e.B, ID{}, false, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer r.close()
	d := Decoder{B: r.result}
	info := readObjectInfo(&d)
	var rangeLength uint64
	if ranged {
		d.U64()
		rangeLength = d.U64()
		total := d.U64()
		if total != info.Length {
			d.Err = ErrProtocol
		}
	}
	if version != nil && info.Version != *version {
		return info, ErrPrecondition
	}
	mode, total, digest, data := decodeTransfer(&d)
	if d.Done() != nil {
		return ObjectInfo{}, ErrProtocol
	}
	if !ranged && total != info.Length || ranged && total != rangeLength {
		return ObjectInfo{}, ErrProtocol
	}
	if mode == 0 {
		if r.transfer != nil {
			return info, ErrProtocol
		}
		if uint64(len(data)) != total || sha256.Sum256(data) != digest {
			return info, ErrProtocol
		}
		if err := c.acceptObjectInfo(info); err != nil {
			return info, err
		}
		if len(data) == 0 {
			return info, nil
		}
		n, err := dst.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		return info, err
	}
	if mode != 1 || len(data) != 0 || r.transfer == nil || r.transfer.length != total || r.transfer.digest != digest {
		return info, ErrProtocol
	}
	if err := c.acceptObjectInfo(info); err != nil {
		return info, err
	}
	_, err = io.CopyBuffer(dst, contextReader{ctx, r.transfer.file}, make([]byte, 64<<10))
	return info, err
}
func decodeTransfer(d *Decoder) (mode uint8, total uint64, digest [32]byte, inline []byte) {
	mode = d.U8()
	total = d.U64()
	copy(digest[:], d.Take(32))
	inline = d.Blob()
	if mode > 1 {
		d.Err = ErrProtocol
	}
	return
}
func (c *Client) Stat(ctx context.Context, id ObjectID) (ObjectInfo, error) {
	if e := c.ready(ctx); e != nil {
		return ObjectInfo{}, e
	}
	e := Encoder{}
	c.objectRef(&e, id)
	r, err := c.do(ctx, objectStat, e.B, ID{}, false, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer r.close()
	d := Decoder{B: r.result}
	info := readObjectInfo(&d)
	if d.Done() != nil {
		return ObjectInfo{}, ErrProtocol
	}
	if err := c.acceptObjectInfo(info); err != nil {
		return info, err
	}
	return info, nil
}
func (c *Client) Put(ctx context.Context, id ObjectID, data []byte, opt PutOptions) (ObjectInfo, error) {
	r, e := c.writePayload(ctx, objectPut, id, 0, bytes.NewReader(data), uint64(len(data)), opt)
	return r.ObjectInfo, e
}
func (c *Client) PutRange(ctx context.Context, id ObjectID, off uint64, data []byte, opt PutOptions) (ObjectInfo, error) {
	r, e := c.writePayload(ctx, objectPutRange, id, off, bytes.NewReader(data), uint64(len(data)), opt)
	return r.ObjectInfo, e
}
func (c *Client) Append(ctx context.Context, id ObjectID, data []byte, opt AppendOptions) (AppendResult, error) {
	return c.writePayload(ctx, objectAppend, id, 0, bytes.NewReader(data), uint64(len(data)), opt)
}
func (c *Client) PutFrom(ctx context.Context, id ObjectID, src io.Reader, length uint64, opt PutOptions) (ObjectInfo, error) {
	r, e := c.writePayload(ctx, objectPut, id, 0, src, length, opt)
	return r.ObjectInfo, e
}
func (c *Client) PutRangeFrom(ctx context.Context, id ObjectID, off uint64, src io.Reader, length uint64, opt PutOptions) (ObjectInfo, error) {
	r, e := c.writePayload(ctx, objectPutRange, id, off, src, length, opt)
	return r.ObjectInfo, e
}
func (c *Client) AppendFrom(ctx context.Context, id ObjectID, src io.Reader, length uint64, opt AppendOptions) (AppendResult, error) {
	return c.writePayload(ctx, objectAppend, id, 0, src, length, opt)
}
func (c *Client) Trunc(ctx context.Context, id ObjectID, length uint64, opt TruncOptions) (ObjectInfo, error) {
	if e := c.ready(ctx); e != nil {
		return ObjectInfo{}, e
	}
	e := Encoder{}
	c.objectRef(&e, id)
	e.U64(length)
	if err := encodePrecondition(&e, opt); err != nil {
		return ObjectInfo{}, err
	}
	r, err := c.do(ctx, objectTrunc, e.B, opt.RequestID, true, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer r.close()
	d := Decoder{B: r.result}
	info := readObjectInfo(&d)
	if d.Done() != nil {
		return ObjectInfo{}, ErrProtocol
	}
	if err := c.acceptObjectInfo(info); err != nil {
		return info, err
	}
	return info, nil
}
func (c *Client) Delete(ctx context.Context, id ObjectID, opt DeleteOptions) error {
	if e := c.ready(ctx); e != nil {
		return e
	}
	e := Encoder{}
	c.objectRef(&e, id)
	if err := encodePrecondition(&e, opt); err != nil {
		return err
	}
	r, err := c.do(ctx, objectDelete, e.B, opt.RequestID, true, nil)
	defer r.close()
	if err == nil && len(r.result) != 0 {
		return ErrProtocol
	}
	return err
}
func (c *Client) CAS(ctx context.Context, id ObjectID, off uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	if e := c.ready(ctx); e != nil {
		return ObjectInfo{}, e
	}
	if opt.Scope > CASObject || len(expected) > 1<<20 || len(replacement) > 1<<20 || opt.Scope == CASRange && len(expected) != len(replacement) || opt.Scope == CASObject && (off != 0 || !opt.Absent && opt.ExpectedVersion == nil) || uint64(len(replacement)) > ^uint64(0)-off {
		return ObjectInfo{}, ErrRange
	}
	e := Encoder{}
	c.objectRef(&e, id)
	e.U8(uint8(opt.Scope))
	e.U64(off)
	if err := encodePrecondition(&e, opt.MutationOptions); err != nil {
		return ObjectInfo{}, err
	}
	e.Blob(expected)
	e.Blob(replacement)
	r, err := c.do(ctx, objectCAS, e.B, opt.RequestID, true, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer r.close()
	d := Decoder{B: r.result}
	info := readObjectInfo(&d)
	if d.Done() != nil {
		return ObjectInfo{}, ErrProtocol
	}
	if err := c.acceptObjectInfo(info); err != nil {
		return info, err
	}
	return info, nil
}
