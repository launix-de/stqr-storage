// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"os"
	"syscall"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func objectStatus(err error) int {
	var redirect *objectRedirect
	if errors.As(err, &redirect) {
		return 301
	}
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return 507
	}
	if errors.Is(err, volume.ErrPrecondition) {
		return 412
	}
	if errors.Is(err, volume.ErrRequestID) {
		return 400
	}
	if errors.Is(err, context.Canceled) {
		return 499
	}
	return statusOf(err)
}
func encodeObjectInfo(e *client.Encoder, info volume.Info, runtime client.ID) {
	e.U64(info.Version)
	e.U64(info.Length)
	e.Bool(false)
	e.U32(0)
	e.ID(runtime)
	e.U8(uint8(info.Compression))
	e.U16(volume.FormatVersion)
}
func decodeObjectOptions(d *client.Decoder, id client.ID) volume.Options {
	opt := volume.Options{RequestID: volume.ID(id)}
	if d.Bool() {
		v := d.U64()
		opt.ExpectedVersion = &v
	}
	if d.Bool() {
		v := d.U64()
		opt.ExpectedLength = &v
	}
	opt.CreateOnly = d.Bool()
	if opt.CreateOnly && opt.ExpectedVersion != nil {
		d.Err = volume.ErrRange
	}
	return opt
}

type objectMutation struct {
	message        client.Message
	bucket         *Bucket
	object         volume.ObjectID
	offset, length uint64
	options        volume.Options
	expected       []byte
	digest         [32]byte
	inline         []byte
	stream         bool
	accepted       bool
}

func (s *Server) parseObject(m client.Message, ws *wsSession, p principal) (*objectMutation, *client.Decoder, error) {
	d := &client.Decoder{B: m.Body}
	bucketID := volume.ID(d.ID())
	var oid volume.ObjectID
	copy(oid[:], d.Take(32))
	revision := d.U64()
	epoch := d.U64()
	runtime := d.ID()
	if d.Err != nil {
		return nil, d, fail(400, "invalid object reference")
	}
	var b *Bucket
	if ws != nil {
		if ws.authorization == nil || ws.authorization.bucket == nil {
			return nil, d, fail(400, "a bucket-bound connection is required")
		}
		if bucketID != ws.authorization.bucketID {
			return nil, d, ErrBucketForbidden
		}
		b = ws.authorization.bucket
	} else {
		var err error
		b, err = s.Bucket(bucketID)
		if err != nil {
			return nil, d, err
		}
	}
	description := b.storageDescription()
	bucketScoped := m.Opcode == client.BucketMaintenance || m.Opcode == client.BucketBlockMaintenance || m.Opcode == client.BucketMaintenanceEnd || m.Opcode == client.BucketScan || m.Opcode == client.BucketObjectList
	// No caller may enter the reserved metadata namespace through object RPCs.
	if bucketID == systemBucket {
		return nil, d, ErrBucketForbidden
	}
	if s.controller != nil {
		permission := BucketRead
		switch m.Opcode {
		case 0x0102, 0x0103, 0x0105, 0x0108:
			permission = BucketWrite
		case 0x0104:
			permission = BucketAppend
		case 0x0106:
			permission = blockDeletePermission(p)
		}
		if _, err := b.authorize(p, permission); err != nil {
			return nil, d, err
		}
		if err := s.checkObjectOwner(bucketID, oid, permission == BucketRead); err != nil && !bucketScoped {
			return nil, d, err
		}
		patch, _ := s.routingPatch(bucketID, oid)
		if !bucketScoped && epoch != 0 && epoch != patch.Entries[0].OwnerEpoch {
			return nil, d, &objectRedirect{patch: patch}
		}
	} else if revision != 0 && !bucketScoped {
		s.mu.RLock()
		generation := s.cfg.Generation
		s.mu.RUnlock()
		if revision != generation {
			return nil, d, fail(503, "stale placement map")
		}
	}
	if !bucketScoped && s.controller == nil && epoch > 1 {
		return nil, d, fail(503, "unknown ownership epoch")
	}
	if bucketScoped {
		return &objectMutation{message: m, bucket: b, object: oid}, d, nil
	}
	if description.Policy.StorageStrategy == "ram" {
		currentRuntime := s.boot
		if s.controller != nil {
			patch, _ := s.routingPatch(bucketID, oid)
			currentRuntime = patch.Entries[0].Runtime
		}
		if runtime != (client.ID{}) && runtime != currentRuntime {
			return nil, d, fail(410, "RAM runtime reset")
		}
	} else if runtime != (client.ID{}) {
		return nil, d, fail(410, "bucket runtime changed")
	}
	return &objectMutation{message: m, bucket: b, object: oid}, d, nil
}
func (s *Server) executeObjects(m client.Message, ws *wsSession, p principal) ([]byte, int, error) {
	accepted := m.Opcode == client.ObjectAccepted
	if accepted {
		if len(m.Body) < 82 {
			return nil, 0, fail(400, "invalid accepted mutation")
		}
		inner := client.Decoder{B: m.Body[80:82]}
		m.Opcode = inner.U16()
		if m.Opcode < 0x102 || m.Opcode > 0x108 || m.Opcode == 0x107 {
			return nil, 0, fail(400, "accepted wrapper requires mutation")
		}
		m.Body = append(append([]byte{}, m.Body[:80]...), m.Body[82:]...)
	}
	op, d, err := s.parseObject(m, ws, p)
	if err != nil {
		return nil, 0, err
	}
	op.accepted = accepted
	ctx := s.ctx
	if ws != nil {
		ctx = ws.objectContext(m.ID)
	}
	if m.Opcode == 0x0100 || m.Opcode == 0x0101 || m.Opcode == 0x0107 {
		ctx = bindObjectRequest(ctx, m)
	}
	runtime := client.ID{}
	if op.bucket.Describe().Policy.StorageStrategy == "ram" {
		runtime = s.boot
		if s.controller != nil {
			patch, _ := s.routingPatch(op.bucket.ID(), op.object)
			runtime = patch.Entries[0].Runtime
		}
	}
	switch m.Opcode {
	case client.BucketObjectList:
		return s.executeObjectList(ctx, op, d, p, runtime)
	case client.BucketMaintenance, client.BucketBlockMaintenance, client.BucketMaintenanceEnd, client.BucketScan:
		return s.executeMaintenance(ctx, op, d, ws, p, runtime)
	case 0x0100, 0x0101:
		length := uint64(math.MaxUint64)
		if m.Opcode == 0x0101 {
			op.offset = d.U64()
			length = d.U64()
			if length > math.MaxUint64-op.offset {
				return nil, 0, volume.ErrRange
			}
		}
		if d.Done() != nil {
			return nil, 0, fail(400, "invalid GET body")
		}
		// Spooling pins just one snapshot while computing its transfer checksum;
		// the volume lock is released before network backpressure can stall it.
		file, err := os.CreateTemp("", "stqr-server-download-*")
		if err != nil {
			return nil, 0, err
		}
		defer func() { file.Close(); os.Remove(file.Name()) }()
		hash := sha256.New()
		info, n, err := op.bucket.CopyTo(ctx, p, op.object, op.offset, length, &objectBudgetWriter{dst: io.MultiWriter(file, hash), remaining: maxObjectStagingBytes})
		if err != nil {
			return nil, 0, err
		}
		if _, err = file.Seek(0, io.SeekStart); err != nil {
			return nil, 0, err
		}
		var digest [32]byte
		copy(digest[:], hash.Sum(nil))
		e := client.Encoder{}
		encodeObjectInfo(&e, info, runtime)
		if m.Opcode == 0x0101 {
			e.U64(op.offset)
			e.U64(n)
			e.U64(info.Length)
		}
		if n <= 1<<20 {
			payload, err := io.ReadAll(file)
			if err != nil {
				return nil, 0, err
			}
			encodeObjectTransfer(&e, 0, n, digest, payload)
		} else {
			if ws == nil {
				return nil, 0, fail(400, "large GET requires WebSocket streaming")
			}
			if err = ws.sendObjectStream(m.ID, file, n, digest); err != nil {
				return nil, 0, err
			}
			encodeObjectTransfer(&e, 1, n, digest, nil)
		}
		status := 200
		if m.Opcode == 0x0101 {
			status = 206
		}
		return e.B, status, nil
	case 0x0107:
		if d.Done() != nil {
			return nil, 0, fail(400, "invalid STAT body")
		}
		info, err := op.bucket.Stat(ctx, p, op.object)
		if err != nil {
			return nil, 0, err
		}
		e := client.Encoder{}
		encodeObjectInfo(&e, info, runtime)
		return e.B, 200, nil
	case 0x0102, 0x0103, 0x0104:
		if m.Opcode == 0x0103 {
			op.offset = d.U64()
		}
		op.options = decodeObjectOptions(d, m.ID)
		mode := d.U8()
		op.length = d.U64()
		copy(op.digest[:], d.Take(32))
		op.inline = d.Blob()
		op.stream = mode == 1
		if d.Done() != nil || mode > 1 || op.length > math.MaxInt64 || op.length > math.MaxUint64-op.offset {
			return nil, 0, fail(400, "invalid mutation transfer")
		}
		if op.stream {
			if accepted {
				return nil, 0, fail(400, "accepted wrapper requires inline payload")
			}
			if len(op.inline) != 0 || ws == nil {
				return nil, 0, fail(400, "streaming upload requires WebSocket")
			}
			if err = ws.beginObjectUpload(op, p); err != nil {
				return nil, 0, err
			}
			return nil, 0, errObjectStaged
		}
		if uint64(len(op.inline)) != op.length || sha256.Sum256(op.inline) != op.digest {
			return nil, 0, fail(400, "transfer length/digest mismatch")
		}
		return op.commit(ctx, p, bytes.NewReader(op.inline), runtime)
	case 0x0105:
		op.length = d.U64()
		op.options = decodeObjectOptions(d, m.ID)
	case 0x0106:
		op.options = decodeObjectOptions(d, m.ID)
	case 0x0108:
		scope := d.U8()
		op.offset = d.U64()
		op.options = decodeObjectOptions(d, m.ID)
		op.options.WholeObject = scope == 1
		op.expected = d.Blob()
		op.inline = d.Blob()
		op.length = uint64(len(op.inline))
		if scope > 1 || len(op.expected) > 1<<20 || op.length > 1<<20 {
			d.Err = volume.ErrRange
		}
	default:
		return nil, 0, volume.ErrUnsupported
	}
	if d.Done() != nil {
		return nil, 0, fail(400, "invalid object operation body")
	}
	return op.commit(ctx, p, bytes.NewReader(op.inline), runtime)
}
func encodeObjectTransfer(e *client.Encoder, mode uint8, length uint64, digest [32]byte, data []byte) {
	e.U8(mode)
	e.U64(length)
	e.B = append(e.B, digest[:]...)
	e.Blob(data)
}
func (op *objectMutation) commit(ctx context.Context, p principal, src io.Reader, runtime client.ID) ([]byte, int, error) {
	op.options.Accepted = op.accepted
	ctx = bindObjectRequest(ctx, op.message)
	var result volume.Result
	var err error
	switch op.message.Opcode {
	case 0x0102:
		result.Info, err = op.bucket.PutReader(ctx, p, op.object, src, op.length, op.options)
	case 0x0103:
		result.Info, err = op.bucket.PutRangeReader(ctx, p, op.object, op.offset, src, op.length, op.options)
	case 0x0104:
		result, err = op.bucket.AppendReader(ctx, p, op.object, src, op.length, op.options)
	case 0x0105:
		result.Info, err = op.bucket.Trunc(ctx, p, op.object, op.length, op.options)
	case 0x0106:
		err = op.bucket.Delete(ctx, p, op.object, op.options)
	case 0x0108:
		result.Info, err = op.bucket.CAS(ctx, p, op.object, op.offset, op.expected, op.inline, op.options)
	}
	if err != nil {
		return nil, 0, err
	}
	if op.message.Opcode == 0x0106 {
		return nil, 200, nil
	}
	e := client.Encoder{}
	encodeObjectInfo(&e, result.Info, runtime)
	if op.message.Opcode == 0x0103 {
		e.U64(op.offset)
		e.U64(op.length)
		e.U64(result.Info.Length)
	}
	if op.message.Opcode == 0x0104 {
		e.U64(result.Offset)
	}
	return e.B, 200, nil
}

var errObjectStaged = errors.New("object request staged until TRANSFER_END")

type objectBudgetWriter struct {
	dst       io.Writer
	remaining uint64
}

func (w *objectBudgetWriter) Write(data []byte) (int, error) {
	if uint64(len(data)) > w.remaining {
		return 0, fail(507, "snapshot staging budget exhausted")
	}
	n, err := w.dst.Write(data)
	w.remaining -= uint64(n)
	return n, err
}
