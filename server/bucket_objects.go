// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"

	"github.com/launix-de/stqr-storage/volume"
)

// Bucket object operations own authorization, lifecycle, codec and admission.
// The transport only decodes/encodes. Current placement is single-node: existing
// version/tombstone/retry records select the volume; new objects choose a stable
// eligible local target. No remote replicas or automatic failover are simulated.
func (b *Bucket) authorize(p principal, permission BucketPermissions) (BucketDescription, error) {
	b.mu.RLock()
	maintenance := b.maintenance
	b.mu.RUnlock()
	if maintenance != nil && (p.connection != maintenance || permission != BucketRead && permission != BucketDelete && permission != BucketWrite) {
		return BucketDescription{}, fail(409, "bucket is under exclusive maintenance")
	}
	d := b.storageDescription()
	if p.connection != nil {
		auth := p.connection
		if auth.invalid.Load() || auth.bucket != b || auth.permissions&permission == 0 {
			return d, ErrBucketForbidden
		}
		if d.DeletionComplete || d.State == BucketDeleting {
			return d, ErrBucketNotFound
		}
		return d, nil
	}
	if b.server == nil {
		return d, fail(503, "bucket is not attached to a server")
	}
	if d.DeletionComplete || d.State == BucketDeleting {
		return d, ErrBucketNotFound
	}
	if b.ID() == systemBucket {
		return d, ErrBucketForbidden
	}
	user, err := b.server.findAccount(p.User.Name)
	if err != nil || user.ID != p.User.ID || user.Disabled || (user.Role != "ADMIN" && user.Role != "USER") || !constantEqual(user.PasswordHash, p.User.PasswordHash) {
		return d, ErrBucketForbidden
	}
	id, err := bucketID(user.ID)
	if err != nil {
		return d, ErrBucketForbidden
	}
	info, err := b.Info(id, user.Role == "ADMIN")
	if err != nil {
		return d, err
	}
	if info.Permissions&permission == 0 {
		return d, ErrBucketForbidden
	}
	return d, nil
}
func (b *Bucket) allVolumes() []*volume.Volume {
	return b.server.placementVolumes(b.storageDescription().Policy.PlacementGroup)
}
func (b *Bucket) localVolumes() []*volume.Volume { return b.volumesFor(b.Describe()) }
func (b *Bucket) volumesFor(d BucketDescription) []*volume.Volume {
	all := b.server.placementVolumes(d.Policy.PlacementGroup)
	if len(d.LocalVolumes) == 0 {
		return all
	}
	allowed := map[string]bool{}
	for _, id := range d.LocalVolumes {
		allowed[id] = true
	}
	out := []*volume.Volume{}
	for _, v := range all {
		if allowed[v.Identity().VolumeID.String()] {
			out = append(out, v)
		}
	}
	return out
}
func bucketClass(policy BucketPolicy) volume.Class {
	switch policy.StorageStrategy {
	case "ssd":
		return volume.SSD
	case "ram":
		return volume.RAM
	}
	return volume.HDD
}
func bucketCodec(policy BucketPolicy) volume.Compression {
	switch policy.Compression {
	case "gzip":
		return volume.Gzip
	case "lz4":
		return volume.LZ4
	case "xz":
		return volume.XZ
	}
	return volume.Raw
}
func bucketLocalPolicy(p BucketPolicy) error {
	if p.Redundancy.Mode == "replication" && p.PlacementMode == "count" && p.Redundancy.Replicas != 1 {
		return fail(501, "multiple-node replication is not implemented")
	}
	if p.Limits.Guaranteed != 0 {
		return fail(501, "bucket guaranteed reservations are not implemented")
	}
	if !p.Limits.Unlimited && p.Limits.Accounting != "logical" {
		return fail(501, "finite physical bucket quotas are not implemented")
	}
	return nil
}
func (b *Bucket) selectVolume(d BucketDescription, key volume.Key, request volume.ID, create bool) (*volume.Volume, volume.Info, bool, error) {
	var selected, target *volume.Volume
	var latest volume.Info
	found := false
	class := bucketClass(d.Policy)
	for _, v := range b.volumesFor(d) {
		h, err := v.Open(false)
		if err != nil {
			return nil, latest, false, err
		}
		if request != (volume.ID{}) {
			result, ok, e := h.OperationStatus(request)
			if e != nil {
				h.Close()
				return nil, latest, false, e
			}
			if ok {
				h.Close()
				return v, result.Info, true, nil
			}
		}
		info, err := h.RecordInfo(key)
		h.Close()
		if err == nil {
			if !found || info.Version > latest.Version {
				selected = v
				latest = info
				found = true
			}
		} else if !errors.Is(err, volume.ErrNotFound) {
			return nil, latest, false, err
		}
		if v.State() == volume.Active && class.Allows(v.Identity().Class) && (target == nil || v.Stats().AdmissionFreePhysical > target.Stats().AdmissionFreePhysical) {
			target = v
		}
	}
	if found {
		if !class.Allows(selected.Identity().Class) {
			return nil, latest, false, fail(503, "bucket placement does not satisfy its effective storage class")
		}
		return selected, latest, true, nil
	}
	if !create {
		return nil, latest, false, volume.ErrNotFound
	}
	if target == nil {
		return nil, latest, false, volume.ErrFull
	}
	return target, latest, false, nil
}

// logicalUsage runs under the local mutation/admin lock. Counting duplicate
// copies after interrupted evacuation is conservative; it never grants excess
// growth credits. A distributed/physical-accounting planner is future work.
func (b *Bucket) logicalUsage() (uint64, error) {
	var total uint64
	for _, v := range b.localVolumes() {
		h, err := v.Open(false)
		if err != nil {
			return 0, err
		}
		err = h.Scan(b.ID(), volume.ObjectID{}, volume.ObjectID{}, func(_ volume.Key, info volume.Info) error {
			if math.MaxUint64-total < info.Length {
				return volume.ErrRange
			}
			total += info.Length
			return nil
		})
		h.Close()
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
func (b *Bucket) Get(ctx context.Context, p principal, id volume.ObjectID) ([]byte, volume.Info, error) {
	return b.GetRange(ctx, p, id, 0, math.MaxUint64)
}
func (b *Bucket) GetRange(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64) ([]byte, volume.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, volume.Info{}, err
	}
	d, err := b.authorize(p, BucketRead)
	if err != nil {
		return nil, volume.Info{}, err
	}
	v, _, _, err := b.selectVolume(d, volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}, false)
	if err != nil {
		return nil, volume.Info{}, err
	}
	h, err := v.Open(false)
	if err != nil {
		return nil, volume.Info{}, err
	}
	defer h.Close()
	return h.GetRange(volume.Key{Bucket: b.ID(), Object: id}, offset, length)
}

// CopyTo holds one snapshot handle for the complete transfer, without buffering
// the whole object. Mutations never expose partially updated object versions.
func (b *Bucket) CopyTo(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64, dst io.Writer) (volume.Info, uint64, error) {
	if err := ctx.Err(); err != nil {
		return volume.Info{}, 0, err
	}
	d, err := b.authorize(p, BucketRead)
	if err != nil {
		return volume.Info{}, 0, err
	}
	key := volume.Key{Bucket: b.ID(), Object: id}
	v, _, _, err := b.selectVolume(d, key, volume.ID{}, false)
	if err != nil {
		return volume.Info{}, 0, err
	}
	h, err := v.Open(false)
	if err != nil {
		return volume.Info{}, 0, err
	}
	defer h.Close()
	info, err := h.Stat(key)
	if err != nil {
		return info, 0, err
	}
	n, err := h.CopyTo(key, offset, length, bucketContextWriter{ctx: ctx, w: dst})
	return info, n, err
}

type bucketContextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w bucketContextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

type bucketContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r bucketContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
func (b *Bucket) Stat(ctx context.Context, p principal, id volume.ObjectID) (volume.Info, error) {
	if err := ctx.Err(); err != nil {
		return volume.Info{}, err
	}
	d, err := b.authorize(p, BucketRead)
	if err != nil {
		return volume.Info{}, err
	}
	_, info, _, err := b.selectVolume(d, volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}, false)
	if info.Deleted && err == nil {
		err = volume.ErrNotFound
	}
	return info, err
}
func (b *Bucket) mutate(ctx context.Context, p principal, id volume.ObjectID, opcode uint16, offset, length uint64, src io.Reader, expected []byte, opt volume.Options) (volume.Result, error) {
	if b.server == nil {
		return volume.Result{}, fail(503, "bucket not attached")
	}
	s := b.server
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if b.underMaintenance() && opcode != 0x0108 && opcode != 0x0106 {
		return volume.Result{}, fail(409, "maintenance permits only recovery CAS and DELETE")
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return volume.Result{}, fail(503, "server is stopping")
	}
	if err := ctx.Err(); err != nil {
		return volume.Result{}, err
	}
	permission := BucketWrite
	if opcode == 0x0104 {
		permission = BucketAppend
	}
	if opcode == 0x0106 {
		permission = BucketDelete
	}
	d, err := b.authorize(p, permission)
	if err != nil {
		return volume.Result{}, err
	}
	if d.DesiredPolicy != nil {
		return volume.Result{}, fail(503, "bucket policy transition in progress")
	}
	if err = bucketLocalPolicy(d.Policy); err != nil {
		return volume.Result{}, err
	}
	if opt.RequestID == (volume.ID{}) {
		return volume.Result{}, fail(400, "mutation request ID is required")
	}
	key := volume.Key{Bucket: b.ID(), Object: id}
	v, old, _, err := b.selectVolume(d, key, opt.RequestID, true)
	if err != nil {
		return volume.Result{}, err
	}
	// Persist the first local placement before acknowledging any object bytes.
	if len(d.LocalVolumes) == 0 {
		d.LocalVolumes = []string{v.Identity().VolumeID.String()}
		saved, e := s.buckets.Save(d, d.Revision)
		if e != nil {
			return volume.Result{}, e
		}
		d = saved.Describe()
	}
	// Inspect retry presence before applying current admission. The volume still
	// validates the complete original fingerprint; status alone is not a success.
	h, err := v.Open(false)
	if err != nil {
		return volume.Result{}, err
	}
	retry, repeated, err := h.OperationStatus(opt.RequestID)
	h.Close()
	if err != nil {
		return volume.Result{}, err
	}
	codec := bucketCodec(d.Policy)
	if repeated {
		codec = retry.Info.Compression
	}
	opt.Compression = &codec
	if !repeated && !d.Policy.Limits.Unlimited {
		oldLength := old.Length
		if old.Deleted {
			oldLength = 0
		}
		next := oldLength
		switch opcode {
		case 0x0102:
			next = length
		case 0x0103:
			if length > math.MaxUint64-offset {
				return volume.Result{}, volume.ErrRange
			}
			next = max(next, offset+length)
		case 0x0104:
			if length > math.MaxUint64-next {
				return volume.Result{}, volume.ErrRange
			}
			next += length
		case 0x0105:
			next = length
		case 0x0106:
			next = 0
		case 0x0108:
			if opt.WholeObject {
				next = length
			}
		}
		usage, e := b.logicalUsage()
		if e != nil {
			return volume.Result{}, e
		}
		if next > oldLength && (usage > d.Policy.Limits.Absolute || next-oldLength > d.Policy.Limits.Absolute-usage) {
			return volume.Result{}, volume.ErrFull
		}
	}
	h, err = v.Open(true)
	if err != nil {
		return volume.Result{}, err
	}
	defer h.Close()
	if src != nil {
		if seeker, ok := src.(io.ReadSeeker); ok {
			src = bucketContextReadSeeker{bucketContextReader{ctx: ctx, r: src}, seeker}
		} else {
			src = bucketContextReader{ctx: ctx, r: src}
		}
	}
	var result volume.Result
	switch opcode {
	case 0x0102:
		result.Info, err = h.PutReader(key, src, length, opt)
	case 0x0103:
		result.Info, err = h.PutRangeReader(key, offset, src, length, opt)
	case 0x0104:
		result, err = h.AppendReader(key, src, length, opt)
	case 0x0105:
		result.Info, err = h.Trunc(key, length, opt)
	case 0x0106:
		err = h.Delete(key, opt)
	case 0x0108:
		if length > 1<<20 {
			return result, volume.ErrRange
		}
		replacement := make([]byte, int(length))
		_, err = io.ReadFull(src, replacement)
		if err == nil {
			result.Info, err = h.CAS(key, offset, expected, replacement, opt)
		}
	default:
		err = volume.ErrUnsupported
	}
	return result, err
}
func (b *Bucket) Put(ctx context.Context, p principal, id volume.ObjectID, data []byte, opt volume.Options) (volume.Info, error) {
	return b.PutReader(ctx, p, id, bytes.NewReader(data), uint64(len(data)), opt)
}
func (b *Bucket) PutReader(ctx context.Context, p principal, id volume.ObjectID, src io.Reader, length uint64, opt volume.Options) (volume.Info, error) {
	r, err := b.mutate(ctx, p, id, 0x0102, 0, length, src, nil, opt)
	return r.Info, err
}
func (b *Bucket) PutRange(ctx context.Context, p principal, id volume.ObjectID, offset uint64, data []byte, opt volume.Options) (volume.Info, error) {
	r, err := b.mutate(ctx, p, id, 0x0103, offset, uint64(len(data)), bytes.NewReader(data), nil, opt)
	return r.Info, err
}
func (b *Bucket) Append(ctx context.Context, p principal, id volume.ObjectID, data []byte, opt volume.Options) (volume.Result, error) {
	return b.AppendReader(ctx, p, id, bytes.NewReader(data), uint64(len(data)), opt)
}
func (b *Bucket) AppendReader(ctx context.Context, p principal, id volume.ObjectID, src io.Reader, length uint64, opt volume.Options) (volume.Result, error) {
	return b.mutate(ctx, p, id, 0x0104, 0, length, src, nil, opt)
}
func (b *Bucket) CAS(ctx context.Context, p principal, id volume.ObjectID, offset uint64, expected, replacement []byte, opt volume.Options) (volume.Info, error) {
	r, err := b.mutate(ctx, p, id, 0x0108, offset, uint64(len(replacement)), bytes.NewReader(replacement), expected, opt)
	return r.Info, err
}
func (b *Bucket) Trunc(ctx context.Context, p principal, id volume.ObjectID, length uint64, opt volume.Options) (volume.Info, error) {
	r, err := b.mutate(ctx, p, id, 0x0105, 0, length, nil, nil, opt)
	return r.Info, err
}
func (b *Bucket) Delete(ctx context.Context, p principal, id volume.ObjectID, opt volume.Options) error {
	_, err := b.mutate(ctx, p, id, 0x0106, 0, 0, nil, nil, opt)
	return err
}

type bucketContextReadSeeker struct {
	bucketContextReader
	seeker io.Seeker
}

func (r bucketContextReadSeeker) Seek(offset int64, whence int) (int64, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.seeker.Seek(offset, whence)
}

func (b *Bucket) PutRangeReader(ctx context.Context, p principal, id volume.ObjectID, offset uint64, src io.Reader, length uint64, opt volume.Options) (volume.Info, error) {
	r, err := b.mutate(ctx, p, id, 0x0103, offset, length, src, nil, opt)
	return r.Info, err
}
