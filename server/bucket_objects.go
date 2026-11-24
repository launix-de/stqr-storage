// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Bucket object operations own authorization, lifecycle, codec and admission.
// The transport only decodes/encodes. Committed ranges select the authoritative
// node and optional pinned volume; local versions/tombstones/receipts resolve
// unrestricted local placement. No automatic failover or data replicas exist.
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
		required := permission
		if auth.frontendPermission != 0 {
			required = auth.frontendPermission
		}
		if auth.invalid.Load() || auth.bucket != b || auth.permissions&required == 0 {
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
	if p.frontendPermission != 0 {
		permission = p.frontendPermission
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
	p := b.storageDescription().Policy
	return b.server.placementVolumesAt(p.PlacementGroup, p.PlacementMaskVersion)
}
func (b *Bucket) localVolumes() []*volume.Volume { return b.volumesFor(b.Describe()) }
func (b *Bucket) volumesFor(d BucketDescription) []*volume.Volume {
	all := b.server.placementVolumesAt(d.Policy.PlacementGroup, d.Policy.PlacementMaskVersion)
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
	if b.server.controller != nil && d.DesiredPolicy != nil {
		if entry, known := b.server.placement.Load().lookup(client.ID(key.Bucket), client.ObjectID(key.Object)); known && entry.PolicyVersion == d.DesiredPolicy.Version {
			d.Policy = *d.DesiredPolicy
		}
	}
	var selected, target *volume.Volume
	var latest volume.Info
	found := false
	class := bucketClass(d.Policy)
	for _, v := range b.volumesFor(d) {
		if !class.Allows(v.Identity().Class) {
			continue
		}
		if b.server.controller != nil {
			if patch, ok := b.server.routingPatch(key.Bucket, key.Object); ok {
				if !b.server.volumeAllowedForEntry(client.ID(b.ID()), patch.Entries[0], v.Identity().VolumeID, b.server.currentSystem.Load()) {
					continue
				}
			}
		}
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
		allocatable := true
		if b.server.controller != nil {
			b.server.mu.RLock()
			managed := b.server.volumes[v.Identity().VolumeID.String()]
			allocatable = managed != nil && managed.config.State == "active"
			b.server.mu.RUnlock()
		}
		if allocatable && v.State() == volume.Active && class.Allows(v.Identity().Class) && (target == nil || v.Stats().AdmissionFreePhysical > target.Stats().AdmissionFreePhysical) {
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

// logicalUsage counts each locally authoritative live object once, regardless
// of interrupted copies or staged incoming handoffs. Pagination bounds memory
// even when the index cache cannot hold the bucket's complete object set.
// Callers serialize quota decisions with the local mutation/admin lock.
func (b *Bucket) scanLogicalUsage() (uint64, error) {
	var total uint64
	description := b.Describe()
	var after *volume.ObjectID
	for {
		records, next, more, err := b.scan(context.Background(), description, after, 128)
		if err != nil {
			return 0, err
		}
		for _, record := range records {
			if math.MaxUint64-total < record.Info.Length {
				return 0, volume.ErrRange
			}
			total += record.Info.Length
		}
		if !more {
			return total, nil
		}
		after = &next
	}
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
	if err := b.server.checkObjectOwner(b.ID(), id); err != nil {
		return nil, volume.Info{}, err
	}
	v, prior, _, err := b.selectVolume(d, volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}, false)
	if err != nil {
		if bindingErr := b.server.checkObjectBinding(ctx, b.ID(), id); bindingErr != nil {
			return nil, volume.Info{}, bindingErr
		}
		return nil, volume.Info{}, b.absenceError(id, prior, err)
	}
	h, err := v.Open(false)
	if err != nil {
		return nil, volume.Info{}, err
	}
	defer h.Close()
	if err := b.server.checkObjectBinding(ctx, b.ID(), id); err != nil {
		return nil, volume.Info{}, err
	}
	return h.GetRange(volume.Key{Bucket: b.ID(), Object: id}, offset, length)
}

// CopyTo holds one snapshot handle for the complete transfer, without buffering
// the whole object. Mutations never expose partially updated object versions.
func (b *Bucket) CopyTo(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64, dst io.Writer) (volume.Info, uint64, error) {
	return b.copyToVersion(ctx, p, id, offset, length, dst, nil)
}

// Check while holding the same native read handle that copies the data.
func (b *Bucket) copyToVersion(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64, dst io.Writer, version *uint64) (volume.Info, uint64, error) {
	if err := ctx.Err(); err != nil {
		return volume.Info{}, 0, err
	}
	d, err := b.authorize(p, BucketRead)
	if err != nil {
		return volume.Info{}, 0, err
	}
	if err := b.server.checkObjectOwner(b.ID(), id); err != nil {
		return volume.Info{}, 0, err
	}
	key := volume.Key{Bucket: b.ID(), Object: id}
	v, prior, _, err := b.selectVolume(d, key, volume.ID{}, false)
	if err != nil {
		if bindingErr := b.server.checkObjectBinding(ctx, b.ID(), id); bindingErr != nil {
			return volume.Info{}, 0, bindingErr
		}
		return volume.Info{}, 0, b.absenceError(id, prior, err)
	}
	h, err := v.Open(false)
	if err != nil {
		return volume.Info{}, 0, err
	}
	defer h.Close()
	if err := b.server.checkObjectBinding(ctx, b.ID(), id); err != nil {
		return volume.Info{}, 0, err
	}
	info, err := h.Stat(key)
	if err != nil {
		return info, 0, err
	}
	if version != nil && info.Version != *version {
		return info, 0, volume.ErrPrecondition
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
	if err := b.server.checkObjectOwner(b.ID(), id); err != nil {
		return volume.Info{}, err
	}
	_, info, _, err := b.selectVolume(d, volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}, false)
	if bindingErr := b.server.checkObjectBinding(ctx, b.ID(), id); bindingErr != nil {
		err = bindingErr
	}
	if info.Deleted && err == nil {
		err = volume.ErrNotFound
	}
	return info, b.absenceError(id, info, err)
}
func (b *Bucket) mutate(ctx context.Context, p principal, id volume.ObjectID, opcode uint16, offset, length uint64, src io.Reader, expected []byte, opt volume.Options) (volume.Result, error) {
	if b.server == nil {
		return volume.Result{}, fail(503, "bucket not attached")
	}
	s := b.server
	if s.metadataPreview {
		return volume.Result{}, fail(503, "metadata preview cannot mutate user objects")
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if b.underMaintenance() {
		if view := s.currentSystem.Load(); view != nil && len(view.manifest.Membership.Members) > 1 {
			return volume.Result{}, fail(501, "exclusive maintenance cannot mutate after multi-node admission")
		}
	}
	if b.underMaintenance() && p.connection != nil && p.connection.blockReadOnly.Load() {
		return volume.Result{}, ErrBucketForbidden
	}
	if b.underMaintenance() && opcode != 0x0108 && opcode != 0x0106 && (p.connection == nil || !p.connection.offlineCopy && !(p.connection.blockDevice.Load() && (opcode == 0x0102 || opcode == 0x0103))) {
		return volume.Result{}, fail(409, "maintenance permits only recovery CAS and DELETE")
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed || s.shortStopping.Load() {
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
	if err := s.checkObjectOwner(b.ID(), id); err != nil {
		return volume.Result{}, err
	}
	if err := s.checkObjectBinding(ctx, b.ID(), id); err != nil {
		return volume.Result{}, err
	}
	if s.bucketFrozen(b.ID()) {
		return volume.Result{}, fail(503, "bucket quota/policy change is in progress")
	}
	if s.scopes != nil {
		release, err := s.scopes.guard(b.ID(), id)
		if err != nil {
			return volume.Result{}, err
		}
		defer release()
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
		if errors.Is(err, volume.ErrFull) && s.scopes != nil && s.scopes.requestSpace(b.ID(), id, addCapacity(length, 64<<10)) {
			return volume.Result{}, fail(503, "waiting for range capacity handoff")
		}
		return volume.Result{}, err
	}
	if err := s.switchAcceptedVolume(b.ID(), v, opt.Accepted); err != nil {
		return volume.Result{}, err
	}
	if old.Version == 0 && opcode != 0x0102 && opcode != 0x0106 && b.lostRange(id) {
		return volume.Result{}, fail(410, "partial mutation cannot recreate a lost object")
	}
	// Persist the first local placement before acknowledging any object bytes.
	if len(d.LocalVolumes) == 0 && s.controller == nil {
		d.LocalVolumes = []string{v.Identity().VolumeID.String()}
		saved, e := s.buckets.Save(d, d.Revision)
		if e != nil {
			return volume.Result{}, e
		}
		d = saved.Describe()
	}
	if err := s.ensureVolumeAuthorityLocked(); err != nil {
		return volume.Result{}, err
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
		limit := d.Policy.Limits.Absolute
		if s.controller != nil {
			limit, e = s.quotaAllowance(client.ID(b.ID()))
			if e != nil {
				return volume.Result{}, e
			}
		}
		if next > oldLength && (usage > limit || next-oldLength > limit-usage) {
			if s.quotas != nil && next-oldLength <= d.Policy.Limits.Absolute && s.quotas.request(b.ID(), id, next-oldLength) {
				return volume.Result{}, fail(503, "waiting for bucket growth credits")
			}
			return volume.Result{}, volume.ErrFull
		}
	}
	b.usage.mu.Lock()
	usageLocked := true
	defer func() {
		if usageLocked {
			b.usage.mu.Unlock()
		}
	}()
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
	defer func() {
		if err == nil && s.quotas != nil {
			s.quotas.fulfilled(b.ID(), id)
		}
	}()
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
	if err == nil && opt.Accepted {
		generation, gerr := h.Generation()
		if gerr != nil {
			return result, gerr
		}
		s.recordAccepted(b.ID(), v, generation)
	}
	if err == nil && !repeated && s.objectSubscriptions.watched(key) {
		published, readErr := h.RecordInfo(key)
		if readErr == nil {
			s.objectSubscriptions.publish(key, old.Version, published, h)
		} else {
			s.objectSubscriptions.resyncWhere(func(k volume.Key) bool { return k == key })
		}
	}
	// Release the write handle before consulting capacity/index statistics.
	// An unchanged logical request ID safely retries after ownership cutover.
	h.Close()
	oldLength, newLength := old.Length, result.Info.Length
	if old.Deleted {
		oldLength = 0
	}
	if opcode == 0x0106 || result.Info.Deleted {
		newLength = 0
	}
	b.updateUsage(oldLength, newLength, repeated, err)
	b.usage.mu.Unlock()
	usageLocked = false
	if errors.Is(err, volume.ErrFull) && !errors.Is(err, volume.ErrRecovery) && !repeated && s.scopes != nil {
		needed := addCapacity(length, 64<<10)
		if opcode == 0x0105 || opcode == 0x0106 {
			needed = 64 << 10
		}
		if s.scopes.requestSpace(b.ID(), id, needed) {
			return result, fail(503, "waiting for range capacity handoff")
		}
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
