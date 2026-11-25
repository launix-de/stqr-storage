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
// node and optional pinned volume; local records/tombstones/receipts resolve
// unrestricted local placement. Required replicas use the staged ALL path;
// transport failures never elect a replacement writer.
func (b *Bucket) authorize(p principal, permission BucketPermissions) (BucketDescription, error) {
	if p.cowInternal {
		d := b.storageDescription()
		if d.DeletionComplete {
			return d, ErrBucketNotFound
		}
		return d, nil
	}
	b.mu.RLock()
	maintenance := b.maintenance
	b.mu.RUnlock()
	if p.connection != nil && p.connection.lease.Load() != nil && !b.leaseAllows(p.connection) {
		return BucketDescription{}, ErrBucketForbidden
	}
	if maintenance != nil && (p.connection != maintenance && !b.leaseAllows(p.connection) || permission != BucketRead && permission != BucketDelete && permission != BucketWrite) {
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
	return b.withPinnedVolumes(b.server.placementVolumesAt(p.PlacementGroup, p.PlacementMaskVersion))
}

// A committed handoff can pin a desired-mask destination before every group
// has completed the PG transition. Reads and retries follow that authority,
// rather than losing the copy behind the still-effective old PG filter.
func (b *Bucket) withPinnedVolumes(all []*volume.Volume) []*volume.Volume {
	s := b.server
	image := s.placement.Load()
	if s.controller == nil || s.control == nil || image == nil {
		return all
	}
	ids := image.pins[placementNodeKey{client.ID(b.ID()), s.control.ledger.node}]
	if len(ids) == 0 {
		return all
	}
	seen := make(map[volume.ID]bool, len(all))
	for _, store := range all {
		seen[store.Identity().VolumeID] = true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, id := range ids {
		if seen[volume.ID(id)] {
			continue
		}
		managed := s.volumes[id.String()]
		if managed != nil && !managed.config.System && !managed.config.RecoveryOnly {
			all = append(all, managed.store)
		}
	}
	return all
}
func (b *Bucket) localVolumes() []*volume.Volume { return b.volumesFor(b.Describe()) }
func (b *Bucket) volumesFor(d BucketDescription) []*volume.Volume {
	all := b.server.placementVolumesAt(d.Policy.PlacementGroup, d.Policy.PlacementMaskVersion)
	if len(d.LocalVolumes) == 0 {
		return b.withPinnedVolumes(all)
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
	return b.withPinnedVolumes(out)
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
	if b.server.controller != nil {
		if entry, known := b.server.placement.Load().lookup(client.ID(key.Bucket), client.ObjectID(key.Object)); known {
			class = b.server.rangeClass(d.Policy, entry)
		}
	}
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
			prepared, known, e := h.ReplicaStatus(request)
			if e != nil {
				h.Close()
				return nil, latest, false, e
			}
			if known {
				if prepared.Request.Key != key {
					h.Close()
					return nil, latest, false, volume.ErrRequestID
				}
				current, e := h.RecordInfo(key)
				h.Close()
				if errors.Is(e, volume.ErrNotFound) {
					return v, volume.Info{}, false, nil
				}
				return v, current, e == nil, e
			}
		}
		info, err := h.RecordInfo(key)
		if err == nil && b.server.controller != nil {
			if entry, ok := b.server.placement.Load().lookup(client.ID(key.Bucket), client.ObjectID(key.Object)); ok && len(entry.Locations) == 1 && entry.Locations[0].Volume == (client.ID{}) {
				shadow, e := h.ImportShadow(key)
				if e != nil {
					err = e
				} else if shadow {
					err = volume.ErrNotFound
				}
			}
		}
		h.Close()
		if err == nil {
			if found && selected != v {
				return nil, latest, false, fail(503, "ambiguous physical copies without an authoritative volume pin")
			}
			if !found {
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
func (b *Bucket) getRange(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64) ([]byte, volume.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, volume.Info{}, err
	}
	d, err := b.authorize(p, BucketRead)
	if err != nil {
		return nil, volume.Info{}, err
	}
	if err := b.server.checkObjectOwner(b.ID(), id, true); err != nil {
		return nil, volume.Info{}, err
	}
	v, prior, _, err := b.selectVolume(d, volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}, false)
	if err != nil {
		if bindingErr := b.server.checkObjectBinding(ctx, b.ID(), id, true); bindingErr != nil {
			return nil, volume.Info{}, bindingErr
		}
		if errors.Is(err, volume.ErrNotFound) && len(d.ClonePeers) > 0 {
			var data bytes.Buffer
			info, _, e := b.cowCopyTo(ctx, id, offset, min(length, uint64(volume.MaxBufferedRead)+1), &data, nil)
			if data.Len() > volume.MaxBufferedRead {
				return nil, info, volume.ErrRange
			}
			return data.Bytes(), info, e
		}
		return nil, volume.Info{}, b.absenceError(id, prior, err)
	}
	b.server.ioPriority.users.Add(1)
	defer b.server.ioPriority.users.Add(-1)
	h, err := v.Open(false)
	if err != nil {
		return nil, volume.Info{}, err
	}
	defer h.Close()
	if err := b.server.checkObjectBinding(ctx, b.ID(), id, true); err != nil {
		return nil, volume.Info{}, err
	}
	return h.GetRange(volume.Key{Bucket: b.ID(), Object: id}, offset, length)
}

// CopyTo holds one snapshot handle for the complete transfer, without buffering
// the whole object. Mutations never expose partially applied object contents.
func (b *Bucket) CopyTo(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64, dst io.Writer) (volume.Info, uint64, error) {
	return b.copyToChecked(ctx, p, id, offset, length, dst, nil)
}

// Check while holding the same native read handle that copies the data.
func (b *Bucket) copyToCheckedUnmetered(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64, dst io.Writer, condition *volume.Options) (volume.Info, uint64, error) {
	if err := ctx.Err(); err != nil {
		return volume.Info{}, 0, err
	}
	d, err := b.authorize(p, BucketRead)
	if err != nil {
		return volume.Info{}, 0, err
	}
	if err := b.server.checkObjectOwner(b.ID(), id, true); err != nil {
		return volume.Info{}, 0, err
	}
	key := volume.Key{Bucket: b.ID(), Object: id}
	v, prior, _, err := b.selectVolume(d, key, volume.ID{}, false)
	if err != nil {
		if bindingErr := b.server.checkObjectBinding(ctx, b.ID(), id, true); bindingErr != nil {
			return volume.Info{}, 0, bindingErr
		}
		if errors.Is(err, volume.ErrNotFound) && len(d.ClonePeers) > 0 {
			return b.cowCopyTo(ctx, id, offset, length, dst, condition)
		}
		return volume.Info{}, 0, b.absenceError(id, prior, err)
	}
	b.server.ioPriority.users.Add(1)
	defer b.server.ioPriority.users.Add(-1)
	h, err := v.Open(false)
	if err != nil {
		return volume.Info{}, 0, err
	}
	defer h.Close()
	if err := b.server.checkObjectBinding(ctx, b.ID(), id, true); err != nil {
		return volume.Info{}, 0, err
	}
	info, err := h.Stat(key)
	if err != nil {
		return info, 0, err
	}
	if condition != nil {
		if err := h.Check(key, *condition); err != nil {
			return info, 0, err
		}
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
	if err := b.server.checkObjectOwner(b.ID(), id, true); err != nil {
		return volume.Info{}, err
	}
	_, info, _, err := b.selectVolume(d, volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}, false)
	if bindingErr := b.server.checkObjectBinding(ctx, b.ID(), id, true); bindingErr != nil {
		err = bindingErr
	}
	if errors.Is(err, volume.ErrNotFound) && len(d.ClonePeers) > 0 {
		snapshot, e := b.server.cowLogicalSnapshot(ctx, b.ID(), id, true)
		if e != nil {
			return volume.Info{}, e
		}
		defer snapshot.close()
		if snapshot == nil || snapshot.Info.Deleted {
			return volume.Info{}, volume.ErrNotFound
		}
		return snapshot.Info, nil
	}
	if info.Deleted && err == nil {
		err = volume.ErrNotFound
	}
	return info, b.absenceError(id, info, err)
}
func (b *Bucket) mutateUnmetered(ctx context.Context, p principal, id volume.ObjectID, opcode uint16, offset, length uint64, src io.Reader, expected []byte, opt volume.Options) (volume.Result, error) {
	if b.server == nil {
		return volume.Result{}, fail(503, "bucket not attached")
	}
	s := b.server
	if s.metadataPreview {
		return volume.Result{}, fail(503, "metadata preview cannot mutate user objects")
	}
	if !p.cowInternal {
		permission := BucketWrite
		if opcode == 0x0104 {
			permission = BucketAppend
		}
		if opcode == 0x0106 {
			permission = BucketDelete
		}
		if _, err := b.authorize(p, permission); err != nil {
			return volume.Result{}, err
		}
	}
	cowProof, cowLinked, cowErr := s.cowPrepareWrite(ctx, b, id, p)
	if cowErr != nil {
		return volume.Result{}, cowErr
	}
	var replicaRelease func()
	if s.replicas != nil {
		replicaRelease = s.replicas.lock(volume.Key{Bucket: b.ID(), Object: id})
		defer func() {
			if replicaRelease != nil {
				replicaRelease()
			}
		}()
	}
	s.lockUserObjectIO()
	opLocked := true
	defer func() {
		if opLocked {
			s.unlockUserObjectIO()
		}
	}()
	if b.underMaintenance() {
		if view := s.currentSystem.Load(); view != nil && len(view.manifest.Membership.Members) > 1 && s.leaseRecord(b) == nil {
			return volume.Result{}, fail(501, "exclusive maintenance cannot mutate after multi-node admission")
		}
	}
	if l := s.leaseRecord(b); l != nil && !l.Released && l.ReadOnly && !b.cowMaintenanceAllowed(ctx, p) {
		return volume.Result{}, ErrBucketForbidden
	}
	if b.underMaintenance() && p.connection != nil && p.connection.leaseReadOnly.Load() {
		return volume.Result{}, ErrBucketForbidden
	}
	if b.underMaintenance() && !b.cowMaintenanceAllowed(ctx, p) && opcode != 0x0108 && opcode != 0x0106 && (p.connection == nil || !p.connection.offlineCopy && !p.connection.objectLease.Load()) {
		return volume.Result{}, fail(409, "restricted maintenance permits only CAS and DELETE")
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
	if !p.cowInternal {
		if cowLinked {
			g, e := s.cowGraph()
			if e != nil {
				return volume.Result{}, e
			}
			current, e := s.cowGraphDigest(g, b.ID())
			if e != nil {
				return volume.Result{}, e
			}
			if current != cowProof {
				return volume.Result{}, fail(503, "clone graph changed before mutation")
			}
		} else if len(d.ClonePeers) > 0 {
			return volume.Result{}, fail(503, "clone connection appeared before mutation")
		}
	}
	if opcode == cowRestoreOpcode {
		restore, ok := ctx.Value(cowRestoreKey{}).(cowRestore)
		if !p.cowInternal || !ok {
			return volume.Result{}, ErrBucketForbidden
		}
		if err := s.cowValidateRestore(b.ID(), restore); err != nil {
			return volume.Result{}, err
		}
	}
	if err := s.checkObjectOwner(b.ID(), id); err != nil {
		return volume.Result{}, err
	}
	if err := s.checkObjectBinding(ctx, b.ID(), id); err != nil {
		return volume.Result{}, err
	}
	if s.scopes != nil && s.scopes.codecFence(b.ID(), id) {
		return volume.Result{}, fail(503, "PG codec generation is committing")
	}
	if s.bucketFrozen(b.ID()) {
		return volume.Result{}, fail(503, "bucket quota/policy change is in progress")
	}
	var mutationRelease func()
	if s.scopes != nil {
		release, err := s.scopes.guard(b.ID(), id)
		if err != nil {
			return volume.Result{}, err
		}
		mutationRelease = release
		defer func() {
			if mutationRelease != nil {
				mutationRelease()
			}
		}()
	}
	requestSpace := func(needed uint64, failed volume.ID) (bool, error) {
		// A helper must not wait on its own range lease, object lock, or
		// foreground I/O counter while executing the already planned move.
		if mutationRelease != nil {
			mutationRelease()
			mutationRelease = nil
		}
		if opLocked {
			s.unlockUserObjectIO()
			opLocked = false
		}
		if replicaRelease != nil {
			replicaRelease()
			replicaRelease = nil
		}
		if helped, err := s.scopes.stealSpace(ctx, b.ID(), failed); helped {
			return true, err
		}
		return s.scopes.requestSpace(b.ID(), id, needed, failed), nil
	}
	if s.controller == nil {
		err = bucketLocalPolicy(d.Policy)
		if d.Policy.Redundancy.Mode == "replication" && (d.Policy.PlacementMode == "everywhere" || d.Policy.Redundancy.Replicas > 1) {
			return volume.Result{}, fail(501, "replication requires the cluster controller")
		}
	}
	if err != nil {
		return volume.Result{}, err
	}
	if s.controller == nil && d.DesiredPolicy != nil && bucketCodec(d.Policy) != bucketCodec(*d.DesiredPolicy) {
		return volume.Result{}, fail(503, "bucket codec conversion is in progress")
	}
	if opt.RequestID == (volume.ID{}) {
		return volume.Result{}, fail(400, "mutation request ID is required")
	}
	key := volume.Key{Bucket: b.ID(), Object: id}
	v, old, exists, err := b.selectVolume(d, key, opt.RequestID, true)
	if err != nil {
		if errors.Is(err, volume.ErrFull) && s.scopes != nil {
			failed := volume.ID{}
			if v != nil {
				failed = v.Identity().VolumeID
			}
			if waiting, helpErr := requestSpace(addCapacity(length, 64<<10), failed); waiting {
				if helpErr != nil {
					return volume.Result{}, helpErr
				}
				return volume.Result{}, fail(503, "retry after range capacity handoff")
			}
		}
		return volume.Result{}, err
	}
	if opcode == cowRestoreOpcode && exists {
		return volume.Result{Info: old}, nil
	}
	if err := s.switchAcceptedVolume(b.ID(), v, opt.Accepted); err != nil {
		return volume.Result{}, err
	}
	if !exists && opcode != 0x0102 && opcode != 0x0106 && b.lostRange(id) {
		return volume.Result{}, fail(410, "partial mutation cannot recreate a lost object")
	}
	var replicatedEntry client.RangeEntry
	if s.controller != nil {
		entry, known := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(id))
		if known && d.DesiredPolicy != nil {
			if entry.PolicyVersion == d.DesiredPolicy.Version {
				d.Policy = *d.DesiredPolicy
			} else if s.requiredReplicas(*d.DesiredPolicy, s.currentSystem.Load().manifest.Membership) == 1 && bucketCodec(d.Policy) != bucketCodec(*d.DesiredPolicy) {
				return volume.Result{}, fail(503, "bucket codec conversion is in progress")
			}
		}
		if known && len(entry.Locations) > 1 {
			replicatedEntry = entry
		}
		if known && len(entry.Locations) < s.requiredReplicas(d.Policy, s.currentSystem.Load().manifest.Membership) {
			return volume.Result{}, fail(503, "required replica set is being built")
		}
		if known && len(entry.Locations) > 1 && d.Policy.StorageStrategy == "ram" {
			return volume.Result{}, fail(501, "RAM replica runtime restore is not implemented")
		}
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
	if err := s.refreshQuotaProtectionLocked(); err != nil {
		return volume.Result{}, err
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
	if err == nil && len(replicatedEntry.Locations) > 1 && !repeated {
		prepared, exists, e := h.ReplicaStatus(opt.RequestID)
		if e != nil {
			err = e
		} else if exists {
			retry = prepared.Request.Result
			repeated = true
		}
	}
	h.Close()
	if err != nil {
		return volume.Result{}, err
	}
	codec := bucketCodec(d.Policy)
	if repeated {
		codec = retry.Info.Compression
	}
	opt.Compression = &codec
	var snapshotInfo *volume.Info
	if opcode == cowRestoreOpcode {
		restore := ctx.Value(cowRestoreKey{}).(cowRestore)
		restore.info.Compression = codec
		ctx = context.WithValue(ctx, cowRestoreKey{}, restore)
		snapshotInfo = &restore.info
	}

	if len(replicatedEntry.Locations) > 1 {
		// Capture an incoming stream once. The primary and every follower hash
		// the identical original operation; retries retain its APPEND offset.
		spool, e := newReplicaSpool(v.Identity().Class == volume.RAM)
		if e != nil {
			return volume.Result{}, e
		}
		defer spool.Close()
		if opcode != 0x0105 && opcode != 0x0106 && length > 0 {
			if _, e = io.CopyN(spool, bucketContextReader{ctx: ctx, r: src}, int64(length)); e != nil {
				return volume.Result{}, e
			}
		}
		if _, e = spool.Seek(0, io.SeekStart); e != nil {
			return volume.Result{}, e
		}
		src = spool
	}
	if !p.cowInternal && !repeated && !d.Policy.Limits.Unlimited && d.Policy.Limits.Accounting == "logical" {
		oldLength := old.Length
		if old.Deleted {
			oldLength = 0
		}
		next := oldLength
		switch opcode {
		case cowRestoreOpcode:
			next = ctx.Value(cowRestoreKey{}).(cowRestore).info.Length
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
		if s.controller != nil {
			pending, e := s.pendingReplicaGrowth(b.ID())
			if e != nil {
				return volume.Result{}, e
			}
			usage = addCapacity(usage, pending)
		}
		limit := d.Policy.Limits.Absolute
		if s.controller != nil {
			limit, e = s.quotaAllowance(client.ID(b.ID()))
			if e != nil {
				return volume.Result{}, e
			}
		}
		if next > oldLength && (usage > limit || next-oldLength > limit-usage) {
			// Local committed usage is a lower bound on global usage. Once even
			// that lower bound cannot fit, borrowing can never make the write legal.
			if usage >= d.Policy.Limits.Absolute || next-oldLength > d.Policy.Limits.Absolute-usage {
				return volume.Result{}, volume.ErrFull
			}
			if s.quotas != nil && s.quotas.request(b.ID(), id, next-oldLength) {
				return volume.Result{}, fail(503, "waiting for bucket growth credits")
			}
			return volume.Result{}, volume.ErrFull
		}
	}
	if len(replicatedEntry.Locations) > 1 {
		result, e := b.prepareReplicated(ctx, v, key, replicatedEntry, opcode, offset, length, src, expected, opt)
		if e != nil {
			return result, e
		}
		h, e := v.Open(false)
		if e != nil {
			return result, e
		}
		record, ok, e := h.ReplicaStatus(opt.RequestID)
		h.Close()
		if e != nil {
			return result, e
		}
		if !ok {
			if repeated {
				return result, nil
			}
			return result, volume.ErrRecovery
		}
		if record.State == volume.ReplicaPublished {
			return result, nil
		}
		header := replicaHeader{Volume: v.Identity().VolumeID, Request: record.Request, Binding: record.Binding, Opcode: replicaOpcode(opcode), Offset: offset, Length: length, Expected: expected, Options: opt, Snapshot: snapshotInfo}
		if mutationRelease != nil {
			mutationRelease()
			mutationRelease = nil
		}
		s.unlockUserObjectIO()
		opLocked = false
		if record.State == volume.ReplicaPrepared {
			if e = s.replicas.preparePeers(ctx, replicatedEntry, header, src.(io.ReadSeeker)); e != nil {
				if status := statusOf(e); status == 507 || status == 412 || status == 409 {
					return result, s.replicas.abortPrepared(ctx, v, replicatedEntry, record, uint16(status))
				}
				return result, fail(503, "waiting for required replicas: "+e.Error())
			}
			s.lockUserObjectIO()
			if s.forceFrozen() {
				s.unlockUserObjectIO()
				return result, fail(503, "replica decision is frozen for explicit exclusion")
			}
			h, e = v.Open(true)
			if e == nil {
				e = h.DecideReplica(opt.RequestID, record.Binding)
				h.Close()
			}
			s.unlockUserObjectIO()
			if e != nil {
				return result, e
			}
		}
		if e = s.replicas.commitPeers(ctx, replicatedEntry, header); e != nil {
			return result, fail(503, "commit is waiting for required replicas: "+e.Error())
		}
		s.lockUserObjectIO()
		opLocked = true
		if s.forceFrozen() {
			return result, fail(503, "replica publication is frozen for explicit exclusion")
		}
		h, e = v.Open(true)
		if e != nil {
			return result, e
		}
		defer h.Close()
		current, _, statusErr := h.ReplicaStatus(opt.RequestID)
		if statusErr != nil {
			return result, statusErr
		}
		result, e = h.PublishReplica(opt.RequestID, record.Binding)
		if e == nil {
			if !p.cowInternal && current.State != volume.ReplicaPublished && s.objectSubscriptions.watched(key) {
				s.objectSubscriptions.publish(key, result.Info, h)
			}
			h.Close()
			b.usage.mu.Lock()
			b.usage.valid = false
			b.usage.mu.Unlock()
		}
		return result, e
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
	case cowRestoreOpcode:
		restore := ctx.Value(cowRestoreKey{}).(cowRestore)
		err = h.Restore(key, restore.info, cowExtentSource(src), opt)
		result.Info = restore.info
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
	if err == nil && !p.cowInternal && !repeated && s.objectSubscriptions.watched(key) {
		published, readErr := h.RecordInfo(key)
		if readErr == nil {
			s.objectSubscriptions.publish(key, published, h)
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
	if !p.cowInternal {
		b.updateUsage(oldLength, newLength, repeated, err)
	}
	b.usage.mu.Unlock()
	usageLocked = false
	var quotaFull *physicalQuotaFull
	if errors.As(err, &quotaFull) {
		if quotaFull.Waiting {
			return result, fail(503, "waiting for physical bucket growth credits")
		}
		return result, err
	}
	if errors.Is(err, volume.ErrFull) && !errors.Is(err, volume.ErrRecovery) && !repeated && s.scopes != nil {
		needed := addCapacity(length, 64<<10)
		if opcode == 0x0105 || opcode == 0x0106 {
			needed = 64 << 10
		}
		if waiting, helpErr := requestSpace(needed, v.Identity().VolumeID); waiting {
			if helpErr != nil {
				return result, helpErr
			}
			return result, fail(503, "retry after range capacity handoff")
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
