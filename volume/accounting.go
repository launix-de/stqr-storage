// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"context"
	"errors"
	"math/bits"
	"slices"
	"sort"
)

// BucketStorage measures allocated bytes, including tombstones, retry receipts,
// append arenas, checksums and an exact deterministic share of shared metadata.
// Logical counts live payload length once. No sparse or compression estimate is
// used in Physical. Fixed volume headers and empty-volume metadata are operating
// overhead; shared metadata above that baseline is apportioned by record count.
type BucketStorage struct {
	Bucket            ID
	Logical, Physical uint64
	Objects           uint64
}

// Admission runs under the exclusive volume lock, after allocation planning and
// before publication or WAL writes. It must not reenter this volume. An error
// aborts the whole transaction. The owner must serialize cross-volume budgets.
// Slices are sorted by Bucket and are valid only during the callback.
type StorageChange struct {
	Before, After, BeforeOwned, AfterOwned []BucketStorage
	Temporary                              bool
}
type StorageSnapshot struct{ All, Owned []BucketStorage }
type Admission func(change StorageChange) (protected uint64, err error)

type bucketAllocation struct{ logical, payload, metadata, records, objects uint64 }

// SetAdmission also reconstructs accounting from the on-disk index. The cache
// holds five counters per bucket, never an unbounded directory of object keys.
// Later transactions update only keys they changed; reopen reconstructs counters
// from the same canonical records rather than trusting a separately synced log.
func (v *Volume) SetAdmission(f Admission, observers ...func(StorageSnapshot)) error {
	if !v.AccountingReady() {
		return ErrAccountingNotReady
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return err
	}
	if err := v.initAccounting(); err != nil {
		return err
	}
	v.admission = f
	if len(observers) > 0 {
		v.observer = observers[0]
		v.observer(v.storageSnapshot(v.accounting, v.owned, v.sb))
	}
	return nil
}

func (h *Handle) StorageUsage() ([]BucketStorage, error) {
	return withHandle(h, false, func(v *Volume) ([]BucketStorage, error) {
		v.accountMu.Lock()
		defer v.accountMu.Unlock()
		if err := v.initAccounting(); err != nil {
			return nil, err
		}
		return v.usage(v.accounting, v.sb), nil
	})
}

// The canonical index is the recovery source, so a cold volume needs one
// census. Build all and selected totals together when the first ownership
// filter arrives. Retain only O(buckets) counters/proofs and bounded existing
// object/page caches, never a second object directory or per-object statistics.
func (v *Volume) initAccounting() error {
	if v.accounting != nil {
		return nil
	}
	if v.accountManaged.Load() && !v.accountReady.Load() {
		return ErrAccountingNotReady
	}
	all, _, err := v.scanAccounting(nil, nil, false)
	if err == nil {
		v.accounting = all
	}
	return err
}

type accountingRecord struct {
	key        Key
	allocation bucketAllocation
	imported   bool
}

type bucketOwnershipProof struct{ include, known bool }

func addAllocation(totals map[ID]bucketAllocation, id ID, a bucketAllocation) {
	b := totals[id]
	b.logical += a.logical
	b.payload += a.payload
	b.metadata += a.metadata
	b.records += a.records
	b.objects += a.objects
	totals[id] = b
}

func (v *Volume) scanAccounting(stamp any, include func(Key, bool) bool, owned bool) (map[ID]bucketAllocation, map[ID]bucketAllocation, error) {
	return v.scanAccountingWithContext(context.Background(), stamp, include, owned)
}
func (v *Volume) scanAccountingContext(ctx context.Context, stamp any, include func(Key, bool) bool) (map[ID]bucketAllocation, map[ID]bucketAllocation, error) {
	return v.scanAccountingWithContext(ctx, stamp, include, true)
}
func (v *Volume) scanAccountingWithContext(ctx context.Context, stamp any, include func(Key, bool) bool, owned bool) (map[ID]bucketAllocation, map[ID]bucketAllocation, error) {
	all := make(map[ID]bucketAllocation)
	selected := make(map[ID]bucketAllocation)
	proofs := make(map[ID]bucketOwnershipProof)
	totals, aggregate := stamp.(OwnershipBucketTotals)
	err := v.walkIndex(func(x indexEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := v.accountRecordAt(x)
		if err != nil {
			return err
		}
		addAllocation(all, r.key.Bucket, r.allocation)
		if !owned {
			return nil
		}
		proof, found := proofs[r.key.Bucket]
		if !found {
			if aggregate {
				proof.include, proof.known = totals.OwnershipForEntireBucket(r.key.Bucket)
			}
			proofs[r.key.Bucket] = proof
		}
		own := proof.include
		if !proof.known {
			own = include == nil || include(r.key, r.imported)
		}
		if own {
			addAllocation(selected, r.key.Bucket, r.allocation)
		}
		return nil
	})
	return all, selected, err
}

// A census already has each physical address. Decode it directly instead of
// searching the B-tree again, or rereading a receipt once for its bucket, again
// for its imported flag, and a third time for its footprint.
func (v *Volume) accountRecordAt(x indexEntry) (accountingRecord, error) {
	var r accountingRecord
	k, ptr := x.Key, x.Value
	if k[0] == 3 {
		r.key = k.object()
		return r, nil
	}
	if k[0] == 0 {
		r.key = k.object()
		if ptr == 0 {
			return r, nil
		}
		o := v.cache.get(r.key)
		if o == nil || o.Base != ptr {
			var err error
			o, err = v.loadObjectAt(ptr)
			if err != nil {
				return r, err
			}
			v.cache.put(r.key, o)
		}
		r.imported = o.importShadow
		r.allocation = accountObject(o)
		return r, nil
	}
	if ptr == 0 && (k[0] == 1 || k[0] == 2) {
		return r, nil
	}
	if k[0] == 1 {
		d, err := v.dedupeAt(ptr)
		if err != nil {
			return r, err
		}
		r.key, r.imported = d.Key, d.importShadow
		r.allocation.records, r.allocation.metadata = 1, receiptBytes
		return r, nil
	}
	if k[0] == 2 {
		replica, err := v.replicaAt(ptr)
		if err != nil {
			return r, err
		}
		var id ID
		copy(id[:], k[1:17])
		if replica.Request.ID != id {
			return r, ErrCorrupt
		}
		r.key = replica.Request.Key
		a := bucketAllocation{records: 1, metadata: replicaBytes}
		if replica.object != 0 {
			a.metadata += receiptBytes
			o, err := v.loadObjectAt(replica.object)
			if err != nil {
				return r, err
			}
			a.metadata += o.Size
			old, err := v.loadObject(replica.Request.Key)
			if errors.Is(err, ErrNotFound) {
				old = nil
			} else if err != nil {
				return r, err
			}
			for _, allocation := range exclusiveAllocations(o, old) {
				if allocation.meta {
					a.metadata += allocation.n
				} else {
					a.payload += allocation.n
				}
			}
		}
		r.allocation = a
		return r, nil
	}
	return r, ErrCorrupt
}

func accountObject(o *object) bucketAllocation {
	a := bucketAllocation{records: 1, metadata: o.Size}
	if !o.Info.Deleted {
		a.logical, a.objects = o.Info.Length, 1
	}
	for _, allocation := range objectAllocations(o) {
		if allocation.meta {
			a.metadata += allocation.n
		} else {
			a.payload += allocation.n
		}
	}
	return a
}

func (v *Volume) accountRecord(k indexKey) (ID, bucketAllocation, error) {
	if k[0] == 3 {
		return k.object().Bucket, bucketAllocation{}, nil
	}
	if k[0] == 0 {
		if o := v.cache.get(k.object()); o != nil {
			return k.object().Bucket, accountObject(o), nil
		}
	}
	ptr, err := v.lookup(k)
	if err != nil {
		return ID{}, bucketAllocation{}, err
	}
	r, err := v.accountRecordAt(indexEntry{Key: k, Value: ptr})
	return r.key.Bucket, r.allocation, err
}

func (v *Volume) usage(accounts map[ID]bucketAllocation, sb Superblock) []BucketStorage {
	var records, private uint64
	out := make([]BucketStorage, 0, len(accounts))
	for id, a := range accounts {
		if a.records == 0 {
			continue
		}
		records += a.records
		private += a.metadata
		out = append(out, BucketStorage{Bucket: id, Logical: a.logical, Objects: a.objects, Physical: a.payload + a.metadata})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Bucket[:], out[j].Bucket[:]) < 0 })
	// Local recovery/identity metadata is operational, never charged to clients.
	local := (sb.LocalMetadataSize + localMetadataPayload - 1) / localMetadataPayload * PageSize
	baseline := uint64(6*PageSize) + local
	shared := uint64(0)
	if sb.MetadataBytes > baseline+private {
		shared = sb.MetadataBytes - baseline - private
	}
	if records == 0 {
		return out
	}
	// Quotient/remainder distribution avoids multiplication overflow and assigns
	// every shared byte exactly once, with ties settled by stable bucket ID.
	var distributed uint64
	for i := range out {
		n := accounts[out[i].Bucket].records
		hi, lo := bits.Mul64(shared%records, n)
		q, _ := bits.Div64(hi, lo, records)
		part := shared/records*n + q
		out[i].Physical += part
		distributed += part
	}
	for i := 0; distributed < shared; i++ {
		out[i%len(out)].Physical++
		distributed++
	}
	return out
}
func (t *transaction) projectAccounting() (map[ID]bucketAllocation, error) {
	return t.projectAccounts(t.v.accounting, nil)
}
func (t *transaction) projectAccounts(base map[ID]bucketAllocation, include func(Key, bool) bool) (map[ID]bucketAllocation, error) {
	if base == nil {
		return nil, nil
	}
	next := make(map[ID]bucketAllocation, len(base))
	for id, a := range base {
		next[id] = a
	}
	view := &Volume{dev: t.v.dev, sb: t.sb, limit: t.v.limit, overlay: t, cache: newCache(-1, 1), nodes: newNodeCache(-1, 1)}
	for key := range t.changed {
		id, old, err := t.v.accountRecord(key)
		if err != nil {
			return nil, err
		}
		if include != nil {
			ok, err := t.v.includeRecord(key, include)
			if err != nil {
				return nil, err
			}
			if !ok {
				old = bucketAllocation{}
			}
		}
		a := next[id]
		if old.logical > a.logical || old.payload > a.payload || old.metadata > a.metadata || old.records > a.records || old.objects > a.objects {
			return nil, ErrCorrupt
		}
		a.logical -= old.logical
		a.payload -= old.payload
		a.metadata -= old.metadata
		a.records -= old.records
		a.objects -= old.objects
		next[id] = a
		id, new, err := view.accountRecord(key)
		if err != nil {
			return nil, err
		}
		if include != nil {
			ok, err := view.includeRecord(key, include)
			if err != nil {
				return nil, err
			}
			if !ok {
				new = bucketAllocation{}
			}
		}
		a = next[id]
		a.logical += new.logical
		a.payload += new.payload
		a.metadata += new.metadata
		a.records += new.records
		a.objects += new.objects
		next[id] = a
	}
	return next, nil
}

// StorageUsageWhere measures a portable handoff footprint. It includes retained
// requests and tombstones; the shared-metadata share follows the same record
// weights as StorageUsage. It scans metadata only and never reads payload bytes.
func (h *Handle) StorageUsageWhere(include func(Key) bool, authoritative ...bool) (uint64, error) {
	return withHandle(h, false, func(v *Volume) (uint64, error) {
		if !v.AccountingReady() {
			return 0, ErrAccountingNotReady
		}
		if include == nil {
			return 0, ErrRange
		}
		v.accountMu.Lock()
		defer v.accountMu.Unlock()
		if err := v.initAccounting(); err != nil {
			return 0, err
		}
		selected := make(map[ID]bucketAllocation)
		err := v.walkIndex(func(x indexEntry) error {
			key, err := v.recordKey(x.Key)
			if err != nil {
				return err
			}
			if !include(key) {
				return nil
			}
			if len(authoritative) > 0 && authoritative[0] && v.ownerFilter != nil {
				flag, err := v.recordImported(x.Key)
				if err != nil {
					return err
				}
				if !v.ownerFilter(key, flag) {
					return nil
				}
			}
			id, a, err := v.accountRecord(x.Key)
			if err != nil {
				return err
			}
			b := selected[id]
			b.payload += a.payload
			b.metadata += a.metadata
			b.records += a.records
			b.objects += a.objects
			selected[id] = b
			return nil
		})
		if err != nil {
			return 0, err
		}
		var total uint64
		if len(authoritative) > 0 && authoritative[0] {
			for _, u := range v.ownedUsage(selected, v.accounting, v.sb) {
				total += u.Physical
			}
			return total, nil
		}
		for _, full := range v.usage(v.accounting, v.sb) {
			s := selected[full.Bucket]
			if s.records == 0 {
				continue
			}
			a := v.accounting[full.Bucket]
			shared := full.Physical - a.payload - a.metadata
			hi, lo := bits.Mul64(shared, s.records)
			q, rem := bits.Div64(hi, lo, a.records)
			if rem != 0 {
				q++
			}
			total += s.payload + s.metadata + q
		}
		return total, nil
	})
}

// OwnershipDelta optionally describes an immutable filter stamp. The method is
// called under the volume lock and must not acquire server or volume locks.
// When known is true, the predicate is unchanged outside the returned buckets.
// Over-reporting is safe; omitting a changed bucket violates the contract.
// Returning known=false requests a complete ownership recount.
type OwnershipDelta interface {
	ChangedOwnershipBuckets(previous any) (buckets []ID, known bool)
}

// OwnershipBucketTotals can prove that the new predicate includes either all
// records of a bucket or none, including imported objects and retry receipts.
// It is evaluated under the volume lock and must not acquire any other locks.
// Mixed ranges or import-dependent predicates must return known=false.
type OwnershipBucketTotals interface {
	OwnershipForEntireBucket(bucket ID) (include bool, known bool)
}

// SetOwnershipFilter selects records that are authoritative at this owner. An
// uncommitted incoming copy or retired source copy is operating space, not an
// additional required replica charged against a bucket's absolute quota.
// stamp must be comparable and change whenever the ownership predicate changes.
func (v *Volume) SetOwnershipFilter(stamp any, include func(Key, bool) bool) error {
	if !v.AccountingReady() {
		return ErrAccountingNotReady
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return err
	}
	if v.owned != nil && v.ownerStamp == stamp {
		return nil
	}
	if v.accounting == nil {
		all, selected, err := v.scanAccounting(stamp, include, true)
		if err != nil {
			return err
		}
		v.accounting, v.owned = all, selected
		v.ownerFilter, v.ownerStamp = include, stamp
		if v.observer != nil {
			v.observer(v.storageSnapshot(all, selected, v.sb))
		}
		return nil
	}
	if v.owned == nil {
		if totals, ok := stamp.(OwnershipBucketTotals); ok {
			selected := make(map[ID]bucketAllocation)
			complete := true
			for id, a := range v.accounting {
				if a.records == 0 {
					continue
				}
				own, exact := totals.OwnershipForEntireBucket(id)
				if !exact {
					complete = false
					break
				}
				if own {
					selected[id] = a
				}
			}
			if complete {
				v.owned, v.ownerFilter, v.ownerStamp = selected, include, stamp
				if v.observer != nil {
					v.observer(v.storageSnapshot(v.accounting, selected, v.sb))
				}
				return nil
			}
		}
	}
	if v.owned != nil {
		if delta, ok := stamp.(OwnershipDelta); ok {
			changed, known := delta.ChangedOwnershipBuckets(v.ownerStamp)
			unchanged := known
			for _, bucket := range changed {
				if v.accounting[bucket].records != 0 {
					unchanged = false
					break
				}
			}
			if unchanged {
				// Empty newly placed buckets cannot change existing totals, but
				// future writes must use the newly published predicate.
				v.ownerFilter, v.ownerStamp = include, stamp
				return nil
			}
			if known {
				if totals, ok := stamp.(OwnershipBucketTotals); ok {
					selected := make(map[ID]bucketAllocation, len(v.owned))
					for id, value := range v.owned {
						selected[id] = value
					}
					complete := true
					for _, id := range changed {
						if v.accounting[id].records == 0 {
							continue
						}
						own, exact := totals.OwnershipForEntireBucket(id)
						if !exact {
							complete = false
							break
						}
						if own {
							selected[id] = v.accounting[id]
						} else {
							delete(selected, id)
						}
					}
					if complete {
						v.owned, v.ownerFilter, v.ownerStamp = selected, include, stamp
						if v.observer != nil {
							v.observer(v.storageSnapshot(v.accounting, v.owned, v.sb))
						}
						return nil
					}
				}
			}
		}
	}
	selected := make(map[ID]bucketAllocation)
	err := v.walkIndex(func(x indexEntry) error {
		r, err := v.accountRecordAt(x)
		if err != nil {
			return err
		}
		if include != nil && !include(r.key, r.imported) {
			return nil
		}
		addAllocation(selected, r.key.Bucket, r.allocation)
		return nil
	})
	if err != nil {
		return err
	}
	v.owned = selected
	v.ownerFilter = include
	v.ownerStamp = stamp
	if v.observer != nil {
		v.observer(v.storageSnapshot(v.accounting, v.owned, v.sb))
	}
	return nil
}
func (v *Volume) recordKey(key indexKey) (Key, error) {
	if key[0] == 0 || key[0] == 3 {
		return key.object(), nil
	}
	var id ID
	copy(id[:], key[1:17])
	if key[0] == 2 {
		r, _, err := v.loadReplica(id)
		return r.Request.Key, err
	}
	d, _, err := v.loadDedupe(id)
	return d.Key, err
}
func (h *Handle) OwnedStorageUsage() ([]BucketStorage, error) {
	return withHandle(h, false, func(v *Volume) ([]BucketStorage, error) {
		if !v.AccountingReady() {
			return nil, ErrAccountingNotReady
		}
		v.accountMu.Lock()
		defer v.accountMu.Unlock()
		if err := v.initAccounting(); err != nil {
			return nil, err
		}
		return v.ownedUsage(v.owned, v.accounting, v.sb), nil
	})
}
func (v *Volume) ownedUsage(selected, all map[ID]bucketAllocation, sb Superblock) []BucketStorage {
	return ownedUsageFromFull(v.usage(all, sb), selected, all)
}

// Reuse the sorted all-bucket image: shared metadata apportionment must remain
// exact, but does not need another full materialization for the owned subset.
func ownedUsageFromFull(full []BucketStorage, selected, all map[ID]bucketAllocation) []BucketStorage {
	if selected == nil {
		return full
	}
	out := make([]BucketStorage, 0, len(full))
	for _, u := range full {
		a := all[u.Bucket]
		s := selected[u.Bucket]
		if s.records == 0 {
			continue
		}
		shared := u.Physical - a.payload - a.metadata
		hi, lo := bits.Mul64(shared, s.records)
		q, _ := bits.Div64(hi, lo, a.records)
		out = append(out, BucketStorage{Bucket: u.Bucket, Logical: s.logical, Objects: s.objects, Physical: s.payload + s.metadata + q})
	}
	return out
}

func (v *Volume) storageSnapshot(all, owned map[ID]bucketAllocation, sb Superblock) StorageSnapshot {
	full := v.usage(all, sb)
	selected := ownedUsageFromFull(full, owned, all)
	if owned == nil {
		// Callbacks historically receive independent All/Owned slices.
		selected = slices.Clone(full)
	}
	return StorageSnapshot{All: full, Owned: selected}
}

func (v *Volume) recordImported(key indexKey) (bool, error) {
	if key[0] == 2 || key[0] == 3 {
		return false, nil
	}
	if key[0] == 0 {
		if o := v.cache.get(key.object()); o != nil {
			return o.importShadow, nil
		}
		ptr, err := v.lookup(key)
		if err != nil {
			return false, err
		}
		if ptr == 0 {
			return false, nil
		}
		return v.objectImportedAt(ptr)
	}
	var id ID
	copy(id[:], key[1:17])
	d, _, err := v.loadDedupe(id)
	return d.importShadow, err
}
func (v *Volume) includeRecord(key indexKey, include func(Key, bool) bool) (bool, error) {
	k, err := v.recordKey(key)
	if err != nil {
		return false, err
	}
	flag, err := v.recordImported(key)
	if err != nil {
		return false, err
	}
	return include(k, flag), nil
}

// ImportShadow distinguishes a trusted uncommitted incoming copy from an
// original object in an unpinned local cohort. It never changes client Info.
func (h *Handle) ImportShadow(k Key) (bool, error) {
	return withHandle(h, false, func(v *Volume) (bool, error) { return v.recordImported(objectKey(k)) })
}
