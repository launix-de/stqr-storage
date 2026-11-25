// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"math/bits"
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
		v.observer(StorageSnapshot{All: v.usage(v.accounting, v.sb), Owned: v.ownedUsage(v.owned, v.accounting, v.sb)})
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
func (v *Volume) initAccounting() error {
	if v.accounting != nil {
		return nil
	}
	accounts := make(map[ID]bucketAllocation)
	err := v.walkIndex(func(x indexEntry) error {
		id, a, err := v.accountRecord(x.Key)
		if err != nil {
			return err
		}
		b := accounts[id]
		b.logical += a.logical
		b.payload += a.payload
		b.metadata += a.metadata
		b.records += a.records
		b.objects += a.objects
		accounts[id] = b
		return nil
	})
	if err == nil {
		v.accounting = accounts
	}
	return err
}
func (v *Volume) accountRecord(k indexKey) (ID, bucketAllocation, error) {
	var a bucketAllocation
	if k[0] == 3 {
		return k.object().Bucket, a, nil
	}
	if k[0] == 2 {
		var id ID
		copy(id[:], k[1:17])
		r, found, err := v.loadReplica(id)
		if err != nil || !found {
			return ID{}, a, err
		}
		a.records, a.metadata = 1, replicaBytes
		if r.object != 0 {
			a.metadata += receiptBytes
			o, err := v.loadObjectAt(r.object)
			if err != nil {
				return ID{}, a, err
			}
			a.metadata += o.Size
			old, err := v.loadObject(r.Request.Key)
			if errors.Is(err, ErrNotFound) {
				old = nil
			} else if err != nil {
				return ID{}, a, err
			}
			for _, allocation := range exclusiveAllocations(o, old) {
				if allocation.meta {
					a.metadata += allocation.n
				} else {
					a.payload += allocation.n
				}
			}
		}
		return r.Request.Key.Bucket, a, nil
	}
	if k[0] == 0 {
		key := k.object()
		o, err := v.loadObject(key)
		if errors.Is(err, ErrNotFound) {
			return key.Bucket, a, nil
		}
		if err != nil {
			return ID{}, a, err
		}
		a.records = 1
		a.metadata = o.Size
		if !o.Info.Deleted {
			a.logical = o.Info.Length
			a.objects = 1
		}
		for _, allocation := range objectAllocations(o) {
			if allocation.meta {
				a.metadata += allocation.n
			} else {
				a.payload += allocation.n
			}
		}
		return key.Bucket, a, nil
	}
	if k[0] == 1 {
		var id ID
		copy(id[:], k[1:17])
		d, found, err := v.loadDedupe(id)
		if err != nil || !found {
			return ID{}, a, err
		}
		a.records = 1
		a.metadata = receiptBytes
		return d.Key.Bucket, a, nil
	}
	return ID{}, a, ErrCorrupt
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
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return err
	}
	if v.owned != nil && v.ownerStamp == stamp {
		return nil
	}
	if err := v.initAccounting(); err != nil {
		return err
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
							v.observer(StorageSnapshot{All: v.usage(v.accounting, v.sb), Owned: v.ownedUsage(v.owned, v.accounting, v.sb)})
						}
						return nil
					}
				}
			}
		}
	}
	selected := make(map[ID]bucketAllocation)
	err := v.walkIndex(func(x indexEntry) error {
		k, err := v.recordKey(x.Key)
		if err != nil {
			return err
		}
		if include != nil {
			flag, err := v.recordImported(x.Key)
			if err != nil {
				return err
			}
			if !include(k, flag) {
				return nil
			}
		}
		id, a, err := v.accountRecord(x.Key)
		if err != nil {
			return err
		}
		b := selected[id]
		b.logical += a.logical
		b.payload += a.payload
		b.metadata += a.metadata
		b.records += a.records
		b.objects += a.objects
		selected[id] = b
		return nil
	})
	if err != nil {
		return err
	}
	v.owned = selected
	v.ownerFilter = include
	v.ownerStamp = stamp
	if v.observer != nil {
		v.observer(StorageSnapshot{All: v.usage(v.accounting, v.sb), Owned: v.ownedUsage(v.owned, v.accounting, v.sb)})
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
		v.accountMu.Lock()
		defer v.accountMu.Unlock()
		if err := v.initAccounting(); err != nil {
			return nil, err
		}
		return v.ownedUsage(v.owned, v.accounting, v.sb), nil
	})
}
func (v *Volume) ownedUsage(selected, all map[ID]bucketAllocation, sb Superblock) []BucketStorage {
	full := v.usage(all, sb)
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

func (v *Volume) recordImported(key indexKey) (bool, error) {
	if key[0] == 2 || key[0] == 3 {
		return false, nil
	}
	if key[0] == 0 {
		o, err := v.loadObject(key.object())
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return o.importShadow, nil
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
