// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

// ErrAccountingNotReady means quota/ownership totals are unknown, never zero.
// Reads and general Stats remain available; mutations must wait or return 503.
var ErrAccountingNotReady = errors.New("volumes: accounting is warming")

// OwnershipCheckpointKey must describe the ACTUAL immutable ownership predicate:
// normalized ranges, bucket/volume/node identity and recovery/import masks. A
// pointer, process identity, map generation alone, or unrelated user settings do
// not suffice. A changed predicate MUST produce a different key. Methods must
// not acquire server locks while invoked under the volume lock.
type OwnershipCheckpointKey interface{ OwnershipCheckpointKey() [32]byte }

// AccountingOptions configures startup without traversing object records.
// CachePath is a node-owned metadata file, never derived from a raw-device path.
// The cache is advisory, O(buckets), checksum checked and tied to an exact durable
// superblock. Missing/stale/corrupt caches require WarmAccounting, not zero quota.
// Admission/Observer obey SetAdmission's non-reentrant callback contract.
type AccountingOptions struct {
	CachePath string
	Stamp     any
	Include   func(Key, bool) bool
	Admission Admission
	Observer  func(StorageSnapshot)
}

// ConfigureAccounting installs hooks and attempts only a bounded sidecar read.
// It never scans object metadata. Call before serving mutations. The stamp must
// be comparable; repeat calls while cold are permitted and invalidate a running
// warmup. Once managed, synchronous census APIs return ErrAccountingNotReady
// until WarmAccounting succeeds. A cache error is a miss, not data corruption.
func (v *Volume) ConfigureAccounting(o AccountingOptions) (bool, error) {
	if o.Stamp != nil && !reflect.TypeOf(o.Stamp).Comparable() {
		return false, ErrRange
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(false); err != nil {
		return false, err
	}
	v.accountManaged.Store(true)
	v.accountReady.Store(false)
	v.accountEpoch++
	v.accountCachePath = o.CachePath
	v.ownerStamp, v.ownerFilter = o.Stamp, o.Include
	v.admission, v.observer = o.Admission, o.Observer
	v.accountOwnerKey, v.accountOwnerKnown = ownershipKey(o.Stamp, o.Include)
	// A prior complete live census remains valid only if it was selected by the
	// same predicate. Configure is a startup operation; ownership changes use the
	// existing incremental SetOwnershipFilter after readiness.
	v.accounting, v.owned = nil, nil
	if v.sb.Identity.Class != RAM && v.sb.Identity.Class != RAMCache && o.CachePath != "" && v.sb.Generation == v.durableGeneration && v.deferred == nil && !v.checkpointing {
		all, owned, ok := v.loadAccountingCacheLocked()
		v.accounting, v.owned = all, owned
		if ok {
			v.accountReady.Store(true)
		}
	}
	if !v.accountReady.Load() {
		// Objects==0 alone is insufficient: receipts and tombstones also count.
		// One validated empty root leaf is an exact O(1) proof for bootstrap.
		root, err := v.node(v.sb.ObjectRoot)
		if err != nil {
			return false, err
		}
		if root.level == 0 && len(root.entries) == 0 {
			v.accounting = make(map[ID]bucketAllocation)
			v.owned = make(map[ID]bucketAllocation)
			v.accountReady.Store(true)
		}
	}
	if v.accountReady.Load() && v.observer != nil {
		v.observer(StorageSnapshot{All: v.usage(v.accounting, v.sb), Owned: v.ownedUsage(v.owned, v.accounting, v.sb)})
	}
	return v.accountReady.Load(), nil
}

// AccountingReady is lock-free. Unconfigured volumes retain their legacy lazy
// accounting API. Servers must gate writes BEFORE acquiring a write handle:
// queued RWMutex writers would otherwise also delay fresh readers during census.
func (v *Volume) AccountingReady() bool { return !v.accountManaged.Load() || v.accountReady.Load() }

// WarmAccounting performs the optional cold census into local, bounded-cache
// temporaries under a SHARED volume lock. Payload readers and Stats continue.
// The server must hold mutation/control admission while cold. Only publication
// takes the exclusive lock; cancellation/error leaves totals explicitly unknown.
// No object directory or per-object accounting is retained. Context is checked
// before every backend page read, as well as every canonical index entry.
func (v *Volume) WarmAccounting(ctx context.Context) error {
	if ctx == nil {
		return ErrRange
	}
	v.accountWarm.Lock()
	defer v.accountWarm.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	var epoch uint64
	var sb Superblock
	var all, owned map[ID]bucketAllocation
	alreadyReady := false
	err := func() error {
		v.mu.RLock()
		defer v.mu.RUnlock()
		if err := v.check(false); err != nil {
			return err
		}
		if !v.accountManaged.Load() || v.accountReady.Load() {
			alreadyReady = true
			return nil
		}
		epoch, sb = v.accountEpoch, v.sb
		// Never copy mutexes or populate a full object directory. Scan caches stay
		// private and bounded, independently of foreground cache contents.
		scan := &Volume{dev: &meteredIO{VolumeIO: &accountingContextIO{VolumeIO: v.dev, ctx: ctx}}, sb: sb, limit: v.limit, cache: newCache(256, 2<<20), nodes: newNodeCache(256, 2<<20), state: v.state, deferred: v.deferred, staging: v.staging, fence: v.fence, checkpointing: v.checkpointing}
		var err error
		all, owned, err = scan.scanAccountingContext(ctx, v.ownerStamp, v.ownerFilter)
		return err
	}()
	if err != nil {
		return err
	}
	if alreadyReady {
		return nil
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	err = func() error {
		v.mu.Lock()
		defer v.mu.Unlock()
		// An existing reader may delay publication after the scan completed.
		// Cancellation during that wait must not turn unknown totals ready.
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = v.check(false); err != nil {
			return err
		}
		if epoch != v.accountEpoch || sb != v.sb {
			return ErrBusy
		}
		v.accounting, v.owned = all, owned
		v.accountReady.Store(true)
		if v.observer != nil {
			v.observer(StorageSnapshot{All: v.usage(all, v.sb), Owned: v.ownedUsage(owned, all, v.sb)})
		}
		return nil
	}()
	if err != nil {
		return err
	}
	// Advisory filesystem work never holds the exclusive publication lock.
	_ = v.SaveAccountingCache()
	return nil
}

type accountingContextIO struct {
	VolumeIO
	ctx context.Context
}

func (d *accountingContextIO) ReadAt(p []byte, off int64) (int, error) {
	if err := d.ctx.Err(); err != nil {
		return 0, err
	}
	return d.VolumeIO.ReadAt(p, off)
}

// The census holds the source volume's shared lock for the entire scan. Forward
// its read-only view capability without extending that lifetime or bypassing
// cancellation. Hiding this optional capability would allocate and copy a whole
// page for every small object header, even on a mapped backend.
func (d *accountingContextIO) borrowAt(off uint64, n int) ([]byte, int, error) {
	if err := d.ctx.Err(); err != nil {
		return nil, 0, err
	}
	if reader, ok := d.VolumeIO.(interface {
		borrowAt(uint64, int) ([]byte, int, error)
	}); ok {
		return reader.borrowAt(off, n)
	}
	return nil, 0, ErrUnsupported
}

func ownershipKey(stamp any, include func(Key, bool) bool) ([32]byte, bool) {
	if key, ok := stamp.(OwnershipCheckpointKey); ok {
		return key.OwnershipCheckpointKey(), true
	}
	if include == nil {
		// A bucket proof can override the nil fallback and exclude records.
		// Such a predicate still needs an explicit stable checkpoint key.
		if _, hasBucketProof := stamp.(OwnershipBucketTotals); !hasBucketProof {
			return sha256.Sum256([]byte("stqr-accounting-all-records-v1")), true
		}
	}
	return [32]byte{}, false
}

// SaveAccountingCache snapshots already durable counters. It never flushes user
// data and never makes an accepted generation durable. Servers may call it after
// an existing checkpoint or on maintenance; clean Close also saves it. There is
// deliberately no per-mutation or per-counter filesystem operation.
// A caller may log cache errors; they must not reject an acknowledged user write.
func (v *Volume) SaveAccountingCache() error {
	path, data, err := func() (string, []byte, error) {
		v.mu.RLock()
		defer v.mu.RUnlock()
		if err := v.check(false); err != nil {
			return "", nil, err
		}
		return v.encodeAccountingCacheLocked()
	}()
	if err != nil {
		return err
	}
	return writeAccountingCache(path, data)
}

const accountingCacheLimit = 32 << 20
const accountingCacheHeader = 104
const accountingCacheEntry = 56

func (v *Volume) accountingDigest() ([32]byte, error) {
	b, err := v.sb.MarshalBinary()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}
func (v *Volume) encodeAccountingCacheLocked() (string, []byte, error) {
	if v.accountCachePath == "" || !v.accountManaged.Load() || !v.accountReady.Load() || v.failed != nil || v.sb.Identity.Class == RAM || v.sb.Identity.Class == RAMCache {
		return "", nil, nil
	}
	if v.sb.Generation != v.durableGeneration || v.deferred != nil || v.checkpointing {
		return "", nil, nil
	}
	key, known := ownershipKey(v.ownerStamp, v.ownerFilter)
	if !known || v.accounting == nil || v.owned == nil {
		return "", nil, nil
	}
	count := len(v.accounting) + len(v.owned)
	if count > (accountingCacheLimit-accountingCacheHeader-sha256.Size)/accountingCacheEntry {
		return "", nil, ErrRange
	}
	digest, err := v.accountingDigest()
	if err != nil {
		return "", nil, err
	}
	b := make([]byte, accountingCacheHeader, accountingCacheHeader+count*accountingCacheEntry+sha256.Size)
	copy(b, "STQRAC01")
	copy(b[8:40], digest[:])
	copy(b[40:56], v.sb.Identity.VolumeID[:])
	copy(b[56:88], key[:])
	binary.LittleEndian.PutUint64(b[88:96], uint64(len(v.accounting)))
	binary.LittleEndian.PutUint64(b[96:104], uint64(len(v.owned)))
	for _, accounts := range []map[ID]bucketAllocation{v.accounting, v.owned} {
		ids := make([]ID, 0, len(accounts))
		for id := range accounts {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
		for _, id := range ids {
			a := accounts[id]
			b = append(b, id[:]...)
			for _, n := range []uint64{a.logical, a.payload, a.metadata, a.records, a.objects} {
				b = binary.LittleEndian.AppendUint64(b, n)
			}
		}
	}
	sum := sha256.Sum256(b)
	b = append(b, sum[:]...)
	return v.accountCachePath, b, nil
}
func (v *Volume) saveAccountingCacheLocked() error {
	path, b, err := v.encodeAccountingCacheLocked()
	if err != nil {
		return err
	}
	return writeAccountingCache(path, b)
}
func writeAccountingCache(path string, b []byte) error {
	if len(b) == 0 {
		return nil
	}
	var err error
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".accounting-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
func (v *Volume) loadAccountingCacheLocked() (map[ID]bucketAllocation, map[ID]bucketAllocation, bool) {
	f, err := os.Open(v.accountCachePath)
	if err != nil {
		return nil, nil, false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() < accountingCacheHeader+sha256.Size || st.Size() > accountingCacheLimit {
		return nil, nil, false
	}
	b, err := io.ReadAll(io.LimitReader(f, accountingCacheLimit+1))
	if err != nil || len(b) > accountingCacheLimit || len(b) < accountingCacheHeader+sha256.Size {
		return nil, nil, false
	}
	sum := sha256.Sum256(b[:len(b)-sha256.Size])
	digest, err := v.accountingDigest()
	if err != nil || string(b[:8]) != "STQRAC01" || !bytes.Equal(sum[:], b[len(b)-sha256.Size:]) || !bytes.Equal(digest[:], b[8:40]) || !bytes.Equal(v.sb.Identity.VolumeID[:], b[40:56]) {
		return nil, nil, false
	}
	n, m := binary.LittleEndian.Uint64(b[88:96]), binary.LittleEndian.Uint64(b[96:104])
	limit := uint64((len(b) - accountingCacheHeader - sha256.Size) / accountingCacheEntry)
	if n > limit || m > limit-n || n+m != limit || uint64(len(b)) != accountingCacheHeader+(n+m)*accountingCacheEntry+sha256.Size {
		return nil, nil, false
	}
	maps := []map[ID]bucketAllocation{make(map[ID]bucketAllocation, int(n)), make(map[ID]bucketAllocation, int(m))}
	pos := accountingCacheHeader
	for which, count := range []uint64{n, m} {
		for i := uint64(0); i < count; i++ {
			var id ID
			copy(id[:], b[pos:pos+16])
			pos += 16
			values := [5]uint64{}
			for j := range values {
				values[j] = binary.LittleEndian.Uint64(b[pos:])
				pos += 8
			}
			a := bucketAllocation{values[0], values[1], values[2], values[3], values[4]}
			if _, dup := maps[which][id]; dup {
				return nil, nil, false
			}
			if a.objects > a.records {
				return nil, nil, false
			}
			maps[which][id] = a
		}
	}
	for id, a := range maps[1] {
		all, ok := maps[0][id]
		if !ok || a.logical > all.logical || a.payload > all.payload || a.metadata > all.metadata || a.records > all.records || a.objects > all.objects {
			return nil, nil, false
		}
	}
	if !v.accountOwnerKnown || !bytes.Equal(v.accountOwnerKey[:], b[56:88]) {
		return maps[0], nil, false
	}
	return maps[0], maps[1], true
}
