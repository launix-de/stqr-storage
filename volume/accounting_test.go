// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPhysicalAccountingReconstructionAndAtomicAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	v, err := New(path, 4<<20, HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { v.Close() }()
	if err = v.SetAdmission(func(change StorageChange) (uint64, error) {
		after := change.After
		for _, u := range after {
			if u.Physical > 200<<10 {
				return 0, ErrFull
			}
		}
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	h, _ := v.Open(true)
	key := testKey(1)
	request := testID(41)
	if _, err = h.Put(key, bytes.Repeat([]byte{3}, 64<<10), Options{RequestID: request}); err != nil {
		t.Fatal(err)
	}
	first, _ := h.StorageUsage()
	if len(first) != 1 || first[0].Physical <= 64<<10 || first[0].Logical != 64<<10 {
		t.Fatal(first)
	}
	if _, err = h.Put(key, bytes.Repeat([]byte{3}, 64<<10), Options{RequestID: request}); err != nil {
		t.Fatal(err)
	}
	retry, _ := h.StorageUsage()
	if !reflect.DeepEqual(first, retry) {
		t.Fatal("retry charged twice", first, retry)
	}
	if _, err = h.PutRange(key, 0, bytes.Repeat([]byte{7}, 256<<10), Options{RequestID: testID(42)}); !errors.Is(err, ErrFull) {
		t.Fatal("oversize committed", err)
	}
	after, _ := h.StorageUsage()
	if !reflect.DeepEqual(first, after) {
		t.Fatal("failed transaction changed accounting", first, after)
	}
	got, _, err := h.Get(key)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{3}, 64<<10)) {
		t.Fatal("failed transaction changed payload", err)
	}
	if _, found, err := h.OperationStatus(testID(42)); err != nil || found {
		t.Fatal("failed receipt exists", found, err)
	}
	if _, err = h.Append(key, []byte("small"), Options{RequestID: testID(43)}); err != nil {
		t.Fatal(err)
	}
	withArena, _ := h.StorageUsage()
	if withArena[0].Physical <= after[0].Physical {
		t.Fatal("arena uncharged")
	}
	if err = h.Delete(key, Options{RequestID: testID(44)}); err != nil {
		t.Fatal(err)
	}
	deleted, _ := h.StorageUsage()
	if deleted[0].Logical != 0 || deleted[0].Physical == 0 || deleted[0].Physical >= withArena[0].Physical {
		t.Fatal("tombstone/receipts or reclaim", deleted)
	}
	// A second bucket exercises deterministic shared-byte distribution and sorting.
	key2 := Key{Bucket: testID(2), Object: ObjectID{2}}
	if _, err = h.Put(key2, []byte("x"), Options{RequestID: testID(45)}); err != nil {
		t.Fatal(err)
	}
	expected, _ := h.StorageUsage()
	h.Close()
	v.Close()
	v, err = New(path, 0, HDD)
	if err != nil {
		t.Fatal(err)
	}
	h, _ = v.Open(false)
	reopened, err := h.StorageUsage()
	h.Close()
	if err != nil || !reflect.DeepEqual(expected, reopened) {
		t.Fatal("reopen changed physical accounting", expected, reopened, err)
	}
}
func TestPhysicalAccountingTinySparseCompressedAndIndexSplits(t *testing.T) {
	v, err := New("accounting", 16<<20, RAM)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if err = v.SetAdmission(func(_ StorageChange) (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	h, _ := v.Open(true)
	defer h.Close()
	for i := 0; i < 160; i++ {
		k := Key{Bucket: testID(uint64(i%3 + 1)), Object: ObjectID{byte(i)}}
		opt := Options{RequestID: ID{byte(i), 1}}
		if i%3 == 0 {
			opt.Compression = Codec(Gzip)
		}
		if _, err := h.Put(k, bytes.Repeat([]byte{1}, i), opt); err != nil {
			t.Fatal(i, err)
		}
	}
	k := Key{Bucket: testID(1), Object: ObjectID{240}}
	if _, err := h.Trunc(k, 1<<40, Options{RequestID: testID(9)}); err != nil {
		t.Fatal(err)
	}
	usage, err := h.StorageUsage()
	if err != nil {
		t.Fatal(err)
	}
	var charged uint64
	for _, u := range usage {
		charged += u.Physical
	}
	// Header/anchors + initial two index roots are the uncharged operational base.
	expected := v.sb.MetadataBytes + v.sb.PayloadBytes - 6*PageSize
	if charged != expected {
		t.Fatalf("charged=%d allocated-minus-base=%d", charged, expected)
	}
	cached := usage
	v.accounting = nil
	rebuilt, err := h.StorageUsage()
	if err != nil || !reflect.DeepEqual(cached, rebuilt) {
		t.Fatal("incremental counters disagree with index", cached, rebuilt, err)
	}
	subset, err := h.StorageUsageWhere(func(k Key) bool { return k.Bucket == testID(1) })
	if err != nil || subset != rebuilt[0].Physical {
		t.Fatal("handoff footprint differs", subset, rebuilt, err)
	}
}

func TestPhysicalFileBackingPreservesPayloadAndLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backed")
	v, err := New(path, 4<<20, HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	h, _ := v.Open(true)
	payload := []byte("do not overwrite while reserving")
	if _, err = h.Put(testKey(1), payload, Options{}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if err = v.EnsurePhysicalBacking(); err != nil {
		t.Fatal(err)
	}
	if v.Size() != 4<<20 {
		t.Fatal("backing changed volume length")
	}
	h, _ = v.Open(false)
	defer h.Close()
	got, _, err := h.Get(testKey(1))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("physical reservation destroyed data", err)
	}
}

func TestImportShadowMarkersSurviveSnapshotAndAccountingRebuild(t *testing.T) {
	v, err := New("source", 4<<20, RAM)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	imported, ordinary := testKey(1), testKey(2)
	imported.Bucket = testID(1)
	ordinary.Bucket = testID(2)
	h, _ := v.Open(true)
	err = h.Restore(imported, Info{Version: 1, Length: 3, Compression: Raw}, func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader([]byte("abc")), 3)
	}, Options{ImportShadow: true, RequestID: testID(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Put(ordinary, []byte("x"), Options{RequestID: testID(2)}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	var exported bytes.Buffer
	if err = v.Export(&exported); err != nil {
		t.Fatal(err)
	}
	target, err := New("target", 4<<20, RAM)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err = target.SetOwnershipFilter("unpinned", func(_ Key, shadow bool) bool { return !shadow }); err != nil {
		t.Fatal(err)
	}
	if err = target.SetAdmission(func(_ StorageChange) (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	if err = target.Import(bytes.NewReader(exported.Bytes()), nil); err != nil {
		t.Fatal(err)
	}
	h, _ = target.Open(false)
	defer h.Close()
	shadow, err := h.ImportShadow(imported)
	if err != nil || !shadow {
		t.Fatal("import marker lost", shadow, err)
	}
	shadow, err = h.ImportShadow(ordinary)
	if err != nil || shadow {
		t.Fatal("omitted JSON flag inherited prior frame", shadow, err)
	}
	usage, err := h.OwnedStorageUsage()
	if err != nil || len(usage) != 1 || usage[0].Bucket != ordinary.Bucket || usage[0].Objects != 1 || usage[0].Logical != 1 {
		t.Fatal("import counters were not replaced", usage, err)
	}
	charged, err := h.StorageUsageWhere(func(k Key) bool { return k.Bucket == imported.Bucket }, true)
	if err != nil || charged != 0 {
		t.Fatal("shadow receipt charged as canonical", charged, err)
	}
}
