// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func testBucketCatalog(t *testing.T) (*BucketCatalog, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admin")
	v, err := volume.New(path, volume.MinVolumeSize, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	c, err := NewBucketCatalog(v)
	if err != nil {
		t.Fatal(err)
	}
	return c, path
}
func newTestBucket(t *testing.T, name string) *Bucket {
	t.Helper()
	id, err := volume.NewID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBucket(id, name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestBucketCatalogExplicitBootstrapAndReopen(t *testing.T) {
	c, path := testBucketCatalog(t)
	if _, err := c.List(); !errors.Is(err, ErrBucketCatalogMissing) {
		t.Fatal("missing catalog was silently initialized", err)
	}
	if err := c.Initialize(); err != nil {
		t.Fatal(err)
	}
	list, err := c.List()
	if err != nil || len(list) != 1 || list[0].ID() != systemBucket {
		t.Fatal(list, err)
	}
	if err = c.Initialize(); !errors.Is(err, ErrBucketConflict) {
		t.Fatal("second initialization overwrote catalog", err)
	}
	want := newTestBucket(t, "application")
	saved, err := c.Save(want.Describe(), 0)
	if err != nil {
		t.Fatal(err)
	}
	user := volume.ID{9}
	description := saved.Describe()
	description.Grants = []BucketGrant{{UserID: user.String(), Permissions: BucketRead | BucketAppend}}
	desired := description.Policy
	desired.Version = 2
	desired.StorageStrategy = "ssd"
	description.DesiredPolicy = &desired
	saved, err = c.Save(description, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Save(description, 1); !errors.Is(err, ErrBucketConflict) {
		t.Fatal("stale revision accepted", err)
	}
	// Descriptions never expose writable aliases into a Bucket snapshot.
	detached := saved.Describe()
	detached.Grants[0].Permissions = BucketAllPermissions
	detached.DesiredPolicy.StorageStrategy = "ram"
	if saved.Describe().Grants[0].Permissions != BucketRead|BucketAppend || saved.Describe().DesiredPolicy.StorageStrategy != "ssd" {
		t.Fatal("description aliases live metadata")
	}
	if err = c.admin.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := volume.New(path, 0, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	catalog, err := NewBucketCatalog(reopened)
	if err != nil {
		t.Fatal(err)
	}
	got, err := catalog.LookupName("application")
	if err != nil || !reflect.DeepEqual(got.Describe(), saved.Describe()) {
		t.Fatal("description did not survive reopen", err)
	}
	if _, err = catalog.Lookup(want.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err = catalog.Lookup(volume.ID{8}); !errors.Is(err, ErrBucketNotFound) {
		t.Fatal(err)
	}
	// The index contains raw hashes only, and each addresses its exact canonical
	// JSON self-description, not another embedded copy or a memory-layout dump.
	h, err := reopened.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	raw, _, err := h.Get(BucketCatalogKey())
	if err != nil || len(raw) != 64 {
		t.Fatal("catalog is not a two-hash list", err)
	}
	if bytes.Compare(raw[:32], raw[32:]) >= 0 {
		t.Fatal("catalog hashes not sorted")
	}
	for i := 0; i < len(raw); i += 32 {
		var hash volume.ObjectID
		copy(hash[:], raw[i:i+32])
		data, _, err := h.Get(volume.Key{Bucket: systemBucket, Object: hash})
		if err != nil || sha256.Sum256(data) != [32]byte(hash) {
			t.Fatal("bad config reference", err)
		}
	}
}
func TestBucketInfoWireAndPermissions(t *testing.T) {
	c, _ := testBucketCatalog(t)
	if err := c.Initialize(); err != nil {
		t.Fatal(err)
	}
	user := volume.ID{7}
	b := newTestBucket(t, "filesystem")
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.String(), Permissions: BucketRead | BucketWrite}}
	desired := d.Policy
	desired.Version = 2
	desired.StorageStrategy = "ssd"
	d.DesiredPolicy = &desired
	b, err := c.Save(d, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Info(volume.ID{8}, false); !errors.Is(err, ErrBucketForbidden) {
		t.Fatal("ungranted user got bucket info", err)
	}
	info, err := b.Info(user, false)
	if err != nil {
		t.Fatal(err)
	}
	public := info.Describe()
	if len(public.Grants) != 0 || public.DesiredPolicy != nil {
		t.Fatal("info leaks other grants or pending policy")
	}
	wire, err := info.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoder := client.Decoder{B: wire}
	if decoder.ID() != client.ID(b.ID()) || decoder.U64() != 1 || decoder.Text() != "filesystem" {
		t.Fatal("wrong identity/version/name")
	}
	// Effective HDD/raw/count/single policy must remain encoded, even when SSD is desired.
	for _, want := range []uint8{0, 0, 0, 0, 0} {
		if got := decoder.U8(); got != want {
			t.Fatalf("enum %d != %d", got, want)
		}
	}
	if decoder.U16() != 1 || decoder.U16() != 0 || decoder.U16() != 0 {
		t.Fatal("bad replica profile")
	}
	if decoder.Bool() || decoder.U16() != 0 || decoder.U64() != 0 || decoder.U8() != 0 || decoder.U64() != 0 || !decoder.Bool() || decoder.U64() != 0 || decoder.U32() != DefaultBucketPolicy().Limits.MetadataReservePPM {
		t.Fatal("bad cache/limits profile")
	}
	if decoder.U32() != uint32(BucketRead|BucketWrite) || decoder.Done() != nil {
		t.Fatal("wrong rights or trailing bytes")
	}
	d = b.Describe()
	d.State = BucketLocked
	b, err = c.Save(d, 1)
	if err != nil {
		t.Fatal(err)
	}
	locked, err := b.Info(user, false)
	if err != nil || locked.Permissions != BucketRead {
		t.Fatal("locked bucket advertises writes", err)
	}
	system, err := c.Lookup(systemBucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = system.Info(user, false); !errors.Is(err, ErrBucketForbidden) {
		t.Fatal("system metadata exposed", err)
	}
	if _, err = system.Info(volume.ID{}, true); err != nil {
		t.Fatal(err)
	}
}
func TestBucketCatalogConcurrentHandles(t *testing.T) {
	c, _ := testBucketCatalog(t)
	if err := c.Initialize(); err != nil {
		t.Fatal(err)
	}
	other, err := NewBucketCatalog(c.admin)
	if err != nil {
		t.Fatal(err)
	}
	const count = 12
	descriptions := make([]BucketDescription, count)
	for i := range descriptions {
		descriptions[i] = newTestBucket(t, string(rune('a'+i))).Describe()
	}
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i, d := range descriptions {
		wg.Add(1)
		go func(i int, d BucketDescription) {
			defer wg.Done()
			catalog := c
			if i%2 != 0 {
				catalog = other
			}
			_, err := catalog.Save(d, 0)
			errs <- err
		}(i, d)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	list, err := c.List()
	if err != nil || len(list) != count+1 {
		t.Fatal("lost catalog updates", len(list), err)
	}
	duplicate := newTestBucket(t, descriptions[0].Name)
	if _, err = c.Save(duplicate.Describe(), 0); !errors.Is(err, ErrBucketConflict) {
		t.Fatal("duplicate bucket name accepted", err)
	}
}
func TestBucketCatalogUnpublishedDescriptionAndAutoGrowth(t *testing.T) {
	c, path := testBucketCatalog(t)
	if err := c.Initialize(); err != nil {
		t.Fatal(err)
	}
	b := newTestBucket(t, "unpublished")
	d := b.Describe()
	// Large ACL exceeds the initial admin volume despite byte-packed allocation.
	for i := 0; i < 6000; i++ {
		id, err := volume.NewID()
		if err != nil {
			t.Fatal(err)
		}
		d.Grants = append(d.Grants, BucketGrant{UserID: id.String(), Permissions: BucketRead})
	}
	normalized, err := normalizeBucketDescription(d)
	if err != nil {
		t.Fatal(err)
	}
	before := c.admin.Size()
	if _, err = c.persistDescription(normalized); err != nil {
		t.Fatal(err)
	}
	if c.admin.Size() <= before {
		t.Fatal("admin volume did not grow")
	}
	// Simulate interruption after the config sync, before catalog publication.
	if err = c.admin.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := volume.New(path, 0, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	catalog, err := NewBucketCatalog(reopened)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = catalog.Lookup(b.ID()); !errors.Is(err, ErrBucketNotFound) {
		t.Fatal("orphan config became published", err)
	}
	saved, err := catalog.Save(normalized, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Describe().Grants) != len(normalized.Grants) {
		t.Fatal("ACL lost")
	}
}
func TestBucketCatalogRejectsCorruptionWithoutBootstrap(t *testing.T) {
	cases := []string{"duplicate-hash", "unsorted", "missing-config", "wrong-digest", "missing-root", "partial-hash"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			c, _ := testBucketCatalog(t)
			if err := c.Initialize(); err != nil {
				t.Fatal(err)
			}
			b := newTestBucket(t, "existing")
			if _, err := c.Save(b.Describe(), 0); err != nil {
				t.Fatal(err)
			}
			h, err := c.admin.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			list, _, err := h.Get(BucketCatalogKey())
			if err != nil {
				h.Close()
				t.Fatal(err)
			}
			switch name {
			case "duplicate-hash":
				list = append(list[:32:32], list[:32]...)
			case "unsorted":
				list = append(append([]byte(nil), list[32:]...), list[:32]...)
			case "missing-config":
				copy(list[:32], make([]byte, 32))
				sortBucketHashes(list)
			case "wrong-digest":
				var hash volume.ObjectID
				copy(hash[:], list[:32])
				_, err = h.Put(volume.Key{Bucket: systemBucket, Object: hash}, []byte("changed"), volume.Options{})
			case "missing-root":
				err = h.Delete(BucketCatalogKey(), volume.Options{})
			case "partial-hash":
				list = list[:len(list)-1]
			}
			if err == nil && name != "wrong-digest" && name != "missing-root" {
				_, err = h.Put(BucketCatalogKey(), list, volume.Options{})
			}
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.List()
			if name == "missing-root" {
				if !errors.Is(err, ErrBucketCatalogMissing) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrBucketCatalogCorrupt) {
				t.Fatal("corruption was not rejected", err)
			}
		})
	}
}
func sortBucketHashes(data []byte) {
	if len(data) == 64 && bytes.Compare(data[:32], data[32:]) > 0 {
		a := append([]byte(nil), data[:32]...)
		copy(data[:32], data[32:])
		copy(data[32:], a)
	}
}
func TestBucketDescriptionValidationAndCanonicalJSON(t *testing.T) {
	base := newTestBucket(t, "valid").Describe()
	cases := []func(*BucketDescription){
		func(d *BucketDescription) { d.ID = "bad" }, func(d *BucketDescription) { d.Schema = 2 }, func(d *BucketDescription) { d.Name = "system" },
		func(d *BucketDescription) { d.Policy.StorageStrategy = "cache" }, func(d *BucketDescription) { d.Policy.Redundancy.ACK = "quorum" },
		func(d *BucketDescription) {
			d.Policy.Limits.Unlimited = false
			d.Policy.Limits.Guaranteed = 2
			d.Policy.Limits.Absolute = 1
		},
		func(d *BucketDescription) { d.Grants = []BucketGrant{{UserID: volume.ID{1}.String(), Permissions: 32}} },
		func(d *BucketDescription) { p := d.Policy; d.DesiredPolicy = &p },
	}
	for i, mutate := range cases {
		d := cloneBucketDescription(base)
		mutate(&d)
		if _, err := normalizeBucketDescription(d); !errors.Is(err, ErrBucketInvalid) {
			t.Fatalf("case %d accepted: %v", i, err)
		}
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = strictBucketJSON(encoded); err != nil {
		t.Fatal(err)
	}
	if _, err = strictBucketJSON(append(encoded, []byte(" {}")...)); err == nil {
		t.Fatal("trailing object accepted")
	}
	system, err := NewBucket(systemBucket, "system")
	if err != nil {
		t.Fatal(err)
	}
	d := system.Describe()
	d.State = BucketDeleting
	if _, err = normalizeBucketDescription(d); err == nil {
		t.Fatal("system bucket can be deleted")
	}
}

func TestBucketPolicyVersionCannotChangeInPlace(t *testing.T) {
	c, _ := testBucketCatalog(t)
	if err := c.Initialize(); err != nil {
		t.Fatal(err)
	}
	b := newTestBucket(t, "versioned")
	saved, err := c.Save(b.Describe(), 0)
	if err != nil {
		t.Fatal(err)
	}
	d := saved.Describe()
	d.Policy.StorageStrategy = "ssd"
	if _, err = c.Save(d, 1); !errors.Is(err, ErrBucketInvalid) {
		t.Fatal("policy changed without new version", err)
	}
	d.Policy.Version = 2
	updated, err := c.Save(d, 1)
	if err != nil || updated.Describe().Policy.StorageStrategy != "ssd" {
		t.Fatal(err)
	}
	d = updated.Describe()
	d.Policy.Version = 1
	if _, err = c.Save(d, 2); !errors.Is(err, ErrBucketInvalid) {
		t.Fatal("policy version rolled back", err)
	}
}
