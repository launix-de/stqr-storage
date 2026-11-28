// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

type accountingViewProbe struct {
	VolumeIO
	views, reads int
}

func (p *accountingViewProbe) ReadAt(b []byte, off int64) (int, error) {
	p.reads++
	return p.VolumeIO.ReadAt(b, off)
}
func (p *accountingViewProbe) borrowAt(off uint64, n int) ([]byte, int, error) {
	p.views++
	return p.VolumeIO.(interface {
		borrowAt(uint64, int) ([]byte, int, error)
	}).borrowAt(off, n)
}

func TestAccountingContextBorrowCancellationAndFallback(t *testing.T) {
	mem, err := NewMemory(MinVolumeSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()
	p := &accountingViewProbe{VolumeIO: mem}
	ctx, cancel := context.WithCancel(context.Background())
	d := &accountingContextIO{VolumeIO: p, ctx: ctx}
	if _, n, err := d.borrowAt(0, 40); err != nil || n != 40 || p.views != 1 {
		t.Fatal(n, err, p.views)
	}
	cancel()
	if _, _, err := d.borrowAt(0, 40); !errors.Is(err, context.Canceled) || p.views != 1 {
		t.Fatal("canceled borrow touched backend", err, p.views)
	}
	if _, err := d.ReadAt(make([]byte, 40), 0); !errors.Is(err, context.Canceled) || p.reads != 0 {
		t.Fatal(err, p.reads)
	}
	// A custom backend without the optional capability retains bounded ReadAt.
	d = &accountingContextIO{VolumeIO: struct{ VolumeIO }{mem}, ctx: context.Background()}
	if _, _, err := d.borrowAt(0, 40); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	if n, err := d.ReadAt(make([]byte, 40), 0); err != nil || n != 40 {
		t.Fatal(n, err)
	}
}

func TestWarmAccountingBorrowsMappedMetadataWithoutPageCopies(t *testing.T) {
	v, err := New(filepath.Join(t.TempDir(), "data.vol"), 8<<20, HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	raw := v.dev.(*meteredIO).VolumeIO
	f, ok := raw.(*FileIO)
	if !ok || !f.hasMapping() {
		t.Skip("backend does not support mmap")
	}
	for i := uint64(1); i <= 96; i++ {
		if _, err = testPut(v, testKey(i), bytes.Repeat([]byte{byte(i)}, int(i%17)), Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err = v.Sync(); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	want, err := h.StorageUsage()
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	p := &accountingViewProbe{VolumeIO: raw}
	v.dev.(*meteredIO).VolumeIO = p
	if ready, err := v.ConfigureAccounting(AccountingOptions{}); err != nil || ready {
		t.Fatal(ready, err)
	}
	objects, nodes := len(v.cache.items), len(v.nodes.items)
	before := v.IOStats()
	if err = v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := v.IOStats()
	got := accountingUsage(t, v)
	if !reflect.DeepEqual(got.All, want) || !reflect.DeepEqual(got.Owned, want) {
		t.Fatal("wrong census", got, want)
	}
	if p.reads != 0 || p.views < 96 {
		t.Fatal("cold census copied pages instead of borrowing", p.reads, p.views)
	}
	if after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
		t.Fatal("read-only census wrote backend", before, after)
	}
	if objects != len(v.cache.items) || nodes != len(v.nodes.items) {
		t.Fatal("census polluted foreground metadata caches")
	}
	// A positive fallback control proves that the ReadAt probe is effective and
	// that copying versus borrowing has identical accounting semantics.
	v.dev.(*meteredIO).VolumeIO = struct{ VolumeIO }{p}
	// Deliberately use an unkeyed equivalent predicate to force a fresh census;
	// an unchanged known predicate now correctly reuses maintained counters.
	if _, err = v.ConfigureAccounting(AccountingOptions{Include: func(Key, bool) bool { return true }}); err != nil {
		t.Fatal(err)
	}
	if err = v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.reads == 0 {
		t.Fatal("fallback never reached positioned read positive control")
	}
	if got = accountingUsage(t, v); !reflect.DeepEqual(got.All, want) {
		t.Fatal("fallback census differs", got, want)
	}
}

// Measures the same bounded-cache census with and without the backend view
// capability. The fallback is also the old cancellation-wrapper behavior.
func BenchmarkWarmAccountingMapped(b *testing.B) {
	v, err := New(filepath.Join(b.TempDir(), "data.vol"), 16<<20, HDD)
	if err != nil {
		b.Fatal(err)
	}
	defer v.Close()
	raw := v.dev.(*meteredIO).VolumeIO
	if f, ok := raw.(*FileIO); !ok || !f.hasMapping() {
		b.Skip("backend does not support mmap")
	}
	h, err := v.Open(true)
	if err != nil {
		b.Fatal(err)
	}
	for i := uint64(1); i <= 512; i++ {
		if _, err = h.Put(testKey(i), []byte("data"), Options{Accepted: true}); err != nil {
			h.Close()
			b.Fatal(err)
		}
	}
	h.Close()
	if err = v.Sync(); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"page-copy-fallback", "mapped"} {
		b.Run(mode, func(b *testing.B) {
			if mode == "mapped" {
				v.dev.(*meteredIO).VolumeIO = raw
			} else {
				v.dev.(*meteredIO).VolumeIO = struct{ VolumeIO }{raw}
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err = v.ConfigureAccounting(AccountingOptions{Include: func(Key, bool) bool { return true }}); err != nil {
					b.Fatal(err)
				}
				if err = v.WarmAccounting(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
