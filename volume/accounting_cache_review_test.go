// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestAccountingWarmCancelWhilePublicationWaits(t *testing.T) {
	_, disk, _ := startupAccountingFixture(t, 32)
	v, err := Open(disk, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	observations := 0
	if ready, err := v.ConfigureAccounting(AccountingOptions{Observer: func(StorageSnapshot) { observations++ }}); err != nil || ready {
		t.Fatal(ready, err)
	}
	reader, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- v.WarmAccounting(ctx) }()
	// A queued exclusive publisher prevents new shared admissions. Keep the
	// pre-existing reader alive so cancellation occurs after the scan, not in it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		probe, acquired, err := v.TryOpen(false)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			break
		}
		probe.Close()
		if time.Now().After(deadline) {
			t.Fatal("warmup did not reach publication")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	reader.Close()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || v.AccountingReady() || v.accounting != nil || v.owned != nil || observations != 0 {
			t.Fatalf("canceled publication became ready: err=%v ready=%v", err, v.AccountingReady())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled warmup did not finish")
	}
	if err := v.WarmAccounting(context.Background()); err != nil || !v.AccountingReady() || observations != 1 {
		t.Fatal("cancellation did not leave retryable cold accounting", err, observations)
	}
}

func TestAccountingUnkeyedBucketProofIsNotAllRecordsFingerprint(t *testing.T) {
	v, name, cache, want := accountingCacheFixture(t)
	// A nil fallback includes everything only where no bucket proof overrides it.
	// This proof selects nothing and intentionally supplies no checkpoint key.
	if _, err := v.ConfigureAccounting(AccountingOptions{CachePath: cache, Stamp: startupOwnershipStamp{uniform: true, own: false}}); err != nil {
		t.Fatal(err)
	}
	if err := v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := accountingUsage(t, v); len(got.Owned) != 0 {
		t.Fatal("fixture did not exclude proven buckets", got)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	v = reopenAccountingFile(t, name)
	ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: cache, Stamp: startupOwnershipStamp{uniform: true, own: true}})
	if err != nil || ready || v.AccountingReady() {
		t.Fatalf("unkeyed changed bucket proof reused ownership totals: ready=%v err=%v", ready, err)
	}
	if err := v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := accountingUsage(t, v); !reflect.DeepEqual(got.All, want.All) || !reflect.DeepEqual(got.Owned, want.All) {
		t.Fatal("proof change lost authoritative totals", got, want.All)
	}
}

func TestAccountingNilPredicateStillRestoresAllRecordsCache(t *testing.T) {
	v, name, cache, want := accountingCacheFixture(t)
	if _, err := v.ConfigureAccounting(AccountingOptions{CachePath: cache}); err != nil {
		t.Fatal(err)
	}
	if err := v.WarmAccounting(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	v = reopenAccountingFile(t, name)
	before := v.IOStats()
	ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: cache})
	if err != nil || !ready {
		t.Fatal("unconditional all-records cache hit lost", ready, err)
	}
	if got := accountingUsage(t, v); !reflect.DeepEqual(got.All, want.All) || !reflect.DeepEqual(got.Owned, want.All) {
		t.Fatal(got, want.All)
	}
	if after := v.IOStats(); after != before {
		t.Fatal("all-records cache hit read canonical records", before, after)
	}
}
