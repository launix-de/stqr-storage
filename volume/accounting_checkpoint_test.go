// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestAccountingCheckpointCachesConfirmedCutWithConcurrentAcceptedTail(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		t.Run(map[bool]string{true: "accepted-fence", false: "durable-mutation"}[accepted], func(t *testing.T) {
			v, name, path, _ := accountingCacheFixture(t)
			raw := v.dev.(*meteredIO).VolumeIO
			gate := &checkpointGateIO{VolumeIO: raw}
			v.dev.(*meteredIO).VolumeIO = gate
			key, tail := testKey(990), testKey(991)
			if accepted {
				if _, err := testPut(v, key, []byte("confirmed cut"), Options{Accepted: true}); err != nil {
					t.Fatal(err)
				}
			}
			entered, release := gate.arm(nil)
			defer release()
			done := make(chan error, 1)
			before := v.IOStats()
			go func() {
				if accepted {
					done <- v.Sync()
					return
				}
				_, err := testPut(v, key, []byte("confirmed cut"), Options{})
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("checkpoint did not reach physical I/O")
			}
			// The confirmed snapshot has been frozen, but payload/metadata readers and
			// a later accepted publisher remain independent of the physical fence.
			want := accountingUsage(t, v)
			next := make(chan error, 1)
			go func() {
				_, err := testPut(v, tail, bytes.Repeat([]byte("unconfirmed"), 43), Options{Accepted: true})
				next <- err
			}()
			awaitCheckpointTest(t, next)
			later := accountingUsage(t, v)
			if reflect.DeepEqual(later, want) {
				t.Fatal("later write did not change accounting")
			}
			release()
			awaitCheckpointTest(t, done)
			after := v.IOStats()
			if after.SyncOps-before.SyncOps != 4 {
				t.Fatal("cache added physical fences", before, after)
			}
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
			v = reopenAccountingFile(t, name)
			before = v.IOStats()
			ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: path, Stamp: cacheOwnershipStamp(1), Include: cacheInclude})
			if err != nil || !ready {
				t.Fatal("confirmed checkpoint cache was not restored", ready, err)
			}
			if got := accountingUsage(t, v); !reflect.DeepEqual(got, want) {
				t.Fatal("unconfirmed counters leaked into durable cache", got, want)
			}
			if after = v.IOStats(); after != before {
				t.Fatal("cache hit traversed record storage", before, after)
			}
			if _, _, err := testGet(v, tail); !errors.Is(err, ErrNotFound) {
				t.Fatal("unrequested tail unexpectedly persisted", err)
			}
			if got, _, err := testGet(v, key); err != nil || string(got) != "confirmed cut" {
				t.Fatal(got, err)
			}
		})
	}
}

func TestAccountingFailedCheckpointKeepsPreviousCache(t *testing.T) {
	v, name, path, want := accountingCacheFixture(t)
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(v, testKey(992), []byte("not confirmed"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	gate := &checkpointGateIO{VolumeIO: v.dev.(*meteredIO).VolumeIO}
	v.dev.(*meteredIO).VolumeIO = gate
	entered, release := gate.arm(errors.New("injected checkpoint failure"))
	defer release()
	done := make(chan error, 1)
	go func() { done <- v.Sync() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint did not start")
	}
	release()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("checkpoint failure ignored")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint did not finish")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatal("failed fence replaced cache", err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	v = reopenAccountingFile(t, name)
	ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: path, Stamp: cacheOwnershipStamp(1), Include: cacheInclude})
	if err != nil || !ready {
		t.Fatal(ready, err)
	}
	if got := accountingUsage(t, v); !reflect.DeepEqual(got, want) {
		t.Fatal(got, want)
	}
}

func TestAccountingCheckpointCacheFailureDoesNotFailExplicitSync(t *testing.T) {
	v, name, path, _ := accountingCacheFixture(t)
	blocker := path + ".blocker"
	if err := os.WriteFile(blocker, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	v.accountCachePath = blocker + "/cache"
	key := testKey(993)
	before := v.IOStats()
	if _, err := testPut(v, key, []byte("confirmed despite advisory failure"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if got := v.IOStats(); got.SyncOps != before.SyncOps {
		t.Fatal("accepted write requested sync", before, got)
	}
	if err := v.Sync(); err != nil {
		t.Fatal("derived-cache error failed confirmed client fence", err)
	}
	if got := v.IOStats(); got.SyncOps-before.SyncOps != 4 {
		t.Fatal("sidecar introduced additional Sync", before, got)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	v = reopenAccountingFile(t, name)
	// The old cache is stale, so reopening must remain unknown, never restore
	// old counters as if the newly confirmed object were absent.
	if ready, err := v.ConfigureAccounting(AccountingOptions{CachePath: path, Stamp: cacheOwnershipStamp(1), Include: cacheInclude}); err != nil || ready {
		t.Fatal("stale cache was accepted", ready, err)
	}
	if got, _, err := testGet(v, key); err != nil || string(got) != "confirmed despite advisory failure" {
		t.Fatal(got, err)
	}
}
