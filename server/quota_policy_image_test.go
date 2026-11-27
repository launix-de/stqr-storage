// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/volume"
)

func policyImageFixture(count int) *Server {
	s := &Server{bucketsByID: make(map[volume.ID]*Bucket), bucketsByName: make(map[string]*Bucket)}
	s.cfg.Node.ID = volume.ID{1}.String()
	for i := 0; i < count; i++ {
		id := volume.ID{byte(i), byte(i >> 8), 42}
		b, _ := NewBucket(id, fmt.Sprintf("quota-%d", i))
		d := b.Describe()
		d.Policy.Limits.Accounting = "physical"
		d.Policy.Limits.Absolute = 100
		d.Policy.Limits.Unlimited = false
		d.Grants = make([]BucketGrant, 64)
		d.Receipts = make([]BucketReceipt, 128)
		d.ClonePeers = []BucketClonePeer{{Bucket: "unrelated"}}
		d.LocalVolumes = []string{"local"}
		b.setDescription(d)
		s.bucketsByID[id] = b
		s.bucketsByName[d.Name] = b
	}
	return s
}
func imageDescription(t *testing.T, s *Server, id string) BucketDescription {
	t.Helper()
	for _, d := range s.ensureQuotaPolicies().descriptions {
		if d.ID == id {
			return d
		}
	}
	t.Fatalf("missing %s", id)
	return BucketDescription{}
}
func TestQuotaPolicyImagePublication(t *testing.T) {
	s := policyImageFixture(1)
	b := s.bucketsByName["quota-0"]
	old := s.ensureQuotaPolicies()
	d := b.Describe()
	compact := imageDescription(t, s, d.ID)
	if len(compact.Grants)+len(compact.Receipts)+len(compact.ClonePeers)+len(compact.LocalVolumes) != 0 {
		t.Fatal("image retained unrelated arrays")
	}
	d.Revision++
	d.Policy.Limits.Absolute = 200
	desired := d.Policy
	desired.Limits.Absolute = 300
	d.DesiredPolicy = &desired
	s.publishBucket(d)
	image := s.ensureQuotaPolicies()
	if image == old {
		t.Fatal("publication reused old policy")
	}
	got := imageDescription(t, s, d.ID)
	if got.Policy.Limits.Absolute != 200 || quotaPolicy(got).Limits.Absolute != 300 {
		t.Fatal("stale current/desired quota")
	}
	desired.Limits.Absolute = 999
	if quotaPolicy(got).Limits.Absolute != 300 {
		t.Fatal("desired policy alias")
	}
	if old.descriptions[0].Policy.Limits.Absolute != 100 {
		t.Fatal("old image mutated")
	}
	// ACL/clone-only publication also keeps the active quota and immutable image.
	d = b.Describe()
	d.Revision++
	d.Grants = []BucketGrant{{UserID: "changed", Permissions: BucketRead}}
	d.ClonePeers = nil
	s.publishBucket(d)
	if quotaPolicy(imageDescription(t, s, d.ID)).Limits.Absolute != 300 {
		t.Fatal("ACL/clone edit changed quota")
	}
	d = b.Describe()
	d.Revision++
	d.DeletionComplete = true
	s.publishBucket(d)
	if !imageDescription(t, s, d.ID).DeletionComplete {
		t.Fatal("deletion not published")
	}
	fresh, _ := NewBucket(volume.ID{99}, "clone")
	clone := fresh.Describe()
	clone.Policy.Limits.Absolute = 123
	s.publishBucket(clone)
	if imageDescription(t, s, clone.ID).Policy.Limits.Absolute != 123 {
		t.Fatal("clone quota missing")
	}
}
func TestQuotaPolicyImageBulkInstallAndRollback(t *testing.T) {
	s := policyImageFixture(1)
	b := s.bucketsByName["quota-0"]
	a := &systemView{}
	z := &systemView{}
	s.currentSystem.Store(a)
	first := s.ensureQuotaPolicies()
	replace := func(view *systemView, limit uint64) {
		s.bucketMu.Lock()
		d := b.Describe()
		d.Policy.Limits.Absolute = limit
		b.mu.Lock()
		b.setDescription(d)
		b.mu.Unlock()
		s.currentSystem.Store(view)
		s.bucketMu.Unlock()
	}
	replace(z, 500)
	next := s.ensureQuotaPolicies()
	if next == first || imageDescription(t, s, b.ID().String()).Policy.Limits.Absolute != 500 {
		t.Fatal("bulk install stale")
	}
	replace(a, 100)
	rollback := s.ensureQuotaPolicies()
	if rollback == next || imageDescription(t, s, b.ID().String()).Policy.Limits.Absolute != 100 {
		t.Fatal("rollback stale")
	}
	// Pending reserve changes do not alter policies, but remain independently live.
	s.pendingQuotas.Store(z)
	if s.ensureQuotaPolicies() != rollback {
		t.Fatal("pending reserves rebuilt policy")
	}
}
func TestQuotaPolicyImageConcurrentPublication(t *testing.T) {
	s := policyImageFixture(1)
	b := s.bucketsByName["quota-0"]
	s.ensureQuotaPolicies()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			image := s.ensureQuotaPolicies()
			if len(image.descriptions) != 1 {
				t.Error("incomplete image")
				return
			}
		}
	}()
	for i := 0; i < 40; i++ {
		d := b.Describe()
		d.Revision++
		d.Policy.Limits.Absolute++
		s.publishBucket(d)
	}
	wg.Wait()
}
func TestQuotaPolicyImageAllocations(t *testing.T) {
	s := policyImageFixture(200)
	if e := s.applyQuotaProtectionLocked(); e != nil {
		t.Fatal(e)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if e := s.applyQuotaProtectionLocked(); e != nil {
			panic(e)
		}
	})
	if allocs != 0 {
		t.Fatalf("ordinary no-volume refresh allocated %.0f times", allocs)
	}
}

var quotaImageBenchmarkSink any

func BenchmarkQuotaPolicyImage(b *testing.B) {
	s := policyImageFixture(200)
	s.applyQuotaProtectionLocked()
	b.Run("PreviousDescriptionSnapshots", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			quotaImageBenchmarkSink = s.bucketSnapshots()
		}
	})
	b.Run("CachedQuotaRefresh", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if e := s.applyQuotaProtectionLocked(); e != nil {
				b.Fatal(e)
			}
		}
	})
}
