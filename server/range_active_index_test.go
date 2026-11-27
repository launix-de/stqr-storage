// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/binary"
	"encoding/json"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func activeIndexPlan(t testing.TB) rangePlan {
	t.Helper()
	entry := client.RangeEntry{PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{{Node: client.ID{1}, Volume: client.ID{4}, Class: uint8(volume.HDD)}}}
	before := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID{3}, ToEnd: true, Entries: []client.RangeEntry{entry}, Addresses: []client.NodeAddress{{Node: client.ID{1}, Addresses: []string{"http://127.0.0.1:7419"}}, {Node: client.ID{2}, Addresses: []string{"http://127.0.0.1:7420"}}}}
	plan, err := planRangeMove(before, entry, client.ID{2}, volume.ID{5}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
func activeIndexReplay(t testing.TB, r *rangeManager, kind uint16, event rangeEvent) {
	t.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	err = r.replay(controlRecord{Kind: kind, Data: data, Proof: client.Proof{RecordSequence: 7}})
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}
func TestRangeActiveIndexReplayLifecycle(t *testing.T) {
	for _, role := range []string{"source", "target", "abort"} {
		t.Run(role, func(t *testing.T) {
			p := activeIndexPlan(t)
			node := p.Target
			if role == "source" {
				node = p.Source
			}
			r := &rangeManager{node: node, initialized: true, txns: map[[32]byte]*rangeTxn{}}
			bucket := volume.ID(p.Before.Bucket)
			if r.incoming(bucket, volume.ObjectID{}) {
				t.Fatal("empty manager incoming")
			}
			activeIndexReplay(t, r, rangeReserved, rangeEvent{ID: p.id(), Plan: mustRangeWire(p)})
			r.mu.Lock()
			if len(r.activeTxns[bucket]) != 1 {
				t.Fatal("reservation not indexed")
			}
			r.mu.Unlock()
			if role == "target" && !r.incoming(bucket, volume.ObjectID{}) {
				t.Fatal("incoming reservation invisible")
			}
			if role == "abort" {
				activeIndexReplay(t, r, rangeAborted, rangeEvent{ID: p.id()})
			} else {
				r.mu.Lock()
				r.txns[p.id()].decision = &rangeCommit{}
				r.mu.Unlock()
				kind := rangeTargetDone
				if role == "source" {
					kind = rangeSourceDone
				}
				activeIndexReplay(t, r, kind, rangeEvent{ID: p.id(), TargetDone: client.Proof{Node: p.Target, RecordSequence: 1}})
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if len(r.activeTxns) != 0 || len(r.txns) != 1 {
				t.Fatal("completed plans must leave index but retain history")
			}
		})
	}
}
func TestRangeActiveIndexRecoveredHistoryAndGuard(t *testing.T) {
	p := activeIndexPlan(t)
	r := &rangeManager{node: p.Source, txns: map[[32]byte]*rangeTxn{}}
	for i := 1; i <= 10000; i++ {
		var id [32]byte
		binary.LittleEndian.PutUint64(id[:], uint64(i))
		r.txns[id] = &rangeTxn{plan: p, sourceDone: client.Proof{RecordSequence: 1}}
	}
	r.txns[p.id()] = &rangeTxn{plan: p, fenced: true}
	if _, err := r.guard(volume.ID(p.Before.Bucket), volume.ObjectID{}); err == nil {
		t.Fatal("active source fence ignored")
	}
	if len(r.activeTxns[volume.ID(p.Before.Bucket)]) != 1 {
		t.Fatal("history entered active index")
	}
	release, err := r.guard(volume.ID{99}, volume.ObjectID{})
	if err != nil {
		t.Fatal(err)
	}
	release()
	r.mu.Lock()
	r.txns[p.id()].forceRetired = true
	r.updateActiveTxnLocked(p.id())
	r.mu.Unlock()
	release, err = r.guard(volume.ID(p.Before.Bucket), volume.ObjectID{})
	if err != nil {
		t.Fatal(err)
	}
	release()
	if len(r.txns) != 10001 {
		t.Fatal("history removed")
	}
}
func TestRangeActiveIndexConcurrentTransitions(t *testing.T) {
	p := activeIndexPlan(t)
	r := &rangeManager{node: p.Target, txns: map[[32]byte]*rangeTxn{p.id(): {plan: p}}}
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 1000; j++ {
				r.incoming(volume.ID(p.Before.Bucket), volume.ObjectID{})
				release, err := r.guard(volume.ID(p.Before.Bucket), volume.ObjectID{})
				if err == nil {
					release()
				}
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		r.mu.Lock()
		r.txns[p.id()].aborted = i%2 == 0
		r.updateActiveTxnLocked(p.id())
		r.mu.Unlock()
	}
	workers.Wait()
}
func BenchmarkRangeIncomingHistory(b *testing.B) {
	for _, n := range []int{0, 10000} {
		name := "empty"
		if n > 0 {
			name = "10000-completed"
		}
		b.Run(name, func(b *testing.B) {
			p := activeIndexPlan(b)
			r := &rangeManager{node: p.Target, txns: map[[32]byte]*rangeTxn{}}
			for i := 0; i < n; i++ {
				var id [32]byte
				binary.LittleEndian.PutUint64(id[:], uint64(i))
				r.txns[id] = &rangeTxn{plan: p, targetDone: client.Proof{RecordSequence: 1}}
			}
			r.incoming(volume.ID(p.Before.Bucket), volume.ObjectID{})
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.incoming(volume.ID(p.Before.Bucket), volume.ObjectID{})
			}
		})
	}
}

func TestRangeActiveIndexDisjointIntervals(t *testing.T) {
	p := activeIndexPlan(t)
	p.Moving.PrefixBits = 1
	q := p
	q.Moving.Start[0] = 128
	r := &rangeManager{node: p.Source, txns: map[[32]byte]*rangeTxn{{1}: {plan: p, fenced: true}, {2}: {plan: q}}}
	bucket := volume.ID(p.Before.Bucket)
	if _, err := r.guard(bucket, volume.ObjectID{1}); err == nil {
		t.Fatal("matching fenced half allowed")
	}
	release, err := r.guard(bucket, volume.ObjectID{128})
	if err != nil {
		t.Fatal("unrelated fenced interval blocked write", err)
	}
	release()
	if len(r.activeTxns[bucket]) != 2 {
		t.Fatal("one overlapping bucket interval lost")
	}
}
