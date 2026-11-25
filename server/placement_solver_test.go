// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestPlacementCostsRemoveAndQuery(t *testing.T) {
	values := []uint64{0, 3, 7, 20, 91}
	f := placementCosts{make([]int, 6), make([]uint64, 6)}
	for i, n := range values {
		f.change(i, 1, n)
	}
	for k := 0; k <= 5; k++ {
		got, ok := f.cheapest(k)
		var want uint64
		for _, n := range values[:k] {
			want += n
		}
		if !ok || got != want {
			t.Fatal(k, got, want, ok)
		}
	}
	f.change(1, -1, 3)
	f.change(3, -1, 20)
	for k, want := range []uint64{0, 0, 7, 98} {
		if got, ok := f.cheapest(k); !ok || got != want {
			t.Fatal(k, got, want, ok)
		}
	}
	if _, ok := f.cheapest(4); ok {
		t.Fatal("nonexistent replica option")
	}
}

func TestPlacementSolverScarceFirstAndStays(t *testing.T) {
	bucket := client.ID{1}
	resources := []placementResource{{Node: client.ID{1}, Free: 10, Capacity: 10}, {Node: client.ID{2}, Free: 10, Capacity: 10}}
	demands := []placementDemand{{Bucket: bucket, Copies: 1, Bytes: 10}, {Bucket: bucket, Copies: 1, Bytes: 10}}
	options := []placementOption{{Range: 0, Volume: 0, Allocate: 10, Move: 10}, {Range: 0, Volume: 1, Allocate: 10, Move: 10}, {Range: 1, Volume: 0, Allocate: 10, Move: 10}}
	got := solvePlacement(demands, resources, options, nil)
	if len(got.Pending) != 0 || got.Order[0] != 1 || options[got.Assignments[0][0]].Volume != 1 {
		t.Fatalf("scarce SSD/mask choice was consumed: %+v", got)
	}
	options = append(options, placementOption{Range: 0, Volume: 0})
	got = solvePlacement(demands, resources, options, nil)
	if len(got.Pending) != 0 || got.Assignments[0][0] != 3 {
		t.Fatalf("valid existing copy was moved: %+v", got)
	}
}

func TestPlacementSolverProtectedBackingAndQuota(t *testing.T) {
	a, b := client.ID{1}, client.ID{2}
	resources := []placementResource{{Node: client.ID{1}, Capacity: 100, Free: 0, Reserved: map[client.ID]uint64{a: 10}}, {Node: client.ID{2}, Capacity: 100, Free: 10}}
	demands := []placementDemand{{Bucket: b, Copies: 1, Bytes: 10}, {Bucket: a, Copies: 1, Bytes: 10}}
	options := []placementOption{{Range: 0, Volume: 0, Allocate: 10, Move: 10}, {Range: 0, Volume: 1, Allocate: 10, Move: 10}, {Range: 1, Volume: 0, Allocate: 10, Move: 10}, {Range: 1, Volume: 1, Allocate: 10, Move: 10}}
	got := solvePlacement(demands, resources, options, map[client.ID]uint64{a: 10, b: 10})
	if len(got.Pending) != 0 || options[got.Assignments[0][0]].Volume != 1 || options[got.Assignments[1][0]].Volume != 0 {
		t.Fatalf("borrowed another bucket's guarantee: %+v", got)
	}
	got = solvePlacement(demands, resources, options, map[client.ID]uint64{a: 9, b: 10})
	if len(got.Assignments[1]) != 0 {
		t.Fatal("physical quota exceeded")
	}
}

func TestPlacementSolverReplicasAndBudgetLookahead(t *testing.T) {
	bucket := client.ID{1}
	resources := []placementResource{{Node: client.ID{1}, Free: 100, Capacity: 100}, {Node: client.ID{1}, Free: 100, Capacity: 100}, {Node: client.ID{2}, Free: 100, Capacity: 100}, {Node: client.ID{3}, Free: 100, Capacity: 100}}
	options := []placementOption{{Range: 0, Volume: 0, Allocate: 10, Move: 0}, {Range: 0, Volume: 1, Allocate: 8, Move: 8}, {Range: 0, Volume: 2, Allocate: 8, Move: 8}, {Range: 0, Volume: 3, Allocate: 8, Move: 8}}
	got := solvePlacement([]placementDemand{{Bucket: bucket, Copies: 2}}, resources, options, map[client.ID]uint64{bucket: 16})
	if len(got.Pending) != 0 {
		t.Fatalf("greedy choice stranded the remaining replica: %+v", got)
	}
	used := map[client.ID]bool{}
	for _, index := range got.Assignments[0] {
		o := options[index]
		if o.Allocate != 8 || used[resources[o.Volume].Node] {
			t.Fatal("replica constraints", got)
		}
		used[resources[o.Volume].Node] = true
	}
}

func TestPlacementSolverRandomResourceInvariants(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		resources := make([]placementResource, 8)
		for i := range resources {
			resources[i] = placementResource{Node: client.ID{byte(i/2 + 1)}, Capacity: 50, Free: 50}
		}
		var demands []placementDemand
		var options []placementOption
		for i := 0; i < 40; i++ {
			demands = append(demands, placementDemand{Bucket: client.ID{byte(i%3 + 1)}, Copies: 1 + rng.Intn(3), Bytes: uint64(1 + rng.Intn(12))})
			for v := range resources {
				if rng.Intn(3) != 0 {
					options = append(options, placementOption{Range: i, Volume: v, Allocate: demands[i].Bytes, Move: demands[i].Bytes})
				}
			}
		}
		budgets := map[client.ID]uint64{{1}: 70, {2}: 70, {3}: 70}
		got := solvePlacement(demands, resources, options, budgets)
		used := make([]uint64, len(resources))
		charged := map[client.ID]uint64{}
		for i, assignment := range got.Assignments {
			if len(assignment) == 0 {
				continue
			}
			if len(assignment) != demands[i].Copies {
				t.Fatal("partial replica set", seed)
			}
			nodes := map[client.ID]bool{}
			for _, index := range assignment {
				o := options[index]
				if o.Range != i || nodes[resources[o.Volume].Node] {
					t.Fatal("wrong replica placement", seed)
				}
				nodes[resources[o.Volume].Node] = true
				used[o.Volume] += o.Allocate
				charged[demands[i].Bucket] += o.Allocate
			}
		}
		for i, n := range used {
			if n > resources[i].Free {
				t.Fatal("volume overcommitted", seed, i, n)
			}
		}
		for bucket, n := range charged {
			if n > budgets[bucket] {
				t.Fatal("quota overcommitted", seed)
			}
		}
	}
}

func placementScalingInput(n int) ([]placementDemand, []placementResource, []placementOption) {
	demands := make([]placementDemand, n)
	resources := make([]placementResource, 4)
	options := make([]placementOption, 0, 4*n)
	for i := range resources {
		resources[i] = placementResource{Node: client.ID{byte(i + 1)}, Capacity: uint64(n), Free: uint64(n / 4)}
	}
	for i := range demands {
		demands[i] = placementDemand{Bucket: client.ID{1}, Copies: 1, Bytes: 1}
		for v := range resources {
			options = append(options, placementOption{Range: i, Volume: v, Allocate: 1, Move: 1})
		}
	}
	return demands, resources, options
}
func TestPlacementSolverUpdatesOnlyCrossedThresholds(t *testing.T) {
	d, v, o := placementScalingInput(16384)
	got := solvePlacement(d, v, o, nil)
	if len(got.Pending) != 0 {
		t.Fatal("feasible plan incomplete", len(got.Pending))
	}
	if got.Invalidations > uint64(len(o)) || got.Comparisons > uint64(len(o))*64 {
		t.Fatalf("nonlocal/quadratic queue maintenance: %+v", got)
	}
	q := newPlacementQueue(100, func(a, b int) bool { return a < b })
	var ids []int
	for len(q.items) > 0 {
		ids = append(ids, q.pop())
	}
	if !sort.IntsAreSorted(ids) {
		t.Fatal("queue order")
	}
}
func BenchmarkPlacementSolver(b *testing.B) {
	for _, n := range []int{1024, 16384, 65536} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			d, v, o := placementScalingInput(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				solvePlacement(d, v, o, nil)
			}
		})
	}
}
