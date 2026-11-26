// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func improvementInput(d []placementDemand, o []placementOption) placementImproveInput {
	return placementImproveInput{Work: make([]placementWorkload, len(d)), Held: make([]uint64, len(o)), Network: make([]uint64, len(o)), Weights: &placementScore{Move: 1}}
}
func assertImprovementValid(t *testing.T, d []placementDemand, v []placementResource, o []placementOption, q map[client.ID]uint64, x placementSolution) {
	t.Helper()
	allocation := make(map[int]map[client.ID]*big.Int)
	quota := make(map[client.ID]*big.Int)
	for r, ids := range x.Assignments {
		if len(ids) != d[r].Copies {
			t.Fatal("replica count", r, ids)
		}
		seen := make(map[client.ID]bool)
		for _, id := range ids {
			if id < 0 || id >= len(o) || o[id].Range != r {
				t.Fatal("ineligible option", id)
			}
			edge := o[id]
			node := v[edge.Volume].Node
			if seen[node] {
				t.Fatal("duplicate replica node", node)
			}
			seen[node] = true
			if allocation[edge.Volume] == nil {
				allocation[edge.Volume] = make(map[client.ID]*big.Int)
			}
			if allocation[edge.Volume][d[r].Bucket] == nil {
				allocation[edge.Volume][d[r].Bucket] = new(big.Int)
			}
			allocation[edge.Volume][d[r].Bucket].Add(allocation[edge.Volume][d[r].Bucket], new(big.Int).SetUint64(edge.Allocate))
			if quota[d[r].Bucket] == nil {
				quota[d[r].Bucket] = new(big.Int)
			}
			quota[d[r].Bucket].Add(quota[d[r].Bucket], new(big.Int).SetUint64(edge.Quota))
		}
	}
	for volume, buckets := range allocation {
		shared := new(big.Int)
		for bucket, n := range buckets {
			rest := new(big.Int).Sub(n, new(big.Int).SetUint64(v[volume].Reserved[bucket]))
			if rest.Sign() > 0 {
				shared.Add(shared, rest)
			}
		}
		if shared.Cmp(new(big.Int).SetUint64(v[volume].Free)) > 0 {
			t.Fatal("overbooked volume", volume, shared)
		}
	}
	for bucket, limit := range q {
		if quota[bucket] != nil && quota[bucket].Cmp(new(big.Int).SetUint64(limit)) > 0 {
			t.Fatal("overbooked bucket", bucket, quota[bucket])
		}
	}
}

func TestPlacementImproveCopyCost(t *testing.T) {
	d := []placementDemand{{Copies: 1}, {Copies: 1}}
	v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 1}, {Node: client.ID{2}, Capacity: 1000, Free: 1}, {Node: client.ID{3}, Capacity: 1000, Free: 1}}
	o := []placementOption{{Range: 0, Volume: 0, Allocate: 1, Move: 100}, {Range: 0, Volume: 1, Allocate: 1, Move: 1}, {Range: 1, Volume: 1, Allocate: 1}, {Range: 1, Volume: 2, Allocate: 1, Move: 2}}
	x := placementSolution{Assignments: [][]int{{0}, {2}}, Order: []int{0, 1}}
	r := improvePlacement(context.Background(), d, v, o, nil, &x, improvementInput(d, o), placementImproveBudget, 7)
	assertImprovementValid(t, d, v, o, nil, x)
	if r.Before.Move != 100 || r.After.Move != 3 || r.Accepted == 0 {
		t.Fatal("missed paired improvement", r, x)
	}
}
func TestPlacementImproveFutureCapacity(t *testing.T) {
	d := []placementDemand{{Copies: 1}}
	v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 100}, {Node: client.ID{2}, Capacity: 1000, Free: 900}}
	o := []placementOption{{Range: 0, Volume: 0, Existing: true}, {Range: 0, Volume: 1, Allocate: 100, Move: 100}}
	x := placementSolution{Assignments: [][]int{{0}}, Order: []int{0}}
	in := improvementInput(d, o)
	in.Weights = &placementScore{Move: 1, Future: 1}
	in.Work[0].Physical = 100
	in.Held[0] = 100
	r := improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 9)
	assertImprovementValid(t, d, v, o, nil, x)
	if x.Assignments[0][0] != 1 || r.After.Future >= r.Before.Future || r.After.Move <= r.Before.Move {
		t.Fatal("future pressure was not traded against transfer cost", r, x)
	}
}
func TestPlacementImproveNetworkTraffic(t *testing.T) {
	d := []placementDemand{{Copies: 1}}
	v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 100}, {Node: client.ID{2}, Capacity: 1000, Free: 100}}
	o := []placementOption{{Range: 0, Volume: 0, Allocate: 10, Move: 10}, {Range: 0, Volume: 1, Allocate: 10, Move: 10}}
	x := placementSolution{Assignments: [][]int{{0}}, Order: []int{0}}
	in := improvementInput(d, o)
	in.Network[0] = 10
	in.Weights = &placementScore{Traffic: 1}
	r := improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 11)
	assertImprovementValid(t, d, v, o, nil, x)
	if x.Assignments[0][0] != 1 || r.After.Move != r.Before.Move || r.After.Traffic >= r.Before.Traffic {
		t.Fatal("equal-copy local route lost to network transfer", r, x)
	}
}

func TestPlacementImproveUnboundCohort(t *testing.T) {
	for _, hot := range []bool{false, true} {
		d := []placementDemand{{Copies: 1}}
		v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 800}, {Node: client.ID{1}, Capacity: 1000, Free: 800}, {Node: client.ID{1}}}
		if hot {
			v[0].Free, v[1].Free = 100, 900
		}
		o := []placementOption{{Range: 0, Volume: 0, Allocate: 100, Move: 100, Existing: true}, {Range: 0, Volume: 1, Allocate: 100, Move: 100, Existing: true}, {Range: 0, Volume: 2, Existing: true, Current: true}}
		x := solvePlacement(d, v, o, nil)
		if x.Assignments[0][0] != 2 {
			t.Fatal("greedy changed the old unpinned map", x)
		}
		in := improvementInput(d, o)
		in.Work[0].Physical = 200
		in.Held = []uint64{100, 100, 0}
		in.Unbound = []bool{true}
		in.Cohorts = map[int][]placementCohort{2: {{0, 100}, {1, 100}}}
		in.Weights = &placementScore{Move: 1, Future: 1}
		improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 13)
		assertImprovementValid(t, d, v, o, nil, x)
		want := 2
		if hot {
			want = 1
		}
		if x.Assignments[0][0] != want {
			t.Fatal("cohort did not retain exact map or retain useful bytes", hot, x.Assignments)
		}
	}
}
func TestPlacementImproveStableAndHardConstraints(t *testing.T) {
	for _, name := range []string{"stable", "quota", "replica", "private-reserve", "uint53", "uint64", "quota-overflow"} {
		t.Run(name, func(t *testing.T) {
			bucket := client.ID{9}
			d := []placementDemand{{Bucket: bucket, Copies: 1}}
			v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 100}, {Node: client.ID{2}, Capacity: 1000, Free: 100}}
			o := []placementOption{{Range: 0, Volume: 0, Existing: true, Move: 100}, {Range: 0, Volume: 1, Allocate: 10}}
			x := placementSolution{Assignments: [][]int{{0}}, Order: []int{0}}
			var q map[client.ID]uint64
			switch name {
			case "stable":
				o[0].Move = 0
			case "quota":
				o[1].Quota = 1
				q = map[client.ID]uint64{bucket: 0}
			case "replica":
				o[0].Move = 0
				d[0].Copies = 2
				v = append(v, placementResource{Node: v[0].Node, Capacity: 1000, Free: 100})
				o = append(o, placementOption{Range: 0, Volume: 2})
				x.Assignments[0] = []int{0, 1}
				o[1].Move = 100
			case "private-reserve":
				v[1].Free = 0
				v[1].Reserved = map[client.ID]uint64{{8}: 100}
			case "uint53", "uint64", "quota-overflow":
				limit := uint64(1 << 53)
				if name != "uint53" {
					limit = math.MaxUint64
				}
				v[1].Free, v[1].Capacity = limit, limit
				d = append(d, placementDemand{Bucket: bucket, Copies: 1})
				o[1].Allocate = limit
				o = append(o, placementOption{Range: 1, Volume: 1, Allocate: 1})
				x.Assignments = append(x.Assignments, []int{2})
				x.Order = append(x.Order, 1)
				if name == "quota-overflow" {
					o[1].Allocate = 0
					o[2].Allocate = 0
					o[1].Quota = limit
					o[2].Quota = 1
					q = map[client.ID]uint64{bucket: limit}
				}
			}
			original := make([][]int, len(x.Assignments))
			for i := range original {
				original[i] = append([]int(nil), x.Assignments[i]...)
			}
			improvePlacement(context.Background(), d, v, o, q, &x, improvementInput(d, o), placementImproveBudget, 19)
			assertImprovementValid(t, d, v, o, q, x)
			if !reflect.DeepEqual(x.Assignments, original) {
				t.Fatal("invalid or unnecessary transform", x.Assignments, original)
			}
		})
	}
}
func TestPlacementImproveDeadline(t *testing.T) {
	d := []placementDemand{{Copies: 1}}
	v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 100}}
	o := []placementOption{{Range: 0, Volume: 0, Existing: true}, {Range: 0, Volume: 0, Move: 1}}
	for _, budget := range []time.Duration{0, time.Nanosecond, time.Second} {
		x := placementSolution{Assignments: [][]int{{0}}, Order: []int{0}}
		in := improvementInput(d, o)
		clock := time.Unix(0, 0)
		in.now = func() time.Time { clock = clock.Add(time.Millisecond); return clock }
		r := improvePlacement(context.Background(), d, v, o, nil, &x, in, budget, 1)
		if !r.Stopped || r.Elapsed > min(budget, placementImproveBudget)+3*time.Millisecond {
			t.Fatal("deadline not enforced", budget, r)
		}
		assertImprovementValid(t, d, v, o, nil, x)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	x := placementSolution{Assignments: [][]int{{0}}, Order: []int{0}}
	if r := improvePlacement(ctx, d, v, o, nil, &x, improvementInput(d, o), placementImproveBudget, 1); r.Attempts != 0 || !r.Stopped {
		t.Fatal("cancellation ignored", r)
	}
}

func TestPlacementImproveRandomHardConstraints(t *testing.T) {
	random := rand.New(rand.NewSource(91))
	checked := 0
	for graph := 0; graph < 300; graph++ {
		d := make([]placementDemand, 2+random.Intn(8))
		v := make([]placementResource, 6)
		for i := range v {
			v[i] = placementResource{Node: client.ID{byte(i/2 + 1)}, Capacity: 100, Free: uint64(20 + random.Intn(60)), Reserved: map[client.ID]uint64{{1}: uint64(random.Intn(5))}}
		}
		q := map[client.ID]uint64{{1}: uint64(10 + random.Intn(30)), {2}: uint64(10 + random.Intn(30))}
		var o []placementOption
		for r := range d {
			d[r] = placementDemand{Bucket: client.ID{byte(1 + r%2)}, Copies: 1 + random.Intn(2), Bytes: uint64(1 + random.Intn(4))}
			for resource := range v {
				if random.Intn(5) != 0 {
					o = append(o, placementOption{Range: r, Volume: resource, Allocate: uint64(1 + random.Intn(8)), Move: uint64(random.Intn(100)), Quota: uint64(random.Intn(4))})
				}
			}
		}
		x := solvePlacement(d, v, o, q)
		if len(x.Pending) > 0 {
			continue
		}
		before := uint64(0)
		for _, ids := range x.Assignments {
			for _, id := range ids {
				before += o[id].Move
			}
		}
		assertImprovementValid(t, d, v, o, q, x)
		improvePlacement(context.Background(), d, v, o, q, &x, improvementInput(d, o), time.Millisecond, uint64(graph+1))
		assertImprovementValid(t, d, v, o, q, x)
		after := uint64(0)
		for _, ids := range x.Assignments {
			for _, id := range ids {
				after += o[id].Move
			}
		}
		if after > before {
			t.Fatal("accepted worse incumbent", graph, before, after)
		}
		checked++
	}
	if checked < 100 {
		t.Fatal("insufficient feasible property fixtures", checked)
	}
}

func BenchmarkPlacementRangeTraffic(b *testing.B) {
	s := &Server{}
	r := &rangeManager{server: s, node: client.ID{1}}
	s.scopes = r
	object := client.ObjectID{7}
	bucket := client.ID{9}
	image, err := buildPlacement([]client.MapPatch{{Membership: 1, Generation: 1, Revision: 1, Bucket: bucket, ToEnd: true, Entries: []client.RangeEntry{{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: r.node, Volume: client.ID{2}, Class: uint8(volume.HDD)}}}}, Addresses: []client.NodeAddress{{Node: r.node, Addresses: []string{"http://node:7419"}}}}})
	if err != nil {
		b.Fatal(err)
	}
	s.placement.Store(image)
	store, err := NewBucket(volume.ID(bucket), "traffic")
	if err != nil {
		b.Fatal(err)
	}
	store.server = s
	store.recordRangeTraffic(principal{}, volume.ObjectID(object), 1, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.recordRangeTraffic(principal{}, volume.ObjectID(object), 1, 0)
	}
}

func BenchmarkPlacementImprove(b *testing.B) {
	for _, count := range []int{100, 1000, 10000} {
		b.Run(big.NewInt(int64(count)).String(), func(b *testing.B) {
			d := make([]placementDemand, count)
			v := make([]placementResource, 4)
			for i := range v {
				v[i] = placementResource{Node: client.ID{byte(i + 1)}, Capacity: 1 << 40, Free: 1 << 39}
			}
			var o []placementOption
			for r := range d {
				d[r] = placementDemand{Copies: 1}
				for resource := range v {
					o = append(o, placementOption{Range: r, Volume: resource, Allocate: 1 << 20, Move: uint64(resource+1) << 20})
				}
			}
			in := improvementInput(d, o)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				x := solvePlacement(d, v, o, nil)
				r := improvePlacement(context.Background(), d, v, o, nil, &x, in, 10*time.Millisecond, uint64(i+1))
				b.ReportMetric(float64(r.Attempts), "trials/op")
				b.ReportMetric(float64(r.Elapsed.Microseconds()), "improve-us/op")
			}
		})
	}
}
