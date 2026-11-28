// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestPlacementTransferCostAndUnknownPaths(t *testing.T) {
	// Client and peer transfers use this exact function, independent of who
	// initiates the transfer. Equal byte-distance must have equal cost.
	if clientCost, peerCost := transferCost(100, 4), transferCost(200, 2); clientCost != peerCost || clientCost != 400 {
		t.Fatal(clientCost, peerCost)
	}
	for _, cost := range []float64{transferCost(math.MaxFloat64, 2), transferCost(1, math.NaN()), transferCost(-1, 1), transferCost(1, math.Inf(1))} {
		if cost != math.MaxFloat64 {
			t.Fatal("invalid or overflowing observation became a cheap path", cost)
		}
	}
	if transferCost(0, 4) != 0 || transferCost(100, 0) != 0 {
		t.Fatal("empty or node-local transfer has network cost")
	}
	a, b, c := client.ID{1}, client.ID{2}, client.ID{3}
	paths := map[[2]client.ID]float64{{a, b}: 4, {b, c}: math.NaN()}
	unknown := placementDistanceFallback(paths)
	if unknown != 4 || placementPeerDistance(paths, a, b, unknown) != 4 || placementPeerDistance(paths, b, a, unknown) != 4 || placementPeerDistance(paths, a, a, unknown) != 0 {
		t.Fatal("missing/directed path was optimistically discounted", unknown, paths)
	}
}

func TestPlacementColocationMigrationBreakEven(t *testing.T) {
	for _, saved := range []float64{399, 400, 401} {
		d := []placementDemand{{Copies: 1}}
		v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 1000}, {Node: client.ID{2}, Capacity: 1000, Free: 1000}}
		o := []placementOption{{Range: 0, Volume: 0, Current: true}, {Range: 0, Volume: 1}}
		x := placementSolution{Assignments: [][]int{{0}}, Order: []int{0}}
		in := improvementInput(d, o)
		in.Weights = &placementScore{Traffic: 1}
		in.Network = []uint64{0, 100}
		in.NetworkDistances = []float64{0, 4}
		in.ClientNetwork = []float64{saved, 0}
		r := improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 27)
		want := 0
		if saved > 400 {
			want = 1
		}
		if x.Assignments[0][0] != want {
			t.Fatal("client benefit and migration transfer did not break even at 100*4", saved, r, x.Assignments)
		}
		if want == 1 && (r.Before.Traffic != saved || r.After.Traffic != 400) {
			t.Fatal("network migration was discounted or counted twice", r)
		}
		assertImprovementValid(t, d, v, o, nil, x)
	}
}

func TestPlacementColocationPrimaryAndReplicationDirection(t *testing.T) {
	a, b := client.ID{1}, client.ID{2}
	for _, saved := range []float64{799, 800, 801} {
		d := []placementDemand{{Copies: 2}}
		v := []placementResource{{Node: a, Capacity: 1 << 40, Free: 1 << 40}, {Node: b, Capacity: 1 << 40, Free: 1 << 40}}
		o := []placementOption{{Range: 0, Volume: 0, Current: true}, {Range: 0, Volume: 1, Current: true}}
		x := placementSolution{Assignments: [][]int{{0, 1}}, Order: []int{0}}
		in := improvementInput(d, o)
		in.Horizon = 1
		in.Weights = &placementScore{Traffic: 1}
		in.Work[0] = placementWorkload{Write: 100, Primary: a, Replicated: true}
		in.ClientNetwork = []float64{saved, 0}
		in.PeerDistances = map[[2]client.ID]float64{{a, b}: 1, {b, a}: 9}
		r := improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 31)
		want := 0
		if saved > 800 { // 100 future write bytes * (9-1) path difference.
			want = 1
		}
		if x.Assignments[0][0] != want {
			t.Fatal("primary role did not weigh client and directed replication equally", saved, r, x.Assignments)
		}
		if want == 1 && math.Abs((r.Before.Traffic-r.After.Traffic)-1) > 1e-6 {
			t.Fatal("paired role swap double-counted replication", r)
		}
		assertImprovementValid(t, d, v, o, nil, x)
	}
}

func TestPlacementColocationCannotBuyConstraints(t *testing.T) {
	for _, constraint := range []string{"capacity", "quota", "node"} {
		d := []placementDemand{{Bucket: client.ID{9}, Copies: 1}}
		v := []placementResource{{Node: client.ID{1}, Capacity: 1000, Free: 1000}, {Node: client.ID{2}, Capacity: 1000, Free: 0}}
		o := []placementOption{{Range: 0, Volume: 0}, {Range: 0, Volume: 1, Allocate: 1}}
		x := placementSolution{Assignments: [][]int{{0}}, Order: []int{0}}
		in := improvementInput(d, o)
		in.Weights = &placementScore{Traffic: 1}
		in.ClientNetwork = []float64{1e12, 0}
		var limits map[client.ID]uint64
		switch constraint {
		case "quota":
			v[1].Free = 1000
			o[1].Quota = 1
			limits = map[client.ID]uint64{{9}: 0}
		case "node":
			d[0].Copies = 2
			v[1].Free = 1000
			v = append(v, placementResource{Node: v[0].Node, Capacity: 1000, Free: 1000})
			o = append(o, placementOption{Range: 0, Volume: 2})
			x.Assignments[0] = []int{1, 0}
			in = improvementInput(d, o)
			in.Weights = &placementScore{Traffic: 1}
			in.ClientNetwork = []float64{1e12, 1e12, 0}
		}
		before := append([]int(nil), x.Assignments[0]...)
		improvePlacement(context.Background(), d, v, o, limits, &x, in, placementImproveBudget, 37)
		assertImprovementValid(t, d, v, o, limits, x)
		if constraint == "node" {
			if x.Assignments[0][0] == 2 {
				// Node 1 may become primary using volume 2 only if the old node-1
				// volume has left the set, retaining two distinct nodes.
				for _, id := range x.Assignments[0][1:] {
					if v[o[id].Volume].Node == v[2].Node {
						t.Fatal("local striping was counted as another node", x.Assignments)
					}
				}
			}
		} else if !reflect.DeepEqual(before, x.Assignments[0]) {
			t.Fatal("client benefit bypassed", constraint, x.Assignments)
		}
	}
}

func TestPlacementColocationLaggingBackgroundUsesAllLocalVolumes(t *testing.T) {
	for _, observed := range []float64{0, 50, 250} {
		node := client.ID{1}
		d := []placementDemand{{Copies: 1}, {Copies: 1}}
		v := []placementResource{{Node: node, Capacity: 2000, Free: 1000}, {Node: node, Capacity: 2000, Free: 1000}}
		o := []placementOption{{Range: 0, Volume: 0, Current: true}, {Range: 0, Volume: 1, Move: 1}, {Range: 1, Volume: 0, Current: true}, {Range: 1, Volume: 1, Move: 1}}
		x := placementSolution{Assignments: [][]int{{0}, {2}}, Order: []int{0, 1}}
		in := improvementInput(d, o)
		in.Horizon = 1
		in.Weights = &placementScore{Move: 1, Traffic: 1}
		in.BackgroundRead = []float64{observed, 0}
		in.Held = []uint64{10, 0, 10, 0}
		in.ClientNetwork = []float64{100, 100, 100, 100} // same node, same path
		for i := range in.Work {
			in.Work[i] = placementWorkload{Physical: 10, Read: 100, Primary: node}
		}
		r := improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 41)
		if o[x.Assignments[0][0]].Volume == o[x.Assignments[1][0]].Volume || r.Accepted == 0 {
			t.Fatal("lagging volume snapshot hid local striping benefit", observed, r, x.Assignments)
		}
		assertImprovementValid(t, d, v, o, nil, x)
	}
}

func TestPlacementColocationRetainsUnboundStriping(t *testing.T) {
	node := client.ID{1}
	d := []placementDemand{{Copies: 1}}
	v := []placementResource{{Node: node, Capacity: 1000, Free: 800}, {Node: node, Capacity: 1000, Free: 800}, {Node: node}}
	o := []placementOption{{Range: 0, Volume: 0, Move: 100}, {Range: 0, Volume: 1, Move: 100}, {Range: 0, Volume: 2, Current: true}}
	x := placementSolution{Assignments: [][]int{{2}}, Order: []int{0}}
	in := improvementInput(d, o)
	in.Horizon = 1
	in.Weights = &placementScore{Move: 1, Traffic: 1}
	in.Work[0] = placementWorkload{Physical: 200, Read: 200, Primary: node}
	in.Held = []uint64{100, 100, 0}
	in.Unbound = []bool{true}
	in.Cohorts = map[int][]placementCohort{2: {{0, 100}, {1, 100}}}
	in.ClientNetwork = []float64{100, 100, 100}
	r := improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 43)
	if x.Assignments[0][0] != 2 || r.Accepted != 0 {
		t.Fatal("healthy existing local striping was consolidated", r, x.Assignments)
	}
}

func colocationTrialFixture() *placementImproveState {
	a, b, c := client.ID{1}, client.ID{2}, client.ID{3}
	d := []placementDemand{{Bucket: client.ID{9}, Copies: 2}}
	v := []placementResource{{Node: a, Capacity: 1000, Free: 100}, {Node: b, Capacity: 1000, Free: 100}, {Node: c, Capacity: 1000, Free: 100}}
	o := []placementOption{{Range: 0, Volume: 0}, {Range: 0, Volume: 1}, {Range: 0, Volume: 2, Allocate: 500}}
	x := &placementSolution{Assignments: [][]int{{0, 1}}}
	in := improvementInput(d, o)
	in.Horizon = 2
	in.Weights = &placementScore{Traffic: 1}
	in.Work[0] = placementWorkload{Physical: 20, Read: 10, Write: 5, Primary: a, Replicated: true}
	in.ClientNetwork = []float64{1000, 1000, 0}
	in.PeerDistances = map[[2]client.ID]float64{{a, b}: 3, {c, b}: 1}
	s := &placementImproveState{d: d, v: v, o: o, x: x, in: in, allocated: make(map[placementAllocation]placementAmount), shared: make([]placementAmount, 3), used: []float64{100, 100, 100}, read: make([]float64, 3), write: make([]float64, 3), growth: make([]float64, 3), quota: make(map[client.ID]placementAmount), roles: make(map[client.ID]float64), nodeCapacity: map[client.ID]float64{a: 1000, b: 1000, c: 1000}, stamps: make([]uint32, 3), peerFallback: 3}
	for slot, id := range x.Assignments[0] {
		s.adjust(0, slot, id, 1)
	}
	for i := range v {
		s.loadScale += s.read[i] + s.write[i]
		s.score.Future += s.volumeScore(i).Future
	}
	for i := range v {
		s.score.Traffic += s.trafficScore(i)
	}
	for node := range s.roles {
		s.score.Traffic += s.roleScore(node)
	}
	traffic, _ := s.replicationTraffic(0, func() bool { return false })
	s.score.Traffic += traffic
	return s
}

func TestPlacementColocationRejectedAndCancelledTrialRestoresState(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		s := colocationTrialFixture()
		beforeScore := s.score
		beforeRead, beforeWrite, beforeUsed := append([]float64(nil), s.read...), append([]float64(nil), s.write...), append([]float64(nil), s.used...)
		stop := func() bool { return cancel && s.x.Assignments[0][0] == 2 }
		if s.trial([]placementEdit{{rangeID: 0, slot: 0, option: 2}}, stop) {
			t.Fatal("accepted an over-capacity or cancelled improvement")
		}
		if s.score != beforeScore || !reflect.DeepEqual(s.x.Assignments[0], []int{0, 1}) || !reflect.DeepEqual(s.read, beforeRead) || !reflect.DeepEqual(s.write, beforeWrite) || !reflect.DeepEqual(s.used, beforeUsed) {
			t.Fatal("failed trial changed the incumbent", cancel, s.score, beforeScore, s.x.Assignments, s.read, s.write, s.used)
		}
		for _, amount := range s.shared {
			if amount != (placementAmount{}) {
				t.Fatal("failed trial leaked staging reservation", amount)
			}
		}
	}
}

func TestPlacementColocationInvalidObservationsKeepIncumbent(t *testing.T) {
	d := []placementDemand{{Copies: 1}}
	v := []placementResource{{Node: client.ID{1}, Capacity: 100, Free: 100}, {Node: client.ID{2}, Capacity: 100, Free: 100}}
	o := []placementOption{{Range: 0, Volume: 0}, {Range: 0, Volume: 1}}
	for _, invalid := range []float64{-1, math.NaN(), math.Inf(1)} {
		x := placementSolution{Assignments: [][]int{{0}}}
		in := improvementInput(d, o)
		in.ClientNetwork = []float64{100, invalid}
		r := improvePlacement(context.Background(), d, v, o, nil, &x, in, time.Second, 1)
		if r.Attempts != 0 || x.Assignments[0][0] != 0 {
			t.Fatal("invalid hint changed placement", invalid, r, x.Assignments)
		}
	}
}

// Single-owner ranges also participate in the production primary-load image.
// Moving one must remove its old role and add its new role exactly once.
func TestPlacementColocationSingleOwnerRoles(t *testing.T) {
	a, b := client.ID{1}, client.ID{2}
	d := []placementDemand{{Copies: 1}, {Copies: 1}}
	v := []placementResource{{Node: a, Capacity: 1 << 40, Free: 1 << 40}, {Node: b, Capacity: 1 << 40, Free: 1 << 40}}
	o := []placementOption{{Range: 0, Volume: 0, Current: true}, {Range: 0, Volume: 1}, {Range: 1, Volume: 0, Current: true}, {Range: 1, Volume: 1}}
	x := placementSolution{Assignments: [][]int{{0}, {2}}, Order: []int{0, 1}}
	in := improvementInput(d, o)
	in.Weights = &placementScore{Traffic: 1}
	in.Work[0].Primary = a
	in.Work[1].Primary = a
	in.PrimaryLoads = map[client.ID]float64{a: 2 * (1 << 20)}
	result := improvePlacement(context.Background(), d, v, o, nil, &x, in, placementImproveBudget, 29)
	first, second := v[o[x.Assignments[0][0]].Volume].Node, v[o[x.Assignments[1][0]].Volume].Node
	if first == second || result.After.Traffic >= result.Before.Traffic {
		t.Fatal("single-owner role concentration stayed fixed", x, result)
	}
	if math.Abs(result.Before.Traffic-4) > 1e-9 || math.Abs(result.After.Traffic-2) > 1e-9 {
		t.Fatal("primary role counted twice or not removed", result)
	}
}
