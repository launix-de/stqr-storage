// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"math"
	"sort"

	"github.com/launix-de/stqr-storage/client"
)

// Phase one supplies range requirements, not destinations. Eligibility is
// compiled once from the PG's class/mask/domain constraints. The executor must
// never select a destination outside these explicit options.
type placementDemand struct {
	Bucket client.ID
	Copies int
	Bytes  uint64
}
type placementResource struct {
	Node           client.ID
	Capacity, Free uint64
	Reserved       map[client.ID]uint64 // protected, unused backing of each bucket
}
type placementOption struct {
	Range, Volume  int
	Allocate, Move uint64 // staying costs zero payload movement
	Quota          uint64 // final additional bucket charge; replacement staging is not growth
	Existing       bool   // prefer retaining a copy when payload movement ties
}
type placementSolution struct {
	Assignments                [][]int // option indices, one distinct node per required copy
	Order, Pending             []int
	Invalidations, Comparisons uint64
}
type placementWatch struct {
	bucket   client.ID
	options  []int // immutable ascending allocation thresholds
	cursor   int
	reserved uint64
}
type placementVolumeState struct {
	free    uint64
	watches []placementWatch
	queue   *placementQueue
}

// The iterative solver never rescans all ranges after assigning a location.
// A volume has a heap of bucket-specific invalidation thresholds. Only crossed
// thresholds notify the indexed range heap. Each option is invalidated at most
// once. With R demands and E explicit eligible options, memory is O(R+E+V), and
// scheduling is O((R+E) log(R+E)); there is no R-by-R rescoring loop.
func solvePlacement(demands []placementDemand, resources []placementResource, options []placementOption, budgets map[client.ID]uint64) placementSolution {
	result := placementSolution{Assignments: make([][]int, len(demands))}
	byRange := make([][]int, len(demands))
	nodes := make([]map[client.ID]int, len(demands))
	available := make([]int, len(demands))
	alive := make([]bool, len(options))
	done := make([]bool, len(demands))
	volumes := make([]placementVolumeState, len(resources))
	groupIDs := make([]map[client.ID]int, len(resources))
	quotaOptions := make(map[client.ID][]int)
	quotaCursor := make(map[client.ID]int)
	quota := make(map[client.ID]uint64)
	for bucket, budget := range budgets {
		quota[bucket] = budget
	}
	budgetOf := func(bucket client.ID) uint64 {
		if n, ok := quota[bucket]; ok {
			return n
		}
		return math.MaxUint64
	}
	for i, v := range resources {
		volumes[i].free = v.Free
		groupIDs[i] = make(map[client.ID]int)
	}
	for i, o := range options {
		if o.Range < 0 || o.Range >= len(demands) || o.Volume < 0 || o.Volume >= len(resources) {
			continue
		}
		d := demands[o.Range]
		v := resources[o.Volume]
		if o.Allocate > addCapacity(v.Free, v.Reserved[d.Bucket]) || o.Quota > budgetOf(d.Bucket) {
			continue
		}
		alive[i] = true
		byRange[o.Range] = append(byRange[o.Range], i)
		if nodes[o.Range] == nil {
			nodes[o.Range] = make(map[client.ID]int)
		}
		if nodes[o.Range][v.Node] == 0 {
			available[o.Range]++
		}
		nodes[o.Range][v.Node]++
		group, ok := groupIDs[o.Volume][d.Bucket]
		if !ok {
			group = len(volumes[o.Volume].watches)
			groupIDs[o.Volume][d.Bucket] = group
			volumes[o.Volume].watches = append(volumes[o.Volume].watches, placementWatch{bucket: d.Bucket, reserved: v.Reserved[d.Bucket]})
		}
		volumes[o.Volume].watches[group].options = append(volumes[o.Volume].watches[group].options, i)
		if _, limited := budgets[d.Bucket]; limited {
			quotaOptions[d.Bucket] = append(quotaOptions[d.Bucket], i)
		}
	}
	sortOptions := func(ids []int) {
		sort.Slice(ids, func(i, j int) bool { return options[ids[i]].Allocate < options[ids[j]].Allocate })
	}
	for bucket, ids := range quotaOptions {
		sort.Slice(ids, func(i, j int) bool { return options[ids[i]].Quota < options[ids[j]].Quota })
		quotaCursor[bucket] = len(ids) - 1
	}
	ranges := newPlacementQueue(len(demands), func(a, b int) bool {
		result.Comparisons++
		x, y := available[a]-demands[a].Copies, available[b]-demands[b].Copies
		if x != y {
			return x < y
		}
		if demands[a].Bytes != demands[b].Bytes {
			return demands[a].Bytes > demands[b].Bytes
		}
		return a < b
	})
	invalidate := func(id int) {
		if !alive[id] {
			return
		}
		alive[id] = false
		result.Invalidations++
		o := options[id]
		if done[o.Range] {
			return
		}
		node := resources[o.Volume].Node
		nodes[o.Range][node]--
		if nodes[o.Range][node] == 0 {
			available[o.Range]--
			ranges.fix(o.Range)
		}
	}
	trigger := func(v, g int) uint64 {
		w := &volumes[v].watches[g]
		if w.cursor < 0 {
			return 0
		}
		return subtractCapacity(options[w.options[w.cursor]].Allocate, w.reserved)
	}
	for v := range volumes {
		for g := range volumes[v].watches {
			w := &volumes[v].watches[g]
			sortOptions(w.options)
			w.cursor = len(w.options) - 1
		}
		volumes[v].queue = newPlacementQueue(len(volumes[v].watches), func(a, b int) bool { return trigger(v, a) > trigger(v, b) })
	}
	consume := func(id int) {
		o := options[id]
		bucket := demands[o.Range].Bucket
		v := &volumes[o.Volume]
		g := groupIDs[o.Volume][bucket]
		w := &v.watches[g]
		backing := min(w.reserved, o.Allocate)
		w.reserved -= backing
		v.free -= o.Allocate - backing
		v.queue.fix(g)
		for len(v.queue.items) > 0 {
			g := v.queue.items[0]
			w := &v.watches[g]
			if trigger(o.Volume, g) <= v.free {
				break
			}
			limit := addCapacity(v.free, w.reserved)
			for w.cursor >= 0 && options[w.options[w.cursor]].Allocate > limit {
				invalidate(w.options[w.cursor])
				w.cursor--
			}
			v.queue.fix(g)
		}
		if remaining, limited := quota[bucket]; limited {
			remaining -= o.Quota
			quota[bucket] = remaining
			ids := quotaOptions[bucket]
			cursor := quotaCursor[bucket]
			for cursor >= 0 && options[ids[cursor]].Quota > remaining {
				invalidate(ids[cursor])
				cursor--
			}
			quotaCursor[bucket] = cursor
		}
	}
	for len(ranges.items) > 0 {
		id := ranges.pop()
		done[id] = true
		demand := demands[id]
		if demand.Copies < 1 || available[id] < demand.Copies {
			result.Pending = append(result.Pending, id)
			continue
		}
		var choices []int
		minimum := make(map[client.ID]uint64)
		for _, edge := range byRange[id] {
			if alive[edge] {
				choices = append(choices, edge)
				o := options[edge]
				node := resources[o.Volume].Node
				if n, ok := minimum[node]; !ok || o.Quota < n {
					minimum[node] = o.Quota
				}
			}
		}
		type nodeCost struct {
			node client.ID
			cost uint64
		}
		costs := make([]nodeCost, 0, len(minimum))
		var total uint64
		overflow := false
		for node, cost := range minimum {
			if cost > math.MaxUint64-total {
				overflow = true
			}
			total += cost
			costs = append(costs, nodeCost{node, cost})
		}
		if overflow {
			result.Pending = append(result.Pending, id)
			continue
		}
		sort.Slice(costs, func(i, j int) bool {
			if costs[i].cost != costs[j].cost {
				return costs[i].cost < costs[j].cost
			}
			return bytes.Compare(costs[i].node[:], costs[j].node[:]) < 0
		})
		ranks := make(map[client.ID]int, len(costs))
		f := placementCosts{make([]int, len(costs)+1), make([]uint64, len(costs)+1)}
		for rank, cost := range costs {
			ranks[cost.node] = rank
			f.change(rank, 1, cost.cost)
		}
		least, ok := f.cheapest(demand.Copies)
		if !ok || least > budgetOf(demand.Bucket) {
			result.Pending = append(result.Pending, id)
			continue
		}
		sort.Slice(choices, func(i, j int) bool {
			a, b := options[choices[i]], options[choices[j]]
			if a.Move != b.Move {
				return a.Move < b.Move
			}
			if a.Existing != b.Existing {
				return a.Existing
			}
			if a.Allocate != b.Allocate {
				return a.Allocate < b.Allocate
			}
			x, y := resources[a.Volume], resources[b.Volume]
			// Among equally cheap choices prefer relative usable headroom.
			xFree, yFree := volumes[a.Volume].free, volumes[b.Volume].free
			xScore, yScore := float64(xFree)/float64(max(uint64(1), x.Capacity)), float64(yFree)/float64(max(uint64(1), y.Capacity))
			if xScore != yScore {
				return xScore > yScore
			}
			if x.Node != y.Node {
				return bytes.Compare(x.Node[:], y.Node[:]) < 0
			}
			return a.Volume < b.Volume
		})
		used := make(map[client.ID]bool, demand.Copies)
		var selected []int
		remaining := budgetOf(demand.Bucket)
		for _, edge := range choices {
			o := options[edge]
			node := resources[o.Volume].Node
			if used[node] || o.Quota > remaining {
				continue
			}
			rank := ranks[node]
			f.change(rank, -1, minimum[node])
			needed, ok := f.cheapest(demand.Copies - len(selected) - 1)
			if !ok || needed > remaining-o.Quota {
				f.change(rank, 1, minimum[node])
				continue
			}
			selected = append(selected, edge)
			used[node] = true
			remaining -= o.Quota
			if len(selected) == demand.Copies {
				break
			}
		}
		if len(selected) != demand.Copies {
			result.Pending = append(result.Pending, id)
			continue
		}
		result.Assignments[id] = selected
		result.Order = append(result.Order, id)
		for _, edge := range selected {
			consume(edge)
		}
	}
	return result
}
