// SPDX-License-Identifier: GPL-3.0-only
package server

// An indexed heap uses two contiguous integer arrays, at most 2*n words.
// Children are 2*i+1 and 2*i+2; an affected key is repaired in O(log n).
// The same implementation serves the range queue and capacity event queues.
type placementQueue struct {
	items, positions []int
	less             func(int, int) bool
}

func newPlacementQueue(n int, less func(int, int) bool) *placementQueue {
	q := &placementQueue{items: make([]int, n), positions: make([]int, n), less: less}
	for i := range q.items {
		q.items[i], q.positions[i] = i, i
	}
	for i := n/2 - 1; i >= 0; i-- {
		q.down(i)
	}
	return q
}
func (q *placementQueue) swap(a, b int) {
	q.items[a], q.items[b] = q.items[b], q.items[a]
	q.positions[q.items[a]], q.positions[q.items[b]] = a, b
}
func (q *placementQueue) down(i int) int {
	for {
		child := 2*i + 1
		if child >= len(q.items) {
			return i
		}
		if child+1 < len(q.items) && q.less(q.items[child+1], q.items[child]) {
			child++
		}
		if !q.less(q.items[child], q.items[i]) {
			return i
		}
		q.swap(i, child)
		i = child
	}
}
func (q *placementQueue) fix(id int) {
	i := q.positions[id]
	if i < 0 {
		return
	}
	for i > 0 {
		parent := (i - 1) / 2
		if !q.less(q.items[i], q.items[parent]) {
			break
		}
		q.swap(i, parent)
		i = parent
	}
	q.down(i)
}
func (q *placementQueue) pop() int {
	id := q.items[0]
	q.swap(0, len(q.items)-1)
	q.items = q.items[:len(q.items)-1]
	q.positions[id] = -1
	if len(q.items) > 0 {
		q.down(0)
	}
	return id
}

// Counts and byte sums over allocation-cost ranks. This lets the greedy
// choice check that its remaining replica slots still fit the bucket budget,
// without sorting or rescanning all remaining alternatives after each choice.
type placementCosts struct {
	counts []int
	sums   []uint64
}

func (f *placementCosts) change(rank, count int, cost uint64) {
	for i := rank + 1; i < len(f.counts); i += i & -i {
		f.counts[i] += count
		if count > 0 {
			f.sums[i] += cost
		} else {
			f.sums[i] -= cost
		}
	}
}
func (f *placementCosts) cheapest(count int) (uint64, bool) {
	if count == 0 {
		return 0, true
	}
	index, found, sum := 0, 0, uint64(0)
	bit := 1
	for bit < len(f.counts) {
		bit <<= 1
	}
	for bit >>= 1; bit > 0; bit >>= 1 {
		next := index + bit
		if next < len(f.counts) && found+f.counts[next] < count {
			index = next
			found += f.counts[next]
			sum += f.sums[next]
		}
	}
	if index+1 >= len(f.counts) {
		return 0, false
	}
	// At a rank there is exactly one node, so the final increment costs one
	// rank's value. Prefix sums recover that value even after sparse removals.
	prefix := func(at int) uint64 {
		var s uint64
		for at > 0 {
			s += f.sums[at]
			at -= at & -at
		}
		return s
	}
	return sum + prefix(index+1) - prefix(index), true
}
