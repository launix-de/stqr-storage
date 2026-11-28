// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"math"
	"math/bits"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

const placementImproveBudget = 100 * time.Millisecond

// Costs are byte-equivalents over one horizon. They are preferences only: no
// coefficient can buy permission to violate eligibility, copies or admission.
type placementScore struct{ Move, Future, Traffic float64 }

func (s placementScore) total() float64 { return s.Move + s.Future + s.Traffic }

type placementWorkload struct {
	Physical, Read, Write, Growth float64 // physical bytes; bytes/second
	Primary                       client.ID
	Replicated                    bool // whether CURRENT already has a replica set
}
type placementImproveInput struct {
	Work                                              []placementWorkload
	BackgroundRead, BackgroundWrite, BackgroundGrowth []float64
	Network                                           []uint64                 // copied bytes crossing a node boundary, per option
	NetworkDistances                                  []float64                // optional migration path distances, per option
	ClientNetwork                                     []float64                // optional forecast client byte-distance cost at primary, per option
	PeerDistances                                     map[[2]client.ID]float64 // optional directed replication path observations
	Held                                              []uint64                 // bytes already present, per option
	Horizon                                           float64
	PrimaryLoads                                      map[client.ID]float64
	Weights                                           *placementScore
	now                                               func() time.Time // deadline tests; production uses time.Now
	Cohorts                                           map[int][]placementCohort
	Unbound                                           []bool
}
type placementCohort struct {
	volume int
	bytes  uint64
}
type placementImproveResult struct {
	Before, After      placementScore
	Attempts, Accepted uint64
	Elapsed            time.Duration
	Stopped            bool
}
type placementAllocation struct {
	volume int
	bucket client.ID
}

// Exact counters retain overflow while considering an invalid candidate. Soft
// scores may use floats; hard byte limits must work above 2^53 and at MaxUint64.
type placementAmount struct{ hi, lo uint64 }

func (a placementAmount) add(b placementAmount) placementAmount {
	lo, c := bits.Add64(a.lo, b.lo, 0)
	hi, _ := bits.Add64(a.hi, b.hi, c)
	return placementAmount{hi, lo}
}
func (a placementAmount) sub(b placementAmount) placementAmount {
	lo, c := bits.Sub64(a.lo, b.lo, 0)
	hi, _ := bits.Sub64(a.hi, b.hi, c)
	return placementAmount{hi, lo}
}
func (a placementAmount) over(n uint64) bool { return a.hi != 0 || a.lo > n }
func (a placementAmount) unreserved(n uint64) placementAmount {
	if !a.over(n) {
		return placementAmount{}
	}
	return a.sub(placementAmount{lo: n})
}

type placementEdit struct{ rangeID, slot, option int }
type placementImproveState struct {
	d                         []placementDemand
	v                         []placementResource
	o                         []placementOption
	x                         *placementSolution
	in                        placementImproveInput
	allocated                 map[placementAllocation]placementAmount
	shared                    []placementAmount
	used, read, write, growth []float64
	quota                     map[client.ID]placementAmount
	budgets                   map[client.ID]uint64
	loadScale                 float64
	peerFallback              float64
	roles, nodeCapacity       map[client.ID]float64
	score                     placementScore
	touched                   []int
	stamps                    []uint32
	stamp                     uint32
}

// A single valid incumbent, not an exponentially growing frontier. Every trial
// changes at most two locations/roles and touches only their resource counters.
// Hypothetical moves cause no I/O. Admission does not credit retired copies:
// copy-before-retire must fit even if the final assignment would free space.
func improvePlacement(ctx context.Context, d []placementDemand, v []placementResource, o []placementOption, budgets map[client.ID]uint64, x *placementSolution, in placementImproveInput, budget time.Duration, seed uint64) placementImproveResult {
	now := in.now
	if now == nil {
		now = time.Now
	}
	started := now()
	deadline := started.Add(min(max(time.Duration(0), budget), placementImproveBudget))
	result := placementImproveResult{}
	stop := func() bool { return ctx.Err() != nil || !now().Before(deadline) }
	finish := func() placementImproveResult {
		result.Elapsed = now().Sub(started)
		result.Stopped = stop()
		return result
	}
	if stop() || len(d) == 0 || len(in.Work) != len(d) || len(x.Pending) != 0 || len(x.Assignments) != len(d) {
		return finish()
	}
	if math.IsNaN(in.Horizon) || math.IsInf(in.Horizon, 0) {
		return finish()
	}
	if in.Horizon <= 0 {
		in.Horizon = 30
	}
	if len(in.Held) != len(o) || len(in.Network) != len(o) ||
		len(in.NetworkDistances) != 0 && len(in.NetworkDistances) != len(o) ||
		len(in.ClientNetwork) != 0 && len(in.ClientNetwork) != len(o) {
		return finish()
	}
	// Bound auxiliary workspace independently of the input graph. There is no
	// population/frontier retaining a growing history of rejected states.
	if uint64(len(d))*128+uint64(len(o))*96+uint64(len(v))*256+uint64(len(in.PrimaryLoads))*64+uint64(len(in.PeerDistances))*64 > 64<<20 {
		return finish()
	}
	s := placementImproveState{d: d, v: v, o: o, x: x, in: in, budgets: budgets, allocated: make(map[placementAllocation]placementAmount), quota: make(map[client.ID]placementAmount), shared: make([]placementAmount, len(v)), used: make([]float64, len(v)), read: make([]float64, len(v)), write: make([]float64, len(v)), growth: make([]float64, len(v))}
	s.roles, s.nodeCapacity = make(map[client.ID]float64), make(map[client.ID]float64)
	s.stamps = make([]uint32, len(v))
	s.peerFallback = 1
	index := 0
	for _, distance := range in.PeerDistances {
		if index&63 == 0 && stop() {
			return finish()
		}
		index++
		if validTransferValue(distance) {
			s.peerFallback = math.Max(s.peerFallback, distance)
		}
	}
	for option, cohort := range in.Cohorts {
		if stop() || option < 0 || option >= len(o) || o[option].Volume < 0 || o[option].Volume >= len(v) {
			return finish()
		}
		for _, load := range cohort {
			if stop() || load.volume < 0 || load.volume >= len(v) || v[load.volume].Node != v[o[option].Volume].Node {
				return finish()
			}
		}
	}
	index = 0
	for node, load := range in.PrimaryLoads {
		if index&63 == 0 && stop() {
			return finish()
		}
		index++
		if !validTransferValue(load) {
			return finish()
		}
		s.roles[node] = load
	}
	byRange := make([][]int, len(d))
	for i, option := range o {
		if i&63 == 0 && stop() {
			return finish()
		}
		if option.Range < 0 || option.Range >= len(d) || option.Volume < 0 || option.Volume >= len(v) {
			return finish()
		}
		if len(in.NetworkDistances) != 0 && !validTransferValue(in.NetworkDistances[i]) ||
			len(in.ClientNetwork) != 0 && !validTransferValue(in.ClientNetwork[i]) {
			return finish()
		}
		byRange[option.Range] = append(byRange[option.Range], i)
	}
	var movable []int
	for r := range d {
		if r&63 == 0 && stop() {
			return finish()
		}
		w := in.Work[r]
		if !validTransferValue(w.Physical) || !validTransferValue(w.Read) || !validTransferValue(w.Write) || !validTransferValue(w.Growth) {
			return finish()
		}
		if len(byRange[r]) > d[r].Copies || d[r].Copies > 1 {
			movable = append(movable, r)
		}
	}
	if len(movable) == 0 {
		return finish()
	}
	for i, resource := range v {
		if i&63 == 0 && stop() {
			return finish()
		}
		s.used[i] = float64(subtractCapacity(resource.Capacity, resource.Free))
		s.nodeCapacity[resource.Node] += float64(resource.Capacity)
		if i < len(in.BackgroundRead) {
			if !validTransferValue(in.BackgroundRead[i]) {
				return finish()
			}
			s.read[i] = in.BackgroundRead[i] * in.Horizon
		}
		if i < len(in.BackgroundWrite) {
			if !validTransferValue(in.BackgroundWrite[i]) {
				return finish()
			}
			s.write[i] = in.BackgroundWrite[i] * in.Horizon
		}
		if i < len(in.BackgroundGrowth) {
			if !validTransferValue(in.BackgroundGrowth[i]) {
				return finish()
			}
			s.growth[i] = in.BackgroundGrowth[i] * in.Horizon
		}
	}
	// Volume snapshots include CURRENT workload, but may lag range counters or
	// be absent during bootstrap. Remove only that known CURRENT work from a
	// nonnegative fixed background before adding the candidate assignment. Doing
	// this after candidate addition would let negative phantom background cancel
	// the candidate's work and bias moves back toward a stale source.
	for i, option := range o {
		if i&63 == 0 && stop() {
			return finish()
		}
		s.used[option.Volume] -= float64(in.Held[i])
		if in.Held[i] > 0 {
			w := in.Work[option.Range]
			fraction := 1.0
			if option.Range < len(in.Unbound) && in.Unbound[option.Range] {
				fraction = float64(in.Held[i]) / math.Max(1, w.Physical)
			}
			s.write[option.Volume] = math.Max(0, s.write[option.Volume]-fraction*w.Write*in.Horizon)
			if v[option.Volume].Node == w.Primary {
				s.read[option.Volume] = math.Max(0, s.read[option.Volume]-fraction*w.Read*in.Horizon)
			}
		}
	}
	// This initialization is advisory and bounded too. No assignment is changed
	// until every initial hard constraint has been checked.
	for r, ids := range x.Assignments {
		if r&63 == 0 && stop() {
			return finish()
		}
		if r >= len(d) || len(ids) != d[r].Copies {
			return finish()
		}
		seen := make(map[client.ID]bool, len(ids))
		if in.PrimaryLoads != nil && in.Work[r].Primary != (client.ID{}) {
			s.roles[in.Work[r].Primary] -= 1 << 20
		}
		for slot, id := range ids {
			if slot&63 == 0 && stop() {
				return finish()
			}
			if id < 0 || id >= len(o) || o[id].Range != r || seen[v[o[id].Volume].Node] {
				return finish()
			}
			seen[v[o[id].Volume].Node] = true
			s.adjust(r, slot, id, 1)
		}
		traffic, complete := s.replicationTraffic(r, stop)
		if !complete {
			return finish()
		}
		s.score.Traffic += traffic
	}
	for i := range v {
		if i&63 == 0 && stop() {
			return finish()
		}
		if !s.validVolume(i) {
			return finish()
		}
		part := s.volumeScore(i)
		s.score.Future += part.Future
		// The scale is invariant under moves/role swaps; include fixed work.
		s.loadScale += math.Max(0, s.read[i]) + math.Max(0, s.write[i])
	}
	index = 0
	for bucket, n := range s.quota {
		if index&63 == 0 && stop() {
			return finish()
		}
		index++
		if limit, limited := budgets[bucket]; limited && n.over(limit) {
			return finish()
		}
	}
	for i := range v {
		if i&63 == 0 && stop() {
			return finish()
		}
		s.score.Traffic += s.trafficScore(i)
	}
	index = 0
	for node := range s.roles {
		if index&63 == 0 && stop() {
			return finish()
		}
		index++
		s.score.Traffic += s.roleScore(node)
	}
	result.Before, result.After = s.score, s.score
	// A single unassigned location needs only one pass over its eligible
	// options. Use the same trial, hard checks and objective as all rebuilds;
	// avoid randomized repeated trials on the object-creation hot path.
	if len(d) == 1 && d[0].Copies == 1 {
		for _, candidate := range byRange[0] {
			if stop() {
				break
			}
			result.Attempts++
			if s.trial([]placementEdit{{0, 0, candidate}}, stop) {
				result.Accepted++
				result.After = s.score
			}
		}
		return finish()
	}
	// Deterministic given seed, with a changing per-PG seed across cycles. Random
	// trials avoid rescoring all R ranges after every accepted transformation.
	if seed == 0 {
		seed = 1
	}
	next := func(n int) int {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return int(seed % uint64(n))
	}
	for result.Attempts < min(uint64(65536), uint64(len(o))*32+256) && !stop() {
		r := movable[next(len(movable))]
		ids := x.Assignments[r]
		if len(ids) == 0 || len(byRange[r]) == 0 {
			continue
		}
		slot := next(len(ids))
		candidate := byRange[r][next(len(byRange[r]))]
		var edits [2]placementEdit
		edits[0] = placementEdit{r, slot, candidate}
		editCount := 1
		if result.Attempts%4 == 0 && len(ids) > 1 {
			other := next(len(ids))
			edits[0], edits[1] = placementEdit{r, slot, ids[other]}, placementEdit{r, other, ids[slot]}
			editCount = 2
		} else if result.Attempts%4 == 1 && len(movable) > 1 {
			other := movable[next(len(movable))]
			if other != r && len(x.Assignments[other]) > 0 && len(byRange[other]) > 0 {
				edits[1] = placementEdit{other, next(len(x.Assignments[other])), byRange[other][next(len(byRange[other]))]}
				editCount = 2
			}
		}
		result.Attempts++
		if s.trial(edits[:editCount], stop) {
			result.Accepted++
			result.After = s.score
		}
	}
	return finish()
}

func (s *placementImproveState) adjust(r, slot, id int, sign float64) {
	o := s.o[id]
	bucket := s.d[r].Bucket
	k := placementAllocation{o.Volume, bucket}
	old := s.allocated[k]
	reserve := s.v[o.Volume].Reserved[bucket]
	allocate, charge := placementAmount{lo: o.Allocate}, placementAmount{lo: o.Quota}
	if sign > 0 {
		s.allocated[k] = old.add(allocate)
		s.quota[bucket] = s.quota[bucket].add(charge)
	} else {
		s.allocated[k] = old.sub(allocate)
		s.quota[bucket] = s.quota[bucket].sub(charge)
	}
	s.shared[o.Volume] = s.shared[o.Volume].sub(old.unreserved(reserve)).add(s.allocated[k].unreserved(reserve))
	w := s.in.Work[r]
	apply := func(v int, size, fraction float64) {
		s.used[v] += sign * size
		s.write[v] += sign * w.Write * s.in.Horizon * fraction
		s.growth[v] += sign * w.Growth * s.in.Horizon * fraction
		if slot == 0 {
			s.read[v] += sign * w.Read * s.in.Horizon * fraction
		}
	}
	if cohort, ok := s.in.Cohorts[id]; ok {
		for _, load := range cohort {
			apply(load.volume, float64(load.bytes), float64(load.bytes)/math.Max(1, w.Physical))
		}
	} else {
		apply(o.Volume, w.Physical, 1)
	}
	if slot == 0 && s.in.PrimaryLoads != nil {
		s.roles[s.v[o.Volume].Node] += sign * (1 << 20)
	}
	s.score.Move += sign * float64(o.Move)
	// Copy/storage work remains in Move. Every network byte-distance, whether
	// one-time migration or future client traffic, enters the same Traffic term.
	distance := 1.0
	if len(s.in.NetworkDistances) != 0 {
		distance = s.in.NetworkDistances[id]
	}
	s.score.Traffic += sign * transferCost(float64(s.in.Network[id]), distance)
	if slot == 0 && len(s.in.ClientNetwork) != 0 {
		s.score.Traffic += sign * s.in.ClientNetwork[id]
	}
}

// Replication is an assignment-wide cost: changing the primary changes all
// directed primary-to-follower paths. Trial callers recompute only their at
// most two affected ranges, including a paired role swap exactly once.
func (s *placementImproveState) replicationTraffic(r int, stop func() bool) (float64, bool) {
	ids := s.x.Assignments[r]
	if s.in.PeerDistances == nil || len(ids) < 2 {
		return 0, true
	}
	primary := s.v[s.o[ids[0]].Volume].Node
	bytes := s.in.Work[r].Write * s.in.Horizon
	var cost float64
	for slot, id := range ids[1:] {
		if slot&63 == 0 && stop() {
			return cost, false
		}
		peer := s.v[s.o[id].Volume].Node
		cost += transferCost(bytes, placementPeerDistance(s.in.PeerDistances, primary, peer, s.peerFallback))
	}
	return cost, true
}

func (s *placementImproveState) validVolume(id int) bool {
	return !s.shared[id].over(s.v[id].Free)
}
func (s *placementImproveState) volumeScore(id int) placementScore {
	capacity := float64(max(uint64(1), s.v[id].Capacity))
	used := math.Max(0, s.used[id])
	// Squared utilization rewards balanced headroom; forecast deficit estimates
	// bytes that would otherwise have to move again during the horizon.
	return placementScore{Future: used*used/capacity + 4*math.Max(0, used+s.growth[id]-.85*capacity)}
}
func (s *placementImproveState) trafficScore(id int) float64 {
	read, write := math.Max(0, s.read[id]), math.Max(0, s.write[id])
	// Reads and writes contend for the same volume. Keeping independent
	// squares would make a busy reader invisible to initial write placement.
	pressure := read + write
	return pressure * pressure / math.Max(1, s.loadScale)
}
func (s *placementImproveState) roleScore(node client.ID) float64 {
	if s.nodeCapacity[node] == 0 {
		return 0
	} // fixed nodes outside this PG's option graph
	return s.roles[node] * s.roles[node] / math.Max(1, s.nodeCapacity[node])
}
func (s *placementImproveState) trial(edits []placementEdit, stop func() bool) bool {
	if len(edits) == 2 && edits[0].rangeID == edits[1].rangeID && edits[0].slot == edits[1].slot {
		return false
	}
	var old [2]int
	var nodes [4]client.ID
	nodeCount := 0
	s.stamp++
	s.touched = s.touched[:0]
	addVolume := func(id int) {
		if s.stamps[id] == s.stamp {
			return
		}
		s.stamps[id] = s.stamp
		s.touched = append(s.touched, id)
	}
	addNode := func(id client.ID) {
		for i := 0; i < nodeCount; i++ {
			if nodes[i] == id {
				return
			}
		}
		nodes[nodeCount] = id
		nodeCount++
	}
	for i, e := range edits {
		old[i] = s.x.Assignments[e.rangeID][e.slot]
		if len(s.in.Cohorts[old[i]]) > 64 || len(s.in.Cohorts[e.option]) > 64 {
			return false
		}
		addVolume(s.o[old[i]].Volume)
		addVolume(s.o[e.option].Volume)
		for _, load := range s.in.Cohorts[old[i]] {
			addVolume(load.volume)
		}
		for _, load := range s.in.Cohorts[e.option] {
			addVolume(load.volume)
		}
		addNode(s.v[s.o[old[i]].Volume].Node)
		addNode(s.v[s.o[e.option].Volume].Node)
	}
	// Keep an individual atomic trial small even for an unpinned bucket spread
	// over hundreds of disks. Other bounded neighbours can still be explored.
	if len(s.touched) > 64 || stop() {
		return false
	}
	var ranges [2]int
	var previousTraffic [2]float64
	rangeCount := 0
	for _, e := range edits {
		if rangeCount != 0 && ranges[0] == e.rangeID {
			continue
		}
		ranges[rangeCount] = e.rangeID
		traffic, complete := s.replicationTraffic(e.rangeID, stop)
		if !complete {
			return false
		}
		previousTraffic[rangeCount] = traffic
		rangeCount++
	}
	before := s.score
	for _, traffic := range previousTraffic[:rangeCount] {
		s.score.Traffic -= traffic
	}
	for _, v := range s.touched {
		s.score.Future -= s.volumeScore(v).Future
		s.score.Traffic -= s.trafficScore(v)
	}
	for _, node := range nodes[:nodeCount] {
		s.score.Traffic -= s.roleScore(node)
	}
	for i, e := range edits {
		s.adjust(e.rangeID, e.slot, old[i], -1)
		s.adjust(e.rangeID, e.slot, e.option, 1)
		s.x.Assignments[e.rangeID][e.slot] = e.option
	}
	valid := true
	for _, r := range ranges[:rangeCount] {
		traffic, complete := s.replicationTraffic(r, stop)
		if !complete {
			valid = false
		}
		s.score.Traffic += traffic
	}
	for _, e := range edits {
		placed := s.v[s.o[s.x.Assignments[e.rangeID][e.slot]].Volume].Node
		for index, id := range s.x.Assignments[e.rangeID] {
			if index&63 == 0 && stop() {
				valid = false
				break
			}
			node := s.v[s.o[id].Volume].Node
			if index != e.slot && node == placed {
				valid = false
			}
		}
		bucket := s.d[e.rangeID].Bucket
		if limit, limited := s.budgets[bucket]; limited && s.quota[bucket].over(limit) {
			valid = false
		}
	}
	for _, v := range s.touched {
		if !s.validVolume(v) {
			valid = false
		}
		s.score.Future += s.volumeScore(v).Future
		s.score.Traffic += s.trafficScore(v)
	}
	for _, node := range nodes[:nodeCount] {
		s.score.Traffic += s.roleScore(node)
	}
	if valid && !stop() && s.cost(s.score) < s.cost(before)-math.Max(1e-6, math.Abs(s.cost(before))*1e-12) {
		return true
	}
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		s.adjust(e.rangeID, e.slot, e.option, -1)
		s.adjust(e.rangeID, e.slot, old[i], 1)
		s.x.Assignments[e.rangeID][e.slot] = old[i]
	}
	s.score = before
	return false
}

func (s *placementImproveState) cost(score placementScore) float64 {
	if s.in.Weights == nil {
		return score.total()
	}
	w := s.in.Weights
	return w.Move*score.Move + w.Future*score.Future + w.Traffic*score.Traffic
}
