// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Shapes contain requirements and bounds only. Current ownership is a separate
// input to phase two, never a destination chosen while enumerating the map.
type placementShape struct {
	bucket client.ID
	start  client.ObjectID
	bits   uint16
	copies int
}
type placementTarget struct {
	shape       placementShape
	description BucketDescription
	locations   []client.Location
}
type placementTargetMap struct {
	membership  uint64
	targets     []placementTarget
	cursor      int
	pending     int
	improvement placementImproveResult
}

func (r *rangeManager) placementShapes(group string, descriptions []BucketDescription, image *placementImage) ([]placementShape, []client.RangeEntry, map[client.ID]BucketDescription) {
	byBucket := make(map[client.ID]BucketDescription, len(descriptions))
	for _, d := range descriptions {
		if (group == "*" || group == d.Policy.PlacementGroup) && d.ID != systemBucket.String() && d.State != BucketDeleting && !d.DeletionComplete && d.Policy.StorageStrategy != "ram" {
			byBucket[client.ID(newVolumeID(d.ID))] = d
		}
	}
	var shapes []placementShape
	var current []client.RangeEntry
	// Visit only this PG's indexed bucket spans, not the global map per worker.
	buckets := make([]client.ID, 0, len(byBucket))
	for bucket := range byBucket {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return bytes.Compare(buckets[i][:], buckets[j][:]) < 0 })
	for _, bucket := range buckets {
		span := image.bounds[bucket]
		for i := span[0]; i < span[1]; i++ {
			_, entry := image.entry(i)
			d := byBucket[bucket]
			if entry.State != 0 || entry.Runtime != (client.ID{}) || len(entry.Locations) == 0 || entry.Locations[0].Node != r.node {
				continue
			}
			goal := d.Policy
			if d.DesiredPolicy != nil {
				goal = *d.DesiredPolicy
			}
			if goal.StorageStrategy == "ram" || goal.Redundancy.Mode == "erasure" {
				continue
			}
			copies := r.replicaRemovalCount(goal)
			if copies < 1 {
				continue
			}
			// Temporary offline copies remain untouched. Explicit exclusion first
			// commits a smaller surviving set; that set can then be repaired here.
			if entry.Locations[0].Volume != (client.ID{}) && !r.replicaSetOnline(entry) {
				continue
			}
			shapes = append(shapes, placementShape{bucket, entry.Start, entry.PrefixBits, copies})
			current = append(current, entry)
		}
	}
	return shapes, current, byBucket
}

// Compile this source's PG work into one shared capacity problem. Other source
// owners retain their durable handoff authority; destination reservation still
// rechecks live capacity and quota before accepting any of these decisions.
func (r *rangeManager) buildPlacementTargets(group string, descriptions []BucketDescription) *placementTargetMap {
	s := r.server
	image, view := s.placement.Load(), s.currentSystem.Load()
	if image == nil || view == nil {
		return nil
	}
	shapes, current, byBucket := r.placementShapes(group, descriptions, image)
	result := &placementTargetMap{membership: view.manifest.Membership.Epoch}
	if len(shapes) == 0 {
		return result
	}
	type volumeKey struct{ node, volume client.ID }
	resourceIDs := make(map[volumeKey]int)
	var resources []placementResource
	var destinations []rangeDestination
	eligible := make(map[client.ID][]int)
	eligibleVolumes := make(map[client.ID]map[client.ID]bool)
	pressured := make(map[client.ID]bool)
	// Reuse equal policy pools across thousands of buckets. Quota backing is
	// bucket-specific and is filled independently below.
	type poolKey struct {
		group, strategy string
		version         uint64
	}
	pools := make(map[poolKey][]int)
	poolVolumes := make(map[poolKey]map[client.ID]bool)
	buckets := make([]client.ID, 0, len(byBucket))
	for bucket := range byBucket {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(a, b int) bool { return bytes.Compare(buckets[a][:], buckets[b][:]) < 0 })
	for _, bucket := range buckets {
		d := byBucket[bucket]
		goal := d.Policy
		if d.DesiredPolicy != nil {
			goal = *d.DesiredPolicy
		}
		key := poolKey{goal.PlacementGroup, goal.StorageStrategy, goal.PlacementMaskVersion}
		if ids, ok := pools[key]; ok {
			eligible[bucket] = ids
			eligibleVolumes[bucket] = poolVolumes[key]
			continue
		}
		d.Policy = goal
		available, _ := r.destinations(d)
		for id, needed := range r.capacityNeeds(available) {
			if needed > 0 {
				pressured[id] = true
			}
		}
		var ids []int
		pins := make(map[client.ID]bool, len(available))
		for _, destination := range available {
			key := volumeKey{destination.node, destination.volume.ID}
			id, exists := resourceIDs[key]
			if !exists {
				id = len(resources)
				resourceIDs[key] = id
				resources = append(resources, placementResource{Node: destination.node, Capacity: destination.capacity, Free: destination.volume.AdmissionFreePhysical, Reserved: make(map[client.ID]uint64)})
				destinations = append(destinations, destination)
			}
			ids = append(ids, id)
			pins[destination.volume.ID] = true
		}
		pools[key], eligible[bucket] = ids, ids
		poolVolumes[key], eligibleVolumes[bucket] = pins, pins
	}
	// A node's total bucket usage is an upper bound on usage on any one of
	// its volumes. Subtracting that bound is conservative for remote backing;
	// it never lends another bucket's guarantee to this one.
	type usageKey struct{ node, bucket client.ID }
	usage := make(map[usageKey]uint64)
	if catalog := r.pgCatalog.Load(); catalog != nil {
		for _, snapshot := range catalog.snapshots {
			for _, b := range snapshot.Buckets {
				usage[usageKey{snapshot.Node, b.ID}] = b.AttributedPhysical
			}
		}
	} else {
		local := s.CollectStats(client.StatsFilter{})
		for _, b := range local.Buckets {
			usage[usageKey{local.Node, b.ID}] = b.AttributedPhysical
		}
		s.peerStatsMu.RLock()
		for _, peer := range s.peerStats {
			if peer.online && peer.snapshot != nil {
				for _, b := range peer.snapshot.Buckets {
					usage[usageKey{peer.snapshot.Node, b.ID}] = b.AttributedPhysical
				}
			}
		}
		s.peerStatsMu.RUnlock()
	}
	for _, reserve := range view.manifest.QuotaReserves {
		id, ok := resourceIDs[volumeKey{reserve.Node, reserve.Volume}]
		used, known := usage[usageKey{reserve.Node, reserve.Bucket}]
		if !ok || !known {
			continue
		}
		remaining := subtractCapacity(reserve.Backing, used)
		if reserve.Node == r.node {
			if snapshot := s.quotaUsage.Load(); snapshot != nil {
				remaining = subtractCapacity(reserve.Backing, storagePhysical(snapshot.all[volume.ID(reserve.Volume)], volume.ID(reserve.Bucket)))
			}
		}
		resources[id].Reserved[reserve.Bucket] = min(remaining, destinations[id].volume.GuaranteedRemainingPhysical)
	}
	budgets := make(map[client.ID]uint64)
	totalUsage := make(map[client.ID]uint64)
	for key, n := range usage {
		totalUsage[key.bucket] = addCapacity(totalUsage[key.bucket], n)
	}
	for bucket, d := range byBucket {
		goal := d.Policy
		if d.DesiredPolicy != nil {
			goal = *d.DesiredPolicy
		}
		if goal.Limits.Accounting != "physical" || goal.Limits.Unlimited {
			continue
		}
		budgets[bucket] = subtractCapacity(goal.Limits.Absolute, totalUsage[bucket])
	}
	demands := make([]placementDemand, len(shapes))
	required := make([]bool, len(shapes))
	var options []placementOption
	input := placementImproveInput{Work: make([]placementWorkload, len(shapes)), Horizon: 30, Cohorts: make(map[int][]placementCohort), Unbound: make([]bool, len(shapes))}
	input.PrimaryLoads = make(map[client.ID]float64)
	for node, count := range image.primaries {
		input.PrimaryLoads[node] = float64(count) * (1 << 20)
	}
	seed := r.improveSequence.Add(1)
	mixed := seed * 0x9e3779b97f4a7c15
	mixed ^= mixed >> 29
	mixed *= 0xbf58476d1ce4e5b9
	mixed ^= mixed >> 31
	for i, shape := range shapes {
		input.Work[i].Read, input.Work[i].Write = r.rangeWorkload(rangeMetricKey{shape.bucket, shape.start, shape.bits})
	}
	weights := r.placementWeights(image, byBucket)
	for i, shape := range shapes {
		d, entry := byBucket[shape.bucket], current[i]
		goal := d.Policy
		if d.DesiredPolicy != nil {
			goal = *d.DesiredPolicy
		}
		demands[i] = placementDemand{Bucket: shape.bucket, Copies: shape.copies}
		valid := len(entry.Locations) == shape.copies && s.replicaSetSatisfies(goal, entry.Locations)
		unbound := len(entry.Locations) == 1 && entry.Locations[0].Volume == (client.ID{})
		input.Unbound[i] = unbound
		if unbound && d.DesiredPolicy == nil && shape.copies == 1 {
			valid = true
		}
		for _, loc := range entry.Locations {
			if pressured[loc.Volume] || loc.Volume != (client.ID{}) && !eligibleVolumes[shape.bucket][loc.Volume] {
				valid = false
			}
		}
		required[i] = !valid || d.DesiredPolicy != nil && entry.PolicyVersion != goal.Version
		// All scoring is based on maintained RAM accounting. Invalid placements
		// and desired policies do not turn an optimization tick into inventory.
		var cost uint64
		var soleSource volume.ID
		var retained map[volume.ID]uint64
		measurement, measured := r.measuredPlacement(shape, entry, d, weights)
		if measured {
			cost, retained = measurement.cost, measurement.retained
		} else if !valid {
			continue
		}
		if unbound && valid && measured {
			for id, n := range retained {
				if n > 0 {
					if _, ok := resourceIDs[volumeKey{r.node, client.ID(id)}]; !ok {
						measured = false
						cost = 0
						retained = nil
						break
					}
				}
			}
		}
		if !valid && entry.Locations[0].Volume == (client.ID{}) {
			before, ok := s.routingPatch(volume.ID(shape.bucket), volume.ObjectID(shape.start))
			if !ok {
				continue
			}
			if entry.Locations[0].Volume == (client.ID{}) {
				stores := r.sourceVolumes(rangePlan{Before: before, Moving: entry})
				if len(stores) == 1 {
					soleSource = stores[0].Identity().VolumeID
				}
			}
		}
		demands[i].Bytes = cost
		var movement uint64
		for _, n := range retained {
			movement = addCapacity(movement, n)
		}
		read, write := input.Work[i].Read, input.Work[i].Write
		input.Work[i] = placementWorkload{Physical: float64(movement), Read: read, Write: write, Growth: measurement.growth, Primary: entry.Locations[0].Node, Replicated: len(entry.Locations) > 1}
		if unbound && valid {
			key := volumeKey{r.node, client.ID{}}
			virtual, exists := resourceIDs[key]
			if !exists {
				virtual = len(resources)
				resourceIDs[key] = virtual
				resources = append(resources, placementResource{Node: r.node})
				destinations = append(destinations, rangeDestination{node: r.node, volume: client.VolumeStats{Class: entry.Locations[0].Class}})
			}
			option := len(options)
			options = append(options, placementOption{Range: i, Volume: virtual, Existing: true, Current: true})
			input.Network = append(input.Network, 0)
			input.Held = append(input.Held, 0)
			input.Cohorts[option] = nil
			for id, n := range retained {
				if n > 0 {
					resource := resourceIDs[volumeKey{r.node, client.ID(id)}]
					input.Cohorts[option] = append(input.Cohorts[option], placementCohort{resource, n})
				}
			}
			sort.Slice(input.Cohorts[option], func(a, b int) bool { return input.Cohorts[option][a].volume < input.Cohorts[option][b].volume })
		}
		for _, id := range eligible[shape.bucket] {
			destination := destinations[id]
			stays := false
			if soleSource != (volume.ID{}) && destination.node == r.node && destination.volume.ID == client.ID(soleSource) {
				stays = true
			}
			for _, loc := range entry.Locations {
				if loc.Node == destination.node && loc.Volume == destination.volume.ID {
					stays = true
					break
				}
			}
			if stays && pressured[destination.volume.ID] {
				continue
			}
			if valid && !measured && !stays {
				continue
			}
			// The shared index reserve is admission headroom, not copied bytes.
			allocate, move := cost, movement
			existing := stays
			if entry.Locations[0].Volume == (client.ID{}) && destination.node == r.node {
				if saved, present := retained[volume.ID(destination.volume.ID)]; present {
					existing = true
					move = subtractCapacity(movement, saved)
					allocate = subtractCapacity(cost, saved)
					if move == 0 {
						allocate = 0
					}
				}
			}
			if stays {
				allocate, move = 0, 0
			}
			charge := uint64(0)
			if shape.copies > len(entry.Locations) {
				charge = allocate
				if destination.node == entry.Locations[0].Node && entry.Locations[0].Volume == (client.ID{}) {
					charge = 0
				}
			}
			options = append(options, placementOption{Range: i, Volume: id, Allocate: allocate, Move: move, Quota: charge, Existing: existing, Current: stays})
			held := uint64(0)
			if existing {
				held = movement
				if destination.node == r.node {
					if n, ok := retained[volume.ID(destination.volume.ID)]; ok {
						held = n
					}
				}
			}
			input.Held = append(input.Held, held)
			network := uint64(0)
			if destination.node != r.node {
				network = move
			}
			input.Network = append(input.Network, network)
		}
	}
	primaries := make([]client.ID, len(current))
	for i, entry := range current {
		primaries[i] = entry.Locations[0].Node
	}
	// Client and peer transfers share one immutable distance image and one
	// Traffic coefficient. Candidate costs are derived from bounded RAM aggregates.
	now := time.Now()
	input.PeerDistances = r.placementPeerDistances(now)
	unknownDistance := placementDistanceFallback(input.PeerDistances)
	input.NetworkDistances = make([]float64, len(options))
	input.ClientNetwork = make([]float64, len(options))
	byShape := make([][]int, len(shapes))
	for id, option := range options {
		byShape[option.Range] = append(byShape[option.Range], id)
		node := resources[option.Volume].Node
		input.NetworkDistances[id] = placementPeerDistance(input.PeerDistances, r.node, node, unknownDistance)
	}
	for i, ids := range byShape {
		nodes := make([]client.ID, len(ids))
		for j, id := range ids {
			nodes[j] = resources[options[id].Volume].Node
		}
		costs := r.placementReaderCosts(rangeMetricKey{shapes[i].bucket, shapes[i].start, shapes[i].bits}, nodes, input.Horizon, now)
		for j, id := range ids {
			input.ClientNetwork[id] = costs[j]
		}
	}
	input.BackgroundRead, input.BackgroundWrite = r.placementBackground(destinations)
	budget := placementImproveBudget
	if s.ioPriority.users.Load() > 0 {
		budget = 5 * time.Millisecond
	}
	solution, improvement := rebuildPlacement(s.ctx, demands, resources, options, budgets, input, primaries, budget, mixed)
	result.improvement = improvement
	result.pending = len(solution.Pending)
	for _, mandatory := range []bool{true, false} {
		for _, i := range solution.Order {
			if required[i] != mandatory {
				continue
			}
			shape := shapes[i]
			target := placementTarget{shape: shape, description: byBucket[shape.bucket]}
			for _, option := range solution.Assignments[i] {
				destination := destinations[options[option].Volume]
				target.locations = append(target.locations, client.Location{Node: destination.node, Volume: destination.volume.ID, Class: destination.volume.Class, Role: 1})
			}
			if len(target.locations) != 0 {
				target.locations[0].Role = 0
			}
			result.targets = append(result.targets, target)
		}
	}
	return result
}

// Advance an already solved map without rebuilding or rescoring all ranges
// after each handoff. A catalog/membership change discards this advisory cache;
// a bound durable handoff is always resumed before consulting it.
func (r *rangeManager) nextPlacementTarget(m *placementTargetMap) (rangePlan, bool) {
	if m == nil {
		return rangePlan{}, false
	}
	view := r.server.currentSystem.Load()
	if view == nil || view.manifest.Membership.Epoch != m.membership {
		m.cursor = len(m.targets)
		return rangePlan{}, false
	}
	for m.cursor < len(m.targets) {
		target := m.targets[m.cursor]
		b, err := r.server.Bucket(volume.ID(target.shape.bucket))
		if err != nil || b.Describe().Revision != target.description.Revision || r.server.controlBlocksPG(target.shape.bucket) {
			m.cursor++
			continue
		}
		before, ok := r.server.routingPatch(volume.ID(target.shape.bucket), volume.ObjectID(target.shape.start))
		if !ok || len(before.Entries) != 1 {
			m.cursor++
			continue
		}
		entry := before.Entries[0]
		if entry.Start != target.shape.start || entry.PrefixBits != target.shape.bits || len(entry.Locations) == 0 || entry.Locations[0].Node != r.node || entry.State != 0 {
			m.cursor++
			continue
		}
		if entry.Locations[0].Volume != (client.ID{}) && !r.replicaSetOnline(entry) {
			m.pending++
			m.cursor++
			continue
		}
		if p, ok := r.diffPlacementTarget(target, before); ok {
			return p, true
		}
		if !placementTargetSatisfied(target, entry) {
			m.pending++
		}
		m.cursor++
	}
	return rangePlan{}, false
}

func placementTargetSatisfied(target placementTarget, entry client.RangeEntry) bool {
	if len(entry.Locations) != len(target.locations) {
		return false
	}
	if len(entry.Locations) != 0 && entry.Locations[0].Node != target.locations[0].Node {
		return false
	}
	for _, wanted := range target.locations {
		found := false
		for _, loc := range entry.Locations {
			if loc.Node == wanted.Node && loc.Volume == wanted.Volume {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return target.description.DesiredPolicy == nil || entry.PolicyVersion == target.description.DesiredPolicy.Version
}

// Compile one CURRENT -> target-map difference using the existing durable
// handoff operations. New copies precede removals; role swaps never copy bytes.
func (r *rangeManager) diffPlacementTarget(target placementTarget, before client.MapPatch) (rangePlan, bool) {
	entry, d := before.Entries[0], target.description
	if placementTargetSatisfied(target, entry) {
		return rangePlan{}, false
	}
	if len(target.locations) == 0 {
		return rangePlan{}, false
	}
	goal := d.Policy
	if d.DesiredPolicy != nil {
		goal = *d.DesiredPolicy
	}
	makePlan := func(loc client.Location, next []client.Location, add, remove, promote, relocate bool) (rangePlan, bool) {
		var cost uint64
		var err error
		codec := bucketCodec(d.Policy)
		if len(entry.Locations) == 1 && !add {
			codec = bucketCodec(goal)
		}
		if promote {
			cost, err = r.replicaAbortCost(before, entry)
		} else if !remove {
			cost, _, err = r.policyPlacementCost(before, entry, loc.Node, volume.ID(loc.Volume), codec)
		}
		if err != nil {
			return rangePlan{}, false
		}
		if !remove {
			probe := d
			probe.Policy = goal
			available, _ := r.destinations(probe)
			fits := false
			for _, destination := range available {
				if destination.node == loc.Node && destination.volume.ID == loc.Volume && destination.free >= cost {
					fits = true
					break
				}
			}
			if !fits {
				return rangePlan{}, false
			}
		}
		if len(entry.Locations) == 1 && !add {
			p, err := planRangeMove(before, entry, loc.Node, volume.ID(loc.Volume), loc.Class, bucketCodec(goal), cost)
			if err != nil {
				return rangePlan{}, false
			}
			if target.shape.copies == 1 {
				p.After.Entries[0].PolicyVersion = goal.Version
			}
			return p, p.validate() == nil
		}
		after := entry
		after.OwnerEpoch++
		after.Locations = next
		if goal.Compression == d.Policy.Compression && r.server.replicaSetSatisfies(goal, next) {
			after.PolicyVersion = goal.Version
		}
		p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: before, Source: r.node, Target: loc.Node, Volume: volume.ID(loc.Volume), Moving: entry, Compression: bucketCodec(d.Policy), Physical: cost, ReplicaAdd: add, ReplicaRemove: remove, ReplicaPromote: promote, ReplicaRelocate: relocate}
		p.After.Entries = []client.RangeEntry{after}
		return p, p.validate() == nil
	}
	// Legacy single-owner cohorts must become concrete before adding replicas.
	if entry.Locations[0].Volume == (client.ID{}) {
		return makePlan(target.locations[0], nil, false, false, false, false)
	}
	for _, wanted := range target.locations {
		found := -1
		for i, loc := range entry.Locations {
			if loc.Node == wanted.Node {
				found = i
				break
			}
		}
		if found >= 0 && entry.Locations[found].Volume == wanted.Volume {
			continue
		}
		if len(entry.Locations) == 1 && target.shape.copies == 1 {
			return makePlan(wanted, nil, false, false, false, false)
		}
		next := append([]client.Location(nil), entry.Locations...)
		wanted.Role = 1
		if found >= 0 {
			wanted.Role = next[found].Role
			next[found] = wanted
		} else {
			next = append(next, wanted)
		}
		return makePlan(wanted, next, found < 0, false, false, found >= 0)
	}
	wantedNodes := make(map[client.ID]bool, len(target.locations))
	for _, loc := range target.locations {
		wantedNodes[loc.Node] = true
	}
	if !wantedNodes[entry.Locations[0].Node] {
		for i := 1; i < len(entry.Locations); i++ {
			if !wantedNodes[entry.Locations[i].Node] {
				continue
			}
			next := append([]client.Location(nil), entry.Locations...)
			next[0], next[i] = next[i], next[0]
			next[0].Role, next[i].Role = 0, 1
			return makePlan(next[0], next, false, false, true, false)
		}
	}
	for i := 1; i < len(entry.Locations); i++ {
		if wantedNodes[entry.Locations[i].Node] {
			continue
		}
		next := append([]client.Location(nil), entry.Locations[:i]...)
		next = append(next, entry.Locations[i+1:]...)
		return makePlan(entry.Locations[i], next, false, true, false, false)
	}
	if target.locations[0].Node != entry.Locations[0].Node {
		for i := 1; i < len(entry.Locations); i++ {
			if entry.Locations[i].Node != target.locations[0].Node {
				continue
			}
			next := append([]client.Location(nil), entry.Locations...)
			next[0], next[i] = next[i], next[0]
			next[0].Role, next[i].Role = 0, 1
			return makePlan(next[0], next, false, false, true, false)
		}
	}
	if d.DesiredPolicy != nil && entry.PolicyVersion != goal.Version {
		if len(entry.Locations) == 1 {
			return makePlan(entry.Locations[0], nil, false, false, false, false)
		}
		if d.Policy.PlacementGroup == "" {
			return rangePlan{}, false
		}
		after := entry
		after.OwnerEpoch++
		after.PolicyVersion = goal.Version
		p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: before, Moving: entry, Source: r.node, Target: r.node, Volume: volume.ID(entry.Locations[0].Volume), Compression: bucketCodec(goal), CodecOnly: true}
		p.After.Entries = []client.RangeEntry{after}
		return p, p.validate() == nil
	}
	return rangePlan{}, false
}
