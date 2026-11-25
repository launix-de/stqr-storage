// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"sort"

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
	membership uint64
	targets    []placementTarget
	cursor     int
	pending    int
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
	// One traversal of the immutable map, independent of the bucket count.
	for i := 0; i < image.count; i++ {
		bucket, entry := image.entry(i)
		d, ok := byBucket[bucket]
		if !ok || entry.State != 0 || entry.Runtime != (client.ID{}) || len(entry.Locations) == 0 || entry.Locations[0].Node != r.node {
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
		if len(entry.Locations) == 1 && entry.Locations[0].Volume == (client.ID{}) && d.DesiredPolicy == nil && copies == 1 {
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
	var options []placementOption
	for i, shape := range shapes {
		d, entry := byBucket[shape.bucket], current[i]
		goal := d.Policy
		if d.DesiredPolicy != nil {
			goal = *d.DesiredPolicy
		}
		demands[i] = placementDemand{Bucket: shape.bucket, Copies: shape.copies}
		valid := len(entry.Locations) == shape.copies && s.replicaSetSatisfies(goal, entry.Locations)
		for _, loc := range entry.Locations {
			if pressured[loc.Volume] || !eligibleVolumes[shape.bucket][loc.Volume] {
				valid = false
			}
		}
		// Existing valid copies need no payload measurements. For a required
		// move, measure once, shared by every candidate destination.
		var cost uint64
		var soleSource volume.ID
		if !valid {
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
			var err error
			cost, _, err = r.policyPlacementCost(before, entry, client.ID{}, volume.ID{}, bucketCodec(d.Policy))
			if err != nil {
				continue
			}
		}
		demands[i].Bytes = cost
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
			if valid && !stays {
				continue
			}
			allocate, move := cost, max(cost, uint64(1))
			if stays {
				allocate, move = 0, 0
			}
			charge := uint64(0)
			if shape.copies > len(entry.Locations) {
				charge = allocate
			}
			options = append(options, placementOption{Range: i, Volume: id, Allocate: allocate, Move: move, Quota: charge})
		}
	}
	solution := solvePlacement(demands, resources, options, budgets)
	result.pending = len(solution.Pending)
	for _, i := range solution.Order {
		shape := shapes[i]
		target := placementTarget{shape: shape, description: byBucket[shape.bucket]}
		for _, option := range solution.Assignments[i] {
			destination := destinations[options[option].Volume]
			target.locations = append(target.locations, client.Location{Node: destination.node, Volume: destination.volume.ID, Class: destination.volume.Class, Role: 1})
		}
		// Preserve the current primary whenever it is still a chosen copy.
		// Capacity-weighted role balancing is a separate metadata-only diff.
		sort.SliceStable(target.locations, func(a, b int) bool {
			x, y := target.locations[a], target.locations[b]
			primary := current[i].Locations[0].Node
			if (x.Node == primary) != (y.Node == primary) {
				return x.Node == primary
			}
			return bytes.Compare(x.Node[:], y.Node[:]) < 0
		})
		if len(target.locations) != 0 {
			target.locations[0].Role = 0
		}
		result.targets = append(result.targets, target)
	}
	r.balancePlacementTargets(result, image, eligible, resources)
	return result
}

// Role decisions share the solved map's lifetime. Update aggregate work after
// each decision instead of rescanning every range after every promotion.
func (r *rangeManager) balancePlacementTargets(m *placementTargetMap, image *placementImage, eligible map[client.ID][]int, resources []placementResource) {
	assigned := make(map[client.ID]float64)
	const weight = float64(1 << 20)
	for i := 0; i < image.count; i++ {
		bucket, entry := image.entry(i)
		if _, included := eligible[bucket]; included && entry.State == 0 && len(entry.Locations) > 1 {
			assigned[entry.Locations[0].Node] += weight
		}
	}
	capacities := make(map[client.ID]map[client.ID]uint64)
	for bucket, ids := range eligible {
		capacity := make(map[client.ID]uint64)
		for _, id := range ids {
			resource := resources[id]
			capacity[resource.Node] = addCapacity(capacity[resource.Node], resource.Capacity)
		}
		capacities[bucket] = capacity
	}
	for i := range m.targets {
		target := &m.targets[i]
		if len(target.locations) < 2 {
			continue
		}
		old, ok := image.lookup(target.shape.bucket, target.shape.start)
		if !ok || len(old.Locations) < 2 {
			continue
		}
		from := old.Locations[0].Node
		capacity := capacities[target.shape.bucket]
		best, gain := -1, float64(0)
		for j, loc := range target.locations {
			if loc.Node == from || capacity[loc.Node] == 0 {
				continue
			}
			if capacity[from] == 0 {
				if best < 0 || (assigned[loc.Node]+weight)/float64(capacity[loc.Node]) < (assigned[target.locations[best].Node]+weight)/float64(capacity[target.locations[best].Node]) {
					best = j
				}
				continue
			}
			improvement := weight * ((2*assigned[from]-weight)/float64(capacity[from]) - (2*assigned[loc.Node]+weight)/float64(capacity[loc.Node]))
			if improvement > gain {
				best, gain = j, improvement
			}
		}
		if best < 0 {
			continue
		}
		target.locations[0], target.locations[best] = target.locations[best], target.locations[0]
		target.locations[0].Role, target.locations[best].Role = 0, 1
		assigned[from] -= weight
		assigned[target.locations[0].Node] += weight
	}
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
