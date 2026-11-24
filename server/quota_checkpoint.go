// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	peerQuotaCheckpoint uint16 = 0x1060 + iota
	peerQuotaRequest
	peerQuotaGrant
	peerQuotaAck
)

type quotaNodeSnapshot struct {
	Node           client.ID
	Grants, Ranges uint64
	Buckets        []quotaBucketSnapshot
}
type quotaBucketSnapshot struct {
	Bucket          client.ID
	Allowance, Used uint64
	Physical        uint64 `json:",omitempty"`
}

func quotaPolicy(d BucketDescription) BucketPolicy {
	if d.DesiredPolicy != nil {
		return *d.DesiredPolicy
	}
	return d.Policy
}

func (s *Server) quotaCheckpoint(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	if schema != 1 && schema != 2 && schema != 3 {
		return nil, client.ErrProtocol
	}
	claim, err := decodeControlClaim(d.Blob())
	var planned volume.ObjectID
	if schema >= 2 {
		copy(planned[:], d.Take(32))
	}
	count := d.Count(10000)
	var buckets []client.ID
	var usage []bool
	var physical []bool
	for i := 0; i < count; i++ {
		buckets = append(buckets, d.ID())
		usage = append(usage, d.Bool())
		phys := false
		if schema == 3 {
			phys = d.Bool()
		}
		physical = append(physical, phys)
	}
	if err != nil || d.Done() != nil || claim.Round.Initiator != actor || s.quotas == nil {
		return nil, errControlConflict
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	l := s.control.ledger
	l.mu.Lock()
	valid := l.slot != nil && equalControlClaim(l.slot.Claim, claim)
	l.mu.Unlock()
	if !valid || claim.Exclusion == nil && len(s.pendingQuotaGrants()) != 0 {
		return nil, errControlConflict
	}
	var plannedView *systemView
	if planned != (volume.ObjectID{}) {
		if claim.Exclusion == nil {
			return nil, errControlConflict
		}
		plannedView, err = s.loadSystemView(planned)
		if err != nil {
			return nil, err
		}
		if plannedView.manifest.Sequence != claim.Base.Sequence+1 || plannedView.manifest.Membership.Epoch != claim.Base.Membership.Epoch+1 && !(claim.Exclusion != nil && claim.Exclusion.Restore != nil && plannedView.manifest.Membership.Epoch == claim.Base.Membership.Epoch) {
			return nil, errControlConflict
		}
	}
	snap := quotaNodeSnapshot{Node: s.quotas.ledger.node}
	s.quotas.ledger.mu.Lock()
	snap.Grants = s.quotas.ledger.sequence
	s.quotas.ledger.mu.Unlock()
	if r := s.scopes; r != nil {
		r.mu.Lock()
		snap.Ranges = r.journal.sequence
		r.mu.Unlock()
	}
	for i, bucket := range buckets {
		entry := quotaBucketSnapshot{Bucket: bucket}
		if claim.Exclusion == nil && findQuota(s.currentSystem.Load(), bucket) != nil {
			entry.Allowance, err = s.quotaAllowance(bucket)
			if err != nil {
				return nil, err
			}
		}
		if usage[i] {
			if claim.Exclusion == nil && !hasControlNode(claim.FrozenBuckets, bucket) {
				// A new bucket is still absent from the live catalog; its
				// initial usage is necessarily zero, without a mutation fence.
				if _, e := s.Bucket(volume.ID(bucket)); e == nil {
					return nil, errControlConflict
				}
			} else if b, e := s.Bucket(volume.ID(bucket)); e == nil {
				if plannedView != nil {
					entry.Used, err = b.plannedLogicalUsage(plannedView)
				} else {
					entry.Used, err = b.logicalUsage()
				}
				if err != nil {
					return nil, err
				}
			} else if e != ErrBucketNotFound {
				return nil, e
			}
		}
		if b, e := s.Bucket(volume.ID(bucket)); physical[i] && e == nil {
			if plannedView != nil {
				entry.Physical, err = b.plannedPhysicalUsage(plannedView)
			} else {
				entry.Physical, err = b.physicalUsage()
			}
			if err != nil {
				return nil, err
			}
		}
		snap.Buckets = append(snap.Buckets, entry)
	}
	return json.Marshal(snap)
}

// ALL reservations exclude both scoped handoffs and incomplete grants. The
// resulting balances and per-node journal watermarks therefore describe the
// same map checkpoint. Ordinary writes can continue unless a limit changes.
func (c *clusterController) captureQuotaAllocations(ctx context.Context, claim controlClaim, membership client.Membership, descriptions []BucketDescription, planned ...volume.ObjectID) ([]quotaAllocation, error) {
	view := c.server.currentSystem.Load()
	var allocations []quotaAllocation
	var measure []bool
	for _, description := range descriptions {
		p := quotaPolicy(description)
		if description.DeletionComplete || description.ID == systemBucket.String() || p.Limits.Unlimited {
			continue
		}
		id, err := bucketID(description.ID)
		if err != nil {
			return nil, err
		}
		old := findQuota(view, client.ID(id))
		a := quotaAllocation{Bucket: client.ID(id), Limit: p.Limits.Absolute}
		changed := claim.Exclusion != nil || old == nil || old.Limit != a.Limit || quotaAccounting(old.Accounting) != p.Limits.Accounting
		if p.Limits.Accounting == "physical" {
			a.Accounting = "physical"
		}
		if changed {
			a.Epoch = client.NewRequestID()
		} else {
			a.Epoch = old.Epoch
		}
		allocations = append(allocations, a)
		measure = append(measure, changed)
	}
	if len(allocations) == 0 {
		return nil, nil
	}
	claimWire, err := marshalControlClaim(claim)
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	schema := uint16(1)
	if len(planned) > 0 && planned[0] != (volume.ObjectID{}) {
		schema = 2
	}
	for _, a := range allocations {
		if a.Accounting == "physical" {
			schema = 3
			break
		}
	}
	e.U16(schema)
	e.Blob(claimWire)
	if schema >= 2 {
		var root volume.ObjectID
		if len(planned) > 0 {
			root = planned[0]
		}
		e.B = append(e.B, root[:]...)
	}
	e.V(uint64(len(allocations)))
	for i, a := range allocations {
		e.ID(a.Bucket)
		e.Bool(measure[i])
		if schema == 3 {
			e.Bool(a.Accounting == "physical" && measure[i])
		}
	}
	var snapshots []quotaNodeSnapshot
	for _, node := range claim.Required {
		body, err := (peerControlParticipants{runtime: c.server.control}).call(ctx, node, peerQuotaCheckpoint, e.B)
		if err != nil {
			return nil, err
		}
		var snap quotaNodeSnapshot
		if decodeControlRecord(body, &snap) != nil || snap.Node != node || snap.Grants == 0 || len(snap.Buckets) != len(allocations) {
			return nil, client.ErrProtocol
		}
		snapshots = append(snapshots, snap)
	}
	for i := range allocations {
		a := &allocations[i]
		var total uint64
		for _, snap := range snapshots {
			entry := snap.Buckets[i]
			if entry.Bucket != a.Bucket {
				return nil, client.ErrProtocol
			}
			amount := entry.Allowance
			if measure[i] {
				amount = entry.Used
				if a.Accounting == "physical" {
					amount = entry.Physical
				}
			}
			if amount > math.MaxUint64-total {
				return nil, volume.ErrRange
			}
			total += amount
			if hasControlNode(membershipIDs(membership), snap.Node) {
				a.Nodes = append(a.Nodes, quotaNodeAllocation{Node: snap.Node, Bytes: amount, Grants: snap.Grants, Ranges: snap.Ranges})
			} else if measure[i] && amount != 0 {
				return nil, errControlConflict
			}
		}
		if total > a.Limit {
			return nil, volume.ErrFull
		}
		if !measure[i] && total != a.Limit {
			return nil, volume.ErrCorrupt
		}
		var retained uint64
		for _, node := range a.Nodes {
			retained += node.Bytes
		}
		if len(a.Nodes) == 0 {
			return nil, errControlConflict
		}
		// Retired-node unused credits and newly granted headroom go to an
		// admitted node. Lending subsequently moves them to actual writers.
		a.Nodes[0].Bytes += a.Limit - retained
		sortQuotaNodes(a.Nodes)
	}
	sort.Slice(allocations, func(i, j int) bool { return allocations[i].Bucket.String() < allocations[j].Bucket.String() })
	return allocations, nil
}

func setBundleQuotas(bundle *systemBundle, allocations []quotaAllocation) error {
	var manifest systemManifest
	if err := decodeControlRecord(bundle.Objects[bundle.Root], &manifest); err != nil {
		return err
	}
	manifest.Quotas = allocations
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	bundle.Root = bundle.add(raw)
	_, err = decodeSystemView(bundle.Root, func(id volume.ObjectID) ([]byte, error) {
		data, ok := bundle.Objects[id]
		if !ok {
			return nil, volume.ErrNotFound
		}
		return data, nil
	})
	return err
}

func validateQuotaAllocations(view *systemView) error {
	seen := map[client.ID]bool{}
	for _, a := range view.manifest.Quotas {
		if a.Bucket == (client.ID{}) || a.Epoch == (client.ID{}) || seen[a.Bucket] || len(a.Nodes) != len(view.manifest.Membership.Members) {
			return volume.ErrCorrupt
		}
		seen[a.Bucket] = true
		found := false
		for _, description := range view.descriptions {
			if description.ID == a.Bucket.String() && !description.DeletionComplete {
				p := quotaPolicy(description)
				found = !p.Limits.Unlimited && (p.Limits.Accounting == "logical" && (a.Accounting == "" || a.Accounting == "logical") || p.Limits.Accounting == "physical" && a.Accounting == "physical") && p.Limits.Absolute == a.Limit
				break
			}
		}
		if !found {
			return volume.ErrCorrupt
		}
		var total uint64
		for i, n := range a.Nodes {
			if n.Node != view.manifest.Membership.Members[i].ID || n.Grants == 0 || n.Bytes > math.MaxUint64-total {
				return volume.ErrCorrupt
			}
			total += n.Bytes
		}
		if total != a.Limit {
			return volume.ErrCorrupt
		}
	}
	return nil
}

// Upgrade an older finite-bucket checkpoint without silently spending an
// unallocated limit on every node. Allocation uses the same ALL fence as edits.
func (c *clusterController) ensureQuotaAllocations(ctx context.Context) {
	l := c.coordinator.ledger
	l.mu.Lock()
	busy := l.slot != nil || l.request != nil
	members := membershipIDs(l.current.Membership)
	l.mu.Unlock()
	if busy || len(members) == 0 || c.server.quotaRefreshCoordinator(members) != l.node {
		return
	}
	var frozen []client.ID
	for _, d := range c.server.bucketSnapshots() {
		if !d.DeletionComplete && !quotaPolicy(d).Limits.Unlimited {
			id, _ := bucketID(d.ID)
			if findQuota(c.server.currentSystem.Load(), client.ID(id)) == nil {
				frozen = append(frozen, client.ID(id))
			}
		}
	}
	if len(frozen) != 0 || c.server.quotaBackingDrains() {
		_, _, _ = c.change(ctx, nil, nil, frozen...)
	}
}

// A sealed orphan is not owned by the live map yet. Reinitializing credits
// from that old map would omit its bytes. Count the exact staged candidate and
// ignore newly LOST ranges whose stale records PREPARE will remove. This does
// not populate the normal cache or publish speculative placement authority.
func (b *Bucket) plannedLogicalUsage(view *systemView) (uint64, error) {
	placement := view.placement
	current := b.server.placement.Load()
	exclude := func(entry client.RangeEntry) bool {
		if entry.State == 4 {
			return true
		}
		if entry.State != 5 {
			return false
		}
		old, known := current.lookup(client.ID(b.ID()), entry.Start)
		return !known || !reflect.DeepEqual(old, entry)
	}
	var total uint64
	var after *volume.ObjectID
	for {
		records, next, more, err := b.scanPlacement(b.server.ctx, b.Describe(), after, 128, placement, exclude, view)
		if err != nil {
			return 0, err
		}
		for _, record := range records {
			if record.Info.Length > math.MaxUint64-total {
				return 0, volume.ErrRange
			}
			total += record.Info.Length
		}
		if !more {
			return total, nil
		}
		after = &next
	}
}
