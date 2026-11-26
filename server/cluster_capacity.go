// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// These are advisory dashboard gauges, never write-admission or guaranteed-
// quota proofs. A probe deadline reports an unavailable measurement; it cannot
// evict a node, shorten an ownership claim or invent zero usage.
func (b *Bucket) administrativeUsage(ctx context.Context, p principal) (uint64, error) {
	if b.server.storageWarming.Load() {
		return 0, b.server.storageUnavailable()
	}
	if b.server.bucketAwaitingRecovery(client.ID(b.ID())) {
		return 0, fail(503, "bucket has unavailable recovery ranges")
	}
	if b.server.controller == nil {
		return b.accountedUsage()
	}
	return b.clusterLogicalUsage(ctx, p)
}

func (b *Bucket) clusterLogicalUsage(ctx context.Context, p principal) (uint64, error) {
	return b.clusterUsageAs(ctx, p, b.storageDescription().Policy.Limits.Accounting)
}
func (b *Bucket) clusterUsageAs(ctx context.Context, p principal, accounting string) (uint64, error) {
	return b.clusterUsageMode(ctx, p, accounting, false)
}

// Internal capacity aggregation reveals only the authorized bucket totals to clients.
// Member nodes may query maintained counters for reservation accounting.
func (b *Bucket) clusterUsage(ctx context.Context, p principal, internal bool) (uint64, error) {
	return b.clusterUsageMode(ctx, p, b.storageDescription().Policy.Limits.Accounting, internal)
}
func (b *Bucket) clusterUsageMode(ctx context.Context, p principal, accounting string, internal bool) (uint64, error) {
	s := b.server
	entries := s.placement.Load().bucketEntries(client.ID(b.ID()))
	stamp := inventoryStamp(entries, client.ID{})
	var actor volume.ID
	if !internal {
		var err error
		actor, err = parseID(p.User.ID)
		if err != nil {
			return 0, ErrBucketForbidden
		}
	}
	var total uint64
	owners := s.inventoryOwners(b.ID())
	if accounting == "physical" {
		seen := map[client.ID]bool{}
		for _, entry := range entries {
			for _, loc := range entry.Locations {
				if loc.Role <= 1 {
					seen[loc.Node] = true
				}
			}
		}
		owners = nil
		for node := range seen {
			owners = append(owners, node)
		}
		sort.Slice(owners, func(i, j int) bool { return owners[i].String() < owners[j].String() })
	}
	for _, node := range owners {
		e := client.Encoder{}
		schema := uint16(1)
		if accounting == "physical" {
			schema = 2
		}
		if internal {
			schema = 2
			if accounting == "physical" {
				schema = 3
			}
		}
		e.U16(schema)
		e.ID(client.ID(b.ID()))
		e.ID(client.ID(actor))
		if internal {
			e.Text("")
		} else {
			e.Text(p.User.Name)
		}
		ownerStamp := inventoryStamp(entries, node)
		if accounting == "physical" {
			ownerStamp = stamp
		}
		e.B = append(e.B, ownerStamp[:]...)
		if accounting == "physical" {
			e.U8(1)
		}
		body, err := (peerControlParticipants{runtime: s.control}).call(ctx, node, peerBucketUsage, e.B)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return 0, fail(503, "bucket usage measurement is not available")
			}
			if code := statusOf(err); code == 503 || code == 409 {
				_ = s.refreshInventoryMap(ctx, b.ID())
				return 0, fail(503, "bucket usage owner is not ready")
			}
			return 0, err
		}
		d := client.Decoder{B: body}
		used := d.U64()
		if d.Done() != nil {
			return 0, client.ErrProtocol
		}
		if used > math.MaxUint64-total {
			return 0, volume.ErrRange
		}
		total += used
	}
	if inventoryStamp(s.placement.Load().bucketEntries(client.ID(b.ID())), client.ID{}) != stamp {
		return 0, fail(503, "bucket usage placement changed")
	}
	return total, nil
}

func (s *Server) administrativeQuotaCapacity(id volume.ID, strategy uint8, p principal, selection ...BucketPolicy) (bucketQuotaCapacity, error) {
	return s.quotaCapacitySnapshot(s.ctx, id, strategy, p, false, selection...)
}

func (s *Server) quotaCapacitySnapshot(ctx context.Context, id volume.ID, strategy uint8, p principal, internal bool, selection ...BucketPolicy) (bucketQuotaCapacity, error) {
	view := s.currentSystem.Load()
	if s.controller == nil || view == nil || len(view.manifest.Membership.Members) <= 1 {
		return s.bucketQuotaCapacity(id, strategy, selection...)
	}
	if strategy > 2 {
		return bucketQuotaCapacity{}, fail(400, "invalid storage strategy")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var used uint64
	group := ""
	maskVersion := uint64(0)
	if id != (volume.ID{}) {
		b, err := s.Bucket(id)
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
		description := b.Describe()
		group, maskVersion = description.Policy.PlacementGroup, description.Policy.PlacementMaskVersion
		accounting := description.Policy.Limits.Accounting
		if len(selection) > 0 {
			accounting = selection[0].Limits.Accounting
		}
		used, err = b.clusterUsageMode(ctx, p, accounting, internal)
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
	}
	if len(selection) > 0 {
		group, maskVersion = selection[0].PlacementGroup, selection[0].PlacementMaskVersion
	}
	shares, err := s.quotaPreviewShares(ctx, view, id)
	if err != nil {
		return bucketQuotaCapacity{}, err
	}
	local := s.CollectStats(client.StatsFilter{})
	images := map[client.ID]client.StatsSnapshot{local.Node: local}
	s.peerStatsMu.RLock()
	for node, observation := range s.peerStats {
		if observation.snapshot != nil && (!internal || observation.online) {
			images[node] = *observation.snapshot
		}
	}
	s.peerStatsMu.RUnlock()
	groups := s.placementGroups.Load()
	var nodes []quotaNodeCapacity
	for _, member := range view.manifest.Membership.Members {
		image, known := images[member.ID]
		if !known {
			return bucketQuotaCapacity{}, fail(503, "volume statistics for a cluster member are not available")
		}
		var node quotaNodeCapacity
		for _, v := range image.Volumes {
			if image.NodeState != 0 || v.State != 1 || v.Class > 2 {
				continue
			}
			if group == "" {
				if groups.allows(systemPlacementGroup.String(), member.ID.String(), v.ID.String()) {
					continue
				}
			} else if mask, ok := groups.mask(group, maskVersion); ok && mask.NodeSelection == "all" && groups.allows(systemPlacementGroup.String(), member.ID.String(), v.ID.String()) {
				continue
			} else if !groups.allowsVersion(group, maskVersion, member.ID.String(), v.ID.String()) {
				continue
			}
			class := int(v.Class)
			node.Free[class] = addCapacity(node.Free[class], addCapacity(v.AdmissionFreePhysical, v.GuaranteedRemainingPhysical))
			node.Reserved[class] = addCapacity(node.Reserved[class], subtractCapacity(v.GuaranteedRemainingPhysical, shares[member.ID][v.ID.String()]))
		}
		nodes = append(nodes, node)
	}
	var guarantees [3]uint64
	if s.currentSystem.Load() != view {
		return bucketQuotaCapacity{}, fail(503, "cluster configuration changed during capacity measurement")
	}
	return quotaCapacity(nodes, strategy, used, guarantees), nil
}
