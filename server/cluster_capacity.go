// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// These are advisory dashboard gauges, never write-admission or guaranteed-
// quota proofs. A probe deadline reports an unavailable measurement; it cannot
// evict a node, shorten an ownership claim or invent zero usage.
func (b *Bucket) administrativeUsage(ctx context.Context, p principal) (uint64, error) {
	if b.server.bucketAwaitingRecovery(client.ID(b.ID())) {
		return 0, fail(503, "bucket has unavailable recovery ranges")
	}
	if b.server.controller == nil {
		return b.logicalUsage()
	}
	return b.clusterLogicalUsage(ctx, p)
}

func (b *Bucket) clusterLogicalUsage(ctx context.Context, p principal) (uint64, error) {
	return b.clusterUsage(ctx, p, false)
}

// Internal capacity aggregation reveals only the authorized bucket totals to clients.
// Member nodes may query maintained counters for reservation accounting.
func (b *Bucket) clusterUsage(ctx context.Context, p principal, internal bool) (uint64, error) {
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
	for _, node := range s.inventoryOwners(b.ID()) {
		e := client.Encoder{}
		if internal {
			e.U16(2)
		} else {
			e.U16(1)
		}
		e.ID(client.ID(b.ID()))
		e.ID(client.ID(actor))
		if internal {
			e.Text("")
		} else {
			e.Text(p.User.Name)
		}
		ownerStamp := inventoryStamp(entries, node)
		e.B = append(e.B, ownerStamp[:]...)
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

func (s *Server) administrativeQuotaCapacity(id volume.ID, strategy uint8, p principal) (bucketQuotaCapacity, error) {
	return s.quotaCapacitySnapshot(s.ctx, id, strategy, p, false)
}

func (s *Server) quotaCapacitySnapshot(ctx context.Context, id volume.ID, strategy uint8, p principal, internal bool) (bucketQuotaCapacity, error) {
	view := s.currentSystem.Load()
	if s.controller == nil || view == nil || len(view.manifest.Membership.Members) <= 1 {
		return s.bucketQuotaCapacity(id, strategy)
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
		used, err = b.clusterUsage(ctx, p, internal)
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
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
			} else if !groups.allowsVersion(group, maskVersion, member.ID.String(), v.ID.String()) {
				continue
			}
			class := int(v.Class)
			node.Free[class] = addCapacity(node.Free[class], addCapacity(v.AdmissionFreePhysical, v.GuaranteedRemainingPhysical))
			node.Reserved[class] = addCapacity(node.Reserved[class], v.GuaranteedRemainingPhysical)
		}
		nodes = append(nodes, node)
	}
	var guarantees [3]uint64
	for _, description := range s.bucketSnapshots() {
		if description.ID == id.String() || description.ID == systemBucket.String() || description.DeletionComplete || description.Policy.Limits.Guaranteed == 0 {
			continue
		}
		bid, err := bucketID(description.ID)
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
		b, err := s.Bucket(bid)
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
		usage, err := b.clusterUsage(ctx, p, internal)
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
		class := int(bucketClass(description.Policy))
		guarantees[class] = addCapacity(guarantees[class], unusedGuarantee(description.Policy, usage))
	}
	if s.currentSystem.Load() != view {
		return bucketQuotaCapacity{}, fail(503, "cluster configuration changed during capacity measurement")
	}
	return quotaCapacity(nodes, strategy, used, guarantees), nil
}
