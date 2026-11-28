// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/volume"
	"slices"
)

func cloneTopologyChanged(before, after []BucketDescription) bool {
	peers := make(map[string][]BucketClonePeer, len(before))
	for _, d := range before {
		peers[d.ID] = d.ClonePeers
	}
	for _, d := range after {
		if !slices.Equal(peers[d.ID], d.ClonePeers) {
			return true
		}
		delete(peers, d.ID)
	}
	for _, edges := range peers {
		if len(edges) > 0 {
			return true
		}
	}
	return false
}
func (b *Bucket) plannedCowUsage(view *systemView) (uint64, error) {
	graph, err := cowGraphDescriptions(view.descriptions)
	if err != nil {
		return 0, err
	}
	var total uint64
	var after *volume.ObjectID
	for {
		records, next, more, err := b.server.cowGraphPage(b.server.ctx, b.ID(), after, 128, true, graph, view.placement, view.descriptions)
		if err != nil {
			return 0, err
		}
		for _, record := range records {
			if record.Info.Length > ^uint64(0)-total {
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

func cloneQuotaCheckpointNeeded(before, after []BucketDescription) bool {
	if !cloneTopologyChanged(before, after) {
		return false
	}
	for _, d := range after {
		if !d.DeletionComplete && !quotaPolicy(d).Limits.Unlimited {
			return true
		}
	}
	return false
}
