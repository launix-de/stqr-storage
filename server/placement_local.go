// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// initialVolumePlacement compiles an empty local placement into the same phase
// as a rebuild. The caller has already proved that no object or retry receipt
// owns a disk. The committed owner/cohort limits eligible destinations; this
// operation must not pin a whole striped range or create a new replica role.
func (s *Server) initialVolumePlacement(bucket client.ID, class volume.Class, volumes []*volume.Volume) (*volume.Volume, error) {
	var eligible []*volume.Volume
	var resources []placementResource
	var options []placementOption
	var ids []client.ID
	usage := s.quotaUsage.Load()
	for _, v := range volumes {
		identity := v.Identity()
		active := true
		if s.controller != nil {
			s.mu.RLock()
			managed := s.volumes[identity.VolumeID.String()]
			active = managed != nil && managed.config.State == "active"
			s.mu.RUnlock()
		}
		if !active || v.State() != volume.Active || !class.Allows(identity.Class) {
			continue
		}
		stats := v.Stats()
		own := uint64(0)
		if usage != nil && stats.GuaranteedRemainingPhysical > 0 {
			if rows, known := usage.volumes[identity.VolumeID]; known {
				backing := s.bucketVolumeGuarantee(bucket, client.ID(identity.VolumeID), rows)
				backing = subtractCapacity(backing, s.importReserved(bucket, client.ID(identity.VolumeID)))
				own = min(stats.GuaranteedRemainingPhysical, backing)
			}
		}
		if stats.AdmissionFreePhysical == 0 && own == 0 {
			continue
		}
		node := identity.NodeID
		resources = append(resources, placementResource{Node: client.ID(node), Capacity: stats.CapacityPhysical, Free: stats.AdmissionFreePhysical, Reserved: map[client.ID]uint64{bucket: own}})
		options = append(options, placementOption{Range: 0, Volume: len(eligible), Allocate: 1})
		eligible = append(eligible, v)
		ids = append(ids, client.ID(identity.VolumeID))
	}
	if len(eligible) == 0 {
		return nil, volume.ErrFull
	}
	input := inputForInitialPlacement(len(options))
	var catalog *pgRebuildCatalog
	if s.scopes != nil {
		catalog = s.scopes.pgCatalog.Load()
	}
	input.BackgroundRead, input.BackgroundWrite = placementBackgroundRates(catalog, ids, s.placementHalfLives(), time.Now())
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	demands := []placementDemand{{Bucket: bucket, Copies: 1, Bytes: 1}}
	solution, _ := rebuildPlacement(ctx, demands, resources, options, nil, input, nil, initialPlacementBudget, 1)
	if len(solution.Pending) != 0 || len(solution.Assignments[0]) != 1 {
		return nil, volume.ErrFull
	}
	return eligible[options[solution.Assignments[0][0]].Volume], nil
}
