// SPDX-License-Identifier: GPL-3.0-only
package server

func (s *Server) validateVolumeGroupSelection(groups []string) error {
	if len(groups) > 10000 {
		return ErrBucketInvalid
	}
	seen := map[string]bool{}
	for _, id := range groups {
		if _, err := bucketID(id); err != nil || seen[id] || id == systemPlacementGroup.String() {
			return ErrBucketInvalid
		}
		seen[id] = true
		mask, ok := s.placementGroups.Load().mask(id, 0)
		if !ok || mask.NodeSelection == "all" {
			return ErrBucketInvalid
		}
		found := false
		for _, node := range mask.Nodes {
			if node.NodeID == s.cfg.Node.ID && (node.Mode == VolumesBlacklist || node.Mode == VolumesWhitelist) {
				found = true
			}
		}
		if !found {
			return fail(409, "selected PG has no explicit volume filter for this node")
		}
	}
	return nil
}

// Held devices are excluded locally and advertised as PENDING to planners.
// Release requires every selected list to have become effective, so a blacklist
// cannot briefly admit the device before its global metadata commit completes.
func (s *Server) releaseVolumePlacement(operation, id string) (Operation, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.forceFrozen() || s.clusterPaused() {
		return Operation{}, fail(503, "cluster administration is paused")
	}
	s.mu.RLock()
	managed := s.volumes[id]
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	if managed == nil || managed.config.System {
		return Operation{}, ErrBucketNotFound
	}
	for _, group := range managed.config.PlacementGroupsPending {
		mask, ok := s.placementGroups.Load().mask(group, 0)
		if !ok {
			return Operation{}, ErrBucketNotFound
		}
		// An old whitelist still excludes a new device while a committed
		// desired whitelist can already receive migrations into that device.
		// A blacklist must exclude it in the effective version first.
		oldWhitelist := false
		for _, node := range mask.Nodes {
			oldWhitelist = oldWhitelist || node.NodeID == cfg.Node.ID && node.Mode == VolumesWhitelist
		}
		if oldWhitelist {
			for _, g := range s.placementGroups.Load().object.Groups {
				if g.ID == group && g.Desired != nil {
					for _, node := range g.Desired.Nodes {
						if node.NodeID == cfg.Node.ID && node.Mode == VolumesWhitelist {
							for _, volume := range node.Volumes {
								if volume == id {
									mask = *g.Desired
								}
							}
						}
					}
				}
			}
		}
		found := false
		for _, node := range mask.Nodes {
			if node.NodeID == cfg.Node.ID && (node.Mode == VolumesBlacklist || node.Mode == VolumesWhitelist) {
				for _, v := range node.Volumes {
					found = found || v == id
				}
			}
		}
		if !found {
			return Operation{}, fail(409, "PG list changes are still pending; volume remains unavailable for placement")
		}
	}
	op, exists, err := s.beginOperation(operation, "placement-release", id)
	if err != nil {
		return op, err
	}
	if exists && op.State == "fulfilled" {
		return op, nil
	}
	for i := range cfg.Volumes {
		if cfg.Volumes[i].ID == id {
			cfg.Volumes[i].PlacementGroupsPending = nil
		}
	}
	if err := saveConfig(s.configPath, cfg); err != nil {
		return op, err
	}
	s.mu.Lock()
	s.cfg = cfg
	managed.config.PlacementGroupsPending = nil
	s.mu.Unlock()
	s.dirtyVolumeAuthority()
	s.notifyStats()
	op.VolumeID = id
	return s.finishOperation(op, nil)
}
