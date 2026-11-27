// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type stagedRecoveryVolume struct {
	config VolumeConfig
	store  *volume.Volume
}

func (s *Server) openStagedRecoveryVolumes() error {
	s.recoveryVolumes = make(map[volume.ID]*stagedRecoveryVolume)
	for _, cfg := range s.cfg.RecoveryVolumes {
		id, err := parseID(cfg.ID)
		if err != nil {
			return err
		}
		if cfg.OriginNode == "" || s.volumes[cfg.ID] != nil || s.recoveryVolumes[id] != nil {
			return volume.ErrCorrupt
		}
		dev, err := volume.OpenFile(cfg.Path)
		if err != nil {
			return err
		}
		store, err := volume.Open(dev, volume.OpenOptions{})
		if err != nil {
			dev.Close()
			return err
		}
		identity := store.Identity()
		if identity.VolumeID != id || identity.ClusterID.String() != s.cfg.ClusterID || (identity.NodeID.String() != cfg.OriginNode && identity.NodeID.String() != s.cfg.Node.ID) {
			store.Close()
			return volume.ErrIdentity
		}
		if _, err := readVolumeAuthority(store); err != nil {
			store.Close()
			return err
		}
		s.recoveryVolumes[id] = &stagedRecoveryVolume{config: cfg, store: store}
	}
	return nil
}

// StageRecoveryVolume opens an existing formatted file/device exclusively. It
// never creates, reformats, binds, serves or modifies it. Origin metadata remains
// on the original disk until a durable ALL-approved takeover is installed.
func (s *Server) StageRecoveryVolume(name, path string) (volume.ID, error) {
	return s.stageRecoveryVolume(name, path, nil)
}
func (s *Server) stageRecoveryVolume(name, path string, actor *principal) (volume.ID, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	return s.stageRecoveryVolumeLocked(name, path, actor, "")
}

func (s *Server) stageRecoveryVolumeLocked(name, path string, actor *principal, operation string) (volume.ID, error) {
	if actor != nil {
		if err := s.requireAdmin(*actor); err != nil {
			return volume.ID{}, err
		}
	}
	if s.forceFrozen() || s.shortStopping.Load() {
		return volume.ID{}, fail(503, "recovery administration is paused")
	}
	if !validName.MatchString(name) || name == "admin" {
		return volume.ID{}, fail(400, "choose a valid recovery volume name")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return volume.ID{}, err
	}
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	for _, v := range cfg.Volumes {
		if v.Name == name || v.Path == absolute {
			return volume.ID{}, fail(409, "volume name or path is already attached")
		}
	}
	for _, v := range cfg.RecoveryVolumes {
		if v.Path == absolute && v.Name == name {
			return parseID(v.ID)
		}
		if v.Name == name || v.Path == absolute {
			return volume.ID{}, fail(409, "recovery volume name or path is already staged")
		}
	}
	dev, err := volume.OpenFile(absolute)
	if err != nil {
		return volume.ID{}, err
	}
	header, err := volume.ReadHeader(dev)
	if err != nil {
		dev.Close()
		return volume.ID{}, err
	}
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(view.manifest.Membership.Excluded, client.ID(header.Identity.NodeID)) {
		dev.Close()
		return volume.ID{}, fail(409, "begin fenced node recovery before staging its volumes")
	}
	store, err := volume.Open(dev, volume.OpenOptions{})
	if err != nil {
		dev.Close()
		return volume.ID{}, err
	}
	keep := false
	defer func() {
		if !keep {
			store.Close()
		}
	}()
	identity := store.Identity()
	if identity.Class == volume.RAM || identity.Class == volume.RAMCache || identity.ClusterID.String() != cfg.ClusterID || identity.NodeID == (volume.ID{}) || identity.NodeID.String() == cfg.Node.ID {
		return volume.ID{}, fail(409, "recovery requires a persistent foreign volume from this cluster")
	}
	authority, err := readVolumeAuthority(store)
	if errors.Is(err, volume.ErrNotFound) {
		return volume.ID{}, fail(409, "volume has no recovery provenance; preserve its original node config/journals")
	}
	if err != nil {
		return volume.ID{}, err
	}
	if authority.Node != client.ID(identity.NodeID) {
		return volume.ID{}, volume.ErrIdentity
	}
	entry := VolumeConfig{ID: identity.VolumeID.String(), Name: name, Path: absolute, Class: identity.Class.String(), SizeBytes: int64(identity.Size), State: "recovery", OriginNode: identity.NodeID.String(), RecoveryOnly: true, OperationID: operation}
	cfg.RecoveryVolumes = append(cfg.RecoveryVolumes, entry)
	if err := saveConfig(s.configPath, cfg); err != nil {
		return volume.ID{}, err
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	s.recoveryVolumes[identity.VolumeID] = &stagedRecoveryVolume{config: entry, store: store}
	keep = true
	return identity.VolumeID, nil
}

// All members of an unpinned source cohort must agree on the same active old
// ownership. An index on one rescued disk does not prove a missing peer disk
// was empty. A source FENCE requires the exact target cancellation sealed into
// this origin by ALL survivors; an unexplained fence is never guessed away.
func (s *Server) validateRecoveryCohort(origin volumeRecoveryOrigin, selected []client.ID) ([]client.ID, error) {
	if origin.Complete || origin.After.State != 4 || len(origin.Before.Entries) != 1 {
		return nil, errControlConflict
	}
	before := origin.Before.Entries[0]
	if len(before.Locations) != 1 || before.Runtime != (client.ID{}) {
		return nil, errControlConflict
	}
	var cohort []client.ID
	for _, id := range selected {
		store := s.recoveryStore(volume.ID(id))
		if store == nil {
			continue
		}
		identity := store.Identity()
		if identity.ClusterID.String() != s.cfg.ClusterID {
			continue
		}
		record, err := readVolumeAuthority(store)
		if err != nil {
			return nil, err
		}
		if record.Node != client.ID(identity.NodeID) {
			return nil, volume.ErrIdentity
		}
		for _, claim := range record.Entries {
			if claim.Bucket != origin.Before.Bucket || !sameRangeAuthority(claim.Entry, before) {
				continue
			}
			if !origin.acceptsVolumeClaim(claim) {
				return nil, fail(409, "rescued range has an unresolved migration fence")
			}
			if len(claim.Cohort) == 0 || !hasControlNode(claim.Cohort, id) {
				return nil, volume.ErrCorrupt
			}
			if cohort == nil {
				cohort = append([]client.ID(nil), claim.Cohort...)
			} else if !reflect.DeepEqual(cohort, claim.Cohort) {
				return nil, fail(409, "rescued volume cohort witnesses disagree")
			}
		}
	}
	if len(cohort) == 0 {
		return nil, fail(409, "no matching active volume provenance for this range")
	}
	if before.Locations[0].Volume != (client.ID{}) && (len(cohort) != 1 || cohort[0] != before.Locations[0].Volume) {
		return nil, volume.ErrCorrupt
	}
	b, err := s.Bucket(volume.ID(origin.Before.Bucket))
	if err != nil {
		return nil, err
	}
	description := b.Describe()
	for i, id := range cohort {
		if i > 0 && cohort[i-1].String() >= id.String() {
			return nil, volume.ErrCorrupt
		}
		if !hasControlNode(selected, id) {
			return nil, fail(409, fmt.Sprintf("recovery needs volume %s from the same cohort", id.String()))
		}
		store := s.recoveryStore(volume.ID(id))
		if store == nil {
			return nil, fail(409, "required recovery volume is not staged")
		}
		if !bucketClass(description.Policy).Allows(store.Identity().Class) || !s.placementGroups.Load().allowsVersion(description.Policy.PlacementGroup, description.Policy.PlacementMaskVersion, s.cfg.Node.ID, id.String()) {
			return nil, fail(507, "recovered storage does not meet the bucket placement policy")
		}
		record, err := readVolumeAuthority(store)
		if err != nil {
			return nil, err
		}
		found := false
		for _, claim := range record.Entries {
			if claim.Bucket == origin.Before.Bucket && sameRangeAuthority(claim.Entry, before) && origin.acceptsVolumeClaim(claim) && reflect.DeepEqual(claim.Cohort, cohort) {
				found = true
			}
		}
		if !found {
			return nil, fail(409, "cohort member lacks the same active range witness")
		}
	}
	sort.Slice(cohort, func(i, j int) bool { return cohort[i].String() < cohort[j].String() })
	return cohort, nil
}

// Only installSystemView invokes activation, after durable COMMIT. Saving the
// local config precedes exposing any drive in the normal volume map. If a crash
// interrupts reassignment, staged opening accepts the new owner only to resume
// the same committed system head; it still cannot serve that volume on its own.
func (s *Server) recoveryStore(id volume.ID) *volume.Volume {
	if staged := s.recoveryVolumes[id]; staged != nil {
		return staged.store
	}
	if managed := s.volumes[id.String()]; managed != nil && managed.config.RecoveryOnly {
		return managed.store
	}
	return nil
}

func (s *Server) activateRecoveredVolumesLocked(view *systemView) error {
	l := s.control.ledger
	l.mu.Lock()
	var takeover *volumeTakeover
	if slot := l.slot; slot != nil && slot.Proposal != nil && slot.Proposal.Image == [32]byte(view.root) && slot.Committed.RecordSequence != 0 && slot.Applied.RecordSequence == 0 && slot.Claim.Exclusion != nil {
		takeover = slot.Claim.Exclusion.Restore
	}
	l.mu.Unlock()
	needed := map[volume.ID]bool{}
	for _, origin := range view.manifest.VolumeRecoveries {
		if origin.Complete && origin.Target == l.node {
			for _, id := range origin.Volumes {
				needed[volume.ID(id)] = true
			}
		}
	}
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	var activated []*stagedRecoveryVolume
	for id := range needed {
		store := s.recoveryStore(id)
		required := takeover != nil && hasControlNode(takeover.Volumes, client.ID(id))
		if store == nil {
			if required {
				return fail(503, "committed recovered volume is unavailable")
			}
			continue
		}
		if required {
			record, err := readVolumeAuthority(store)
			if err != nil {
				return err
			}
			record.Node = l.node
			for _, origin := range view.manifest.VolumeRecoveries {
				selected := false
				for _, originID := range takeover.Origins {
					if origin.ID == originID {
						selected = true
					}
				}
				if !selected || !hasControlNode(origin.Volumes, client.ID(id)) {
					continue
				}
				var kept []volumeAuthorityEntry
				for _, claim := range record.Entries {
					if claim.Bucket == origin.Before.Bucket && (sameRangeAuthority(claim.Entry, origin.Before.Entries[0]) || sameRangeAuthority(claim.Entry, origin.After)) {
						continue
					}
					kept = append(kept, claim)
				}
				record.Entries = append(kept, volumeAuthorityEntry{Bucket: origin.Before.Bucket, Entry: origin.After, Cohort: origin.Volumes, Active: true})
			}
			compact, err := authorityEncodingForStore(store, s.compactAuthorityReady())
			if err != nil {
				return err
			}
			raw, err := marshalVolumeAuthority(record, compact)
			if err != nil {
				return err
			}
			if err := s.reassignVolumeAuthorityLocked(store, record, volume.ID(l.node), raw); err != nil {
				return err
			}
		}
		if s.volumes[id.String()] != nil {
			continue
		}
		staged := s.recoveryVolumes[id]
		if store.Identity().NodeID != volume.ID(l.node) {
			return fail(503, "staged volume has not committed its new owner")
		}
		entry := staged.config
		entry.State = "active"
		entry.RecoveryOnly = true
		cfg.Volumes = append(cfg.Volumes, entry)
		activated = append(activated, staged)
	}
	if len(activated) == 0 {
		return nil
	}
	var remaining []VolumeConfig
	for _, entry := range cfg.RecoveryVolumes {
		found := false
		for _, staged := range activated {
			if entry.ID == staged.config.ID {
				found = true
			}
		}
		if !found {
			remaining = append(remaining, entry)
		}
	}
	cfg.RecoveryVolumes = remaining
	if err := saveConfig(s.configPath, cfg); err != nil {
		return err
	}
	s.mu.Lock()
	s.cfg = cfg
	for _, staged := range activated {
		entry := staged.config
		entry.State = "active"
		s.volumes[entry.ID] = &managedVolume{config: entry, store: staged.store}
		delete(s.recoveryVolumes, staged.store.Identity().VolumeID)
	}
	s.mu.Unlock()
	s.dirtyVolumeAuthority()
	for _, staged := range activated {
		s.startVolumeMembership(staged.store)
	}
	return nil
}
