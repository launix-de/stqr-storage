// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"reflect"

	"github.com/launix-de/stqr-storage/volume"
)

// Fixed metadata names are derived, recoverable indexes of the committed head.
// Open rebuilds them from that head before authenticating users. This keeps
// ordinary object-backed accounts/catalog APIs while a crash between mirror
// writes can never make a partial mirror authoritative at the next startup.
func (s *Server) installSystemView(view *systemView) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.RLock()
	node, _ := parseID(s.cfg.Node.ID)
	cluster, _ := parseID(s.cfg.ClusterID)
	s.mu.RUnlock()
	if view.manifest.Cluster != [16]byte(cluster) {
		return volume.ErrIdentity
	}
	if prior := s.currentSystem.Load(); prior != nil && (prior.manifest.Sequence > view.manifest.Sequence || prior.manifest.Sequence == view.manifest.Sequence && prior.root != view.root) {
		return errControlConflict
	}
	if err := s.activateRecoveredVolumesLocked(view); err != nil {
		return err
	}
	var old accountsImage
	_ = s.readBlob("accounts", &old) // mirrors may be absent/damaged during recovery
	var next accountsImage
	if err := decodeControlRecord(view.accounts, &next); err != nil {
		return err
	}
	list := make([]byte, 0, len(view.manifest.Buckets)*32)
	for _, id := range view.manifest.Buckets {
		list = append(list, id[:]...)
	}
	if err := s.metadataWrite(func(h *volume.Handle) error {
		for _, item := range []struct {
			key  volume.Key
			data []byte
		}{{systemKey("accounts"), view.accounts}, {systemKey("placement-groups"), view.groups}, {BucketCatalogKey(), list}} {
			if _, err := h.Put(item.key, item.data, volume.Options{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	s.placementGroups.Store(view.groupImage)
	s.bucketMu.Lock()
	if s.bucketsByID == nil {
		s.bucketsByID = make(map[volume.ID]*Bucket)
	}
	names := make(map[string]*Bucket)
	for _, d := range view.descriptions {
		id, _ := bucketID(d.ID)
		b := s.bucketsByID[id]
		if b == nil {
			b = &Bucket{server: s}
			s.bucketsByID[id] = b
		}
		b.mu.Lock()
		b.description = cloneBucketDescription(d)
		b.mu.Unlock()
		if !d.DeletionComplete {
			names[d.Name] = b
		}
		s.invalidateBucketConnections(b, d)
	}
	s.bucketsByName = names
	s.bucketMu.Unlock()
	current := make(map[string]account, len(next.Users))
	for _, user := range next.Users {
		current[user.ID] = user
	}
	for _, user := range old.Users {
		if n, exists := current[user.ID]; !exists || !reflect.DeepEqual(user, n) {
			s.invalidateUserConnections(user.ID)
			s.authMu.Lock()
			for token, session := range s.sessions {
				if session.UserID == user.ID {
					delete(s.sessions, token)
				}
			}
			s.authMu.Unlock()
		}
	}
	s.currentSystem.Store(view)
	if err := s.retirePreviousEpochLeases(view.manifest.Membership.Epoch); err != nil {
		return err
	}
	s.dirtyVolumeAuthority()
	if q := s.quotas; q != nil {
		q.mu.Lock()
		q.balances = nil
		q.mu.Unlock()
	}
	s.invalidateFrontendIssuers(view.manifest.Membership)
	s.closeExcludedTransports(view.manifest.Membership)
	if s.scopes != nil {
		s.scopes.mu.Lock()
		err := s.scopes.rebuildLocked()
		s.scopes.mu.Unlock()
		if err != nil {
			return err
		}
	} else {
		s.resyncMovedSubscriptions(view.placement)
		s.placement.Store(view.placement)
	}
	if s.scopes != nil {
		if err := s.ensureVolumeAuthorityLocked(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.cfg.JoinPending = !s.cfg.Node.Draining && !hasControlNode(membershipIDs(view.manifest.Membership), [16]byte(node))
	// Local volume/config changes may already have advanced this counter.
	// Restoring a global checkpoint must never roll it backwards.
	s.cfg.Generation = max(s.cfg.Generation, view.manifest.Generation)
	s.cfg.BucketCatalogVersion, s.cfg.PlacementGroupsVersion = 1, 1
	err := saveConfig(s.configPath, s.cfg)
	s.mu.Unlock()
	return err
}

func (s *Server) exportSystemBundle(root volume.ObjectID) (*systemBundle, error) {
	view, err := s.loadSystemView(root)
	if err != nil {
		return nil, err
	}
	store, err := s.metadataStore()
	if err != nil {
		return nil, err
	}
	h, err := store.Open(false)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	ids := append([]volume.ObjectID{root, view.manifest.Accounts, view.manifest.Groups}, view.manifest.Buckets...)
	for _, ref := range view.manifest.Maps {
		ids = append(ids, ref.Object)
	}
	bundle := &systemBundle{Root: root, Objects: make(map[volume.ObjectID][]byte)}
	for _, id := range ids {
		data, _, err := h.Get(volume.Key{Bucket: systemBucket, Object: id})
		if err != nil {
			return nil, err
		}
		bundle.Objects[id] = data
	}
	return bundle, nil
}
