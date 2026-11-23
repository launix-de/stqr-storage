// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

var defaultPlacementGroup = volume.ID{0xff, 'S', 'T', 'Q', 'R', 'D', 'E', 'F', 'A', 'U', 'L', 'T', 1}

// StoragePolicy is owned by an immutable PG version. BucketPolicy's storage
// fields are a materialized projection for existing object and range codecs.
// Administrative callers cannot edit that projection independently.
type StoragePolicy struct {
	StorageStrategy  string                 `json:"storage_strategy"`
	Compression      string                 `json:"compression"`
	PlacementMode    string                 `json:"placement_mode"`
	Redundancy       BucketRedundancy       `json:"redundancy"`
	ReadAcceleration BucketReadAcceleration `json:"read_acceleration"`
}

func storagePolicy(p BucketPolicy) StoragePolicy {
	return StoragePolicy{p.StorageStrategy, p.Compression, p.PlacementMode, p.Redundancy, p.ReadAcceleration}
}
func (p StoragePolicy) project(b BucketPolicy) BucketPolicy {
	b.StorageStrategy, b.Compression, b.PlacementMode = p.StorageStrategy, p.Compression, p.PlacementMode
	b.Redundancy, b.ReadAcceleration = p.Redundancy, p.ReadAcceleration
	return b
}
func (p StoragePolicy) validate() error {
	return validateBucketPolicy(p.project(DefaultBucketPolicy()))
}
func (p *placementGroupImage) mask(group string, version uint64) (PlacementMask, bool) {
	if p == nil {
		return PlacementMask{}, false
	}
	versions := p.masks[group]
	if version == 0 {
		version = p.effective[group]
	}
	m, ok := versions[version]
	return m, ok
}
func (s *Server) policyForGroup(base BucketPolicy, group string, target bool) (BucketPolicy, error) {
	if group == "" {
		group = defaultPlacementGroup.String()
	}
	image := s.placementGroups.Load()
	m, ok := image.mask(group, 0)
	if target {
		for _, g := range image.object.Groups {
			if g.ID == group && g.Desired != nil {
				m, ok = *g.Desired, true
				break
			}
		}
	}
	if !ok || m.Storage == nil {
		return base, fail(409, "placement group has no central storage policy; upgrade required")
	}
	base = m.Storage.project(base)
	base.PlacementGroup, base.PlacementMaskVersion = group, m.Version
	if group == systemPlacementGroup.String() {
		base.PlacementMaskVersion = 0
	}
	return base, nil
}

// This upgrade writes PGs before publishing references. Re-running it after a
// crash is safe: IDs derive from the old mask and storage contract. Existing
// desired transitions are migrated separately and retain their version/intent.
func (s *Server) upgradePGStorage() error {
	object := s.placementGroups.Load().object
	if object.Schema >= 2 {
		if err := s.validatePGProjections(); err != nil {
			return err
		}
		return s.resumePGFanout()
	}
	raw, _ := json.Marshal(object)
	var detached placementGroupsObject
	if err := json.Unmarshal(raw, &detached); err != nil {
		return err
	}
	object = detached
	defaults := storagePolicy(DefaultBucketPolicy())
	for i := range object.Groups {
		g := &object.Groups[i]
		policy := defaults
		if g.ID == systemPlacementGroup.String() {
			p := DefaultBucketPolicy()
			p.PlacementMode = "everywhere"
			p.Redundancy = BucketRedundancy{Mode: "replication", ACK: "all"}
			policy = storagePolicy(p)
		}
		if g.Effective.Storage == nil {
			g.Effective.Storage = &policy
		}
		if g.Desired != nil {
			p := policy
			if g.Desired.Storage == nil {
				g.Desired.Storage = &p
			}
		}
	}
	found := false
	for _, g := range object.Groups {
		found = found || g.ID == defaultPlacementGroup.String()
	}
	if !found {
		object.Groups = append(object.Groups, PlacementGroup{ID: defaultPlacementGroup.String(), Name: "Standard", Revision: 1, Effective: PlacementMask{Version: 1, NodeSelection: "all", Storage: &defaults}})
	}
	descriptions := s.bucketSnapshots()
	for i := range descriptions {
		d := &descriptions[i]
		for _, p := range []*BucketPolicy{&d.Policy, d.DesiredPolicy} {
			if p == nil {
				continue
			}
			group := p.PlacementGroup
			if group == systemPlacementGroup.String() {
				continue
			}
			mask := PlacementMask{Version: 1, NodeSelection: "all"}
			if group != "" {
				ok := false
				for _, g := range object.Groups {
					if g.ID == group {
						mask = g.Effective
						if p.PlacementMaskVersion != 0 && p.PlacementMaskVersion != mask.Version {
							if g.Desired == nil || g.Desired.Version != p.PlacementMaskVersion {
								return fmt.Errorf("missing legacy PG version")
							}
							mask = *g.Desired
						}
						ok = true
						break
					}
				}
				if !ok {
					return fmt.Errorf("missing legacy PG %s", group)
				}
			}
			policy := storagePolicy(*p)
			if group == "" && policy == defaults {
				p.PlacementGroup = defaultPlacementGroup.String()
				p.PlacementMaskVersion = 1
				continue
			}
			// Preserve a legacy explicit group if its default policy matches. Other
			// combinations get a dedicated central group, shared by equal contracts.
			if group != "" && mask.Storage != nil && policy == *mask.Storage {
				p.PlacementMaskVersion = mask.Version
				continue
			}
			mask.Storage = &policy
			identity, _ := json.Marshal(struct {
				Group string
				Mask  PlacementMask
			}{group, mask})
			sum := sha256.Sum256(append([]byte("stqr-pg-storage-upgrade:"), identity...))
			var id volume.ID
			copy(id[:], sum[:16])
			exists := false
			for _, g := range object.Groups {
				exists = exists || g.ID == id.String()
			}
			if !exists {
				object.Groups = append(object.Groups, PlacementGroup{ID: id.String(), Name: "Übernommen · " + d.ID, Revision: 1, Effective: mask})
			}
			p.PlacementGroup, p.PlacementMaskVersion = id.String(), mask.Version
		}
	}
	// Keep schema 1 until all references have synced; a crash resumes migration.
	if err := s.savePlacementGroups(object); err != nil {
		return err
	}
	s.buckets.upgrading = true
	defer func() { s.buckets.upgrading = false }()
	var changes []BucketDescription
	for _, d := range descriptions {
		b, _ := s.Bucket(mustBucketID(d.ID))
		if !reflect.DeepEqual(d, b.Describe()) {
			changes = append(changes, d)
		}
	}
	if err := s.buckets.saveBatch(changes); err != nil {
		return err
	}
	object.Schema = 2
	if err := s.savePlacementGroups(object); err != nil {
		return err
	}
	return s.resumePGFanout()
}
func mustBucketID(s string) volume.ID { id, _ := bucketID(s); return id }
func (s *Server) validatePGProjections() error {
	for _, d := range s.bucketSnapshots() {
		if d.DeletionComplete {
			continue
		}
		for _, p := range []*BucketPolicy{&d.Policy, d.DesiredPolicy} {
			if p == nil {
				continue
			}
			mask, ok := s.placementGroups.Load().mask(p.PlacementGroup, p.PlacementMaskVersion)
			if !ok || mask.Storage == nil || *mask.Storage != storagePolicy(*p) {
				return fmt.Errorf("bucket %s has inconsistent PG storage projection", d.Name)
			}
		}
	}
	return nil
}

// Legacy flat admin requests select a centrally owned compatibility PG. They
// never overwrite a shared group's settings or persist independent overrides.
func (s *Server) legacyStorageGroup(base BucketPolicy) (BucketPolicy, error) {
	if err := validateBucketPolicy(base); err != nil {
		return base, err
	}
	policy := storagePolicy(base)
	mask := PlacementMask{Version: 1, NodeSelection: "all", Storage: &policy}
	image := s.placementGroups.Load()
	if base.PlacementGroup != "" {
		source, ok := image.mask(base.PlacementGroup, base.PlacementMaskVersion)
		if !ok {
			return base, ErrBucketNotFound
		}
		mask = source
		mask.Version = 1
		mask.Storage = &policy
	}
	standard, ok := image.mask(defaultPlacementGroup.String(), 0)
	if ok && standard.Storage != nil && *standard.Storage == policy && mask.NodeSelection == "all" && standard.NodeSelection == "all" {
		return s.policyForGroup(base, "", false)
	}
	raw, _ := json.Marshal(mask)
	sum := sha256.Sum256(append([]byte("stqr-legacy-storage:"), raw...))
	var id volume.ID
	copy(id[:], sum[:16])
	for _, g := range image.object.Groups {
		if g.ID == id.String() {
			if g.Desired != nil || g.Effective.Storage == nil || *g.Effective.Storage != policy {
				return base, fail(409, "compatibility PG was changed; select a PG explicitly")
			}
			return s.policyForGroup(base, g.ID, false)
		}
	}
	object := image.object
	object.Groups = append(append([]PlacementGroup{}, object.Groups...), PlacementGroup{ID: id.String(), Name: "Kompatibilität · " + policy.StorageStrategy + " / " + policy.Compression, Revision: 1, Effective: mask})
	if err := s.savePlacementGroups(object); err != nil {
		return base, err
	}
	return s.policyForGroup(base, id.String(), false)
}

func (s *Server) checkPGTransition(d BucketDescription, mask PlacementMask) error {
	if mask.Storage == nil {
		return nil
	}
	target := mask.Storage.project(d.Policy)
	if err := bucketLocalPolicy(target); err != nil {
		return err
	}
	if (d.Policy.StorageStrategy == "ram") != (target.StorageStrategy == "ram") {
		b, err := s.Bucket(mustBucketID(d.ID))
		if err != nil {
			return err
		}
		used, err := b.logicalUsage()
		if err != nil {
			return err
		}
		if used != 0 {
			return fail(501, "nonempty RAM/disk policy conversion is not implemented")
		}
	}
	return nil
}

func (s *Server) bindLegacyGroupStorage(id string, policy StoragePolicy) error {
	object := s.placementGroups.Load().object
	object.Groups = append([]PlacementGroup{}, object.Groups...)
	for i, g := range object.Groups {
		if g.ID == id {
			if g.Effective.Storage != nil && *g.Effective.Storage == policy {
				return nil
			}
			if g.Desired != nil {
				return ErrBucketConflict
			}
			for _, d := range s.bucketSnapshots() {
				if !d.DeletionComplete && (d.Policy.PlacementGroup == id || d.DesiredPolicy != nil && d.DesiredPolicy.PlacementGroup == id) {
					return fail(409, "legacy mask already has a different storage contract; use schema 2")
				}
			}
			g.Effective.Storage = &policy
			object.Groups[i] = g
			return s.savePlacementGroups(object)
		}
	}
	return ErrBucketNotFound
}

func (s *Server) finishLocalPlacementGroups() error {
	if s.controller != nil || s.metadataPreview {
		return nil
	}
	object := s.placementGroups.Load().object
	object.Groups = append([]PlacementGroup{}, object.Groups...)
	object.Receipts = append([]placementGroupReceipt{}, object.Receipts...)
	changed := false
	for i, g := range object.Groups {
		if placementMaskReady(g, s.bucketSnapshots()) {
			g.Effective = *g.Desired
			g.Desired = nil
			g.Revision++
			object.Groups[i] = g
			changed = true
			for j := range object.Receipts {
				r := &object.Receipts[j]
				if r.Target == g.ID && r.Running {
					r.Running = false
					r.Status = 200
				}
			}
		}
	}
	if changed {
		return s.savePlacementGroups(object)
	}
	return nil
}

func decodePGLimits(d *client.Decoder) BucketLimits {
	accounting := d.U8()
	p := BucketLimits{Guaranteed: d.U64(), Unlimited: d.Bool(), Absolute: d.U64(), MetadataReservePPM: d.U32()}
	if accounting > 1 {
		d.Err = ErrBucketInvalid
	} else {
		p.Accounting = []string{"logical", "physical"}[accounting]
	}
	return p
}

// Resume a locally interrupted fanout. Cluster administration uses an isolated
// draft and atomically publishes its complete manifest instead.
func (s *Server) resumePGFanout() error {
	for _, g := range s.placementGroups.Load().object.Groups {
		if g.Desired == nil || g.Desired.Storage == nil {
			continue
		}
		for _, d := range s.bucketSnapshots() {
			if d.DeletionComplete || d.Policy.PlacementGroup != g.ID || d.Policy.PlacementMaskVersion == g.Desired.Version || d.DesiredPolicy != nil {
				continue
			}
			if d.State == BucketDeleting {
				return ErrBucketConflict
			}
			if d.Policy.Version == ^uint64(0) {
				return volume.ErrRange
			}
			policy := g.Desired.Storage.project(d.Policy)
			policy.Version++
			policy.PlacementMaskVersion = g.Desired.Version
			d.DesiredPolicy = &policy
			if _, err := s.buckets.Save(d, d.Revision); err != nil {
				return err
			}
		}
	}
	return nil
}
