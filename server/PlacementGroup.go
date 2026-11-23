// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/launix-de/stqr-storage/volume"
)

var systemPlacementGroup = volume.ID{0x53, 0x54, 0x51, 0x52, 0x53, 0x59, 0x53, 0x50, 0x47, 1}

type VolumeFilterMode string

const (
	VolumesAll       VolumeFilterMode = "all"
	VolumesBlacklist VolumeFilterMode = "blacklist"
	VolumesWhitelist VolumeFilterMode = "whitelist"
)

// PlacementNode uses stable identities, including unavailable nodes/devices.
// Empty whitelist allows nothing; empty blacklist excludes nothing. The filter
// is a mask only: class, capacity, state and replica count are separate checks.
type PlacementNode struct {
	NodeID  string           `json:"node_id"`
	Mode    VolumeFilterMode `json:"volume_filter"`
	Volumes []string         `json:"volumes"`
}

type PlacementMask struct {
	Version        uint64          `json:"version"`
	NodeSelection  string          `json:"node_selection,omitempty"` // empty/list: explicit nodes; all: unrestricted data volumes
	Storage        *StoragePolicy  `json:"storage,omitempty"`
	FailureDomains []string        `json:"failure_domains,omitempty"`
	Nodes          []PlacementNode `json:"nodes"`
}

// Desired is persisted planning input. Only a committed generation may replace
// Effective; merely storing a desired mask must never relocate live objects.
type PlacementGroup struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Revision  uint64         `json:"revision"`
	Effective PlacementMask  `json:"effective"`
	Desired   *PlacementMask `json:"desired,omitempty"`
}

type placementGroupsObject struct {
	Schema   uint16                  `json:"schema"`
	Groups   []PlacementGroup        `json:"groups"`
	Receipts []placementGroupReceipt `json:"receipts,omitempty"`
}

type placementNodeFilter struct {
	mode    VolumeFilterMode
	volumes map[string]bool
}

// Published images are immutable. Object placement does one map lookup per node
// and volume; it never scans or copies a potentially large administrative mask.
type placementGroupImage struct {
	object    placementGroupsObject
	filters   map[string]map[string]placementNodeFilter
	versions  map[string]map[uint64]map[string]placementNodeFilter
	masks     map[string]map[uint64]PlacementMask
	effective map[string]uint64
}

func normalizePlacementMask(mask PlacementMask) (PlacementMask, error) {
	if mask.Version == 0 || len(mask.Nodes) > 10000 || mask.NodeSelection != "" && mask.NodeSelection != "list" && mask.NodeSelection != "all" || mask.NodeSelection == "all" && len(mask.Nodes) != 0 {
		return mask, ErrBucketInvalid
	}
	mask.FailureDomains = append([]string(nil), mask.FailureDomains...)
	sort.Strings(mask.FailureDomains)
	for i, domain := range mask.FailureDomains {
		if domain != "node" && domain != "host" && domain != "rack" && domain != "zone" || i > 0 && mask.FailureDomains[i-1] == domain {
			return mask, ErrBucketInvalid
		}
	}
	if mask.Storage != nil {
		policy := *mask.Storage
		if err := policy.validate(); err != nil {
			return mask, err
		}
		mask.Storage = &policy
	}
	mask.Nodes = append([]PlacementNode{}, mask.Nodes...)
	for i := range mask.Nodes {
		n := &mask.Nodes[i]
		id, err := bucketID(n.NodeID)
		if err != nil {
			return mask, ErrBucketInvalid
		}
		n.NodeID = id.String()
		if n.Mode == "" {
			n.Mode = VolumesAll
		}
		if n.Mode != VolumesAll && n.Mode != VolumesBlacklist && n.Mode != VolumesWhitelist {
			return mask, ErrBucketInvalid
		}
		n.Volumes = append([]string{}, n.Volumes...)
		for i, value := range n.Volumes {
			id, err := bucketID(value)
			if err != nil {
				return mask, ErrBucketInvalid
			}
			n.Volumes[i] = id.String()
		}
		sort.Strings(n.Volumes)
		for i := 1; i < len(n.Volumes); i++ {
			if n.Volumes[i] == n.Volumes[i-1] {
				return mask, ErrBucketInvalid
			}
		}
	}
	sort.Slice(mask.Nodes, func(i, j int) bool { return mask.Nodes[i].NodeID < mask.Nodes[j].NodeID })
	for i := 1; i < len(mask.Nodes); i++ {
		if mask.Nodes[i].NodeID == mask.Nodes[i-1].NodeID {
			return mask, ErrBucketInvalid
		}
	}
	return mask, nil
}

func compilePlacementGroups(object placementGroupsObject) (*placementGroupImage, error) {
	if object.Schema != 1 && object.Schema != 2 || len(object.Groups) > 10000 {
		return nil, ErrBucketInvalid
	}
	object.Groups = append([]PlacementGroup{}, object.Groups...)
	object.Receipts = append([]placementGroupReceipt(nil), object.Receipts...)
	seenReceipts := make(map[string]bool, len(object.Receipts))
	for _, r := range object.Receipts {
		if _, err := bucketID(r.ID); err != nil {
			return nil, ErrBucketInvalid
		}
		if _, err := bucketID(r.Target); err != nil {
			return nil, ErrBucketInvalid
		}
		if _, err := bucketID(r.Actor); err != nil {
			return nil, ErrBucketInvalid
		}
		if seenReceipts[r.ID] || len(r.Digest) != 64 || r.Opcode != placementGroupSet && r.Opcode != placementGroupDelete || r.Status != 200 && r.Status != 201 && r.Status != 202 || r.Running != (r.Status == 202) {
			return nil, ErrBucketInvalid
		}
		seenReceipts[r.ID] = true
	}
	image := &placementGroupImage{filters: make(map[string]map[string]placementNodeFilter)}
	image.versions = make(map[string]map[uint64]map[string]placementNodeFilter)
	image.masks = make(map[string]map[uint64]PlacementMask)
	image.effective = make(map[string]uint64)
	for i := range object.Groups {
		g := &object.Groups[i]
		id, err := bucketID(g.ID)
		if err != nil || g.Revision == 0 || g.Name == "" || len(g.Name) > 255 || !utf8.ValidString(g.Name) || strings.ContainsRune(g.Name, 0) || image.filters[id.String()] != nil {
			return nil, ErrBucketInvalid
		}
		g.ID = id.String()
		if g.ID == systemPlacementGroup.String() && (g.Effective.NodeSelection == "all" || g.Desired != nil && g.Desired.NodeSelection == "all") {
			return nil, ErrBucketInvalid
		}
		if g.Effective, err = normalizePlacementMask(g.Effective); err != nil {
			return nil, err
		}
		if g.Desired != nil {
			desired, err := normalizePlacementMask(*g.Desired)
			if err != nil || desired.Version <= g.Effective.Version {
				return nil, ErrBucketInvalid
			}
			g.Desired = &desired
		}
		image.masks[g.ID] = map[uint64]PlacementMask{g.Effective.Version: g.Effective}
		image.effective[g.ID] = g.Effective.Version
		if g.Desired != nil {
			image.masks[g.ID][g.Desired.Version] = *g.Desired
		}
		nodes := make(map[string]placementNodeFilter)
		for _, n := range g.Effective.Nodes {
			f := placementNodeFilter{mode: n.Mode, volumes: make(map[string]bool)}
			for _, id := range n.Volumes {
				f.volumes[id] = true
			}
			nodes[n.NodeID] = f
		}
		image.filters[g.ID] = nodes
		image.versions[g.ID] = map[uint64]map[string]placementNodeFilter{g.Effective.Version: nodes}
		if g.Desired != nil {
			desired := make(map[string]placementNodeFilter)
			for _, n := range g.Desired.Nodes {
				f := placementNodeFilter{mode: n.Mode, volumes: make(map[string]bool)}
				for _, id := range n.Volumes {
					f.volumes[id] = true
				}
				desired[n.NodeID] = f
			}
			image.versions[g.ID][g.Desired.Version] = desired
		}
	}
	sort.Slice(object.Groups, func(i, j int) bool { return object.Groups[i].ID < object.Groups[j].ID })
	image.object = object
	return image, nil
}

func (p *placementGroupImage) allows(group, node, vol string) bool {
	return p.allowsVersion(group, 0, node, vol)
}

func (p *placementGroupImage) allowsVersion(group string, version uint64, node, vol string) bool {
	if group == "" {
		return true
	}
	if p == nil {
		return false
	}
	// Data PG masks select data volumes. In particular an empty blacklist
	// must not silently spill payload into the System-PG's metadata devices.
	if group != systemPlacementGroup.String() && p.allowsVersion(systemPlacementGroup.String(), 0, node, vol) {
		return false
	}
	if mask, ok := p.mask(group, version); ok && mask.NodeSelection == "all" {
		return group != systemPlacementGroup.String() && !p.allowsVersion(systemPlacementGroup.String(), 0, node, vol)
	}
	nodes := p.filters[group]
	if version != 0 {
		nodes = p.versions[group][version]
	}
	f, ok := nodes[node]
	if !ok {
		return false
	}
	switch f.mode {
	case VolumesAll:
		return true
	case VolumesBlacklist:
		return !f.volumes[vol]
	case VolumesWhitelist:
		return f.volumes[vol]
	}
	return false
}

func (s *Server) placementVolumes(group string) []*volume.Volume {
	return s.placementVolumesAt(group, 0)
}

func (s *Server) placementVolumesAt(group string, version uint64) []*volume.Volume {
	s.mu.RLock()
	defer s.mu.RUnlock()
	image := s.placementGroups.Load()
	result := []*volume.Volume{}
	for id, v := range s.volumes {
		if len(v.config.PlacementGroupsPending) > 0 {
			continue
		}
		if group == "" {
			if v.config.System {
				continue // unrestricted data-volume pool
			}
		} else if mask, ok := image.mask(group, version); ok && mask.NodeSelection == "all" && v.config.System {
			continue
		} else if !image.allowsVersion(group, version, s.cfg.Node.ID, id) {
			continue
		}
		result = append(result, v.store)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Identity().VolumeID.String() < result[j].Identity().VolumeID.String()
	})
	return result
}

func (s *Server) metadataStore() (*volume.Volume, error) {
	// A voluntary leaver retains its bound recovery anchor solely to finish
	// the already committed ALL barrier. It is no longer a system replica.
	if s.retiring() {
		return s.system, nil
	}
	stores := s.placementVolumes(systemPlacementGroup.String())
	if len(stores) != 1 {
		return nil, fail(503, "metadata placement group requires one committed local store")
	}
	class := stores[0].Identity().Class
	if class != volume.HDD && class != volume.SSD {
		return nil, fail(503, "metadata placement group requires persistent storage")
	}
	return stores[0], nil
}

func (s *Server) bootstrapPlacementGroups() error {
	// Bootstrap anchor: the node's exclusively claimed root volume supplies the
	// initial PG mask needed to read the PG object itself. Afterwards the persisted
	// effective mask is authoritative, with no bucket-ID-to-volume special case.
	root := PlacementGroup{ID: systemPlacementGroup.String(), Name: "system", Revision: 1, Effective: PlacementMask{Version: 1, Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesWhitelist, Volumes: []string{s.system.Identity().VolumeID.String()}}}}}
	bootstrap, err := compilePlacementGroups(placementGroupsObject{Schema: 1, Groups: []PlacementGroup{root}})
	if err != nil {
		return err
	}
	s.placementGroups.Store(bootstrap)
	return nil
}

func (s *Server) initPlacementGroups() error {
	bootstrap := s.placementGroups.Load()
	var object placementGroupsObject
	err := s.readBlob("placement-groups", &object)
	if errors.Is(err, volume.ErrNotFound) && s.cfg.PlacementGroupsVersion == 0 {
		object = bootstrap.object
		err = s.writeBlob("placement-groups", object)
	}
	if err != nil {
		return err
	}
	if s.cfg.PlacementGroupsVersion > 1 {
		return errors.New("unsupported placement-group catalog")
	}
	image, err := compilePlacementGroups(object)
	if err != nil {
		return err
	}
	s.placementGroups.Store(image)
	store, err := s.metadataStore()
	if err != nil || store != s.system {
		return errors.New("metadata PG and bootstrap anchor disagree; committed handoff required")
	}
	s.cfg.PlacementGroupsVersion = 1
	return nil
}
