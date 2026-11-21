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
	Version uint64          `json:"version"`
	Nodes   []PlacementNode `json:"nodes"`
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
	Schema uint16           `json:"schema"`
	Groups []PlacementGroup `json:"groups"`
}

type placementNodeFilter struct {
	mode    VolumeFilterMode
	volumes map[string]bool
}

// Published images are immutable. Object placement does one map lookup per node
// and volume; it never scans or copies a potentially large administrative mask.
type placementGroupImage struct {
	object  placementGroupsObject
	filters map[string]map[string]placementNodeFilter
}

func normalizePlacementMask(mask PlacementMask) (PlacementMask, error) {
	if mask.Version == 0 || len(mask.Nodes) > 10000 {
		return mask, ErrBucketInvalid
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
	if object.Schema != 1 || len(object.Groups) > 10000 {
		return nil, ErrBucketInvalid
	}
	object.Groups = append([]PlacementGroup{}, object.Groups...)
	image := &placementGroupImage{filters: make(map[string]map[string]placementNodeFilter)}
	for i := range object.Groups {
		g := &object.Groups[i]
		id, err := bucketID(g.ID)
		if err != nil || g.Revision == 0 || g.Name == "" || len(g.Name) > 255 || !utf8.ValidString(g.Name) || strings.ContainsRune(g.Name, 0) || image.filters[id.String()] != nil {
			return nil, ErrBucketInvalid
		}
		g.ID = id.String()
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
		nodes := make(map[string]placementNodeFilter)
		for _, n := range g.Effective.Nodes {
			f := placementNodeFilter{mode: n.Mode, volumes: make(map[string]bool)}
			for _, id := range n.Volumes {
				f.volumes[id] = true
			}
			nodes[n.NodeID] = f
		}
		image.filters[g.ID] = nodes
	}
	sort.Slice(object.Groups, func(i, j int) bool { return object.Groups[i].ID < object.Groups[j].ID })
	image.object = object
	return image, nil
}

func (p *placementGroupImage) allows(group, node, vol string) bool {
	if group == "" {
		return true
	}
	if p == nil {
		return false
	}
	f, ok := p.filters[group][node]
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	image := s.placementGroups.Load()
	result := []*volume.Volume{}
	for id, v := range s.volumes {
		if group == "" {
			if v.config.System {
				continue // unrestricted data-volume pool
			}
		} else if !image.allows(group, s.cfg.Node.ID, id) {
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
