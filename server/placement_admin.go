// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	placementGroupList      = client.PlacementGroupList
	placementGroupSet       = client.PlacementGroupSet
	placementGroupDelete    = client.PlacementGroupDelete
	bucketSetPlacementGroup = client.BucketSetPlacementGroup
)

type placementGroupReceipt struct {
	BucketReceipt
	Target string `json:"target"`
}

func placementAdminOpcode(op uint16) bool {
	return op >= placementGroupList && op <= bucketSetPlacementGroup
}

func (s *Server) savePlacementGroups(object placementGroupsObject) error {
	image, err := compilePlacementGroups(object)
	if err != nil {
		return err
	}
	if err := s.writeBlob("placement-groups", image.object); err != nil {
		return err
	}
	s.placementGroups.Store(image)
	return nil
}

// Administrative placement uses its own versioned wire operations. Object
// clients keep their existing Policy/Map bytes; the effective mask version is
// a server-side policy reference, never an implicit physical ownership change.
func (s *Server) executePlacementAdmin(m client.Message, p principal) ([]byte, int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.requireAdmin(p); err != nil {
		return nil, 0, err
	}
	d := client.Decoder{B: m.Body}
	if d.U16() != 1 {
		return nil, 0, client.ErrProtocol
	}
	object := s.placementGroups.Load().object
	if m.Opcode == placementGroupList {
		after, limit := d.Text(), d.U32()
		if d.Done() != nil || limit == 0 || limit > 1000 {
			return nil, 0, client.ErrProtocol
		}
		page := []PlacementGroup{}
		next := ""
		budget := client.MaxMessage - 1024
		for _, g := range object.Groups {
			if g.ID <= after {
				continue
			}
			if len(page) == int(limit) {
				next = page[len(page)-1].ID
				break
			}
			raw, err := json.Marshal(g)
			if err != nil {
				return nil, 0, err
			}
			if len(raw) > budget {
				if len(page) == 0 {
					return nil, 0, fail(413, "placement group exceeds response limit")
				}
				next = page[len(page)-1].ID
				break
			}
			budget -= len(raw) + 1
			page = append(page, g)
		}
		return jsonText(struct {
			Groups []PlacementGroup `json:"groups"`
			Next   string           `json:"next"`
		}{page, next}), 200, nil
	}
	if m.Kind != client.Request || m.ID == (client.ID{}) {
		return nil, 0, fail(400, "placement changes require a request ID")
	}
	receipt, replayed, err := s.bucketReceipt(p, m)
	if err != nil {
		return nil, 0, err
	}
	if replayed != nil {
		return encodeBucketAck(receipt.ID, replayed.ID(), receipt.Running), receipt.Status, nil
	}
	for _, prior := range object.Receipts {
		if prior.ID == receipt.ID {
			if prior.Actor != receipt.Actor || prior.Digest != receipt.Digest {
				return nil, 0, fail(400, "operation ID reused with different input")
			}
			id, _ := bucketID(prior.Target)
			return encodeBucketAck(prior.ID, id, prior.Running), prior.Status, nil
		}
	}
	if m.Opcode == bucketSetPlacementGroup {
		bucket, expected := volume.ID(d.ID()), d.U64()
		group := ""
		if d.Bool() {
			group = d.ID().String()
		}
		if d.Done() != nil || bucket == systemBucket {
			return nil, 0, ErrBucketInvalid
		}
		b, err := s.Bucket(bucket)
		if err != nil {
			return nil, 0, err
		}
		description := b.Describe()
		if description.DesiredPolicy != nil || description.Policy.Version != expected || description.State == BucketDeleting {
			return nil, 0, ErrBucketConflict
		}
		maskVersion := uint64(0)
		if group != "" {
			found := false
			for _, g := range object.Groups {
				if g.ID == group {
					if g.Desired != nil {
						return nil, 0, ErrBucketConflict
					}
					maskVersion = g.Effective.Version
					found = true
					break
				}
			}
			if !found {
				return nil, 0, ErrBucketNotFound
			}
			// The reserved bootstrap PG follows committed membership additions
			// and voluntary leaves. Its per-node anchors remain stable; ordinary
			// user-edited groups bind explicit effective/desired mask versions.
			if group == systemPlacementGroup.String() {
				maskVersion = 0
			}
		}
		if description.Policy.PlacementGroup != group || description.Policy.PlacementMaskVersion != maskVersion {
			if description.Policy.Version == ^uint64(0) {
				return nil, 0, volume.ErrRange
			}
			policy := description.Policy
			policy.Version++
			policy.PlacementGroup, policy.PlacementMaskVersion = group, maskVersion
			description.DesiredPolicy = &policy
			receipt.Running, receipt.Status = true, 202
		}
		description.Receipts = append(description.Receipts, receipt)
		if _, err := s.buckets.Save(description, description.Revision); err != nil {
			return nil, 0, err
		}
		return encodeBucketAck(receipt.ID, bucket, receipt.Running), receipt.Status, nil
	}
	id, expected := volume.ID(d.ID()), d.U64()
	if id == systemPlacementGroup || id == (volume.ID{}) {
		return nil, 0, ErrBucketForbidden
	}
	object.Groups = append([]PlacementGroup{}, object.Groups...)
	object.Receipts = append([]placementGroupReceipt{}, object.Receipts...)
	index := -1
	for i, g := range object.Groups {
		if g.ID == id.String() {
			index = i
			break
		}
	}
	if m.Opcode == placementGroupDelete {
		if d.Done() != nil {
			return nil, 0, client.ErrProtocol
		}
		if index < 0 {
			return nil, 0, ErrBucketNotFound
		}
		g := object.Groups[index]
		if g.Revision != expected || g.Desired != nil {
			return nil, 0, ErrBucketConflict
		}
		for _, description := range s.bucketSnapshots() {
			if !description.DeletionComplete && (description.Policy.PlacementGroup == g.ID || description.DesiredPolicy != nil && description.DesiredPolicy.PlacementGroup == g.ID) {
				return nil, 0, fail(409, "placement group is still referenced")
			}
		}
		object.Groups = append(object.Groups[:index], object.Groups[index+1:]...)
	} else if m.Opcode == placementGroupSet {
		name := d.Text()
		var nodes []PlacementNode
		if err := decodeControlRecord(d.Blob(), &nodes); err != nil || d.Done() != nil {
			return nil, 0, client.ErrProtocol
		}
		if index < 0 {
			if expected != 0 {
				return nil, 0, ErrBucketConflict
			}
			object.Groups = append(object.Groups, PlacementGroup{ID: id.String(), Name: name, Revision: 1, Effective: PlacementMask{Version: 1, Nodes: nodes}})
			receipt.Status = 201
		} else {
			g := object.Groups[index]
			if g.Revision != expected || g.Desired != nil {
				return nil, 0, ErrBucketConflict
			}
			if g.Revision == ^uint64(0) || g.Effective.Version == ^uint64(0) {
				return nil, 0, volume.ErrRange
			}
			mask, err := normalizePlacementMask(PlacementMask{Version: g.Effective.Version + 1, Nodes: nodes})
			if err != nil {
				return nil, 0, err
			}
			g.Revision++
			g.Name = name
			if !reflect.DeepEqual(mask.Nodes, g.Effective.Nodes) {
				g.Desired = &mask
				for _, description := range s.bucketSnapshots() {
					if description.DeletionComplete || description.Policy.PlacementGroup != g.ID {
						continue
					}
					if description.DesiredPolicy != nil || description.State == BucketDeleting {
						return nil, 0, ErrBucketConflict
					}
					if description.Policy.Version == ^uint64(0) {
						return nil, 0, volume.ErrRange
					}
					description.Policy.PlacementMaskVersion = g.Effective.Version
					policy := description.Policy
					policy.Version++
					policy.PlacementMaskVersion = mask.Version
					description.DesiredPolicy = &policy
					if _, err := s.buckets.Save(description, description.Revision); err != nil {
						return nil, 0, err
					}
					receipt.Running, receipt.Status = true, 202
				}
				if !receipt.Running {
					g.Effective, g.Desired = mask, nil
				}
			}
			object.Groups[index] = g
		}
	} else {
		return nil, 0, client.ErrProtocol
	}
	object.Receipts = append(object.Receipts, placementGroupReceipt{BucketReceipt: receipt, Target: id.String()})
	if err := s.savePlacementGroups(object); err != nil {
		return nil, 0, err
	}
	return encodeBucketAck(receipt.ID, id, receipt.Running), receipt.Status, nil
}

func placementMaskReady(group PlacementGroup, descriptions []BucketDescription) bool {
	if group.Desired == nil {
		return false
	}
	for _, d := range descriptions {
		if d.DeletionComplete {
			continue
		}
		for _, policy := range []*BucketPolicy{&d.Policy, d.DesiredPolicy} {
			if policy != nil && policy.PlacementGroup == group.ID && policy.PlacementMaskVersion != group.Desired.Version {
				return false
			}
		}
	}
	return true
}

func (c *clusterController) finishPlacementGroups(ctx context.Context) {
	l := c.coordinator.ledger
	l.mu.Lock()
	members := membershipIDs(l.current.Membership)
	busy := l.slot != nil || l.request != nil
	l.mu.Unlock()
	if busy || len(members) == 0 || members[0] != l.node {
		return
	}
	s := c.server
	for _, group := range s.placementGroups.Load().object.Groups {
		if !placementMaskReady(group, s.bucketSnapshots()) {
			continue
		}
		_, _, _ = c.change(ctx, nil, func() (*adminDraft, error) {
			s.opMu.Lock()
			preview, err := s.newAdminPreview()
			s.opMu.Unlock()
			if err != nil {
				return nil, err
			}
			draft := &adminDraft{server: preview, status: 200}
			complete := false
			defer func() {
				if !complete {
					draft.close()
				}
			}()
			object := preview.placementGroups.Load().object
			object.Groups = append([]PlacementGroup{}, object.Groups...)
			object.Receipts = append([]placementGroupReceipt{}, object.Receipts...)
			found := false
			for i, g := range object.Groups {
				if g.ID == group.ID && g.Revision == group.Revision && placementMaskReady(g, preview.bucketSnapshots()) {
					if g.Revision == ^uint64(0) {
						return nil, volume.ErrRange
					}
					g.Revision++
					g.Effective = *g.Desired
					g.Desired = nil
					object.Groups[i] = g
					found = true
					break
				}
			}
			if !found {
				return nil, errControlConflict
			}
			for i := range object.Receipts {
				r := &object.Receipts[i]
				if r.Target == group.ID && r.Running {
					r.Running = false
					r.Status = 200
				}
			}
			if err := preview.savePlacementGroups(object); err != nil {
				return nil, err
			}
			complete = true
			return draft, nil
		})
		return
	}
}
