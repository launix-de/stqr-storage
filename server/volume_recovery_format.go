// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"

	"github.com/launix-de/stqr-storage/client"
)

// A pending origin preserves the exact old ownership while its replacement
// returns 503. It never invents absence or accepts replacement writes. Completed
// origins retain the allowed source-volume cohort for unpinned imported ranges.
type volumeRecoveryOrigin struct {
	CancelledMigrations []volumeRecoveryCancellation `json:",omitempty"`
	ID                  [32]byte
	Before              client.MapPatch
	After               client.RangeEntry
	Complete            bool
	Target              client.ID
	Volumes             []client.ID
}

type volumeRecoveryCancellation struct {
	Plan  []byte
	Proof client.Proof
}

func (o volumeRecoveryOrigin) acceptsVolumeClaim(claim volumeAuthorityEntry) bool {
	if claim.Active && claim.Fence == ([32]byte{}) {
		return true
	}
	if claim.Active || claim.Fence == ([32]byte{}) {
		return false
	}
	for _, cancelled := range o.CancelledMigrations {
		plan, err := decodeRangePlan(cancelled.Plan)
		if err == nil && plan.id() == claim.Fence {
			return true
		}
	}
	return false
}

func recoveryOriginID(before client.MapPatch) [32]byte {
	raw, _ := before.MarshalBinary()
	return sha256.Sum256(append([]byte("STQR recovery origin v1\x00"), raw...))
}

type volumeTakeover struct {
	Target  client.ID
	Origins [][32]byte
	Volumes []client.ID
}

func committedVolumeTakeover(p controlProposal) bool {
	return p.Claim.Exclusion != nil && p.Claim.Exclusion.Restore != nil
}

// Imported volumes remain quarantined for unrelated ranges. Otherwise a later
// import could expose stale objects already present on the rescued media.
func recoveredVolume(view *systemView, id client.ID) bool {
	for _, origin := range view.manifest.VolumeRecoveries {
		if origin.Complete && hasControlNode(origin.Volumes, id) {
			return true
		}
	}
	return false
}

func cloneRecoveryOrigins(origins []volumeRecoveryOrigin) []volumeRecoveryOrigin {
	raw, _ := json.Marshal(origins)
	var out []volumeRecoveryOrigin
	_ = json.Unmarshal(raw, &out)
	return out
}

func (s *Server) recoveryOriginsForChange(patches []client.MapPatch, x *controlExclusion) ([]volumeRecoveryOrigin, error) {
	var origins []volumeRecoveryOrigin
	if view := s.currentSystem.Load(); view != nil {
		origins = cloneRecoveryOrigins(view.manifest.VolumeRecoveries)
	}
	if x != nil && x.RecoverOnly {
		for _, patch := range patches {
			for _, entry := range patch.Entries {
				if len(entry.Locations) != 1 || !hasControlNode(x.Nodes, entry.Locations[0].Node) || entry.State != 0 && entry.State != 5 || entry.Runtime != (client.ID{}) {
					continue
				}
				end, toEnd, _ := client.RangeEnd(entry.Start, entry.PrefixBits)
				before := patch
				before.Begin, before.End, before.ToEnd, before.Entries = entry.Start, end, toEnd, []client.RangeEntry{entry}
				if view := s.currentSystem.Load(); view != nil {
					before.Membership = view.manifest.Membership.Epoch
					before.Addresses = nil
					for _, member := range view.manifest.Membership.Members {
						if member.ID == entry.Locations[0].Node && len(member.Addresses) > 0 {
							before.Addresses = append(before.Addresses, client.NodeAddress{Node: member.ID, Addresses: member.Addresses})
						}
					}
				}
				origin := volumeRecoveryOrigin{ID: recoveryOriginID(before), Before: before}
				origins = append(origins, origin)
			}
		}
	}
	return origins, nil
}

func (s *Server) bucketAwaitingRecovery(bucket client.ID) bool {
	view := s.currentSystem.Load()
	if view == nil {
		return false
	}
	for _, origin := range view.manifest.VolumeRecoveries {
		if !origin.Complete && origin.Before.Bucket == bucket {
			return true
		}
	}
	return false
}

// Policy/ACL/quota revisions can change without changing the writer or the
// physical cohort. Ownership epochs, scope and locations decide provenance.
func sameRangeAuthority(a, b client.RangeEntry) bool {
	a.PolicyVersion = 0
	b.PolicyVersion = 0
	return reflect.DeepEqual(a, b)
}
