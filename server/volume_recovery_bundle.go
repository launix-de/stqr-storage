// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (s *Server) setRecoveryBundle(bundle *systemBundle, patches []client.MapPatch, origins []volumeRecoveryOrigin, x *controlExclusion) error {
	var manifest systemManifest
	if err := decodeControlRecord(bundle.Objects[bundle.Root], &manifest); err != nil {
		return err
	}
	for i := range origins {
		origin := &origins[i]
		if origin.Complete {
			continue
		}
		for _, patch := range patches {
			if patch.Bucket != origin.Before.Bucket {
				continue
			}
			for _, entry := range patch.Entries {
				if entry.Start == origin.Before.Entries[0].Start && entry.PrefixBits == origin.Before.Entries[0].PrefixBits && entry.State == 4 {
					origin.After = entry
				}
			}
		}
	}
	if x != nil && x.Restore != nil {
		takeover := x.Restore
		if takeover.Target != s.control.ledger.node {
			return errControlConflict
		}
		for _, id := range takeover.Origins {
			index := -1
			for i := range origins {
				if origins[i].ID == id {
					index = i
					break
				}
			}
			if index < 0 || origins[index].Complete {
				return fail(409, "recovery origin is no longer pending")
			}
			origin := &origins[index]
			cohort, err := s.validateRecoveryCohort(*origin, takeover.Volumes)
			if err != nil {
				return err
			}
			changed := false
			for pi := range patches {
				if patches[pi].Bucket != origin.Before.Bucket {
					continue
				}
				for ei, entry := range patches[pi].Entries {
					if !reflect.DeepEqual(entry, origin.After) {
						continue
					}
					if entry.State != 4 || entry.OwnerEpoch == ^uint64(0) {
						return errControlConflict
					}
					entry.OwnerEpoch++
					entry.State = origin.Before.Entries[0].State
					entry.Locations = append([]client.Location{}, entry.Locations...)
					entry.Locations[0] = origin.Before.Entries[0].Locations[0]
					entry.Locations[0].Node = takeover.Target
					origin.After, origin.Target, origin.Volumes, origin.Complete = entry, takeover.Target, cohort, true
					patches[pi].Entries[ei] = entry
					changed = true
				}
			}
			if !changed {
				return fail(409, "recovery range changed before takeover")
			}
		}
		// Replace only the affected immutable map children; no payload movement or
		// generation-builder run is involved in this administrative cutover.
		for _, patch := range patches {
			raw, err := patch.MarshalBinary()
			if err != nil {
				return err
			}
			for i := range manifest.Maps {
				if manifest.Maps[i].Bucket == patch.Bucket {
					manifest.Maps[i].Object = bundle.add(raw)
				}
			}
		}
	}
	sort.Slice(origins, func(i, j int) bool { return bytes.Compare(origins[i].ID[:], origins[j].ID[:]) < 0 })
	manifest.VolumeRecoveries = origins
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	bundle.Root = bundle.add(raw)
	return nil
}

// All participants validate the origin lineage, even when they do not hold the
// rescued disk. HOLD is a deliberate unavailable state, not an empty range.
func validateRecoveryOrigins(view *systemView) error {
	var previous [32]byte
	for i, origin := range view.manifest.VolumeRecoveries {
		if err := origin.Before.Validate(); err != nil {
			return fmt.Errorf("%w: invalid recovery before-map: %v", volume.ErrCorrupt, err)
		}
		if len(origin.Before.Entries) != 1 || origin.ID != recoveryOriginID(origin.Before) || i > 0 && bytes.Compare(previous[:], origin.ID[:]) >= 0 {
			return fmt.Errorf("%w: recovery origin digest/order", volume.ErrCorrupt)
		}
		old := origin.Before.Entries[0]
		if err := validateRecoveryCancellations(origin); err != nil {
			return fmt.Errorf("%w: invalid recovery cancellation: %v", volume.ErrCorrupt, err)
		}
		if len(old.Locations) != 1 || old.Runtime != (client.ID{}) || old.State != 0 && old.State != 5 || !hasControlNode(view.manifest.Membership.Excluded, old.Locations[0].Node) || origin.After.Start != old.Start || origin.After.PrefixBits != old.PrefixBits || origin.After.OwnerEpoch <= old.OwnerEpoch || len(origin.After.Locations) != 1 {
			return fmt.Errorf("%w: recovery lineage: old state %d old epoch %d after epoch %d after locations %d source excluded %t", volume.ErrCorrupt, old.State, old.OwnerEpoch, origin.After.OwnerEpoch, len(origin.After.Locations), hasControlNode(view.manifest.Membership.Excluded, old.Locations[0].Node))
		}
		if origin.Complete {
			if origin.After.State != old.State || origin.Target == (client.ID{}) || origin.After.Locations[0].Node != origin.Target || len(origin.Volumes) == 0 {
				return volume.ErrCorrupt
			}
			for j, id := range origin.Volumes {
				if id == (client.ID{}) || j > 0 && origin.Volumes[j-1].String() >= id.String() {
					return volume.ErrCorrupt
				}
			}
			if old.Locations[0].Volume != (client.ID{}) && (len(origin.Volumes) != 1 || origin.Volumes[0] != old.Locations[0].Volume) {
				return volume.ErrCorrupt
			}
		} else if origin.After.State != 4 || len(origin.Volumes) != 0 || origin.Target != (client.ID{}) {
			return volume.ErrCorrupt
		}
		previous = origin.ID
	}
	return nil
}

func recoveryReplacement(s *Server, view *systemView, x *controlExclusion, bucket client.ID, before, after client.RangeEntry) bool {
	if x == nil || x.Restore == nil || before.State != 4 || (after.State != 0 && after.State != 5) || after.OwnerEpoch != before.OwnerEpoch+1 {
		return false
	}
	current := s.currentSystem.Load()
	if current == nil {
		return false
	}
	for _, old := range current.manifest.VolumeRecoveries {
		if old.Complete || old.Before.Bucket != bucket || !reflect.DeepEqual(old.After, before) {
			continue
		}
		chosen := false
		for _, id := range x.Restore.Origins {
			if id == old.ID {
				chosen = true
			}
		}
		if !chosen {
			return false
		}
		for _, next := range view.manifest.VolumeRecoveries {
			if next.ID == old.ID && next.Complete && next.Target == x.Restore.Target && reflect.DeepEqual(next.After, after) {
				return true
			}
		}
	}
	return false
}
func recoveryHold(view *systemView, x *controlExclusion, bucket client.ID, entry client.RangeEntry) bool {
	if x == nil || !x.RecoverOnly || entry.State != 4 {
		return false
	}
	for _, origin := range view.manifest.VolumeRecoveries {
		if !origin.Complete && origin.Before.Bucket == bucket && reflect.DeepEqual(origin.After, entry) && hasControlNode(x.Nodes, origin.Before.Entries[0].Locations[0].Node) {
			return true
		}
	}
	return false
}

func isRestoredEntry(view *systemView, x *controlExclusion, bucket client.ID, entry client.RangeEntry) bool {
	if x == nil || x.Restore == nil {
		return false
	}
	for _, origin := range view.manifest.VolumeRecoveries {
		if origin.Complete && origin.Before.Bucket == bucket && origin.Target == x.Restore.Target && reflect.DeepEqual(origin.After, entry) {
			for _, id := range x.Restore.Origins {
				if origin.ID == id {
					return true
				}
			}
		}
	}
	return false
}
