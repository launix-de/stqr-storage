// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestQuotaOwnershipIgnoresMetadataButTracksOwnership(t *testing.T) {
	entry := client.RangeEntry{PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{{Node: client.ID{1}, Volume: client.ID{2}}}}
	patch := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID{3}, ToEnd: true, Entries: []client.RangeEntry{entry}}
	build := func(patches ...client.MapPatch) *placementImage {
		t.Helper()
		for i := range patches {
			patches[i].Addresses = []client.NodeAddress{{Node: client.ID{1}, Addresses: []string{"http://one:7419"}}, {Node: client.ID{7}, Addresses: []string{"http://seven:7419"}}}
		}
		image, err := buildPlacement(patches)
		if err != nil {
			t.Fatal(err)
		}
		return image
	}
	before := quotaOwnershipStamp{placement: build(patch), view: &systemView{}, general: true}
	patch.Membership, patch.Generation, patch.Revision = 2, 2, 2
	after := quotaOwnershipStamp{placement: build(patch), view: &systemView{}, general: true}
	assert := func(next quotaOwnershipStamp, known bool, bucket volume.ID) {
		t.Helper()
		changed, gotKnown := next.ChangedOwnershipBuckets(before)
		if gotKnown != known || known && (bucket == (volume.ID{}) && len(changed) != 0 || bucket != (volume.ID{}) && (len(changed) != 1 || changed[0] != bucket)) {
			t.Fatal("wrong ownership delta", changed, gotKnown, bucket, known)
		}
	}
	assert(after, true, volume.ID{})
	after.view.manifest.VolumeRecoveries = []volumeRecoveryOrigin{}
	assert(after, true, volume.ID{})
	added := patch
	added.Bucket = client.ID{4}
	after.placement = build(patch, added)
	assert(after, true, volume.ID(added.Bucket))
	after.placement = build(added)
	// Removal and addition both change ownership.
	if changed, known := after.ChangedOwnershipBuckets(before); !known || len(changed) != 2 {
		t.Fatal(changed, known)
	}
	patch.Entries = append([]client.RangeEntry(nil), patch.Entries...)
	patch.Entries[0].Locations = []client.Location{{Node: client.ID{7}, Volume: client.ID{2}}}
	after.placement = build(patch)
	assert(after, true, volume.ID(patch.Bucket))
	after.general = false
	assert(after, false, volume.ID{})
	after.general = true
	after.view = &systemView{manifest: systemManifest{VolumeRecoveries: []volumeRecoveryOrigin{{Complete: true, Volumes: []client.ID{{9}}}}}}
	assert(after, false, volume.ID{})
	after = before
	after.standalone = true
	assert(after, false, volume.ID{})
}
