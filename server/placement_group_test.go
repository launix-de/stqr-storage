// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func pgID(n byte) string { return volume.ID{n}.String() }

func TestPlacementGroupFilterModesAndDesiredMask(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    VolumeFilterMode
		listed  []string
		allowed [3]bool
	}{
		{"default", "", []string{pgID(3)}, [3]bool{true, true, true}},
		{"all", VolumesAll, []string{pgID(3)}, [3]bool{true, true, true}},
		{"blacklist-empty", VolumesBlacklist, nil, [3]bool{true, true, true}},
		{"blacklist", VolumesBlacklist, []string{pgID(4)}, [3]bool{true, false, true}},
		{"whitelist-empty", VolumesWhitelist, nil, [3]bool{}},
		{"whitelist", VolumesWhitelist, []string{pgID(4)}, [3]bool{false, true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := PlacementGroup{ID: pgID(9), Name: "test", Revision: 1, Effective: PlacementMask{Version: 1, Nodes: []PlacementNode{{NodeID: pgID(1), Mode: tc.mode, Volumes: tc.listed}}}, Desired: &PlacementMask{Version: 2, Nodes: []PlacementNode{}}}
			image, err := compilePlacementGroups(placementGroupsObject{Schema: 1, Groups: []PlacementGroup{g}})
			if err != nil {
				t.Fatal(err)
			}
			for i, v := range []string{pgID(3), pgID(4), pgID(5)} {
				if image.allows(g.ID, pgID(1), v) != tc.allowed[i] {
					t.Fatal("wrong effective filter", i)
				}
			}
			if image.allows(g.ID, pgID(2), pgID(4)) || image.allows(pgID(8), pgID(1), pgID(4)) {
				t.Fatal("missing node/group allowed fallback")
			}
			if !image.allows("", pgID(2), pgID(4)) {
				t.Fatal("absent PG imposed a filter")
			}
		})
	}
}

func installTestPlacementGroup(t *testing.T, s *Server, g PlacementGroup) {
	t.Helper()
	object := s.placementGroups.Load().object
	object.Groups = append(append([]PlacementGroup{}, object.Groups...), g)
	image, err := compilePlacementGroups(object)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.writeBlob("placement-groups", image.object); err != nil {
		t.Fatal(err)
	}
	s.placementGroups.Store(image)
}

func TestPlacementGroupConstrainsWritesAndSurvivesRestart(t *testing.T) {
	s := testServer(t)
	hdd := addTestVolume(t, s, "excluded", "hdd", 2<<20)
	ssd := addTestVolume(t, s, "allowed", "ssd", 2<<20)
	g := PlacementGroup{ID: pgID(9), Name: "one device", Revision: 1, Effective: PlacementMask{Version: 1, Nodes: []PlacementNode{{NodeID: s.running.Node.ID, Mode: VolumesWhitelist, Volumes: []string{ssd.VolumeID}}}}, Desired: &PlacementMask{Version: 2, Nodes: []PlacementNode{}}}
	installTestPlacementGroup(t, s, g)
	if stats := s.CollectStats(client.StatsFilter{}); !stats.NeedRebuild || stats.ReasonBits&(1<<6) == 0 {
		t.Fatal("desired PG change was absent from generation-builder statistics")
	}
	draft := newTestBucket(t, "pg-data").Describe()
	draft.Policy.PlacementGroup = g.ID
	if _, err := s.buckets.Save(draft, 0); err != nil {
		t.Fatal(err)
	}
	b, err := s.BucketByName("pg-data")
	if err != nil {
		t.Fatal(err)
	}
	user, err := s.findAccount(s.running.Auth.BootstrapAdmin.Username)
	if err != nil {
		t.Fatal(err)
	}
	key := volume.ObjectID{12}
	if _, err := b.Put(context.Background(), principal{User: user}, key, []byte("payload"), volume.Options{RequestID: volume.ID{13}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.volumes[ssd.VolumeID].store.Stat(volume.Key{Bucket: b.ID(), Object: key}); err != nil {
		t.Fatal("PG did not permit HDD-on-SSD placement", err)
	}
	if _, err := s.volumes[hdd.VolumeID].store.Stat(volume.Key{Bucket: b.ID(), Object: key}); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("write escaped whitelist", err)
	}
	capacity, err := s.bucketQuotaCapacity(b.ID(), 0)
	if err != nil || capacity.Available != s.volumes[ssd.VolumeID].store.Stats().AdmissionFreePhysical {
		t.Fatal("quota preview included capacity outside the PG", err)
	}
	empty := PlacementGroup{ID: pgID(10), Name: "empty", Revision: 1, Effective: PlacementMask{Version: 1, Nodes: []PlacementNode{{NodeID: s.running.Node.ID, Mode: VolumesWhitelist}}}}
	installTestPlacementGroup(t, s, empty)
	draft = newTestBucket(t, "blocked").Describe()
	draft.Policy.PlacementGroup = empty.ID
	if _, err := s.buckets.Save(draft, 0); err != nil {
		t.Fatal(err)
	}
	blocked, _ := s.BucketByName("blocked")
	if _, err := blocked.Put(context.Background(), principal{User: user}, key, []byte("no escape"), volume.Options{RequestID: volume.ID{14}}); statusOf(err) != 507 {
		t.Fatal("empty whitelist did not fail with 507", err)
	}
	path := s.configPath
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	b, _ = reopened.BucketByName("pg-data")
	data, _, err := b.Get(context.Background(), principal{User: user}, key)
	if err != nil || string(data) != "payload" || b.Describe().Policy.PlacementGroup != g.ID {
		t.Fatal("PG or payload lost on restart", err)
	}
	if reopened.placementGroups.Load().allows(g.ID, reopened.running.Node.ID, ssd.VolumeID) != true {
		t.Fatal("desired mask replaced effective mask before rebuild")
	}
}

func TestSystemMetadataUsesGeneralPlacementGroup(t *testing.T) {
	s := testServer(t)
	other := addTestVolume(t, s, "data", "hdd", 1<<20)
	system, err := s.Bucket(systemBucket)
	if err != nil || system.Describe().Policy.PlacementGroup != systemPlacementGroup.String() {
		t.Fatal("system policy has no PG reference", err)
	}
	stores := system.localVolumes()
	if len(stores) != 1 || stores[0] != s.system {
		t.Fatal("general bucket placement did not resolve the metadata PG")
	}
	if s.placementGroups.Load().allows(systemPlacementGroup.String(), s.running.Node.ID, other.VolumeID) {
		t.Fatal("metadata PG leaked onto arbitrary data volumes")
	}
	// Exactly the same selection works for another bucket ID. The resolver has
	// no special case mapping the system bucket ID onto a physical admin volume.
	draft := newTestBucket(t, "same-mask").Describe()
	draft.Policy.PlacementGroup = systemPlacementGroup.String()
	if got := system.volumesFor(draft); len(got) != 1 || got[0] != s.system {
		t.Fatal("PG resolution depends on bucket identity")
	}
}

func TestEstablishedMetadataPGDoesNotFallBackOrReinitialize(t *testing.T) {
	s := testServer(t)
	object := s.placementGroups.Load().object
	object.Groups = append([]PlacementGroup{}, object.Groups...)
	root := object.Groups[0]
	root.Effective = PlacementMask{Version: root.Effective.Version + 1, Nodes: []PlacementNode{{NodeID: s.running.Node.ID, Mode: VolumesWhitelist}}}
	object.Groups[0] = root
	if err := s.writeBlob("placement-groups", object); err != nil {
		t.Fatal(err)
	}
	path := s.configPath
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := openTransportTestNode(Options{ConfigPath: path}); err == nil {
		reopened.Close()
		t.Fatal("invalid metadata PG silently fell back to the bootstrap volume")
	}
}
