// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestSelectVolumeLoadPreservesExistingStriping(t *testing.T) {
	s, b, vs, key := capacitySelectionFixture(t, 3)
	s.scopes = &rangeManager{server: s}
	now := time.Now()
	seedStatisticsVolume(s, client.ID(vs[0].Identity().VolumeID), now, 128<<20)
	// The most free disk is hot; spare independent HDDs remain eligible.
	if err := vs[1].SetGuaranteedReserve(64 << 10); err != nil {
		t.Fatal(err)
	}
	if err := vs[2].SetGuaranteedReserve(128 << 10); err != nil {
		t.Fatal(err)
	}
	got, _, found, err := b.selectVolume(b.Describe(), key, volume.ID{}, true)
	if err != nil || found || got != vs[1] {
		t.Fatal("did not choose an idle local disk", got, found, err)
	}
	// A retry or existing object must retain its actual disk even when hot.
	request := volume.ID{73}
	selectionPut(t, vs[0], key, request)
	for _, id := range []volume.ID{{}, request} {
		got, _, found, err = b.selectVolume(b.Describe(), key, id, true)
		if err != nil || !found || got != vs[0] {
			t.Fatal("load changed existing ownership", found, err)
		}
	}
	// Placement group/local-volume filters remain hard constraints.
	desc := b.Describe()
	desc.LocalVolumes = []string{vs[2].Identity().VolumeID.String()}
	key.Object[0]++
	got, _, _, err = b.selectVolume(desc, key, volume.ID{}, true)
	if err != nil || got != vs[2] {
		t.Fatal("load ranking bypassed allowed pool", err)
	}
}

func TestDecaySettingsRuntimeUpdate(t *testing.T) {
	s, err := openTransportTestNode(Options{ConfigPath: t.TempDir() + "/node.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.updateConfig(map[string]json.RawMessage{"placement_statistics": json.RawMessage(`{"short_half_life_seconds":2,"long_half_life_seconds":20}`)}); err != nil {
		t.Fatal(err)
	}
	if h := s.placementHalfLives(); h.Short != 2*time.Second || h.Long != 20*time.Second {
		t.Fatal(h)
	}
	if err = s.updateConfig(map[string]json.RawMessage{"placement_statistics": json.RawMessage(`{"short_half_life_seconds":30,"long_half_life_seconds":20}`)}); err == nil {
		t.Fatal("invalid runtime kernel accepted")
	}
}
