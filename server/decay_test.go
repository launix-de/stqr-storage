// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
)

func TestDecaySettingsDefaultsAndValidation(t *testing.T) {
	if h := (PlacementStatisticsConfig{}).halfLives(); h.Short != 10*time.Second || h.Long != 7*24*time.Hour {
		t.Fatal(h)
	}
	for _, c := range []PlacementStatisticsConfig{{-1, 0}, {0, -1}, {61, 60}, {1 << 62, 1 << 62}} {
		if c.validate() == nil {
			t.Fatal("accepted invalid half-lives", c)
		}
	}
	c := PlacementStatisticsConfig{2, 20}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	s, err := openTransportTestNode(Options{ConfigPath: filepath.Join(t.TempDir(), "node.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.mu.Lock()
	s.cfg.PlacementStatistics = c
	snapshot := s.cfg
	s.mu.Unlock()
	if err := saveConfig(s.configPath, snapshot); err != nil {
		t.Fatal(err)
	}
	if got := s.placementHalfLives(); got.Short != 2*time.Second || got.Long != 20*time.Second {
		t.Fatal(got)
	}
	loaded, err := loadConfig(s.configPath, Options{})
	if err != nil || loaded.PlacementStatistics != c {
		t.Fatal("half-lives not persisted", loaded.PlacementStatistics, err)
	}
}

func TestDecayVolumeSnapshotCadenceAndReplay(t *testing.T) {
	h := decay.HalfLives{Short: time.Second, Long: 10 * time.Second}
	node, boot, vol := client.ID{1}, client.ID{2}, client.ID{3}
	base := time.Unix(100, 0)
	snap := func(seq uint64, seconds int64, bytes uint64) client.StatsSnapshot {
		return client.StatsSnapshot{Node: node, BootID: boot, Sequence: seq, CapturedUnixNS: uint64(base.Add(time.Duration(seconds) * time.Second).UnixNano()), Volumes: []client.VolumeStats{{ID: vol, IO: client.IOStats{PhysicalReadBytes: bytes}}}}
	}
	initial := snap(1, 0, 0)
	prior := &pgRebuildCatalog{snapshots: []client.StatsSnapshot{initial}}
	last := snap(2, 2, 200)
	coarse := placementVolumeRates([]client.StatsSnapshot{last}, prior, h)[vol]
	middle := snap(2, 1, 100)
	fine := placementVolumeRates([]client.StatsSnapshot{middle}, prior, h)
	fine = placementVolumeRates([]client.StatsSnapshot{snap(3, 2, 200)}, &pgRebuildCatalog{snapshots: []client.StatsSnapshot{middle}, volumeRates: fine}, h)
	if math.Abs(coarse.read-fine[vol].read) > 1e-10 || math.Abs(coarse.read-75) > 1e-10 {
		t.Fatal(coarse.read, fine[vol].read)
	}
	replay := placementVolumeRates([]client.StatsSnapshot{last}, &pgRebuildCatalog{snapshots: []client.StatsSnapshot{last}, volumeRates: map[client.ID]placementVolumeRate{vol: coarse}}, h)
	if replay[vol].read != coarse.read {
		t.Fatal("snapshot counted twice", replay[vol].read)
	}
	if got := coarse.readWindow.Rates(base.Add(3*time.Second), h).Short; math.Abs(got-37.5) > 1e-10 {
		t.Fatal("idle snapshot load did not decay", got)
	}
	last.BootID = client.ID{4}
	if rates := placementVolumeRates([]client.StatsSnapshot{last}, prior, h); len(rates) != 0 {
		t.Fatal("previous boot supplied load", rates)
	}
}
