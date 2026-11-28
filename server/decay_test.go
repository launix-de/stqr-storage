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
	h := decay.HalfLives{Short: 3 * time.Second, Long: 10 * time.Second}
	start := time.Unix(100, 0)
	node, boot, disk := client.ID{1}, client.ID{2}, client.ID{3}
	sample := func(seq uint64, dt time.Duration, amount uint64) client.StatsSnapshot {
		return client.StatsSnapshot{Node: node, BootID: boot, Sequence: seq, CapturedUnixNS: uint64(start.Add(dt).UnixNano()), Volumes: []client.VolumeStats{{ID: disk, IO: client.IOStats{PhysicalReadBytes: amount}}}}
	}
	var coarse, fine statisticsBook
	coarse.observe(sample(1, 0, 0), nil, h)
	fine.observe(sample(1, 0, 0), nil, h)
	coarse.observe(sample(3, 6*time.Second, 600), nil, h)
	fine.observe(sample(2, 3*time.Second, 300), nil, h)
	fine.observe(sample(3, 6*time.Second, 600), nil, h)
	a, _, _ := coarse.volumeForecast(node, boot, disk, start.Add(6*time.Second))
	b, _, _ := fine.volumeForecast(node, boot, disk, start.Add(6*time.Second))
	if a != 100 || math.Abs(a-b) > 1e-10 {
		t.Fatal(a, b)
	}
	fine.observe(sample(3, 6*time.Second, 600), nil, h)
	if r, _, _ := fine.volumeForecast(node, boot, disk, start.Add(6*time.Second)); r != b {
		t.Fatal("replay", r)
	}
	fine.observe(sample(4, 9*time.Second, 600), nil, h)
	short := fine.volumes[disk.String()].traffic[0].window.Forecast().Short
	if math.Abs(short-50) > 1e-10 {
		t.Fatal("idle tick did not decay", short)
	}
}
