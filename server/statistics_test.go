// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"gopkg.in/yaml.v3"
)

func TestStatisticsHalfLife(t *testing.T) {
	for _, halfLife := range []time.Duration{time.Second, time.Hour, defaultPlacementStatsHalfLife} {
		if got := statisticsRate(100, 0, halfLife, halfLife); math.Abs(got-50) > 1e-10 {
			t.Fatal(got)
		}
		whole := statisticsRate(100, 20, halfLife, halfLife)
		parts := 100.0
		for i := 0; i < 100; i++ {
			parts = statisticsRate(parts, 20, halfLife/100, halfLife)
		}
		if math.Abs(whole-parts) > 1e-10 {
			t.Fatal("sampling cadence changed weighting", whole, parts)
		}
	}
	if statisticsRate(12, 100, 0, time.Hour) != 12 {
		t.Fatal("zero interval changed rate")
	}
	if got := statisticsRate(100, 0, time.Nanosecond, defaultPlacementStatsHalfLife); got > 100 || got < 0 || math.IsNaN(got) {
		t.Fatal(got)
	}
}

func TestStatisticsRangeSampling(t *testing.T) {
	start := time.Unix(100, 0)
	m := &rangeMetric{sampled: start}
	m.read.Store(100)
	m.write.Store(200)
	read, write := m.workload(start.Add(10*time.Second), time.Minute)
	if read != 10 || write != 20 {
		t.Fatal("initial measured rate must not ramp from zero", read, write)
	}
	m.read.Add(100)
	if r, w := m.workload(start.Add(10500*time.Millisecond), time.Minute); r != read || w != write {
		t.Fatal("short frame changed sampling cut", r, w)
	}
	// The short call did not consume the new read bytes.
	r, w := m.workload(start.Add(70*time.Second), time.Minute)
	if math.Abs(r-(10+100.0/60)/2) > 1e-10 || math.Abs(w-10) > 1e-10 {
		t.Fatal(r, w)
	}
	if r2, w2 := m.workload(start, time.Minute); r2 != r || w2 != w {
		t.Fatal("clock rollback consumed counters")
	}
}

func TestStatisticsVolumeFrames(t *testing.T) {
	node, boot, disk := client.ID{1}, client.ID{2}, client.ID{3}
	old := client.StatsSnapshot{Node: node, BootID: boot, Sequence: 1, CapturedUnixNS: uint64(time.Second), Volumes: []client.VolumeStats{{ID: disk}}}
	previous := &pgRebuildCatalog{snapshots: []client.StatsSnapshot{old}, volumeRates: map[client.ID]placementVolumeRate{disk: {read: 100, write: 200}}}
	next := old
	next.Sequence++
	next.CapturedUnixNS += uint64(time.Hour)
	rates := placementVolumeRates([]client.StatsSnapshot{next}, previous, time.Hour)
	if rates[disk].read != 50 || rates[disk].write != 100 {
		t.Fatal(rates)
	}
	duplicate := placementVolumeRates([]client.StatsSnapshot{old}, previous, time.Hour)
	if duplicate[disk] != previous.volumeRates[disk] {
		t.Fatal("duplicate frame decayed twice")
	}
	rollback := old
	rollback.Sequence = 0
	if got := placementVolumeRates([]client.StatsSnapshot{rollback}, previous, time.Hour); got[disk] != previous.volumeRates[disk] {
		t.Fatal("out-of-order frame changed rates", got)
	}
	reboot := next
	reboot.BootID = client.ID{9}
	if got := placementVolumeRates([]client.StatsSnapshot{reboot}, previous, time.Hour); len(got) != 0 {
		t.Fatal("cross-boot subtraction", got)
	}
	initial := &pgRebuildCatalog{snapshots: []client.StatsSnapshot{old}}
	if got := placementVolumeRates([]client.StatsSnapshot{old}, initial, time.Hour); len(got) != 0 {
		t.Fatal("duplicate initial frame invented a zero-rate measurement", got)
	}
	next.Volumes = []client.VolumeStats{{ID: disk, IO: client.IOStats{PhysicalReadBytes: 3600, PhysicalWriteBytes: 7200}}}
	got := placementVolumeRates([]client.StatsSnapshot{next}, initial, time.Hour)
	if got[disk].read != 1 || got[disk].write != 2 {
		t.Fatal("measured initial interval lost", got)
	}
	resetOld := old
	resetOld.Volumes = []client.VolumeStats{{ID: disk, IO: client.IOStats{PhysicalReadBytes: 1000}}}
	previous.snapshots = []client.StatsSnapshot{resetOld}
	next.Volumes = []client.VolumeStats{{ID: disk}}
	if got := placementVolumeRates([]client.StatsSnapshot{next}, previous, time.Hour); len(got) != 0 {
		t.Fatal("reopened volume retained old counter rate", got)
	}
}

func TestStatisticsHalfLifeConfiguration(t *testing.T) {
	for _, text := range []string{"", "placement_stats_half_life: 1h\n", "placement_stats_half_life: 0s\n"} {
		path := filepath.Join(t.TempDir(), "node.yaml")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadConfig(path, Options{})
		if err != nil {
			t.Fatal(err)
		}
		want := defaultPlacementStatsHalfLife
		if text == "placement_stats_half_life: 1h\n" {
			want = time.Hour
		}
		if got := (&Server{cfg: cfg}).placementStatsHalfLife(); got != want {
			t.Fatal(got, want)
		}
		wire, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var roundtrip Config
		if err = yaml.Unmarshal(wire, &roundtrip); err != nil || roundtrip.PlacementStatsHalfLife != cfg.PlacementStatsHalfLife {
			t.Fatal(err, roundtrip.PlacementStatsHalfLife)
		}
		if cfg.PlacementStatsHalfLife == 0 && strings.Contains(string(wire), "placement_stats_half_life") {
			t.Fatal("default setting needlessly rewrites legacy configuration")
		}
	}
	for _, text := range []string{"placement_stats_half_life: -1s\n", "placement_stats_half_life: nonsense\n"} {
		path := filepath.Join(t.TempDir(), "node.yaml")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(path, Options{}); err == nil {
			t.Fatal("invalid half-life accepted", text)
		}
	}
}

func TestStatisticsSortedStorageLookup(t *testing.T) {
	rows := make([]volume.BucketStorage, 1000)
	for i := range rows {
		binary.BigEndian.PutUint64(rows[i].Bucket[8:], uint64(i*2+2))
		rows[i].Physical = uint64(i + 1)
	}
	for i := uint64(0); i <= 2002; i++ {
		var id volume.ID
		binary.BigEndian.PutUint64(id[8:], i)
		if got, want := sortedStoragePhysical(rows, id), storagePhysical(rows, id); got != want {
			t.Fatal(id, got, want)
		}
	}
	if sortedStoragePhysical(nil, volume.ID{}) != 0 {
		t.Fatal("empty image is not empty")
	}
}

func TestStatisticsTrafficConcurrentAndWarmAllocations(t *testing.T) {
	s := &Server{}
	b := &Bucket{server: s}
	p := principal{User: account{ID: "user"}}
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 1000; j++ {
				b.recordTraffic(p, 1, 2)
			}
		}()
	}
	workers.Wait()
	if read, write := s.userTraffic("user"); read != 16000 || write != 32000 {
		t.Fatal("cold counter race lost traffic", read, write)
	}
	if n := testing.AllocsPerRun(100, func() { b.recordTraffic(p, 1, 2) }); n != 0 {
		t.Fatalf("warm traffic allocated %g objects", n)
	}
}

func TestStatisticsOldBootCannotSupplyPlacementCapacity(t *testing.T) {
	node, disk, currentBoot := client.ID{1}, client.ID{2}, client.ID{3}
	s := &Server{peerStats: map[client.ID]*peerObservation{
		node: {
			online:     true,
			connection: &peerConnection{Node: node, Boot: currentBoot, Capabilities: client.CapabilityRangeHandoff},
			snapshot: &client.StatsSnapshot{Node: node, BootID: client.ID{4}, Volumes: []client.VolumeStats{
				{ID: disk, Class: uint8(volume.HDD), State: 1, CapacityPhysical: 1 << 20, AdmissionFreePhysical: 1 << 19},
			}},
		},
	}}
	s.currentSystem.Store(&systemView{manifest: systemManifest{Membership: client.Membership{Epoch: 1, Members: []client.Member{{ID: node}}}}})
	r := &rangeManager{server: s}
	policy := DefaultBucketPolicy()
	policy.PlacementGroup = ""
	if destinations, _ := r.destinations(BucketDescription{Policy: policy}, true); len(destinations) != 0 {
		t.Fatal("reconnected peer reused previous boot's free space", destinations)
	}
	s.peerStats[node].snapshot.BootID = currentBoot
	if destinations, _ := r.destinations(BucketDescription{Policy: policy}, true); len(destinations) != 1 {
		t.Fatal("current-boot positive control missing", destinations)
	}
}
