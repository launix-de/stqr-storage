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
	"github.com/launix-de/stqr-storage/internal/decay"
	"github.com/launix-de/stqr-storage/volume"
	"gopkg.in/yaml.v3"
)

func TestStatisticsHalfLife(t *testing.T) {
	start := time.Unix(100, 0)
	for _, halfLife := range []time.Duration{time.Second, time.Hour, decay.DefaultLongHalfLife} {
		h := decay.HalfLives{Short: halfLife, Long: halfLife}
		var whole, parts, idle decay.Series
		for _, counter := range []*decay.Series{&whole, &parts, &idle} {
			counter.Tick(start, h)
			counter.Add(100)
			counter.Tick(start.Add(time.Second), h)
		}
		from := start.Add(time.Second)
		idle.Tick(from.Add(halfLife), h)
		if got := idle.Forecast().Long; math.Abs(got-50) > 1e-10 {
			t.Fatal(got)
		}
		whole.Add(int64(200 * halfLife.Seconds()))
		whole.Tick(from.Add(halfLife), h)
		for i := 0; i < 100; i++ {
			a := from.Add(time.Duration(i) * halfLife / 100)
			b := from.Add(time.Duration(i+1) * halfLife / 100)
			parts.Add(int64(200 * b.Sub(a).Seconds()))
			parts.Tick(b, h)
		}
		a, b := whole.Forecast().Long, parts.Forecast().Long
		if math.Abs(a-b) > 1e-10 {
			t.Fatal("cadence changed weighting", a, b)
		}
	}
}

func TestStatisticsRangeSampling(t *testing.T) {
	start := time.Unix(100, 0)
	h := decay.HalfLives{Short: time.Minute, Long: time.Minute}
	m := &rangeMetric{sampled: start}
	m.read.Store(100)
	m.write.Store(200)
	m.tick(start.Add(10*time.Second), h)
	read, write := m.workload()
	if read != 10 || write != 20 {
		t.Fatal(read, write)
	}
	m.read.Add(100)
	m.tick(start.Add(10500*time.Millisecond), h)
	if r, w := m.workload(); r != read || w != write {
		t.Fatal("unfinished frame changed rates", r, w)
	}
	m.tick(start.Add(70*time.Second), h)
	r, w := m.workload()
	if math.Abs(r-(10+100.0/60)/2) > 1e-10 || math.Abs(w-10) > 1e-10 {
		t.Fatal(r, w)
	}
	for i := 0; i < 100; i++ {
		if a, b := m.workload(); a != r || b != w {
			t.Fatal("read mutated history")
		}
	}
}
func TestStatisticsVolumeFrames(t *testing.T) {
	start := time.Unix(100, 0)
	node, boot, disk := client.ID{1}, client.ID{2}, client.ID{3}
	h := decay.HalfLives{Short: time.Hour, Long: time.Hour}
	var book statisticsBook
	snap := client.StatsSnapshot{Node: node, BootID: boot, Sequence: 1, CapturedUnixNS: uint64(start.UnixNano()), Volumes: []client.VolumeStats{{ID: disk}}}
	book.observe(snap, nil, h)
	snap.Sequence++
	snap.CapturedUnixNS += uint64(time.Hour)
	snap.Volumes[0].IO.PhysicalReadBytes = 3600
	book.observe(snap, nil, h)
	at := start.Add(time.Hour)
	read, _, _ := book.volumeForecast(node, boot, disk, at)
	if read != 1 {
		t.Fatal("first measurement", read)
	}
	book.observe(snap, nil, h)
	if r, _, _ := book.volumeForecast(node, boot, disk, at); r != read {
		t.Fatal("replay changed mean", r)
	}
	snap.Sequence = 1
	snap.CapturedUnixNS++
	book.observe(snap, nil, h)
	if r, _, _ := book.volumeForecast(node, boot, disk, at); r != read {
		t.Fatal("old sequence changed mean", r)
	}
	snap.Sequence = 3
	snap.CapturedUnixNS = uint64(at.Add(time.Hour).UnixNano())
	snap.Volumes[0].IO.PhysicalReadBytes = 0
	book.observe(snap, nil, h)
	if frame := book.frames(nil).Volumes[disk.String()].Read; frame != nil {
		t.Fatal("counter reset retained measurement", frame)
	}
	snap.BootID = client.ID{9}
	snap.Sequence = 1
	book.observe(snap, nil, h)
	if r, _, _ := book.volumeForecast(node, boot, disk, at); r != 0 {
		t.Fatal("cross-boot lookup", r)
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
		want := decay.DefaultLongHalfLife
		if text == "placement_stats_half_life: 1h\n" {
			want = time.Hour
		}
		if got := (&Server{cfg: cfg}).placementHalfLives().Long; got != want {
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
