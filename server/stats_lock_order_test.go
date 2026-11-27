// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

//go:noinline
func collectStatsNativeLockProbe(s *Server, done chan<- client.StatsSnapshot) {
	done <- s.CollectStats(client.StatsFilter{})
}

// Observe the actual native lock wait, not an assumed sleep. This is the first
// barrier: only after CollectStats reached a blocked Volume accessor do we
// request the node writer. The second barrier requires that writer to finish
// while the native handle is still held.
func awaitStatsNativeWait(t *testing.T) {
	t.Helper()
	buf := make([]byte, 128<<10)
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(stack, "collectStatsNativeLockProbe") && strings.Contains(stack, "volume.(*Volume).") && strings.Contains(stack, "sync.(*RWMutex).RLock") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("stats never reached the held native volume lock")
		}
		runtime.Gosched()
	}
}

func TestCollectStatsReleasesNodeLockBeforeNativeVolumeWait(t *testing.T) {
	v, err := volume.New(filepath.Join(t.TempDir(), "stats-volume"), 8<<20, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	// Produce real backend write history, rather than letting the retirement
	// assertion pass on zero counters from a fresh/RAM-only instance.
	seed, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seed.Put(volume.Key{Bucket: volume.ID{1}, Object: volume.ObjectID{1}}, []byte("history"), volume.Options{Accepted: true})
	seed.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Sync(); err != nil {
		t.Fatal(err)
	}
	id := v.Identity().VolumeID.String()
	m := &managedVolume{store: v, config: VolumeConfig{ID: id, Path: "before", State: "active"}}
	s := &Server{cfg: Config{Node: NodeConfig{ID: newID()}, Generation: 7}, volumes: map[string]*managedVolume{id: m}}
	s.retiredIO.PhysicalReadBytes = 11
	nativeIO := volumeAccessIO(v.IOStats())
	if nativeIO.PhysicalWriteBytes == 0 {
		t.Fatal("fixture has no native I/O history positive control")
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	stats := make(chan client.StatsSnapshot, 1)
	go collectStatsNativeLockProbe(s, stats)
	awaitStatsNativeWait(t)
	changed := make(chan struct{})
	go func() {
		s.mu.Lock()
		s.cfg.Generation = 99
		s.cfg.Node.Paused = true
		m.config.Path = "after"
		m.config.State = "draining"
		m.config.PlacementGroupsPending = []string{"new-policy"}
		delete(s.volumes, id)
		s.retiredIO.PhysicalReadBytes += nativeIO.PhysicalReadBytes
		s.retiredIO.PhysicalWriteBytes += nativeIO.PhysicalWriteBytes
		s.mu.Unlock()
		close(changed)
	}()
	select {
	case <-changed:
	case <-time.After(time.Second):
		h.Close()
		<-stats
		<-changed
		t.Fatal("stats held node lock while waiting for the native volume")
	}
	// Snapshotting must not finish by omitting the blocked volume.
	select {
	case <-stats:
		t.Fatal("stats skipped the held volume")
	default:
	}
	h.Close()
	snap := <-stats
	if snap.Generation != 7 || snap.NodeState != 0 || len(snap.Volumes) != 1 || snap.Volumes[0].Path != "before" || snap.Volumes[0].State == 0 {
		t.Fatalf("configuration escaped its coherent snapshot: %+v", snap)
	}
	if snap.IO.PhysicalReadBytes != 11+nativeIO.PhysicalReadBytes || snap.IO.PhysicalWriteBytes != nativeIO.PhysicalWriteBytes {
		t.Fatal("detach history counted twice or lost", snap.IO)
	}
	next := s.CollectStats(client.StatsFilter{})
	if len(next.Volumes) != 0 || next.Generation != 99 || next.NodeState != 2 || next.IO.PhysicalReadBytes != snap.IO.PhysicalReadBytes || next.IO.PhysicalWriteBytes != snap.IO.PhysicalWriteBytes {
		t.Fatal("subsequent snapshot did not see completed detach/config", next)
	}
}

func TestCollectStatsRetainedClosedStoreHasSafeFinalCounters(t *testing.T) {
	v, err := volume.NewRAM(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	id := v.Identity().VolumeID.String()
	s := &Server{cfg: Config{Node: NodeConfig{ID: newID()}}, volumes: map[string]*managedVolume{id: {store: v, config: VolumeConfig{Path: "closed"}}}}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	snap := s.CollectStats(client.StatsFilter{})
	if len(snap.Volumes) != 1 || snap.Volumes[0].Path != "closed" || snap.Volumes[0].CapacityPhysical != 8<<20 {
		t.Fatal("retained instance lost its final safe stats", snap)
	}
}
