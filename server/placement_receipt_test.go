// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
)

func TestPlacementVolumeClockSkewAndSequence(t *testing.T) {
	h := decay.HalfLives{Short: time.Second, Long: time.Hour}
	local := time.Unix(100000, 0)
	node, vol := client.ID{1}, client.ID{2}
	snapshot := func(seq uint64, remote time.Time, amount uint64) client.StatsSnapshot {
		return client.StatsSnapshot{Node: node, BootID: node, Sequence: seq, CapturedUnixNS: uint64(remote.UnixNano()), Volumes: []client.VolumeStats{{ID: vol, IO: client.IOStats{PhysicalReadBytes: amount}}}}
	}
	for _, offset := range []time.Duration{-12 * time.Hour, 12 * time.Hour} {
		first := snapshot(1, local.Add(offset), 0)
		second := snapshot(2, local.Add(offset+time.Second), 100)
		rates := placementVolumeRatesAt([]client.StatsSnapshot{second}, &pgRebuildCatalog{snapshots: []client.StatsSnapshot{first}}, h, local.Add(time.Second))
		rate := rates[vol]
		if got := rate.readWindow.Rates(local.Add(time.Second), h).Short; math.Abs(got-50) > 1e-9 {
			t.Fatal("remote clock changed load", offset, got)
		}
		if got := rate.readWindow.Rates(local.Add(2*time.Second), h).Short; math.Abs(got-25) > 1e-9 {
			t.Fatal("local idle decay failed", offset, got)
		}
		old := snapshot(1, local.Add(offset+2*time.Second), 10000)
		replay := placementVolumeRatesAt([]client.StatsSnapshot{old}, &pgRebuildCatalog{snapshots: []client.StatsSnapshot{second}, volumeRates: rates}, h, local.Add(2*time.Second))
		if got := replay[vol].readWindow.Rates(local.Add(2*time.Second), h).Short; math.Abs(got-25) > 1e-9 {
			t.Fatal("old sequence injected load", got)
		}
	}
}
