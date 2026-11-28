// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
	"math"
	"testing"
	"time"
)

func TestPlacementVolumeClockSkewAndSequence(t *testing.T) {
	h := decay.HalfLives{Short: 3 * time.Second, Long: time.Hour}
	local := time.Unix(100000, 0)
	node, boot, disk := client.ID{1}, client.ID{2}, client.ID{3}
	for _, offset := range []time.Duration{-12 * time.Hour, 12 * time.Hour} {
		var book statisticsBook
		sample := func(seq uint64, dt time.Duration, amount uint64) client.StatsSnapshot {
			return client.StatsSnapshot{Node: node, BootID: boot, Sequence: seq, CapturedUnixNS: uint64(local.Add(offset + dt).UnixNano()), Volumes: []client.VolumeStats{{ID: disk, IO: client.IOStats{PhysicalReadBytes: amount}}}}
		}
		book.observePeer(sample(1, 0, 0), local, h)
		book.observePeer(sample(2, 3*time.Second, 300), local.Add(3*time.Second), h)
		r, _, _ := book.volumeForecast(node, boot, disk, local.Add(3*time.Second))
		if r != 100 {
			t.Fatal(offset, r)
		}
		book.observePeer(sample(1, 4*time.Second, 100000), local.Add(4*time.Second), h)
		r, _, _ = book.volumeForecast(node, boot, disk, local.Add(4*time.Second))
		if r != 100 {
			t.Fatal("old sequence", r)
		}
		book.observePeer(sample(3, 6*time.Second, 300), local.Add(6*time.Second), h)
		if r := book.peers[node].volumes[disk.String()].traffic[0].window.Forecast().Short; math.Abs(r-50) > 1e-9 {
			t.Fatal("idle", r)
		}
	}
}
