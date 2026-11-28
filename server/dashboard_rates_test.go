// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
)

func TestDashboardRatesCompletedFrames(t *testing.T) {
	start := time.Unix(100, 0)
	node, boot, disk, bucket, user := client.ID{1}, client.ID{2}, client.ID{3}, client.ID{4}, client.ID{5}
	var book dashboardRateBook
	sample := func(now time.Time, bytes uint64) {
		io := client.IOStats{PhysicalReadBytes: bytes, PhysicalWriteBytes: 2 * bytes, LogicalReadBytes: 3 * bytes, LogicalWriteBytes: 4 * bytes}
		book.observe(client.StatsSnapshot{Node: node, BootID: boot, CapturedUnixNS: uint64(now.UnixNano()), IO: io, Volumes: []client.VolumeStats{{ID: disk, IO: io}}, Buckets: []client.BucketStats{{ID: bucket, IO: io}}}, map[string]client.IOStats{user.String(): io})
	}
	sample(start, 100)
	sample(start.Add(time.Second), 1100)
	if pair := book.frames(nil).IO.Read; pair != nil {
		t.Fatal("event publication prematurely completed interval", pair)
	}
	sample(start.Add(3*time.Second), 3100)
	frames := book.frames([]client.ID{user})
	for _, pair := range []*dashboardRatePair{frames.IO.Read, frames.Volumes[disk.String()].Read, frames.Buckets[bucket.String()].Read, frames.Users[user.String()].Read} {
		if pair == nil || pair.Last != 1000 || pair.Average != 1000 {
			t.Fatal("initial completed frame must be normalized and seeded", pair)
		}
	}
	if pair := frames.UserIO.LogicalWrite; pair == nil || pair.Last != 4000 || pair.Average != 4000 {
		t.Fatal("all-account logical traffic lost", pair)
	}
	// Current additions do not leak into the previous completed interval.
	sample(start.Add(4*time.Second), 6100)
	if pair := book.frames(nil).IO.Read; pair.Last != 1000 || pair.Average != 1000 {
		t.Fatal("current frame changed displayed interval", pair)
	}
	// Nine seconds since the last switch, regardless of configured cadence.
	sample(start.Add(12*time.Second), 21100)
	pair := book.frames(nil).IO.Read
	want := 1000 + (2000-1000)*-math.Expm1(-math.Ln2*9/decay.DefaultLongHalfLife.Seconds())
	if pair.Last != 2000 || math.Abs(pair.Average-want) > 1e-9 {
		t.Fatal("variable interval scaled incorrectly", pair, want)
	}
	// Getter calls never tick or change the mean. A real idle half-life halves it.
	for i := 0; i < 10; i++ {
		if got := book.frames(nil).IO.Read; *got != *pair {
			t.Fatal("polling mutated history", got)
		}
	}
	sample(start.Add(12*time.Second+decay.DefaultLongHalfLife), 21100)
	idle := book.frames(nil).IO.Read
	if idle.Last != 0 || math.Abs(idle.Average-pair.Average/2) > 1e-9 {
		t.Fatal("idle weekly half-life incorrect", idle, pair)
	}
}

func TestDashboardRatesPrecisionResetAndBoot(t *testing.T) {
	var counter dashboardRateCounter
	now := time.Unix(100, 0)
	base := uint64(1<<60 + 11)
	counter.observe(now, base, false)
	counter.observe(now.Add(3*time.Second), base+3, true)
	if got := counter.window.Snapshot(); got.Last != 3 || counter.rate().Last != 1 {
		t.Fatal("delta lost precision above float64 integer range", got)
	}
	counter.observe(now.Add(4*time.Second), 1, true)
	if counter.rate() != nil {
		t.Fatal("reset counter retained old frame")
	}
	counter.observe(now.Add(7*time.Second), 7, true)
	if counter.rate().Last != 2 {
		t.Fatal("reset did not recover after real interval")
	}
	counter.observe(now.Add(10*time.Second), math.MaxUint64, true)
	if counter.rate() != nil {
		t.Fatal("unrepresentable amount invented a valid rate")
	}
	var book dashboardRateBook
	snap := client.StatsSnapshot{Node: client.ID{1}, BootID: client.ID{2}, CapturedUnixNS: uint64(now.UnixNano())}
	book.observe(snap, nil)
	snap.CapturedUnixNS += uint64(3 * time.Second)
	snap.IO.PhysicalReadBytes = 300
	book.observe(snap, nil)
	if book.frames(nil).IO.Read.Last != 100 {
		t.Fatal("fixture")
	}
	// Old publications do not rewind the clock or consume counter deltas.
	old := snap
	old.CapturedUnixNS--
	old.IO.PhysicalReadBytes++
	book.observe(old, nil)
	if book.frames(nil).IO.Read.Last != 100 {
		t.Fatal("old publication changed frame")
	}
	snap.BootID = client.ID{9}
	book.observe(snap, nil)
	if book.frames(nil).IO.Read != nil {
		t.Fatal("new process reused a previous process's mean")
	}
}

func TestDashboardRatesNativeAuthorization(t *testing.T) {
	s := &Server{}
	e := client.Encoder{}
	e.U16(1)
	e.V(0)
	m := client.Message{Kind: client.Request, Opcode: client.StatsRatesGet, Body: e.B}
	for _, p := range []principal{{}, {User: account{Role: "USER"}}, {ClusterPeer: true}, {connection: &connectionAuthorization{bucket: &Bucket{}, permissions: BucketRead}}} {
		if _, status := s.dispatchAs(m, nil, p); status != 403 {
			t.Fatal("rate frames exposed without administration capability", status)
		}
	}
	admin := principal{User: account{Role: "ADMIN"}}
	wire, status := s.dispatchAs(m, nil, admin)
	if status != 200 {
		t.Fatal(status)
	}
	d := client.Decoder{B: wire}
	if d.V() != 200 {
		t.Fatal("response status")
	}
	d.Blob()
	var frame dashboardRateFrames
	if err := json.Unmarshal([]byte(d.Text()), &frame); err != nil || d.Done() != nil || frame.Schema != 1 || frame.IO.Read != nil {
		t.Fatal("invalid native frame response", err, frame)
	}
	m.Body = []byte{2, 0, 0}
	if _, status := s.dispatchAs(m, nil, admin); status != 400 {
		t.Fatal("unknown schema accepted", status)
	}
	m.Body = append(e.B, 0)
	if _, status := s.dispatchAs(m, nil, admin); status != 400 {
		t.Fatal("trailing bytes accepted", status)
	}
	if !client.NodeLocalAdminOpcode(client.StatsRatesGet) || baselineOpcode(client.StatsRatesGet) {
		t.Fatal("rate frames must be explicit optional delegated operations")
	}
	if checkPeerOperation(s.protocolVersion(), legacyProfile(), client.StatsRatesGet) == nil {
		t.Fatal("legacy peers assumed to support new rate frames")
	}
	s.dashboardRates.observe(client.StatsSnapshot{CapturedUnixNS: uint64(time.Now().Add(-20 * time.Second).UnixNano())}, nil)
	m.Body = e.B
	if _, status := s.dispatchAs(m, nil, admin); status != 503 {
		t.Fatal("stale publisher renewed a cached rate frame", status)
	}
}
