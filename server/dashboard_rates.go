// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
)

const dashboardRateInterval = 3 * time.Second

// Rate frames are advisory RAM aggregates, bounded by volumes, buckets and
// accounts. They never enumerate object indexes. Lifetime counters remain the
// source for totals; only exact differences enter the common interval counter.
type dashboardRateCounter struct {
	previous uint64
	started  bool
	window   decay.Accumulator
}

func (c *dashboardRateCounter) observe(now time.Time, total uint64, tick bool) {
	if !c.started || total < c.previous || total-c.previous > math.MaxInt64 {
		// A reset or an unrepresentable interval is unknown, never negative I/O.
		*c = dashboardRateCounter{previous: total, started: true}
		c.window.Tick(now, decay.DefaultLongHalfLife)
		return
	}
	c.window.Add(int64(total - c.previous))
	c.previous = total
	if tick {
		c.window.Tick(now, decay.DefaultLongHalfLife)
	}
}

type dashboardRatePair struct {
	Last    float64 `json:"last"`
	Average float64 `json:"average"`
}

func (c *dashboardRateCounter) rate() *dashboardRatePair {
	snapshot := c.window.Snapshot()
	if !snapshot.Known || snapshot.Saturated {
		return nil
	}
	return &dashboardRatePair{Last: float64(snapshot.Last) / snapshot.Interval.Seconds(), Average: snapshot.Average}
}

type dashboardIORates struct {
	Read         *dashboardRatePair `json:"read"`
	Write        *dashboardRatePair `json:"write"`
	LogicalRead  *dashboardRatePair `json:"logicalRead"`
	LogicalWrite *dashboardRatePair `json:"logicalWrite"`
}

type dashboardIOCounters [4]dashboardRateCounter

func (c *dashboardIOCounters) observe(now time.Time, io client.IOStats, tick bool) {
	for i, total := range [4]uint64{io.PhysicalReadBytes, io.PhysicalWriteBytes, io.LogicalReadBytes, io.LogicalWriteBytes} {
		c[i].observe(now, total, tick)
	}
}
func (c *dashboardIOCounters) rates() dashboardIORates {
	return dashboardIORates{c[0].rate(), c[1].rate(), c[2].rate(), c[3].rate()}
}

type dashboardRateBook struct {
	mu             sync.Mutex
	node, boot     client.ID
	observed, tick time.Time
	io, userIO     dashboardIOCounters
	volumes        map[string]*dashboardIOCounters
	buckets        map[string]*dashboardIOCounters
	users          map[string]*dashboardIOCounters
}

func observeDashboardEntities(current map[string]*dashboardIOCounters, totals map[string]client.IOStats, now time.Time, tick bool) map[string]*dashboardIOCounters {
	if current == nil {
		current = make(map[string]*dashboardIOCounters, len(totals))
	}
	for id, io := range totals {
		c := current[id]
		if c == nil {
			c = &dashboardIOCounters{}
			current[id] = c
		}
		c.observe(now, io, tick)
	}
	for id := range current {
		if _, exists := totals[id]; !exists {
			delete(current, id)
		}
	}
	return current
}

func (b *dashboardRateBook) observe(snap client.StatsSnapshot, users map[string]client.IOStats) {
	if snap.CapturedUnixNS > math.MaxInt64 {
		return
	}
	now := time.Unix(0, int64(snap.CapturedUnixNS))
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.boot != snap.BootID || b.node != snap.Node {
		b.node, b.boot = snap.Node, snap.BootID
		b.observed, b.tick = time.Time{}, time.Time{}
		b.io, b.userIO = dashboardIOCounters{}, dashboardIOCounters{}
		b.volumes, b.buckets, b.users = nil, nil, nil
	}
	if !b.observed.IsZero() && !now.After(b.observed) {
		return // racing publications cannot rewind a frame
	}
	tick := !b.tick.IsZero() && now.Sub(b.tick) >= dashboardRateInterval
	if b.tick.IsZero() || tick {
		b.tick = now
	}
	b.observed = now
	b.io.observe(now, snap.IO, tick)
	volumes := make(map[string]client.IOStats, len(snap.Volumes))
	for _, volume := range snap.Volumes {
		volumes[volume.ID.String()] = volume.IO
	}
	buckets := make(map[string]client.IOStats, len(snap.Buckets))
	for _, bucket := range snap.Buckets {
		buckets[bucket.ID.String()] = bucket.IO
	}
	b.volumes = observeDashboardEntities(b.volumes, volumes, now, tick)
	b.buckets = observeDashboardEntities(b.buckets, buckets, now, tick)
	b.users = observeDashboardEntities(b.users, users, now, tick)
	var userIO client.IOStats
	for _, io := range users {
		userIO.LogicalReadBytes = addCapacity(userIO.LogicalReadBytes, io.LogicalReadBytes)
		userIO.LogicalWriteBytes = addCapacity(userIO.LogicalWriteBytes, io.LogicalWriteBytes)
	}
	b.userIO.observe(now, userIO, tick)
}

func (s *Server) observeDashboardRates(snap client.StatsSnapshot) {
	users := make(map[string]client.IOStats)
	// The resident account index is immutable. Do not load persistent metadata
	// or retain deleted account histories as part of telemetry collection.
	if accounts := s.accounts.Load(); accounts != nil {
		for _, user := range accounts.image.Users {
			read, write := s.userTraffic(user.ID)
			users[user.ID] = client.IOStats{LogicalReadBytes: read, LogicalWriteBytes: write}
		}
	}
	s.dashboardRates.observe(snap, users)
}

type dashboardRateFrames struct {
	Schema     uint16                      `json:"schema"`
	Node       string                      `json:"node"`
	Boot       string                      `json:"boot"`
	CapturedNS string                      `json:"captured_ns"`
	HalfLifeNS string                      `json:"half_life_ns"`
	IO         dashboardIORates            `json:"io"`
	UserIO     dashboardIORates            `json:"user_io"`
	Volumes    map[string]dashboardIORates `json:"volumes"`
	Buckets    map[string]dashboardIORates `json:"buckets"`
	Users      map[string]dashboardIORates `json:"users"`
}

func (b *dashboardRateBook) frames(users []client.ID) dashboardRateFrames {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := dashboardRateFrames{Schema: 1, Node: b.node.String(), Boot: b.boot.String(), HalfLifeNS: strconv.FormatInt(int64(decay.DefaultLongHalfLife), 10), IO: b.io.rates(), UserIO: b.userIO.rates(), Volumes: map[string]dashboardIORates{}, Buckets: map[string]dashboardIORates{}, Users: map[string]dashboardIORates{}}
	if !b.observed.IsZero() {
		out.CapturedNS = strconv.FormatInt(b.observed.UnixNano(), 10)
	}
	for id, counters := range b.volumes {
		out.Volumes[id] = counters.rates()
	}
	for id, counters := range b.buckets {
		out.Buckets[id] = counters.rates()
	}
	for _, user := range users {
		if counters := b.users[user.String()]; counters != nil {
			out.Users[user.String()] = counters.rates()
		}
	}
	return out
}

func (s *Server) dashboardRateFrames(body []byte) ([]byte, error) {
	d := client.Decoder{B: body}
	if d.U16() != 1 {
		return nil, fail(400, "unknown rate frame request schema")
	}
	var users []client.ID
	for i, n := 0, d.Count(1000); i < n; i++ {
		users = append(users, d.ID())
	}
	if d.Done() != nil {
		return nil, fail(400, "invalid rate frame request")
	}
	frames := s.dashboardRates.frames(users)
	if frames.CapturedNS != "" {
		captured, _ := strconv.ParseInt(frames.CapturedNS, 10, 64)
		if time.Since(time.Unix(0, captured)) >= 10*time.Second {
			return nil, fail(503, "rate frame source is stale")
		}
	}
	raw, err := json.Marshal(frames)
	if err != nil {
		return nil, err
	}
	if len(raw) > client.MaxMessage-1024 {
		return nil, fail(413, "rate frame response exceeds message budget")
	}
	e := client.Encoder{}
	e.Text(string(raw))
	return e.B, nil
}
