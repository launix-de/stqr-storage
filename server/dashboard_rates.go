// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
)

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

func (b *statisticsBook) frames(users []client.ID) dashboardRateFrames {
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
	frames := s.statistics.frames(users)
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
