// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func placementReadersFixture(t *testing.T) (*Server, *rangeManager, *Bucket, principal, rangeMetricKey, time.Time) {
	t.Helper()
	s := &Server{}
	r := &rangeManager{server: s, node: client.ID{1}}
	s.scopes = r
	b, err := NewBucket(volume.ID{2}, "reader-test")
	if err != nil {
		t.Fatal(err)
	}
	b.server = s
	key := rangeMetricKey{bucket: client.ID(b.ID())}
	image, err := buildPlacement([]client.MapPatch{{Membership: 1, Generation: 1, Revision: 1, Bucket: key.bucket, ToEnd: true, Entries: []client.RangeEntry{{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: r.node, Volume: client.ID{9}}}}}, Addresses: []client.NodeAddress{{Node: r.node, Addresses: []string{"http://node:7419"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	s.currentSystem.Store(&systemView{manifest: systemManifest{Membership: client.Membership{Members: []client.Member{{ID: r.node}, {ID: client.ID{3}}}}}})
	auth := &connectionAuthorization{readerID: client.ID{7}, accountID: "account", bucket: b, bucketID: b.ID(), permissions: BucketRead}
	auth.placementHints.Store(true)
	return s, r, b, principal{connection: auth}, key, time.Unix(100, 0)
}

func readerHints(key rangeMetricKey, p principal, seq uint64) client.PlacementHints {
	return client.PlacementHints{Bucket: key.bucket, Reader: p.connection.readerID, Sequence: seq, Paths: []client.PlacementPath{{Node: client.ID{1}, RTTNS: uint64(time.Millisecond)}, {Node: client.ID{3}, RTTNS: uint64(10 * time.Millisecond)}}}
}

func TestPlacementReadersActualDemandAndConservativeMissingPath(t *testing.T) {
	_, r, _, p, key, now := placementReadersFixture(t)
	h := readerHints(key, p, 1)
	group := placementReaderKey{key.bucket, p.connection.accountID, p.connection.readerID}
	if !r.storePlacementHints(group, h, now) {
		t.Fatal("store")
	}
	nodes := []client.ID{{1}, {3}, {4}}
	if costs := r.placementReaderCosts(key, nodes, 10, now); costs[0] != 0 || costs[1] != 0 {
		t.Fatal("hints invented demand", costs)
	}
	m := &rangeMetric{sampled: now}
	r.metrics.Store(key, m)
	recordPlacementReader(m, key, p, 1000, 0, now)
	costs := r.placementReaderCosts(key, nodes, 10, now.Add(10*time.Second))
	for i, want := range []float64{2000, 11000, 11000} {
		if math.Abs(costs[i]-want) > 1e-7 {
			t.Fatal("forecast or unknown-path optimism", costs)
		}
	}
	// Polling neither re-adds traffic nor changes the forecast.
	again := r.placementReaderCosts(key, nodes, 10, now.Add(10*time.Second))
	for i := range costs {
		if again[i] != costs[i] {
			t.Fatal("poll counted twice", again, costs)
		}
	}
	idle := r.placementReaderCosts(key, nodes, 10, now.Add(20*time.Second))
	half := r.server.placementHalfLives()
	want := costs[0] * math.Exp2(-float64(10*time.Second)/float64(half.Long))
	if math.Abs(idle[0]-want) > 1e-7 {
		t.Fatal("idle forecast did not follow long-window decay", idle, want)
	}
	traffic := m.readers[group]
	if got := traffic.readWindow.Rates(now.Add(20*time.Second), half).Short; math.Abs(got-50) > 1e-7 {
		t.Fatal("short-window idle demand did not halve", got)
	}
	if stale := r.placementReaderCosts(key, nodes, 10, now.Add(placementPathTTL+time.Second)); stale[0] != 0 || stale[1] != 0 || stale[2] != 0 {
		t.Fatal("stale path used", stale)
	}
}

func TestPlacementReadersAccountScopeAndReplay(t *testing.T) {
	_, r, _, p, key, now := placementReadersFixture(t)
	group := placementReaderKey{key.bucket, "other-account", p.connection.readerID}
	h := readerHints(key, p, 1)
	r.storePlacementHints(group, h, now)
	m := &rangeMetric{}
	r.metrics.Store(key, m)
	recordPlacementReader(m, key, p, 1000, 0, now)
	if c := r.placementReaderCosts(key, []client.ID{{1}, {3}}, 10, now.Add(10*time.Second)); c[0] != 0 || c[1] != 0 {
		t.Fatal("other account's profile reused", c)
	}
	wrong := &rangeMetric{}
	recordPlacementReader(wrong, rangeMetricKey{bucket: client.ID{99}}, p, 1000, 0, now)
	if len(wrong.readers) != 0 {
		t.Fatal("cross-bucket traffic entered reader profile")
	}
	group.account = p.connection.accountID
	r.storePlacementHints(group, h, now)
	r.storePlacementHints(group, h, now.Add(time.Minute))
	if !r.readers.groups[group].at.Equal(now) {
		t.Fatal("replay refreshed profile")
	}
	r.prunePlacementReaders(now.Add(placementPathTTL + time.Second))
	if len(r.readers.groups) != 0 {
		t.Fatal("stale groups retained")
	}
}

func TestPlacementReadersBoundsAndRangeAggregation(t *testing.T) {
	_, r, b, p, key, now := placementReadersFixture(t)
	for i := 1; i <= maxPlacementRangeReaders+1; i++ {
		p.connection.readerID = client.ID{byte(i)}
		b.recordRangeTraffic(p, volume.ObjectID{byte(i)}, 1, 0)
	}
	value, _ := r.metrics.Load(key)
	m := value.(*rangeMetric)
	if len(m.readers) != maxPlacementRangeReaders {
		t.Fatal("unbounded per-range groups", len(m.readers))
	}
	for i := 0; i < 10000; i++ {
		b.recordRangeTraffic(p, volume.ObjectID{byte(i), byte(i >> 8)}, 1, 0)
	}
	count := 0
	r.metrics.Range(func(_, _ any) bool { count++; return true })
	if count != 1 || len(m.readers) != maxPlacementRangeReaders || m.read.Load() != 10000+maxPlacementRangeReaders+1 {
		t.Fatal("object history or lost totals", count, len(m.readers), m.read.Load())
	}
	// A bounded slot can be reused after its old reader has gone quiet.
	for _, entry := range m.readers {
		entry.active = now
	}
	recordPlacementReader(m, key, p, 1, 0, now.Add(placementPathTTL+time.Second))
	if len(m.readers) != 1 {
		t.Fatal("inactive slots block new reader", len(m.readers))
	}
	for i := 0; i < maxPlacementReaderGroups; i++ {
		var reader client.ID
		binary.LittleEndian.PutUint64(reader[:8], uint64(i+1))
		h := client.PlacementHints{Bucket: key.bucket, Reader: reader, Sequence: 1, Paths: []client.PlacementPath{{Node: r.node, RTTNS: 1}}}
		if !r.storePlacementHints(placementReaderKey{key.bucket, "a", reader}, h, now) {
			t.Fatal("premature budget", i)
		}
	}
	extra := client.ID{0xff, 0xff, 0xff}
	h := client.PlacementHints{Bucket: key.bucket, Reader: extra, Sequence: 1}
	if r.storePlacementHints(placementReaderKey{key.bucket, "a", extra}, h, now) {
		t.Fatal("global group bound exceeded")
	}
	if !r.storePlacementHints(placementReaderKey{key.bucket, "a", extra}, h, now.Add(placementPathTTL+time.Second)) || len(r.readers.groups) != 1 {
		t.Fatal("stale global state did not make room", len(r.readers.groups))
	}
}

func TestPlacementHintsAuthorizationAndBounds(t *testing.T) {
	s, r, _, p, key, _ := placementReadersFixture(t)
	h := readerHints(key, p, 1)
	body, _ := h.MarshalBinary()
	m := client.Message{Kind: client.Request, Opcode: client.PlacementHintsMessage, Body: body}
	ws := &wsSession{authorization: p.connection, principal: p}
	if _, status := s.dispatchAs(m, ws, p); status != 200 {
		t.Fatal("valid report", status)
	}
	if len(r.readers.groups) != 1 {
		t.Fatal("report not recorded")
	}
	for _, tc := range []struct {
		name      string
		principal principal
		ws        *wsSession
		message   client.Message
		want      int
	}{
		{"http", p, nil, m, 403},
		{"anonymous", principal{}, ws, m, 403},
		{"malformed", p, ws, client.Message{Kind: client.Request, Opcode: m.Opcode, Body: []byte{1}}, 400},
		{"fire", p, ws, client.Message{Kind: client.FireAndForget, Opcode: m.Opcode, Body: body}, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, status := s.dispatchAs(tc.message, tc.ws, tc.principal); status != tc.want {
				t.Fatal(status, tc.want)
			}
		})
	}
	for _, mutate := range []func(*client.PlacementHints){func(h *client.PlacementHints) { h.Reader = client.ID{8} }, func(h *client.PlacementHints) { h.Bucket = client.ID{8} }} {
		copy := h
		mutate(&copy)
		wrong, _ := copy.MarshalBinary()
		m.Body = wrong
		if _, status := s.dispatchAs(m, ws, p); status != 403 {
			t.Fatal("cross-identity report", status)
		}
	}
	h.Paths = []client.PlacementPath{{Node: client.ID{0xff}, RTTNS: 1}}
	m.Body, _ = h.MarshalBinary()
	if _, status := s.dispatchAs(m, ws, p); status != 400 {
		t.Fatal("unknown membership target", status)
	}
	m.Body = client.PlacementProbe()
	if _, status := s.dispatchAs(m, ws, p); status != 200 {
		t.Fatal("probe", status)
	}
	m.Body = m.Body[:len(m.Body)-1]
	if _, status := s.dispatchAs(m, ws, p); status != 400 {
		t.Fatal("unbounded probe", status)
	}
	p.connection.placementHints.Store(false)
	m.Body = body
	if _, status := s.dispatchAs(m, ws, p); status != 403 {
		t.Fatal("unnegotiated report", status)
	}
	p.connection.placementHints.Store(true)
	p.connection.clusterPeer = true
	if _, status := s.dispatchAs(m, ws, p); status != 403 {
		t.Fatal("peer reader claim", status)
	}
	if _, ok := s.protocolVersion().Operations["0123"]; !ok {
		t.Fatal("missing explicit opcode advertisement")
	}
}

func TestPlacementPeerDistanceAndExpiry(t *testing.T) {
	_, r, _, _, _, now := placementReadersFixture(t)
	peer := client.ID{3}
	r.observePlacementPeer(peer, 7*time.Millisecond, now)
	d := r.placementPeerDistances(now)
	if got := d[[2]client.ID{r.node, peer}]; got != client.PlacementDistance(7*time.Millisecond, 0) {
		t.Fatal("client/peer scales differ", got)
	}
	if _, ok := d[[2]client.ID{peer, r.node}]; ok {
		t.Fatal("invented symmetric measurement")
	}
	if len(r.placementPeerDistances(now.Add(placementPathTTL+time.Second))) != 0 || len(r.readers.peers) != 0 {
		t.Fatal("stale peer used")
	}
}
