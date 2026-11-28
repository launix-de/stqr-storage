// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestPlacementHintsCodec(t *testing.T) {
	h := PlacementHints{Bucket: ID{1}, Reader: ID{2}, Sequence: 3, Paths: []PlacementPath{{Node: ID{4}, RTTNS: uint64(time.Millisecond), BytesPerSecond: 1 << 20}}}
	body, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePlacementHints(body)
	if err != nil || decoded.Bucket != h.Bucket || decoded.Reader != h.Reader || decoded.Sequence != h.Sequence || len(decoded.Paths) != 1 || decoded.Paths[0] != h.Paths[0] {
		t.Fatal(decoded, err)
	}
	for i := 0; i < len(body); i++ {
		if _, err := DecodePlacementHints(body[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
	if _, err := DecodePlacementHints(append(append([]byte{}, body...), 0)); err == nil {
		t.Fatal("accepted trailing byte")
	}
	for _, edit := range []func(*PlacementHints){func(v *PlacementHints) { v.Reader = ID{} }, func(v *PlacementHints) { v.Sequence = 0 }, func(v *PlacementHints) { v.Paths[0].RTTNS = 0 }, func(v *PlacementHints) { v.Paths = append(v.Paths, v.Paths[0]) }, func(v *PlacementHints) { v.Paths = make([]PlacementPath, MaxPlacementPaths+1) }} {
		copy := h
		copy.Paths = append([]PlacementPath(nil), h.Paths...)
		edit(&copy)
		if _, err := copy.MarshalBinary(); err == nil {
			t.Fatal("accepted malformed hints", copy)
		}
	}
	if len(PlacementProbe()) != 2+PlacementProbeBytes {
		t.Fatal("probe bound")
	}
}

func TestPlacementDistance(t *testing.T) {
	if got := PlacementDistance(time.Millisecond, 0); got != 2 {
		t.Fatal(got)
	}
	if got := PlacementDistance(time.Millisecond, 100<<20); got != 3 {
		t.Fatal(got)
	}
	if PlacementDistance(10*time.Millisecond, 100<<20) <= PlacementDistance(time.Millisecond, 100<<20) {
		t.Fatal("distance ignored RTT")
	}
	if PlacementDistance(time.Millisecond, 1<<20) <= PlacementDistance(time.Millisecond, 100<<20) {
		t.Fatal("distance ignored throughput")
	}
	for _, rate := range []float64{math.NaN(), math.Inf(1), -1} {
		if got := PlacementDistance(time.Millisecond, rate); math.IsNaN(got) || math.IsInf(got, 0) || got < 1 {
			t.Fatal(got)
		}
	}
	if PlacementDistance(0, 0) != 1e6 || PlacementDistance(2*time.Minute, 0) != 1e6 {
		t.Fatal("invalid latency became cheap")
	}
}

type placementWireFixture struct {
	reports             chan PlacementHints
	probes              chan struct{}
	calls               atomic.Int64
	enabled, stallProbe bool
	reader              chan string
	node                ID
	closed              chan struct{}
}

func newPlacementWire(t *testing.T, enabled, stall bool) (*placementWireFixture, *httptest.Server) {
	t.Helper()
	f := &placementWireFixture{node: ID{4}, closed: make(chan struct{}, 16), reports: make(chan PlacementHints, 16), probes: make(chan struct{}, 16), reader: make(chan string, 16), enabled: enabled, stallProbe: stall}
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.reader <- r.Header.Get(PlacementReaderHeader)
		conn, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		defer func() { f.closed <- struct{}{} }()
		for {
			_, body, err := conn.ReadMessage()
			if err != nil {
				return
			}
			m, err := DecodeMessage(body)
			if err != nil {
				return
			}
			var result []byte
			switch m.Opcode {
			case Hello:
				e := Encoder{}
				e.U16(2)
				caps := uint64(0)
				if enabled {
					caps = CapabilityPlacementHints
				}
				e.U64(caps)
				e.ID(f.node)
				e.ID(ID{5})
				e.U64(1)
				e.U64(1)
				result = e.B
			case Ping:
				result = m.Body
			case PlacementHintsMessage:
				f.calls.Add(1)
				if len(m.Body) > 1 && m.Body[1] == PlacementHintProbe {
					f.probes <- struct{}{}
					if stall {
						continue
					}
					result = m.Body
				} else {
					h, err := DecodePlacementHints(m.Body)
					if err != nil {
						t.Error(err)
						return
					}
					f.reports <- h
				}
			default:
				continue
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: ResponseBody(200, result, "")})); err != nil {
				return
			}
		}
	}))
	t.Cleanup(host.Close)
	return f, host
}

func TestPlacementHintsAutomaticAndClose(t *testing.T) {
	fixture, host := newPlacementWire(t, true, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, host.URL, "u", "p", ID{1}, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case h := <-fixture.reports:
		if h.Bucket != (ID{1}) || h.Reader != (c.placementReader) || len(h.Paths) != 1 || h.Paths[0].Node != (ID{4}) || h.Paths[0].RTTNS == 0 {
			t.Fatal(h)
		}
	case <-ctx.Done():
		t.Fatal("automatic hint not published")
	}
	if reader := <-fixture.reader; reader != c.placementReader.String() {
		t.Fatal(reader)
	}
	c.SetPlacementMeasurements(false)
	if err := c.ReportPlacementHints(ctx, []PlacementPath{{Node: ID{4}, RTTNS: uint64(50 * time.Millisecond)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case h := <-fixture.reports:
		if len(h.Paths) != 1 || h.Paths[0].RTTNS != uint64(50*time.Millisecond) {
			t.Fatal(h)
		}
	case <-ctx.Done():
		t.Fatal("manual hints missing")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.placementDone:
	default:
		t.Fatal("measurement worker outlived Close")
	}
}

func TestPlacementHintsLegacyNoProbe(t *testing.T) {
	fixture, host := newPlacementWire(t, false, false)
	c, err := Dial(context.Background(), host.URL, "u", "p", ID{1}, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.placementDone != nil {
		t.Fatal("legacy started telemetry")
	}
	if err := c.ReportPlacementHints(context.Background(), []PlacementPath{{Node: ID{4}, RTTNS: 1}}); err == nil {
		t.Fatal("unsupported hints accepted")
	}
	if fixture.calls.Load() != 0 {
		t.Fatal("legacy received optional opcode")
	}
}

func TestPlacementHintsCloseCancelsProbe(t *testing.T) {
	fixture, host := newPlacementWire(t, true, true)
	c, err := Dial(context.Background(), host.URL, "u", "p", ID{1}, "fs")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-fixture.probes:
	case <-time.After(3 * time.Second):
		c.Close()
		t.Fatal("probe not started")
	}
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close waits for stalled probe")
	}
}

func TestPlacementHintsProbeBytes(t *testing.T) {
	if !bytes.Equal(PlacementProbe()[2:], make([]byte, PlacementProbeBytes)) {
		t.Fatal("probe contains application data")
	}
}

func TestPlacementHintsIdleSuppressionAndPauseSequence(t *testing.T) {
	fixture, host := newPlacementWire(t, true, false)
	c, err := Dial(context.Background(), host.URL, "u", "p", ID{1}, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case <-fixture.reports:
	case <-time.After(3 * time.Second):
		t.Fatal("initial report missing")
	}
	before := fixture.calls.Load()
	c.placementActivity.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	cursor := 0
	c.placementMeasurementCycle(make(map[ID]placementSample), &cursor)
	if fixture.calls.Load() != before {
		t.Fatal("idle client still probing")
	}
	c.notePlacementActivity()
	c.placementMeasurementCycle(make(map[ID]placementSample), &cursor)
	if fixture.calls.Load() <= before {
		t.Fatal("active client did not resume")
	}
	c.SetPlacementMeasurements(false)
	c.mu.Lock()
	seq := c.placementSequence
	c.mu.Unlock()
	if err := c.reportPlacementHints(context.Background(), []PlacementPath{{Node: ID{4}, RTTNS: 1}}, true); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	after := c.placementSequence
	c.mu.Unlock()
	if after != seq {
		t.Fatal("paused in-flight auto report allocated a later sequence")
	}
}

func TestPlacementHintsAlternativeProbeDoesNotGrowOwnerPool(t *testing.T) {
	primary, host := newPlacementWire(t, true, false)
	alternative, other := newPlacementWire(t, true, false)
	alternative.node = ID{6}
	c, err := Dial(context.Background(), host.URL, "u", "p", ID{1}, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case <-primary.reports:
	case <-time.After(3 * time.Second):
		t.Fatal("initial report missing")
	}
	c.SetPlacementMeasurements(false)
	endpoint := "ws" + strings.TrimPrefix(other.URL, "http") + "/api/ws"
	c.mu.Lock()
	c.routers[endpoint] = ID{6}
	c.mu.Unlock()
	path, ok := c.measurePlacementEndpoint(placementEndpoint{endpoint, ID{6}})
	if !ok || path.Node != (ID{6}) {
		t.Fatal(path, ok)
	}
	c.mu.Lock()
	pooled := len(c.routed)
	c.mu.Unlock()
	if pooled != 0 {
		t.Fatal("measurement retained extra owner transport", pooled)
	}
	select {
	case <-alternative.closed:
	case <-time.After(time.Second):
		t.Fatal("ephemeral probe connection outlived measurement")
	}
	if reader := <-alternative.reader; reader != c.placementReader.String() {
		t.Fatal("redirect probe changed reader identity", reader)
	}
}
