// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"math"
	"sort"
	"time"
)

// Placement hints are optional, bucket-bound advisory telemetry. They never
// change an operation's authority, durability, permissions or retry semantics.
const (
	PlacementHintsMessage    uint16 = 0x0123
	CapabilityPlacementHints uint64 = 1 << 23
	PlacementReaderHeader           = "X-STQR-Reader"
	MaxPlacementPaths               = 16
	PlacementProbeBytes             = 32 << 10
	PlacementHintVersion            = 1
	PlacementHintReport             = 0
	PlacementHintProbe              = 1
)

// PlacementPath measures one directed client-to-node connection. Zero throughput
// means unavailable, not an infinitely fast connection. RTT must be positive.
type PlacementPath struct {
	Node                  ID
	RTTNS, BytesPerSecond uint64
}

type PlacementHints struct {
	Bucket, Reader ID
	Sequence       uint64
	Paths          []PlacementPath
}

// PlacementDistance is shared by client and peer transfer accounting. It is a
// dimensionless byte-distance: 1 ms RTT adds one unit, and known throughput adds
// its inverse relative to 100 MiB/s. All network transfers use the same scale.
func PlacementDistance(rtt time.Duration, bytesPerSecond float64) float64 {
	if rtt <= 0 || rtt > time.Minute || math.IsNaN(bytesPerSecond) || math.IsInf(bytesPerSecond, 0) || bytesPerSecond < 0 {
		return 1e6
	}
	d := 1 + float64(rtt)/float64(time.Millisecond)
	if bytesPerSecond > 0 {
		d += (100 << 20) / bytesPerSecond
	}
	return math.Min(1e6, d)
}

func (h PlacementHints) MarshalBinary() ([]byte, error) {
	if h.Bucket == (ID{}) || h.Reader == (ID{}) || h.Sequence == 0 || len(h.Paths) > MaxPlacementPaths {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.U8(PlacementHintVersion)
	e.U8(PlacementHintReport)
	e.ID(h.Bucket)
	e.ID(h.Reader)
	e.U64(h.Sequence)
	e.V(uint64(len(h.Paths)))
	seen := make(map[ID]bool, len(h.Paths))
	for _, p := range h.Paths {
		if p.Node == (ID{}) || seen[p.Node] || p.RTTNS == 0 || p.RTTNS > uint64(time.Minute) || p.BytesPerSecond > 1<<50 {
			return nil, ErrProtocol
		}
		seen[p.Node] = true
		e.ID(p.Node)
		e.U64(p.RTTNS)
		e.U64(p.BytesPerSecond)
	}
	return e.B, nil
}

func DecodePlacementHints(body []byte) (PlacementHints, error) {
	if len(body) > 43+MaxPlacementPaths*32 {
		return PlacementHints{}, ErrProtocol
	}
	d := Decoder{B: body}
	if d.U8() != PlacementHintVersion || d.U8() != PlacementHintReport {
		return PlacementHints{}, ErrProtocol
	}
	h := PlacementHints{Bucket: d.ID(), Reader: d.ID(), Sequence: d.U64()}
	n := d.Count(MaxPlacementPaths)
	if d.Err != nil || n > len(d.B)/32 {
		return h, ErrProtocol
	}
	h.Paths = make([]PlacementPath, n)
	for i := range h.Paths {
		h.Paths[i] = PlacementPath{Node: d.ID(), RTTNS: d.U64(), BytesPerSecond: d.U64()}
	}
	if d.Done() != nil {
		return h, ErrProtocol
	}
	if _, err := h.MarshalBinary(); err != nil {
		return h, err
	}
	return h, nil
}

// PlacementProbe constructs a fixed, bounded echo payload. Its bytes are never
// stored as objects and are excluded from application access statistics.
func PlacementProbe() []byte {
	b := make([]byte, 2+PlacementProbeBytes)
	b[0], b[1] = PlacementHintVersion, PlacementHintProbe
	return b
}

// SetPlacementMeasurements enables or pauses automatic background probes.
// Manual ReportPlacementHints remains available. Pausing is useful when a caller
// already measures paths or must avoid additional network traffic.
func (c *Client) SetPlacementMeasurements(enabled bool) {
	c.mu.Lock()
	c.placementPaused = !enabled
	c.mu.Unlock()
}

// ReportPlacementHints publishes caller measurements to the currently open
// owner connections. Missing capability returns status 501; failed advisory
// publication never changes the result of an object operation. Reader identity
// and sequence are supplied by the Client and persist across owner redirects.
func (c *Client) ReportPlacementHints(ctx context.Context, paths []PlacementPath) error {
	return c.reportPlacementHints(ctx, paths, false)
}

func (c *Client) reportPlacementHints(ctx context.Context, paths []PlacementPath, automatic bool) error {
	c.mu.Lock()
	if automatic && c.placementPaused {
		c.mu.Unlock()
		return nil
	}
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.placementSequence++
	h := PlacementHints{Bucket: c.bucket, Reader: c.placementReader, Sequence: c.placementSequence, Paths: paths}
	transports := c.placementTransportsLocked()
	c.mu.Unlock()
	body, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	if len(transports) == 0 {
		return &StatusError{Status: 501, Message: "placement hints are unsupported"}
	}
	var first error
	for _, t := range transports {
		r, err := t.roundTrip(ctx, Message{Kind: Request, Opcode: PlacementHintsMessage, ID: NewRequestID(), Body: body}, false, nil)
		r.close()
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *Client) placementTransportsLocked() []*objectTransport {
	out := make([]*objectTransport, 0, MaxPlacementPaths)
	add := func(t *objectTransport) {
		if t == nil || t.capabilities&CapabilityPlacementHints == 0 || len(out) >= MaxPlacementPaths {
			return
		}
		select {
		case <-t.done:
			return
		default:
		}
		for _, old := range out {
			if old == t {
				return
			}
		}
		out = append(out, t)
	}
	add(c.transport)
	for _, t := range c.routed {
		add(t)
	}
	return out
}

type placementSample struct {
	path PlacementPath
	at   time.Time
}
type placementEndpoint struct {
	address string
	node    ID
}

func (c *Client) startPlacementMeasurements() {
	c.mu.Lock()
	if c.closed || c.placementDone != nil || c.transport == nil || c.transport.capabilities&CapabilityPlacementHints == 0 {
		c.mu.Unlock()
		return
	}
	c.placementDone = make(chan struct{})
	done := c.placementDone
	c.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		samples := make(map[ID]placementSample)
		cursor := 0
		for {
			c.placementMeasurementCycle(samples, &cursor)
			select {
			case <-c.life.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// notePlacementActivity records only session activity, never object identity.
// The transport calls it for actual object operations; probes do not self-refresh.
func (c *Client) notePlacementActivity() { c.placementActivity.Store(time.Now().UnixNano()) }

func (c *Client) placementMeasurementCycle(samples map[ID]placementSample, cursor *int) {
	c.mu.Lock()
	lastActivity := c.placementActivity.Load()
	if c.closed || c.placementPaused || lastActivity != 0 && time.Now().UnixNano()-lastActivity > int64(time.Minute) {
		c.mu.Unlock()
		return
	}
	byNode := make(map[ID]string)
	for endpoint, node := range c.routers {
		if node != (ID{}) && c.departed[node] == 0 && !c.paused[node] && (byNode[node] == "" || endpoint < byNode[node]) {
			byNode[node] = endpoint
		}
	}
	c.mu.Unlock()
	endpoints := make([]placementEndpoint, 0, len(byNode))
	for node, address := range byNode {
		endpoints = append(endpoints, placementEndpoint{address, node})
	}
	sort.Slice(endpoints, func(i, j int) bool { return bytes.Compare(endpoints[i].node[:], endpoints[j].node[:]) < 0 })
	for probes := 0; probes < min(2, len(endpoints)); probes++ {
		endpoint := endpoints[*cursor%len(endpoints)]
		*cursor = (*cursor + 1) % len(endpoints)
		if path, ok := c.measurePlacementEndpoint(endpoint); ok {
			if _, exists := samples[path.Node]; !exists && len(samples) >= MaxPlacementPaths {
				var oldest ID
				var at time.Time
				for node, sample := range samples {
					if at.IsZero() || sample.at.Before(at) {
						oldest, at = node, sample.at
					}
				}
				delete(samples, oldest)
			}
			samples[path.Node] = placementSample{path, time.Now()}
		}
	}
	paths := make([]PlacementPath, 0, len(samples))
	now := time.Now()
	for node, sample := range samples {
		if now.Sub(sample.at) > 5*time.Minute || byNode[node] == "" {
			delete(samples, node)
			continue
		}
		paths = append(paths, sample.path)
	}
	if len(paths) == 0 {
		return
	}
	sort.Slice(paths, func(i, j int) bool { return bytes.Compare(paths[i].Node[:], paths[j].Node[:]) < 0 })
	ctx, cancel := context.WithTimeout(c.life, 2*time.Second)
	defer cancel()
	_ = c.reportPlacementHints(ctx, paths, true)
}

func (c *Client) measurePlacementEndpoint(endpoint placementEndpoint) (PlacementPath, bool) {
	ctx, cancel := context.WithTimeout(c.life, 2*time.Second)
	defer cancel()
	c.mu.Lock()
	var t *objectTransport
	if endpoint.address == c.endpoint {
		t = c.transport
	} else {
		t = c.routed[endpoint.address]
	}
	c.mu.Unlock()
	ephemeral := false
	if t == nil || !transportAlive(t) {
		var err error
		t, err = c.connectEndpoint(ctx, endpoint.address)
		if err != nil {
			return PlacementPath{}, false
		}
		ephemeral = true
	}
	if ephemeral {
		defer func() { t.fail(ErrClosed); t.workers.Wait() }()
	}
	if t.node != endpoint.node || t.capabilities&CapabilityPlacementHints == 0 {
		return PlacementPath{}, false
	}
	e := Encoder{}
	e.U64(0)
	start := time.Now()
	r, err := t.roundTrip(ctx, Message{Kind: Request, Opcode: Ping, ID: NewRequestID(), Body: e.B}, false, nil)
	rtt := time.Since(start)
	if err != nil {
		r.close()
		return PlacementPath{}, false
	}
	r.close()
	probe := PlacementProbe()
	start = time.Now()
	r, err = t.roundTrip(ctx, Message{Kind: Request, Opcode: PlacementHintsMessage, ID: NewRequestID(), Body: probe}, false, nil)
	elapsed := time.Since(start)
	if err != nil {
		r.close()
		return PlacementPath{}, false
	}
	valid := bytes.Equal(r.result, probe)
	r.close()
	if !valid {
		return PlacementPath{}, false
	}
	var throughput uint64
	if elapsed > rtt {
		throughput = uint64(math.Min(1<<50, float64(2*PlacementProbeBytes)/(elapsed-rtt).Seconds()))
	}
	return PlacementPath{Node: endpoint.node, RTTNS: uint64(max(time.Nanosecond, rtt)), BytesPerSecond: throughput}, true
}
