// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

// Observations are administrative telemetry, never a membership or ownership
// list. A transport loss marks the observation offline but retains its counters.
// The membership/recovery controller owns dialing and admission; it attaches one
// observer only after validating a peer. No heartbeat timeout evicts a node.
type peerObservation struct {
	connection *peerConnection
	snapshot   *client.StatsSnapshot
	lastSeen   time.Time
	online     bool
}

// startPeerObservation owns consumption of this peer's telemetry push stream.
// It may be reattached after reconnect. An obsolete observer cannot overwrite
// the snapshot or offline status of a newer connection.
func (s *Server) startPeerObservation(p *peerConnection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || p.server != s || p.Node == (client.ID{}) || p.ctx.Err() != nil {
		return fail(503, "peer is not connected")
	}
	s.peerStatsMu.Lock()
	defer s.peerStatsMu.Unlock()
	if s.peerStats == nil {
		s.peerStats = make(map[client.ID]*peerObservation)
	}
	prior := s.peerStats[p.Node]
	if prior != nil && prior.online && prior.connection != p && prior.connection.ctx.Err() == nil {
		return fail(409, "peer already has a telemetry connection")
	}
	if prior != nil && prior.connection == p {
		return fail(409, "peer telemetry already started")
	}
	if prior == nil && len(s.peerStats) >= 10000 {
		return fail(507, "peer telemetry source budget exhausted")
	}
	observation := &peerObservation{connection: p, online: true}
	if prior != nil {
		observation.snapshot, observation.lastSeen = prior.snapshot, prior.lastSeen
	}
	s.peerStats[p.Node] = observation
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		defer func() {
			p.close()
			s.peerStatsMu.Lock()
			if s.peerStats[p.Node] == observation {
				observation.online = false
			}
			s.peerStatsMu.Unlock()
		}()
		s.observePeer(p, observation)
	}()
	return nil
}

func (s *Server) observePeer(p *peerConnection, observation *peerObservation) {
	sub := client.NewRequestID()
	e := client.Encoder{}
	e.ID(sub)
	e.U8(0) // LOCAL: avoid forwarding cluster snapshots in loops.
	e.V(0)
	e.V(0)
	e.V(0)
	e.Bool(true)
	e.U32(3000)
	// This bounds a transport handshake only, not membership or node lifetime.
	ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
	ack, err := p.request(ctx, client.StatsSubscribe, e.B)
	cancel()
	if err != nil {
		return
	}
	d := client.Decoder{B: ack}
	if d.ID() != sub || d.Done() != nil {
		return
	}
	assembler := client.NewStatsAssembler(1)
	var lastProbe time.Time
	for {
		m, err := p.nextPush(p.ctx)
		if err != nil || m.Opcode != client.StatsSnapshotMessage {
			return
		}
		id, part, err := client.DecodeStatsPush(m.Body)
		if err != nil || id != sub || part.Source != p.Node {
			return
		}
		snapshot, err := assembler.Add(part)
		if err != nil {
			return
		}
		if snapshot == nil {
			continue
		}
		s.peerStatsMu.Lock()
		current := s.peerStats[p.Node] == observation
		if current {
			observation.snapshot = snapshot
			observation.lastSeen = time.Now()
		}
		s.peerStatsMu.Unlock()
		if !current {
			return
		}
		s.broadcastStats(*snapshot)
		// Advisory, bounded path observation. A failed probe never changes peer
		// availability, membership, replica counts or placement authority.
		if s.scopes != nil && time.Since(lastProbe) >= 10*time.Second {
			lastProbe = time.Now()
			probe := client.Encoder{}
			probe.U64(uint64(lastProbe.UnixNano()))
			probeCtx, stop := context.WithTimeout(p.ctx, time.Second)
			_, _ = p.request(probeCtx, client.Ping, probe.B)
			stop()
		}
	}
}

func (s *Server) statsSnapshots(f client.StatsFilter) []client.StatsSnapshot {
	var snapshots []client.StatsSnapshot
	local := s.latest.Load()
	if local != nil && matches(f.Nodes, local.Node) {
		snapshots = append(snapshots, *local)
	}
	if f.Scope == 1 {
		s.peerStatsMu.RLock()
		for _, observation := range s.peerStats {
			if snapshot := observation.snapshot; snapshot != nil && matches(f.Nodes, snapshot.Node) {
				snapshots = append(snapshots, *snapshot)
			}
		}
		s.peerStatsMu.RUnlock()
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Node.String() < snapshots[j].Node.String() })
	return snapshots
}
