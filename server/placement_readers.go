// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/internal/decay"
)

const (
	maxPlacementReaderGroups = 4096
	maxPlacementRangeReaders = 8
	maxPlacementPeerPaths    = 4096
	placementPathTTL         = 5 * time.Minute
)

type placementReaderKey struct {
	bucket  client.ID
	account string
	reader  client.ID
}

type placementPathMetric struct {
	distance decay.Mean
	at       time.Time
}

type placementReaderGroup struct {
	sequence uint64
	at       time.Time
	paths    map[client.ID]placementPathMetric
}

// The global table stores bounded bucket/reader path profiles, not object
// histories. Range traffic references these profiles through at most eight
// groups per range; overflow traffic retains the ordinary aggregate counters.
type placementReaders struct {
	mu     sync.Mutex
	groups map[placementReaderKey]*placementReaderGroup
	peers  map[client.ID]placementPathMetric
}

type placementReaderTraffic struct {
	read, write             uint64
	lastRead, lastWrite     uint64
	sampled                 time.Time
	active                  time.Time
	readWindow, writeWindow decay.Windows
}

func (s *Server) executePlacementHints(m client.Message, ws *wsSession, p principal) ([]byte, int) {
	deny := func(status int, detail string) ([]byte, int) { return client.ResponseBody(status, nil, detail), status }
	auth := p.connection
	if ws == nil || auth == nil || auth.clusterPeer || auth.bucket == nil || auth.invalid.Load() || auth.permissions == 0 || auth.readerID == (client.ID{}) || !auth.placementHints.Load() {
		return deny(403, "a negotiated bucket-bound reader connection is required")
	}
	if m.Kind != client.Request {
		return deny(400, "placement hints require a request")
	}
	if len(m.Body) >= 2 && m.Body[0] == client.PlacementHintVersion && m.Body[1] == client.PlacementHintProbe {
		if len(m.Body) != 2+client.PlacementProbeBytes {
			return deny(400, "invalid placement probe")
		}
		return client.ResponseBody(200, m.Body, ""), 200
	}
	h, err := client.DecodePlacementHints(m.Body)
	if err != nil {
		return deny(400, "invalid placement hints")
	}
	if h.Bucket != client.ID(auth.bucketID) || h.Reader != auth.readerID {
		return deny(403, "placement hint identity differs from bound connection")
	}
	if s.scopes == nil {
		return deny(501, "placement telemetry is unavailable")
	}
	// Only committed member identities are useful targets. Client input remains
	// advisory and cannot create nodes or turn a contact into placement authority.
	view := s.currentSystem.Load()
	if view == nil {
		return deny(503, "committed membership is unavailable")
	}
	for _, path := range h.Paths {
		member := false
		for _, node := range view.manifest.Membership.Members {
			if node.ID == path.Node {
				member = true
				break
			}
		}
		if !member {
			return deny(400, "placement hint target is not a committed member")
		}
	}
	key := placementReaderKey{h.Bucket, auth.accountID, h.Reader}
	if !s.scopes.storePlacementHints(key, h, time.Now()) {
		return deny(507, "placement reader telemetry budget exhausted")
	}
	return client.ResponseBody(200, nil, ""), 200
}

func (r *rangeManager) storePlacementHints(key placementReaderKey, h client.PlacementHints, now time.Time) bool {
	half := r.server.placementHalfLives()
	state := &r.readers
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.groups == nil {
		state.groups = make(map[placementReaderKey]*placementReaderGroup)
	}
	group := state.groups[key]
	if group == nil {
		if len(state.groups) >= maxPlacementReaderGroups {
			for old, g := range state.groups {
				if now.Sub(g.at) > placementPathTTL {
					delete(state.groups, old)
				}
			}
		}
		if len(state.groups) >= maxPlacementReaderGroups {
			return false
		}
		group = &placementReaderGroup{paths: make(map[client.ID]placementPathMetric)}
		state.groups[key] = group
	}
	if h.Sequence <= group.sequence {
		return true
	} // replay does not refresh evidence
	group.sequence, group.at = h.Sequence, now
	paths := make(map[client.ID]placementPathMetric, len(h.Paths))
	for _, path := range h.Paths {
		metric, exists := group.paths[path.Node]
		if exists && now.Sub(metric.at) > placementPathTTL {
			metric = placementPathMetric{}
			exists = false
		}
		weight := float64(1)
		if exists && now.After(metric.at) {
			weight = now.Sub(metric.at).Seconds()
		}
		metric.distance.Observe(now, client.PlacementDistance(time.Duration(path.RTTNS), float64(path.BytesPerSecond)), weight, half)
		metric.at = now
		paths[path.Node] = metric
	}
	group.paths = paths
	return true
}

func recordPlacementReader(m *rangeMetric, key rangeMetricKey, p principal, read, write uint64, now time.Time) {
	auth := p.connection
	if auth == nil || auth.readerID == (client.ID{}) || auth.accountID == "" || auth.bucket == nil || client.ID(auth.bucketID) != key.bucket || !auth.placementHints.Load() || auth.invalid.Load() || auth.clusterPeer {
		return
	}
	group := placementReaderKey{key.bucket, auth.accountID, auth.readerID}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readers == nil {
		m.readers = make(map[placementReaderKey]*placementReaderTraffic)
	}
	traffic := m.readers[group]
	if traffic == nil {
		if len(m.readers) >= maxPlacementRangeReaders {
			for old, entry := range m.readers {
				if now.Sub(entry.active) > placementPathTTL {
					delete(m.readers, old)
				}
			}
			if len(m.readers) >= maxPlacementRangeReaders {
				return
			}
		}
		traffic = &placementReaderTraffic{sampled: now}
		m.readers[group] = traffic
	}
	// Saturation keeps advisory rate arithmetic finite under counter overflow.
	traffic.read = addCapacity(traffic.read, read)
	traffic.write = addCapacity(traffic.write, write)
	traffic.active = now
}

func (r *rangeManager) placementReaderCosts(key rangeMetricKey, nodes []client.ID, horizon float64, now time.Time) []float64 {
	costs := make([]float64, len(nodes))
	if horizon <= 0 || math.IsNaN(horizon) || math.IsInf(horizon, 0) {
		return costs
	}
	value, ok := r.metrics.Load(key)
	if !ok {
		return costs
	}
	half := r.server.placementHalfLives()
	type demand struct {
		key   placementReaderKey
		bytes float64
	}
	m := value.(*rangeMetric)
	m.mu.Lock()
	demands := make([]demand, 0, len(m.readers))
	for reader, traffic := range m.readers {
		if half.Long <= time.Duration((1<<63-1)/4) && now.Sub(traffic.active) > half.Long*4 {
			delete(m.readers, reader)
			continue
		}
		if now.Sub(traffic.sampled) >= time.Second {
			observeTrafficFrame(&traffic.readWindow, &traffic.writeWindow, traffic.sampled, now, traffic.read-traffic.lastRead, traffic.write-traffic.lastWrite, half)
			traffic.sampled, traffic.lastRead, traffic.lastWrite = now, traffic.read, traffic.write
		}
		rate := math.Min(math.MaxFloat64, traffic.readWindow.Rates(now, half).Peak()+traffic.writeWindow.Rates(now, half).Peak())
		demands = append(demands, demand{reader, math.Min(math.MaxFloat64, rate*horizon)})
	}
	m.mu.Unlock()
	state := &r.readers
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, d := range demands {
		group := state.groups[d.key]
		if group == nil || now.Sub(group.at) > placementPathTTL {
			continue
		}
		worst := float64(0)
		for _, path := range group.paths {
			if now.Sub(path.at) <= placementPathTTL {
				worst = math.Max(worst, path.distance.Values(now, half).Peak())
			}
		}
		if worst == 0 {
			continue
		}
		for i, node := range nodes {
			distance := worst
			if path, ok := group.paths[node]; ok && now.Sub(path.at) <= placementPathTTL {
				distance = path.distance.Values(now, half).Peak()
			}
			contribution := math.Min(math.MaxFloat64, d.bytes*distance)
			if !math.IsNaN(contribution) {
				costs[i] = math.Min(math.MaxFloat64, costs[i]+contribution)
			}
		}
	}
	return costs
}

// Peer measurements use exactly the same distance function and decay means as
// reader paths. Only directed local-to-peer observations are published; missing
// reverse links are not fabricated from assumed symmetry.
func (r *rangeManager) observePlacementPeer(node client.ID, rtt time.Duration, now time.Time) {
	if node == (client.ID{}) || node == r.node || rtt <= 0 || rtt > time.Minute {
		return
	}
	h := r.server.placementHalfLives()
	state := &r.readers
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.peers == nil {
		state.peers = make(map[client.ID]placementPathMetric)
	}
	metric, exists := state.peers[node]
	if !exists && len(state.peers) >= maxPlacementPeerPaths {
		for old, p := range state.peers {
			if now.Sub(p.at) > placementPathTTL {
				delete(state.peers, old)
			}
		}
		if len(state.peers) >= maxPlacementPeerPaths {
			return
		}
	}
	weight := float64(1)
	if exists && now.After(metric.at) {
		weight = now.Sub(metric.at).Seconds()
	}
	metric.distance.Observe(now, client.PlacementDistance(rtt, 0), weight, h)
	metric.at = now
	state.peers[node] = metric
}

func (r *rangeManager) placementPeerDistances(now time.Time) map[[2]client.ID]float64 {
	h := r.server.placementHalfLives()
	state := &r.readers
	state.mu.Lock()
	defer state.mu.Unlock()
	out := make(map[[2]client.ID]float64, len(state.peers))
	for node, path := range state.peers {
		if now.Sub(path.at) > placementPathTTL {
			delete(state.peers, node)
			continue
		}
		if path.distance.Known(now, h) {
			out[[2]client.ID{r.node, node}] = path.distance.Values(now, h).Peak()
		}
	}
	return out
}

func (r *rangeManager) prunePlacementReaders(now time.Time) {
	state := &r.readers
	state.mu.Lock()
	for key, group := range state.groups {
		if now.Sub(group.at) > placementPathTTL {
			delete(state.groups, key)
		}
	}
	state.mu.Unlock()
}
