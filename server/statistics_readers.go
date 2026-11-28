// SPDX-License-Identifier: GPL-3.0-only
package server

import (
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
	value    float64
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
	read, write               uint64
	sampled                   time.Time
	active                    time.Time
	readCounter, writeCounter statisticsCounter
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
	if !s.storePlacementHints(key, h, time.Now()) {
		return deny(507, "placement reader telemetry budget exhausted")
	}
	return client.ResponseBody(200, nil, ""), 200
}

func (s *Server) storePlacementHints(key placementReaderKey, h client.PlacementHints, now time.Time) bool {
	half := s.placementHalfLives()
	state := &s.statistics.paths
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
		metric.value = metric.distance.Values(now, half).Peak()
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

// Peer measurements use exactly the same distance function and decay means as
// reader paths. Only directed local-to-peer observations are published; missing
// reverse links are not fabricated from assumed symmetry.
func (s *Server) observePlacementPeer(node client.ID, rtt time.Duration, now time.Time) {
	if node == (client.ID{}) || node == s.quotaNode || rtt <= 0 || rtt > time.Minute {
		return
	}
	h := s.placementHalfLives()
	state := &s.statistics.paths
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
	metric.value = metric.distance.Values(now, h).Peak()
	metric.at = now
	state.peers[node] = metric
}

func (s *Server) prunePlacementReaders(now time.Time) {
	state := &s.statistics.paths
	state.mu.Lock()
	for key, group := range state.groups {
		if now.Sub(group.at) > placementPathTTL {
			delete(state.groups, key)
		}
	}
	for node, path := range state.peers {
		if now.Sub(path.at) > placementPathTTL {
			delete(state.peers, node)
		}
	}
	state.mu.Unlock()
}

// Pure views keep consumers away from mutable measurement records. Published
// path maps are replaced as whole immutable values by the hint collector.
type readerDemand struct {
	key  placementReaderKey
	rate float64
}
type readerProfileView struct {
	at    time.Time
	paths map[client.ID]placementPathMetric
}

func (b *statisticsBook) readerDemands(key rangeMetricKey, now time.Time, h decay.HalfLives) []readerDemand {
	value, ok := b.ranges.Load(key)
	if !ok {
		return nil
	}
	metric := value.(*rangeMetric)
	metric.mu.Lock()
	defer metric.mu.Unlock()
	out := make([]readerDemand, 0, len(metric.readers))
	for key, traffic := range metric.readers {
		if h.Long <= time.Duration((1<<63-1)/4) && now.Sub(traffic.active) > h.Long*4 {
			continue
		}
		rate := traffic.readCounter.window.Forecast().Peak() + traffic.writeCounter.window.Forecast().Peak()
		out = append(out, readerDemand{key, rate})
	}
	return out
}
func (b *statisticsBook) readerProfile(key placementReaderKey) (readerProfileView, bool) {
	b.paths.mu.Lock()
	defer b.paths.mu.Unlock()
	group := b.paths.groups[key]
	if group == nil {
		return readerProfileView{}, false
	}
	return readerProfileView{group.at, group.paths}, true
}
func (b *statisticsBook) peerDistances(node client.ID, now time.Time) map[[2]client.ID]float64 {
	b.paths.mu.Lock()
	defer b.paths.mu.Unlock()
	out := make(map[[2]client.ID]float64, len(b.paths.peers))
	for peer, path := range b.paths.peers {
		if now.Sub(path.at) <= placementPathTTL && path.value > 0 {
			out[[2]client.ID{node, peer}] = path.value
		}
	}
	return out
}
