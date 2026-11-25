// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func (s *Server) joinPending() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.JoinPending
}

// Known contacts are local connection hints, not membership or a storage map.
// Reconnect backoff never excludes a member or changes an object's owner.
func (s *Server) startKnownPeers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startPeerContactsLocked(s.cfg.Peers)
}

func (s *Server) startPeerContactsLocked(peers []PeerConfig) {
	if s.closed {
		return
	}
	if s.peerManagers == nil {
		s.peerManagers = make(map[string]bool)
	}
	for _, peer := range peers {
		if s.peerManagers[peer.URL] {
			continue
		}
		s.peerManagers[peer.URL] = true
		s.workers.Add(1)
		go func(peer PeerConfig) { defer s.workers.Done(); s.runKnownPeer(peer) }(peer)
	}
}

func (s *Server) runKnownPeer(peer PeerConfig) {
	backoff := 100 * time.Millisecond
	for s.ctx.Err() == nil {
		var expected client.ID
		if peer.NodeID != "" {
			id, err := parseID(peer.NodeID)
			if err != nil {
				return
			}
			expected = client.ID(id)
		}
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		p, err := s.dialPeer(ctx, peer.URL, expected)
		cancel()
		if err == nil {
			err = s.rememberPeer(p.Node, peer.URL)
			if err == nil {
				peer.NodeID = p.Node.String()
				err = s.startPeerObservation(p)
			}
			if err == nil {
				backoff = 100 * time.Millisecond
				p.sendDirectory(s.peerDirectoryBody())
				s.catchupRangePeer(p)
				<-p.ctx.Done()
			}
			p.close()
		}
		// Transport retry only: no failure deadline, election or placement change.
		timer := time.NewTimer(backoff)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(5*time.Second, backoff*2)
	}
}

func (s *Server) rememberPeer(id client.ID, address string) error {
	if err := validateURL(address); err != nil || id == (client.ID{}) {
		return client.ErrProtocol
	}
	return s.mergePeerDirectory([]client.NodeAddress{{Node: id, Addresses: []string{address}}})
}
