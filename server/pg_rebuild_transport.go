// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"time"
)

// Peer framing/control callbacks must remain available while one PG waits on
// its participants. Each message starts independently; coordinator gates enforce
// real per-PG dependencies without a separate transport worker budget.
func (s *Server) launchPGRequest(ctx context.Context, m client.Message, p principal, reply func([]byte)) bool {
	if s.storageWarming.Load() {
		return false
	}
	if !(m.Opcode >= peerPGRebuildRequest && m.Opcode <= peerPGRebuildRanges || m.Opcode == peerBucketLease) || m.Kind != client.Request || s.scopes == nil {
		return false
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return true
	}
	s.workers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.workers.Done()
		request, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		p.requestContext = request
		body, _ := s.dispatchAs(m, nil, p)
		reply(body)
	}()
	return true
}
