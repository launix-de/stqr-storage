// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"time"
)

// Peer framing/control callbacks must remain available while one PG waits on
// its participants. Coordinator gates limit each PG; this bounds total memory.
func (s *Server) launchPGRequest(ctx context.Context, m client.Message, p principal, reply func([]byte)) bool {
	if !(m.Opcode >= peerPGRebuildRequest && m.Opcode <= peerPGRebuildRanges || m.Opcode == peerBucketLease) || m.Kind != client.Request || s.scopes == nil {
		return false
	}
	runtime := &s.scopes.pgRuntime
	runtime.rpcOnce.Do(func() { runtime.rpcSlots = make(chan struct{}, 64) })
	select {
	case runtime.rpcSlots <- struct{}{}:
	default:
		reply(client.ResponseBody(503, nil, "PG control queue is full"))
		return true
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-runtime.rpcSlots
		return true
	}
	s.workers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.workers.Done()
		defer func() { <-runtime.rpcSlots }()
		request, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		p.requestContext = request
		body, _ := s.dispatchAs(m, nil, p)
		reply(body)
	}()
	return true
}
