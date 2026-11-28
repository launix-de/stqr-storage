// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
)

// prepareShortShutdown retains membership, range ownership and any explicit
// drain intent. Transport notification is best effort, never a client ACK or
// permission to discard accepted state. Persistence policy is separate.
// Operation deadlines never certify a lost peer or authorize another primary.
func (s *Server) prepareShortShutdown(ctx context.Context) error {
	s.shortStopping.Store(true)
	// Stop admission and network waits BEFORE draining node admission. A slow
	// download can hold a shared admission/read view while its socket blocks;
	// waiting for opMu first would let that client prevent daemon shutdown.
	_ = s.sendGoAway(ctx, 1)
	s.cancel()
	s.wsMu.Lock()
	sockets := make([]*wsSession, 0, len(s.sockets))
	for ws := range s.sockets {
		sockets = append(sockets, ws)
	}
	s.wsMu.Unlock()
	for _, ws := range sockets {
		ws.close()
	}
	// Already-published requests retain their outcome; cancelled/unpublished
	// requests remain uncertain. No client reconnect or fence is awaited here.
	s.opMu.Lock()
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	cfg.Node.Paused = true
	if err := saveConfig(s.configPath, cfg); err != nil {
		s.opMu.Unlock()
		return err
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	// A process restart is not a client durability request. Accepted tails keep
	// their unconfirmed status; Close waits for existing fences but starts none.
	s.opMu.Unlock()
	return nil
}

func (s *Server) sendGoAway(ctx context.Context, reason uint8) error {
	s.mu.RLock()
	generation := s.cfg.Generation
	s.mu.RUnlock()
	e := client.Encoder{}
	e.U8(reason)
	e.ID(client.NewRequestID())
	view := s.currentSystem.Load()
	var alternatives []client.Member
	if view != nil {
		for _, m := range view.manifest.Membership.Members {
			if m.ID != s.control.ledger.node {
				alternatives = append(alternatives, m)
			}
		}
	}
	e.V(uint64(len(alternatives)))
	for _, m := range alternatives {
		e.ID(m.ID)
		e.V(uint64(len(m.Addresses)))
		for _, a := range m.Addresses {
			e.Text(a)
		}
	}
	if view != nil {
		e.U64(view.manifest.Membership.Epoch)
		e.U64(view.manifest.Revision)
	} else {
		e.U64(1)
		e.U64(generation)
	}
	wire := client.EncodeMessage(client.Message{Kind: client.Push, Opcode: 0x0003, Body: e.B})
	s.wsMu.Lock()
	sockets := make([]*wsSession, 0, len(s.sockets))
	for ws := range s.sockets {
		sockets = append(sockets, ws)
	}
	s.wsMu.Unlock()
	for _, ws := range sockets {
		if ws.terminal == nil {
			continue
		}
		frame := terminalFrame{wire: wire, done: make(chan struct{})}
		// The writer owns frame completion. Shutdown never waits for it: a
		// stalled or non-reading client learns departure through socket close.
		select {
		case ws.terminal <- frame:
		case <-ws.done:
		default:
			ws.close()
		}
	}
	return nil
}
