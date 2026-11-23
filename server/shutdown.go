// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
)

// prepareShortShutdown retains membership, range ownership and any explicit
// drain intent. The durable paused marker is written before GOAWAY. Operation
// deadlines never certify a lost peer or authorize another primary.
func (s *Server) prepareShortShutdown(ctx context.Context) error {
	s.shortStopping.Store(true)
	// Drain in-flight local mutations; queued/new mutations see shortStopping.
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
	stores := make([]*managedVolume, 0, len(s.volumes))
	for _, v := range s.volumes {
		stores = append(stores, v)
	}
	s.mu.Unlock()
	for _, v := range stores {
		if err := v.store.Sync(); err != nil {
			s.opMu.Unlock()
			return err
		}
	}
	s.opMu.Unlock()
	s.publishStats()
	return s.sendGoAway(ctx, 1)
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
	var pending []struct {
		frame terminalFrame
		ws    *wsSession
	}
	for _, ws := range sockets {
		frame := terminalFrame{wire: wire, done: make(chan struct{})}
		if ws.terminal == nil {
			continue
		}
		select {
		case ws.terminal <- frame:
			pending = append(pending, struct {
				frame terminalFrame
				ws    *wsSession
			}{frame, ws})
		case <-ws.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, frame := range pending {
		select {
		case <-frame.frame.done:
		case <-frame.ws.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
