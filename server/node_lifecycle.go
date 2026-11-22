// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/launix-de/stqr-storage/client"
)

var ErrNodeDrained = errors.New("node completed its voluntary cluster departure")

const peerNodeLeave uint16 = 0x104a

func (s *Server) retiring() bool {
	view := s.currentSystem.Load()
	if view == nil || s.control == nil {
		return false
	}
	s.mu.RLock()
	draining := s.cfg.Node.Draining
	s.mu.RUnlock()
	return draining && !hasControlNode(membershipIDs(view.manifest.Membership), s.control.ledger.node)
}

func (s *Server) beginNodeDrain(actor principal) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.requireAdmin(actor); err != nil {
		return err
	}
	if s.controller == nil {
		return fail(501, "node departure requires the cluster controller")
	}
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	if cfg.Node.Draining {
		return nil
	}
	if view := s.currentSystem.Load(); view == nil || len(view.manifest.Membership.Members) < 2 {
		return fail(409, "the last cluster member requires SHORT or CLUSTER shutdown")
	}
	cfg.Node.Draining = true
	if err := saveConfig(s.configPath, cfg); err != nil {
		return err
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	s.notifyStats()
	return nil
}

// finishNodeDrain runs after locally owned handoffs are DONE. The remaining
// coordinator is chosen and persisted before contacting it, so a lost response
// or crash cannot select a second independent membership-removal requester.
func (r *rangeManager) finishNodeDrain(ctx context.Context) error {
	s := r.server
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	if !cfg.Node.Draining || cfg.Node.DrainComplete {
		return nil
	}
	r.mu.Lock()
	busy := r.busyLocked()
	r.mu.Unlock()
	if busy {
		return errControlConflict
	}
	view := s.currentSystem.Load()
	if view == nil {
		return errControlConflict
	}
	for _, description := range s.bucketSnapshots() {
		id, err := bucketID(description.ID)
		if err != nil {
			return err
		}
		if id == systemBucket || description.DeletionComplete {
			continue
		}
		for _, entry := range s.placement.Load().bucketEntries(client.ID(id)) {
			for _, location := range entry.Locations {
				if location.Node == r.node {
					return errControlConflict
				}
			}
		}
	}
	// Once no local ranges remain, stop accepting fresh frontend work and let
	// existing authenticated HTTP operations finish before removing their issuer
	// from membership. Registration uses the same opMu admission barrier.
	s.opMu.Lock()
	s.drainClosing.Store(true)
	s.authMu.Lock()
	activeFrontends := len(s.frontendGrants)
	s.authMu.Unlock()
	s.opMu.Unlock()
	if activeFrontends != 0 {
		return errControlConflict
	}
	coordinator := client.ID{}
	if cfg.Node.LeaveCoordinator != "" {
		id, err := parseID(cfg.Node.LeaveCoordinator)
		if err != nil {
			return err
		}
		coordinator = client.ID(id)
	} else {
		candidates := membershipIDs(view.manifest.Membership)
		sort.Slice(candidates, func(i, j int) bool { return bytes.Compare(candidates[i][:], candidates[j][:]) < 0 })
		for _, id := range candidates {
			if id != r.node {
				coordinator = id
				break
			}
		}
		if coordinator == (client.ID{}) {
			return fail(409, "the last cluster member cannot drain without a successor")
		}
		s.opMu.Lock()
		s.mu.RLock()
		cfg = copyConfig(s.cfg)
		s.mu.RUnlock()
		cfg.Node.LeaveCoordinator = coordinator.String()
		err := saveConfig(s.configPath, cfg)
		if err == nil {
			s.mu.Lock()
			s.cfg = cfg
			s.mu.Unlock()
		}
		s.opMu.Unlock()
		if err != nil {
			return err
		}
	}
	e := client.Encoder{}
	e.ID(r.node)
	if _, err := r.call(ctx, coordinator, peerNodeLeave, e.B); err != nil {
		return err
	}
	// A successful response certifies ALL-RELEASED and requester completion on
	// the chosen surviving coordinator, not merely its receipt of our intent.
	s.opMu.Lock()
	s.mu.RLock()
	cfg = copyConfig(s.cfg)
	s.mu.RUnlock()
	if cfg.Node.DrainOperation != "" {
		op, err := s.operation(cfg.Node.DrainOperation)
		if err != nil {
			s.opMu.Unlock()
			return err
		}
		if _, err = s.finishOperation(op, nil); err != nil {
			s.opMu.Unlock()
			return err
		}
	}
	cfg.Node.DrainComplete = true
	err := saveConfig(s.configPath, cfg)
	if err == nil {
		s.mu.Lock()
		s.cfg = cfg
		s.mu.Unlock()
		s.shortStopping.Store(true)
	}
	s.opMu.Unlock()
	if err != nil {
		return err
	}
	err = s.sendGoAway(ctx, 0)
	s.stopOnce.Do(func() { close(s.stopRequest) })
	return err
}

func (s *Server) receiveNodeLeave(m client.Message, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: m.Body}
	leaving := d.ID()
	if m.Kind != client.Request || d.Done() != nil || actor != leaving || s.controller == nil || leaving == s.control.ledger.node {
		return nil, fail(403, "invalid voluntary departure identity")
	}
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.resume(s.ctx); err != nil {
		return nil, err
	}
	l := s.control.ledger
	l.mu.Lock()
	base := cloneControlBase(l.current)
	pending := l.request != nil
	l.mu.Unlock()
	if pending {
		return nil, errControlConflict
	}
	if !hasControlNode(membershipIDs(base.Membership), leaving) {
		return nil, nil
	}
	// Complete scoped archives before deciding that the departing member owns
	// no user range; change() then revalidates/fences under ALL reservations.
	if err := s.scopes.syncArchives(s.ctx); err != nil {
		return nil, err
	}
	next := base.Membership
	next.Members = nil
	for _, member := range base.Membership.Members {
		if member.ID != leaving {
			next.Members = append(next.Members, member)
		}
	}
	next.Epoch++
	_, _, err := c.change(s.ctx, &next, func() (*adminDraft, error) {
		for _, description := range s.bucketSnapshots() {
			id, err := bucketID(description.ID)
			if err != nil {
				return nil, err
			}
			for _, entry := range s.placement.Load().bucketEntries(client.ID(id)) {
				for _, location := range entry.Locations {
					if location.Node == leaving {
						return nil, fail(409, "departing node still owns an object range")
					}
				}
			}
		}
		return nil, nil
	})
	return nil, err
}

func (s *Server) nodeDrainAdmin(m client.Message, actor principal) ([]byte, error) {
	d := client.Decoder{B: m.Body}
	node := d.ID()
	if d.Done() != nil || s.control == nil || node != s.control.ledger.node || m.ID == (client.ID{}) {
		return nil, fail(400, "node drain must target this node")
	}
	s.opMu.Lock()
	if err := s.requireAdmin(actor); err != nil {
		s.opMu.Unlock()
		return nil, err
	}
	op, exists, err := s.beginOperation(m.ID.String(), "node-drain", node.String())
	s.opMu.Unlock()
	if err != nil {
		return nil, err
	}
	if exists && (op.State == "fulfilled" || op.State == "failed") {
		return encodeAck(op), nil
	}
	if err = s.beginNodeDrain(actor); err != nil {
		s.opMu.Lock()
		_, _ = s.finishOperation(op, err)
		s.opMu.Unlock()
		return nil, err
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	if cfg.Node.DrainOperation == "" {
		cfg.Node.DrainOperation = op.ID
	}
	if err = saveConfig(s.configPath, cfg); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	op.State = "running"
	op.Status = 202
	op.Detail = "Node wird kooperativ geleert"
	if err = s.saveOperation(op); err != nil {
		return nil, err
	}
	return encodeAck(op), nil
}

func (s *Server) nodeShutdownAdmin(m client.Message, actor principal) ([]byte, error) {
	d := client.Decoder{B: m.Body}
	node, mode := d.ID(), d.U8()
	if mode == 0 && d.Done() == nil {
		drain := m
		drain.Opcode = client.NodeDrain
		e := client.Encoder{}
		e.ID(node)
		drain.Body = e.B
		return s.nodeDrainAdmin(drain, actor)
	}
	if mode == 2 && s.controller != nil && d.Done() == nil && node == s.control.ledger.node && m.ID != (client.ID{}) {
		return s.beginClusterShutdown(m.ID, actor)
	}
	s.mu.RLock()
	local := s.cfg.Node.ID == node.String()
	s.mu.RUnlock()
	if d.Done() != nil || !local || mode != 1 || m.ID == (client.ID{}) {
		return nil, fail(400, "use SHORT mode 1 for a local maintenance shutdown")
	}
	s.opMu.Lock()
	if err := s.requireAdmin(actor); err != nil {
		s.opMu.Unlock()
		return nil, err
	}
	op, exists, err := s.beginOperation(m.ID.String(), "node-short", node.String())
	if err != nil {
		s.opMu.Unlock()
		return nil, err
	}
	if exists {
		s.opMu.Unlock()
		return encodeAck(op), nil
	}
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	cfg.Node.ShortOperation = op.ID
	if err = saveConfig(s.configPath, cfg); err != nil {
		s.opMu.Unlock()
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.opMu.Unlock()
		return nil, fail(503, "server is stopping")
	}
	s.cfg = cfg
	s.workers.Add(1)
	s.mu.Unlock()
	op.State = "running"
	op.Status = 202
	err = s.saveOperation(op)
	s.opMu.Unlock()
	if err != nil {
		s.workers.Done()
		return nil, err
	}
	go func() {
		defer s.workers.Done()
		err := s.prepareShortShutdown(s.ctx)
		s.opMu.Lock()
		_, saveErr := s.finishOperation(op, err)
		s.opMu.Unlock()
		if err == nil && saveErr == nil {
			s.stopOnce.Do(func() { close(s.stopRequest) })
		}
	}()
	return encodeAck(op), nil
}
