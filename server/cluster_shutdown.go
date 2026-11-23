// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerClusterStop uint16 = 0x104c

// Shutdown is part of the immutable, ALL-approved system image. PREPARE fences
// mutations and flushes data before READY; the final ALL-APPLIED certificate
// authorizes STOP without removing a node or changing any object ownership.
type clusterShutdown struct {
	ID, Coordinator      client.ID
	Sequence, Membership uint64
}

type shutdownDelivery struct {
	peer *peerConnection
	id   client.ID
}

func setBundleShutdown(bundle *systemBundle, stop *clusterShutdown) error {
	var manifest systemManifest
	if err := json.Unmarshal(bundle.Objects[bundle.Root], &manifest); err != nil {
		return err
	}
	manifest.Shutdown = stop
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	bundle.Root = bundle.add(raw)
	return nil
}

func (s *Server) clusterPaused() bool {
	view := s.currentSystem.Load()
	if view == nil || view.manifest.Shutdown == nil {
		return false
	}
	stop := view.manifest.Shutdown
	if stop.Membership != view.manifest.Membership.Epoch {
		return false
	}
	s.mu.RLock()
	resumed := s.cfg.Node.ClusterResumedID == stop.ID.String()
	s.mu.RUnlock()
	return !resumed
}

// Caller excludes mutations with opMu. Volume.Sync includes payload, indexes,
// allocator metadata and retry records; a flush error must never yield READY.
func (s *Server) syncVolumes() error {
	s.mu.RLock()
	stores := make([]*volume.Volume, 0, len(s.volumes))
	for _, v := range s.volumes {
		stores = append(stores, v.store)
	}
	s.mu.RUnlock()
	for _, store := range stores {
		if err := store.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) beginClusterShutdown(id client.ID, actor principal) ([]byte, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.requireAdmin(actor); err != nil {
		return nil, err
	}
	s.mu.RLock()
	clusterID := s.cfg.ClusterID
	s.mu.RUnlock()
	op, exists, err := s.beginOperation(id.String(), "cluster-shutdown", clusterID)
	if err != nil {
		return nil, err
	}
	if exists {
		return encodeAck(op), nil
	}
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	if cfg.Node.ClusterShutdownRequest != "" || s.clusterPaused() {
		return nil, fail(409, "a cluster shutdown is already pending")
	}
	cfg.Node.ClusterShutdownRequest = id.String()
	if err := saveConfig(s.configPath, cfg); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	op.State, op.Status, op.Detail = "running", 202, "Cluster wird gemeinsam angehalten"
	if err := s.saveOperation(op); err != nil {
		return nil, err
	}
	select {
	case s.controller.wake <- struct{}{}:
	default:
	}
	return encodeAck(op), nil
}

// A STOP certificate is retained by every released participant. Reconnects can
// deliver it to a member that missed STOP. A restarted, already stopped process
// relays the old certificate but never stops itself a second time for that ID.
func (s *Server) stopCertificate(stop *clusterShutdown) (client.Decision, client.Certificate, bool) {
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	for decision, completed := range l.completed {
		p := completed.Slot.Proposal
		if decision.Sequence == stop.Sequence && p != nil && p.Kind == controlShutdown && p.Claim.Round.Initiator == stop.Coordinator && p.Claim.Base.Membership.Epoch == stop.Membership {
			return decision, completed.Release.Certificate, true
		}
	}
	return client.Decision{}, nil, false
}

func (s *Server) receiveClusterStop(body []byte, actor client.ID) ([]byte, error) {
	decision, cert, err := decodeControlCertificate(body)
	if err != nil {
		return nil, err
	}
	view := s.currentSystem.Load()
	if view == nil || view.manifest.Shutdown == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, errControlConflict
	}
	stop := view.manifest.Shutdown
	if stop.Sequence != decision.Sequence || stop.Membership != view.manifest.Membership.Epoch || !cert.Covers(membershipIDs(view.manifest.Membership)) {
		return nil, errControlConflict
	}
	l := s.control.ledger
	l.mu.Lock()
	valid := false
	if old, ok := l.completed[decision]; ok {
		valid = old.Slot.Proposal.Kind == controlShutdown && containsOwnProof(cert, old.Slot.Applied)
	} else if slot := l.slot; slot != nil && slot.Decision == decision && slot.Proposal != nil && slot.Proposal.Kind == controlShutdown && slot.Applied.RecordSequence != 0 {
		valid = containsOwnProof(cert, slot.Applied)
	}
	l.mu.Unlock()
	if !valid {
		return nil, errControlConflict
	}
	if err := l.release(decision, cert); err != nil {
		return nil, err
	}
	s.mu.RLock()
	known := s.cfg.Node.ClusterStopID == stop.ID.String() || s.cfg.Node.ClusterResumedID == stop.ID.String()
	s.mu.RUnlock()
	if known {
		return nil, nil
	}
	if err := s.rememberClusterStop(stop); err != nil {
		return nil, err
	}
	select {
	case s.controller.wake <- struct{}{}:
	default:
	}
	return nil, nil
}

func (s *Server) rememberClusterStop(stop *clusterShutdown) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	s.mu.RUnlock()
	if cfg.Node.ClusterResumedID == stop.ID.String() || cfg.Node.ClusterStopID == stop.ID.String() {
		return nil
	}
	if err := s.syncVolumes(); err != nil {
		return err
	}
	cfg.Node.ClusterStopID, cfg.Node.Paused = stop.ID.String(), true
	if err := saveConfig(s.configPath, cfg); err != nil {
		return err
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	return nil
}

// Called under controller.mu. A missing required member leaves the shutdown
// pending; no elapsed time or network error substitutes for its durable READY.
func (c *clusterController) processClusterShutdown(ctx context.Context) bool {
	s := c.server
	s.mu.RLock()
	request := s.cfg.Node.ClusterShutdownRequest
	s.mu.RUnlock()
	view := s.currentSystem.Load()
	if request != "" && (view == nil || view.manifest.Shutdown == nil || view.manifest.Shutdown.ID.String() != request) {
		if s.clusterPaused() {
			return true
		}
		id, err := parseID(request)
		if err != nil {
			return true
		}
		_, _, _ = c.changeWithShutdown(ctx, nil, nil, &clusterShutdown{ID: client.ID(id)})
		view = s.currentSystem.Load()
	}
	if view == nil || view.manifest.Shutdown == nil {
		return false
	}
	stop := view.manifest.Shutdown
	if stop.Membership != view.manifest.Membership.Epoch {
		return false
	}
	decision, cert, ready := s.stopCertificate(stop)
	if !ready {
		return s.clusterPaused()
	}
	// Only the original requester initiates STOP, after finishing ALL release.
	// Others relay after receiving STOP; they must not shut down prematurely
	// while their release acknowledgement is still needed by that requester.
	s.mu.RLock()
	resumed, received := s.cfg.Node.ClusterResumedID == stop.ID.String(), s.cfg.Node.ClusterStopID == stop.ID.String()
	s.mu.RUnlock()
	if !resumed && !received {
		if stop.Coordinator != s.control.ledger.node || c.coordinator.requestSnapshot() != nil {
			return true
		}
		if err := s.rememberClusterStop(stop); err != nil {
			return true
		}
	}
	wire, err := marshalControlCertificate(decision, cert)
	if err != nil {
		return true
	}
	// Independent requests avoid one disconnected member blocking delivery to
	// other reachable members. Transport failure is allowed to leave a member
	// paused until a later reconnect delivers the same persistent certificate.
	type destination struct {
		node client.ID
		peer *peerConnection
	}
	var peers []destination
	s.peerStatsMu.RLock()
	for node, observation := range s.peerStats {
		if observation.online && hasControlNode(membershipIDs(view.manifest.Membership), node) {
			prior := c.stopNotified[node]
			if prior.peer != observation.connection || prior.id != stop.ID {
				peers = append(peers, destination{node, observation.connection})
			}
		}
	}
	s.peerStatsMu.RUnlock()
	var sends sync.WaitGroup
	succeeded := make([]bool, len(peers))
	for i, peer := range peers {
		sends.Add(1)
		go func(i int, node client.ID) {
			defer sends.Done()
			// STOP is already certified. A delivery deadline leaves a receiver
			// paused; it cannot shrink membership or manufacture a READY proof.
			delivery, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if c.stopSender != nil {
				succeeded[i] = c.stopSender(delivery, node, wire) == nil
			} else {
				_, err := (peerControlParticipants{runtime: s.control}).call(delivery, node, peerClusterStop, wire)
				succeeded[i] = err == nil
			}
		}(i, peer.node)
	}
	sends.Wait()
	if c.stopNotified == nil {
		c.stopNotified = make(map[client.ID]shutdownDelivery)
	}
	for i, peer := range peers {
		if succeeded[i] {
			c.stopNotified[peer.node] = shutdownDelivery{peer.peer, stop.ID}
		}
	}
	if resumed {
		return false
	}
	if c.coordinator.requestSnapshot() != nil {
		return true
	}
	s.opMu.Lock()
	s.shortStopping.Store(true)
	if request == stop.ID.String() {
		if op, err := s.operation(request); err == nil {
			_, _ = s.finishOperation(op, nil)
		}
	}
	s.opMu.Unlock()
	_ = s.sendGoAway(ctx, 2)
	s.stopOnce.Do(func() { close(s.stopRequest) })
	return true
}
