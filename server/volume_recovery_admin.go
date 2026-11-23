// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"errors"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type volumeRecoveryRequest struct {
	Membership uint64
	Volumes    []client.ID
}

func decodeVolumeRecoveryRequest(body []byte) (volumeRecoveryRequest, error) {
	d := client.Decoder{B: body}
	r := volumeRecoveryRequest{Membership: d.U64()}
	n := d.Count(10000)
	if n == 0 || n > len(d.B)/16 {
		return r, client.ErrProtocol
	}
	r.Volumes = make([]client.ID, n)
	for i := range r.Volumes {
		r.Volumes[i] = d.ID()
		if r.Volumes[i] == (client.ID{}) || i > 0 && r.Volumes[i-1].String() >= r.Volumes[i].String() {
			return r, client.ErrProtocol
		}
	}
	if d.Done() != nil || r.Membership == 0 {
		return r, client.ErrProtocol
	}
	return r, nil
}

func (s *Server) executeVolumeRecovery(m client.Message, p principal) ([]byte, int, error) {
	if m.Kind != client.Request || m.ID == (client.ID{}) {
		return nil, 0, fail(400, "volume recovery requires a request")
	}
	if err := s.requireAdmin(p); err != nil {
		return nil, 0, err
	}
	switch m.Opcode {
	case client.VolumeRecoveryStage:
		d := client.Decoder{B: m.Body}
		name, path := d.Text(), d.Text()
		if d.Done() != nil {
			return nil, 0, client.ErrProtocol
		}
		s.opMu.Lock()
		defer s.opMu.Unlock()
		if err := s.requireAdmin(p); err != nil {
			return nil, 0, err
		}
		op, exists, err := s.beginOperation(m.ID.String(), "recovery-stage", []string{name, path})
		if err != nil {
			return nil, 0, err
		}
		if exists && op.State == "fulfilled" {
			return jsonText(map[string]string{"volume_id": op.VolumeID, "state": "recovery"}), 201, nil
		}
		id, err := s.stageRecoveryVolumeLocked(name, path, &p, m.ID.String())
		if err == nil {
			op.VolumeID = id.String()
		}
		if _, err = s.finishOperation(op, err); err != nil {
			return nil, 0, err
		}
		return jsonText(map[string]string{"volume_id": id.String(), "state": "recovery"}), 201, nil
	case client.VolumeRecoveryRelease:
		d := client.Decoder{B: m.Body}
		id := volume.ID(d.ID())
		if d.Done() != nil {
			return nil, 0, client.ErrProtocol
		}
		s.opMu.Lock()
		defer s.opMu.Unlock()
		if err := s.requireAdmin(p); err != nil {
			return nil, 0, err
		}
		if s.forceFrozen() {
			return nil, 0, fail(503, "recovery decision is pending")
		}
		op, exists, err := s.beginOperation(m.ID.String(), "recovery-release", id.String())
		if err != nil {
			return nil, 0, err
		}
		if exists && op.State == "fulfilled" {
			return jsonText(map[string]bool{"released": true}), 200, nil
		}
		staged := s.recoveryVolumes[id]
		if staged == nil {
			return nil, 0, volume.ErrNotFound
		}
		op.VolumeID = id.String()
		if err := s.saveOperation(op); err != nil {
			return nil, 0, err
		}
		s.mu.RLock()
		cfg := copyConfig(s.cfg)
		s.mu.RUnlock()
		var kept []VolumeConfig
		for _, v := range cfg.RecoveryVolumes {
			if v.ID != id.String() {
				kept = append(kept, v)
			}
		}
		cfg.RecoveryVolumes = kept
		if err := saveConfig(s.configPath, cfg); err != nil {
			return nil, 0, err
		}
		s.mu.Lock()
		s.cfg = cfg
		s.mu.Unlock()
		delete(s.recoveryVolumes, id)
		if err := staged.store.Close(); err != nil {
			return nil, 0, err
		}
		if _, err := s.finishOperation(op, nil); err != nil {
			return nil, 0, err
		}
		return jsonText(map[string]bool{"released": true}), 200, nil
	case client.VolumeRecoveryCommit:
		return s.commitRecoveredVolumes(m, p)
	}
	return nil, 0, client.ErrProtocol
}

func (s *Server) commitRecoveredVolumes(m client.Message, p principal) ([]byte, int, error) {
	request, err := decodeVolumeRecoveryRequest(m.Body)
	if err != nil {
		return nil, 0, err
	}
	if s.controller == nil {
		return nil, 0, fail(501, "volume recovery needs cluster control")
	}
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	s.opMu.Lock()
	if err := s.requireAdmin(p); err != nil {
		s.opMu.Unlock()
		return nil, 0, err
	}
	var old Operation
	receipt := s.readBlob("admin-op/"+m.ID.String(), &old)
	if receipt == nil {
		op, _, err := s.beginOperation(m.ID.String(), "volume-recovery", request)
		if err == nil && s.completedExclusion(m.ID) != nil {
			op, err = s.finishOperation(op, nil)
		}
		if err != nil {
			s.opMu.Unlock()
			return nil, 0, err
		}
		if op.State == "fulfilled" || op.State == "failed" {
			s.opMu.Unlock()
			if op.Status >= 400 {
				return nil, 0, fail(op.Status, op.Detail)
			}
			return encodeAck(op), 200, nil
		}
	} else if !errors.Is(receipt, volume.ErrNotFound) {
		s.opMu.Unlock()
		return nil, 0, receipt
	}
	if pending := c.coordinator.requestSnapshot(); pending != nil && pending.Claim.Exclusion != nil && pending.Claim.Exclusion.Operation == m.ID {
		x := pending.Claim.Exclusion
		if x.Restore == nil || !reflect.DeepEqual(x.Restore.Volumes, request.Volumes) || exclusionObservedEpoch(pending.Claim) != request.Membership {
			s.opMu.Unlock()
			return nil, 0, fail(400, "operation ID reused with another takeover")
		}
		s.opMu.Unlock()
		_, resumeErr := c.resume(s.ctx)
		return s.finishRecoveryAdminOperation(m.ID, resumeErr)
	}
	view := s.currentSystem.Load()
	if view == nil || view.manifest.Membership.Epoch != request.Membership {
		s.opMu.Unlock()
		return nil, 0, fail(409, "membership changed before volume recovery")
	}
	for _, id := range request.Volumes {
		if s.recoveryStore(volume.ID(id)) == nil {
			s.opMu.Unlock()
			return nil, 0, fail(409, "requested recovery volume is not staged")
		}
	}
	var origins [][32]byte
	var missing error
	for _, origin := range view.manifest.VolumeRecoveries {
		if origin.Complete {
			continue
		}
		if _, err := s.validateRecoveryCohort(origin, request.Volumes); err == nil {
			origins = append(origins, origin.ID)
		} else if statusOf(err) == 409 {
			missing = err
		} else {
			s.opMu.Unlock()
			return nil, 0, err
		}
	}
	if len(origins) == 0 {
		s.opMu.Unlock()
		if missing != nil {
			return nil, 0, missing
		}
		return nil, 0, fail(409, "no pending recoverable ranges match these volumes")
	}
	if len(origins) > 10000 {
		s.opMu.Unlock()
		return nil, 0, fail(413, "recovery batch contains too many ranges")
	}
	sort.Slice(origins, func(i, j int) bool { return bytes.Compare(origins[i][:], origins[j][:]) < 0 })
	actor, err := parseID(p.User.ID)
	if err != nil {
		s.opMu.Unlock()
		return nil, 0, err
	}
	x := &controlExclusion{Operation: m.ID, ExpectedMembership: request.Membership, Administrator: client.ID(actor), Username: p.User.Name, ExternalFencingConfirmed: true, Restore: &volumeTakeover{Target: s.control.ledger.node, Origins: origins, Volumes: request.Volumes}}
	if _, _, err := s.beginOperation(m.ID.String(), "volume-recovery", request); err != nil {
		s.opMu.Unlock()
		return nil, 0, err
	}
	next := view.manifest.Membership
	s.opMu.Unlock()
	for _, member := range next.Members {
		if member.ID == s.control.ledger.node {
			continue
		}
		s.peerStatsMu.RLock()
		observation := s.peerStats[member.ID]
		capable := observation != nil && observation.online && observation.connection.Capabilities&client.CapabilityRecoveryFenceClearance != 0
		s.peerStatsMu.RUnlock()
		if !capable {
			return s.finishRecoveryAdminOperation(m.ID, fail(503, "required takeover participant is unavailable or needs an upgrade"))
		}
	}
	_, _, err = c.changeWithExclusion(s.ctx, &next, nil, nil, x)
	return s.finishRecoveryAdminOperation(m.ID, err)
}
func (s *Server) finishRecoveryAdminOperation(id client.ID, cause error) ([]byte, int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	op, err := s.operation(id.String())
	if err != nil {
		return nil, 0, err
	}
	if s.completedExclusion(id) != nil {
		op, err = s.finishOperation(op, nil)
		return encodeAck(op), 200, err
	}
	if pending := s.pendingExclusionClaim(id); pending != nil {
		op.State, op.Status = "running", 202
		if cause != nil {
			op.Detail = cause.Error()
		}
		if err = s.saveOperation(op); err != nil {
			return nil, 0, err
		}
		return encodeAck(op), 202, nil
	}
	if cause == nil {
		cause = errControlConflict
	}
	_, saveErr := s.finishOperation(op, cause)
	if saveErr != nil {
		return nil, 0, saveErr
	}
	return nil, 0, cause
}

const peerRecoveryReady uint16 = 0x1071

func (s *Server) recoveryReady(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	claim, err := decodeControlClaim(d.Blob())
	var root [32]byte
	copy(root[:], d.Take(32))
	if err != nil || d.Done() != nil || claim.Exclusion == nil || claim.Exclusion.Restore == nil || claim.Exclusion.Restore.Target != s.control.ledger.node || !hasControlNode(claim.Required, actor) {
		return nil, client.ErrProtocol
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	slot := l.slot
	if slot == nil || !equalControlClaim(slot.Claim, claim) || slot.Proposal == nil || slot.Proposal.Image != root || slot.Ready.RecordSequence == 0 {
		return nil, errControlConflict
	}
	return slot.Ready.MarshalBinary()
}
func (s *Server) prepareVolumeTakeover(view *systemView, p controlProposal) error {
	x := p.Claim.Exclusion
	if x == nil || x.Restore == nil {
		return nil
	}
	current := s.currentSystem.Load()
	if current == nil {
		return errControlConflict
	}
	if len(current.manifest.VolumeRecoveries) != len(view.manifest.VolumeRecoveries) {
		return errControlConflict
	}
	s.opMu.Lock()
	err := func() error {
		for _, old := range current.manifest.VolumeRecoveries {
			var next *volumeRecoveryOrigin
			for i := range view.manifest.VolumeRecoveries {
				if view.manifest.VolumeRecoveries[i].ID == old.ID {
					next = &view.manifest.VolumeRecoveries[i]
					break
				}
			}
			if next == nil {
				return errControlConflict
			}
			selected := false
			for _, id := range x.Restore.Origins {
				if id == old.ID {
					selected = true
				}
			}
			if !selected {
				if !reflect.DeepEqual(old, *next) {
					return errControlConflict
				}
				continue
			}
			if old.Complete || old.After.State != 4 || !next.Complete || next.Target != x.Restore.Target || !recoveryReplacement(s, view, x, old.Before.Bucket, old.After, next.After) {
				return errControlConflict
			}
			for _, id := range next.Volumes {
				if !hasControlNode(x.Restore.Volumes, id) {
					return errControlConflict
				}
			}
			if x.Restore.Target == s.control.ledger.node {
				cohort, err := s.validateRecoveryCohort(old, x.Restore.Volumes)
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(cohort, next.Volumes) {
					return errControlConflict
				}
			}
		}
		return nil
	}()
	s.opMu.Unlock()
	if err != nil {
		return err
	}
	if x.Restore.Target == s.control.ledger.node {
		return nil
	}
	wire, err := marshalControlClaim(p.Claim)
	if err != nil {
		return err
	}
	e := client.Encoder{}
	e.Blob(wire)
	e.B = append(e.B, p.Image[:]...)
	raw, err := (peerControlParticipants{runtime: s.control}).call(s.ctx, x.Restore.Target, peerRecoveryReady, e.B)
	if err != nil {
		return err
	}
	proof, err := client.DecodeProof(raw)
	if err != nil || proof.Node != x.Restore.Target {
		return client.ErrProtocol
	}
	return nil
}
