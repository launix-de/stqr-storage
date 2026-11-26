// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"github.com/launix-de/stqr-storage/client"
)

type storageWarmFailure struct{ err error }

// Startup contract: journals, membership, accounts and the committed placement
// (including source FENCE/target activation) are replayed before Open returns.
// Data accounting is a bounded derived cache. A cold census may run once in the
// background, under shared volume locks, while authenticated reads remain live.
// Until all counters and recovery reserves are exact, capacity is UNKNOWN and
// mutations are rejected before taking either opMu or a volume write lock.
func (s *Server) storageUnavailable() error {
	if f := s.storageWarmError.Load(); f != nil {
		return fail(503, fmt.Sprintf("storage recovery failed: %v", f.err))
	}
	return fail(503, "storage accounting and recovery are warming up; retry later")
}
func (s *Server) storageAllAccountingReady() bool {
	for _, v := range s.quotaVolumes() {
		if !v.AccountingReady() {
			return false
		}
	}
	return true
}
func (s *Server) storageCanFinishInline() bool {
	// Only proven empty volumes can bypass background recovery. A cache hit on
	// populated media must not move reservation/replica censuses back in front
	// of the listener.
	if s.scopes != nil {
		s.scopes.mu.Lock()
		active := false
		for _, t := range s.scopes.txns {
			if s.scopes.active(t) {
				active = true
				break
			}
		}
		s.scopes.mu.Unlock()
		if active {
			return false
		}
	}
	for _, v := range s.quotaVolumes() {
		if !v.AccountingReady() {
			return false
		}
		h, err := v.Open(false)
		if err != nil {
			return false
		}
		usage, err := h.StorageUsage()
		h.Close()
		if err != nil || len(usage) != 0 {
			return false
		}
	}
	return true
}
func (s *Server) warmStorage(opts Options) error {
	for _, v := range s.quotaVolumes() {
		if v == s.system {
			continue
		}
		if err := v.WarmAccounting(s.ctx); err != nil {
			return err
		}
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return s.finishStorageWarmup(opts)
}
func (s *Server) finishStorageWarmup(opts Options) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	// No control/range/replica worker has started and incoming mutations are
	// gated. Reservation reconstruction can therefore inspect stable plans while
	// GET uses the already replayed immutable placement and source fences.
	s.storageAccountingWarmed.Store(true)
	if s.controller != nil {
		if err := s.controller.initialCheckpoint(); err != nil {
			return err
		}
	}
	if s.scopes != nil {
		if err := s.scopes.restoreReservations(); err != nil {
			return err
		}
	}
	if err := s.initReplicas(); err != nil {
		return err
	}
	if err := s.recoverOperations(); err != nil {
		return err
	}
	s.opMu.Lock()
	// Install exact reserve floors before advertising READY. apply uses the
	// configured accounting fast path while the gate remains closed.
	err := s.installWarmReserves()
	if err == nil {
		err = s.ensureVolumeAuthorityLocked()
	}
	s.opMu.Unlock()
	if err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.storageWarming.Store(false)
	s.storageWarmDoneOnce.Do(func() { close(s.storageWarmDone) })
	s.publishStats()
	s.startStorageWorkers(opts)
	return nil
}
func (s *Server) installWarmReserves() error {
	for _, v := range s.quotaVolumes() {
		if v == s.system {
			continue
		}
		h, err := v.Open(false)
		if err != nil {
			return err
		}
		usage, err := h.StorageUsage()
		h.Close()
		if err != nil {
			return err
		}
		if err := v.SetGuaranteedReserve(s.quotaReserve(client.ID(v.Identity().VolumeID), usage)); err != nil {
			return err
		}
	}
	return nil
}
func storageWarmReadMessage(m client.Message) bool {
	switch m.Opcode {
	case client.NodeAdminRequest:
		inner, err := client.DecodeNodeAdmin(m.Body)
		return err == nil && storageWarmReadMessage(client.Message{Opcode: inner.Opcode, Body: inner.Body})
	case client.PeerAdminRequest:
		inner, err := client.DecodePeerAdmin(m.Body)
		return err == nil && storageWarmReadMessage(client.Message{Opcode: inner.Opcode, Body: inner.Body})
	}
	switch m.Opcode {
	case client.Hello, client.Ping, client.StatsGet, client.StatsSubscribe, client.StatsUnsubscribe,
		client.MapGetMessage, client.BucketInfoGet, client.BucketScan, client.BucketObjectList, client.BucketFormatOpen,
		client.ObjectSubscribe, client.ObjectUnsubscribe, client.SubscriptionAck,
		client.AdminGet, client.NodeConfigGet, client.UserList, client.UserListPage, client.UserS3KeyList,
		client.BucketList, client.BucketListPage, client.PlacementGroupList, client.AdminOperationStatus,
		client.VolumeCandidates, client.VolumeCapacityGet, client.VolumeGrowInfo,
		client.NodeHelloMessage, client.PeerDirectoryMessage, nodeVersionsMessage, client.PeerVerifyAdmin, peerControlInfo, peerSystemChunk,
		peerVerifyFrontend, peerCowCapture, peerCowRead, peerCowClose, peerCowInventory,
		0x0100, 0x0101, 0x0107:
		return true
	}
	return false
}

// WaitStorageReady is useful for embedded consumers that explicitly require
// write availability. The daemon serves HTTP immediately and exposes warmup in
// statistics instead of blocking its listener on this barrier.
func (s *Server) WaitStorageReady(ctx context.Context) error {
	if !s.storageWarming.Load() {
		return nil
	}
	select {
	case <-s.storageWarmDone:
		if f := s.storageWarmError.Load(); f != nil {
			return f.err
		}
		if s.storageWarming.Load() {
			return s.ctx.Err()
		}
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) startStorageWorkers(opts Options) {
	s.startLeaseRecovery()
	s.startAcceptedCheckpoints()
	s.startBucketReclaimer()
	// Transport fixtures drive recovery explicitly; autonomous housekeeping
	// belongs to the production daemon lifecycle, including startup recovery.
	if !opts.transportOnly {
		s.startS3Maintenance()
	}
	for _, d := range s.bucketSnapshots() {
		if !d.DeletionComplete && (d.State == BucketDeleting || d.DesiredPolicy != nil || d.CloneDetach) {
			id, _ := bucketID(d.ID)
			s.startBucketCleanup(id)
		}
	}
	for _, v := range s.running.Volumes {
		if v.State == "draining" {
			id := v.OperationID
			if id == "" {
				id = newID()
			}
			s.startDrain(id, v.ID)
		}
	}
	if s.quotas != nil {
		s.startQuotaWorker()
	}
	if s.controller != nil {
		s.replicas.start()
	}
	if s.controller != nil {
		s.controller.start()
	}
	if s.scopes != nil && !opts.manualRanges {
		s.scopes.start()
	}
}
