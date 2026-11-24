// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"sort"
	"time"
)

const peerBucketPurge uint16 = 0x1049

func (c *clusterController) finishBucketDeletions(ctx context.Context) {
	s := c.server
	l := c.coordinator.ledger
	l.mu.Lock()
	base := cloneControlBase(l.current)
	busy := l.slot != nil || l.request != nil
	l.mu.Unlock()
	members := membershipIDs(base.Membership)
	if busy || len(members) == 0 || members[0] != l.node {
		return
	}
	for _, description := range s.bucketSnapshots() {
		if description.State != BucketDeleting || description.DeletionComplete || len(description.ClonePeers) > 0 {
			continue
		}
		bucket, err := bucketID(description.ID)
		if err != nil {
			return
		}
		// Probe local background completion before reserving a global round.
		// The committed DELETING description survives subsequent generations.
		ready := true
		for _, node := range members {
			e := client.Encoder{}
			e.Blob(nil)
			e.ID(client.ID(bucket))
			e.U64(description.Revision)
			if _, err := (peerControlParticipants{runtime: s.control}).call(ctx, node, peerBucketPurge, e.B); err != nil {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		_, _, _ = c.change(ctx, nil, func() (*adminDraft, error) {
			current, err := s.Bucket(bucket)
			if err != nil {
				return nil, err
			}
			expected := current.Describe()
			if expected.State != BucketDeleting || expected.DeletionComplete {
				return nil, errControlConflict
			}

			s.opMu.Lock()
			preview, err := s.newAdminPreview()
			s.opMu.Unlock()
			if err != nil {
				return nil, err
			}
			draft := &adminDraft{server: preview, status: 200}
			complete := false
			defer func() {
				if !complete {
					draft.close()
				}
			}()
			local, err := preview.Bucket(bucket)
			if err != nil {
				return nil, err
			}
			next := local.Describe()
			if next.Revision != expected.Revision {
				return nil, errControlConflict
			}
			next.DeletionComplete = true
			for i := range next.Receipts {
				if next.Receipts[i].Running {
					next.Receipts[i].Running = false
					next.Receipts[i].Status = 200
					if next.Receipts[i].Opcode != client.BucketDelete {
						next.Receipts[i].Status = 409
					}
				}
			}
			if _, err := preview.buckets.Save(next, next.Revision); err != nil {
				return nil, err
			}
			complete = true
			return draft, nil
		})
		return
	}
}

// A committed DELETING catalog entry is the durable cleanup intent. Peer
// requests only observe completion, never perform bulk work inside ALL/control.
// Empty claim is the new status-only form; old claim-bearing requests are also
// status-only, so mixed-version callers cannot pin cleanup inside a generation.
func (s *Server) purgeDeletedBucket(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	raw := d.Blob()
	bucket, revision := volume.ID(d.ID()), d.U64()
	if d.Done() != nil || bucket == systemBucket || s.control == nil {
		return nil, fail(403, "invalid bucket cleanup status")
	}
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, fail(403, "cleanup status requires a cluster member")
	}
	if len(raw) > 0 {
		claim, err := decodeControlClaim(raw)
		if err != nil || claim.Round.Initiator != actor {
			return nil, fail(403, "invalid legacy cleanup claim")
		}
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	desc := b.Describe()
	if desc.State != BucketDeleting || desc.Revision != revision || len(desc.ClonePeers) > 0 {
		return nil, errControlConflict
	}
	s.cleanupMu.Lock()
	job := s.cleanupJobs[bucket]
	done := job != nil && job.done
	s.cleanupMu.Unlock()
	if !done {
		return nil, fail(503, "bucket cleanup continues in idle cycles")
	}
	return nil, nil
}

type cleanupVolumeProgress struct {
	id                volume.ID
	objectsDone, done bool
	objectAfter       *volume.ObjectID
	requestAfter      *volume.ID
}
type bucketCleanupJob struct {
	volumes []cleanupVolumeProgress
	next    int
	done    bool
}

func (s *Server) startBucketReclaimer() {
	if s.metadataPreview || s.controller == nil {
		return
	}
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		delay := 5 * time.Millisecond
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(delay):
			}
			begin := time.Now()
			worked := s.reclaimBucketStep()
			delay = 5 * time.Millisecond
			if !worked {
				delay = 250 * time.Millisecond
			}
			// A busy foreground stream is never held behind an entire bucket purge.
			if s.ioPriority.users.Load() > 0 {
				delay = max(delay, 9*time.Since(begin))
			}
		}
	}()
}

// One bounded page per call. TryLock/TryOpen never queue idle work ahead of
// users or a rebuilding range. Cursors are RAM hints; the catalog intent is
// persistent and a restart safely reconstructs cleanup from the indexes.
func (s *Server) reclaimBucketStep() bool {
	if !s.cleanupIdle() {
		return false
	}
	view := s.currentSystem.Load()
	if view == nil {
		return false
	}
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if s.cleanupJobs == nil {
		s.cleanupJobs = make(map[volume.ID]*bucketCleanupJob)
	}
	if s.cleanupView != view {
		pending := make(map[volume.ID]bool)
		s.cleanupOrder = nil
		for _, desc := range view.descriptions {
			if desc.State != BucketDeleting || desc.DeletionComplete || len(desc.ClonePeers) > 0 {
				continue
			}
			id := newVolumeID(desc.ID)
			pending[id] = true
			s.cleanupOrder = append(s.cleanupOrder, id)
			if s.cleanupJobs[id] != nil {
				continue
			}
			job := &bucketCleanupJob{}
			s.mu.RLock()
			for _, v := range s.volumes {
				if !v.config.System {
					job.volumes = append(job.volumes, cleanupVolumeProgress{id: v.store.Identity().VolumeID})
				}
			}
			s.mu.RUnlock()
			sort.Slice(job.volumes, func(i, j int) bool { return job.volumes[i].id.String() < job.volumes[j].id.String() })
			s.cleanupJobs[id] = job
		}
		for id := range s.cleanupJobs {
			if !pending[id] {
				delete(s.cleanupJobs, id)
			}
		}
		s.cleanupView = view
	}
	for i := 0; i < len(s.cleanupOrder); i++ {
		s.cleanupCursor = (s.cleanupCursor + 1) % len(s.cleanupOrder)
		id := s.cleanupOrder[s.cleanupCursor]
		job := s.cleanupJobs[id]
		if job.done {
			continue
		}
		if job.next == len(job.volumes) {
			job.done = true
			s.notifyStats()
			continue
		}
		if !s.cleanupIdle() || !s.opMu.TryLock() {
			return false
		}
		worked := func() bool {
			defer s.opMu.Unlock()
			if !s.cleanupIdle() {
				return false
			}
			progress := &job.volumes[job.next]
			s.mu.RLock()
			managed := s.volumes[progress.id.String()]
			s.mu.RUnlock()
			if managed == nil {
				job.next++
				return true
			}
			h, acquired, err := managed.store.TryOpen(true)
			if !acquired || err != nil {
				return false
			}
			defer h.Close()
			if !progress.objectsDone {
				records, err := h.ScanBucketObjects(id, progress.objectAfter, 8)
				if err != nil {
					return false
				}
				if len(records) == 0 {
					progress.objectsDone = true
					return true
				}
				if err = h.PurgeBatch(records); err != nil {
					return false
				}
				next := records[len(records)-1].Key.Object
				progress.objectAfter = &next
				return true
			}
			records, err := h.ScanRequestRecords(progress.requestAfter, 128)
			if err != nil {
				return false
			}
			if len(records) == 0 {
				job.next++
				return true
			}
			var remove []volume.ID
			for _, record := range records {
				if record.Key.Bucket == id {
					remove = append(remove, record.ID)
				}
			}
			if err = h.ForgetRequests(remove); err != nil {
				return false
			}
			next := records[len(records)-1].ID
			progress.requestAfter = &next
			return true
		}()
		return worked
	}
	return false
}
