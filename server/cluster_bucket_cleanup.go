// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"runtime"
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

// Cursors are advisory; the committed DELETING description remains the durable
// intent. A returned generation is confirmed before any cursor/completion moves.
type cleanupVolumeProgress struct {
	id                        volume.ID
	objectsDone, requestsDone bool
	objectAfter               *volume.ObjectID
	checkpoint                uint64
}
type bucketCleanupJob struct {
	volumes []cleanupVolumeProgress
	done    bool
}

// A sweep freezes its participant set. Newly deleting buckets join the next
// sweep, so receipts before the current cursor cannot be missed.
type cleanupReceiptSweep struct {
	after   *volume.ID
	buckets map[volume.ID]*cleanupVolumeProgress
}

const cleanupMaxWait = 250 * time.Millisecond

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
			runtime.Gosched()
			delay = 5 * time.Millisecond
			if !worked {
				delay = 25 * time.Millisecond
			}
			if s.ioPriority.users.Load() > 0 {
				delay = min(cleanupMaxWait, max(delay, 9*time.Since(begin)))
			}
		}
	}()
}

func (s *Server) refreshCleanupJobs(view *systemView) {
	if s.cleanupJobs == nil {
		s.cleanupJobs = map[volume.ID]*bucketCleanupJob{}
	}
	if s.cleanupReceipts == nil {
		s.cleanupReceipts = map[volume.ID]*cleanupReceiptSweep{}
	}
	if s.cleanupView == view {
		return
	}
	pending := map[volume.ID]bool{}
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
	sort.Slice(s.cleanupOrder, func(i, j int) bool { return s.cleanupOrder[i].String() < s.cleanupOrder[j].String() })
	for id := range s.cleanupJobs {
		if !pending[id] {
			delete(s.cleanupJobs, id)
		}
	}
	// Keep existing sweeps/cursors across unrelated catalog edits. Removed
	// participants are revalidated against the current description before work.
	s.cleanupView = view
}

// DELETING is an irreversible committed fence: admin updates and clone creation
// reject that state. Physical cleanup therefore needs only the volume lock and
// a fresh description check, never node admission across I/O. Waiting for a
// starved volume happens without any node lock.
func (s *Server) openCleanupVolume(id volume.ID, overdue bool) (*volume.Handle, *volume.Volume) {
	s.mu.RLock()
	managed := s.volumes[id.String()]
	s.mu.RUnlock()
	if managed == nil {
		return nil, nil
	}
	var h *volume.Handle
	var err error
	if overdue {
		h, err = managed.store.Open(true)
	} else {
		h, _, err = managed.store.TryOpen(true)
	}
	if err != nil {
		return nil, managed.store
	}
	return h, managed.store
}
func (s *Server) cleanupBucketValid(id volume.ID) bool {
	b, err := s.Bucket(id)
	if err != nil {
		return false
	}
	d := b.Describe()
	return d.State == BucketDeleting && !d.DeletionComplete && len(d.ClonePeers) == 0
}

func (s *Server) reclaimBucketStep(preferObjects ...bool) bool {
	if s.ioPriority.rebuilds.Load() > 0 {
		return false
	}
	view := s.currentSystem.Load()
	if view == nil {
		return false
	}
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if s.cleanupLast.IsZero() {
		s.cleanupLast = time.Now()
	}
	overdue := time.Since(s.cleanupLast) >= cleanupMaxWait
	if s.ioPriority.users.Load() > 0 && !overdue {
		return false
	}
	s.refreshCleanupJobs(view)
	defer func() { s.cleanupReceiptTurn = !s.cleanupReceiptTurn }()
	receiptFirst := s.cleanupReceiptTurn && !(len(preferObjects) > 0 && preferObjects[0])
	try := func(wait bool) bool {
		if receiptFirst && s.cleanupReceiptPage(wait) {
			return true
		}
		if s.cleanupObjectPage(wait) {
			return true
		}
		return !receiptFirst && s.cleanupReceiptPage(wait)
	}
	// Try every runnable candidate before allowing an overdue writer to queue.
	// Queueing once eventually excludes a continuous stream of readers as well.
	worked := try(false)
	if !worked && overdue {
		worked = try(true)
	}
	if worked {
		s.cleanupLast = time.Now()
	}
	for _, job := range s.cleanupJobs {
		if job.done {
			continue
		}
		done := true
		for _, p := range job.volumes {
			if !p.objectsDone || !p.requestsDone || p.checkpoint != 0 {
				done = false
				break
			}
		}
		if done {
			job.done = true
			s.notifyStats()
		}
	}
	return worked
}

func (s *Server) cleanupObjectPage(overdue bool) bool {
	for i := 0; i < len(s.cleanupOrder); i++ {
		s.cleanupCursor = (s.cleanupCursor + 1) % len(s.cleanupOrder)
		id := s.cleanupOrder[s.cleanupCursor]
		job := s.cleanupJobs[id]
		if job.done || !s.cleanupBucketValid(id) {
			continue
		}
		for j := range job.volumes {
			p := &job.volumes[j]
			if p.objectsDone && p.checkpoint == 0 {
				continue
			}
			s.mu.RLock()
			managed := s.volumes[p.id.String()]
			s.mu.RUnlock()
			if managed == nil {
				p.objectsDone = true
				p.requestsDone = true
				p.checkpoint = 0
				return true
			}
			if p.checkpoint != 0 {
				if err := managed.store.CheckpointThrough(p.checkpoint); err != nil {
					continue
				}
				p.checkpoint = 0
			}
			h, v := s.openCleanupVolume(p.id, overdue)
			if h == nil {
				continue
			}
			if !s.cleanupBucketValid(id) {
				h.Close()
				continue
			}
			usage, err := h.StorageUsage()
			if err != nil {
				h.Close()
				continue
			}
			found := false
			for _, u := range usage {
				if u.Bucket == id && u.Physical > 0 {
					found = true
					break
				}
			}
			if !found {
				h.Close()
				p.objectsDone = true
				p.requestsDone = true
				return true
			}
			records, err := h.ScanBucketObjects(id, p.objectAfter, 32)
			if err != nil {
				h.Close()
				continue
			}
			if len(records) == 0 {
				h.Close()
				p.objectsDone = true
				return true
			}
			generation, err := h.PurgeBatchAccepted(records)
			h.Close()
			if err != nil {
				continue
			}
			p.checkpoint = generation
			if err = v.CheckpointThrough(generation); err != nil {
				return false
			}
			p.checkpoint = 0
			next := records[len(records)-1].Key.Object
			p.objectAfter = &next
			return true
		}
	}
	return false
}

func (s *Server) cleanupReceiptPage(overdue bool) bool {
	// One traversal serves all deleting buckets on the same volume, even while
	// their object pages are still being removed. Deleted namespaces cannot add
	// fresh client receipts. The participant set is fixed until end-of-index.
	volumes := map[volume.ID]bool{}
	for _, job := range s.cleanupJobs {
		for _, p := range job.volumes {
			if !p.requestsDone {
				volumes[p.id] = true
			}
		}
	}
	ids := make([]volume.ID, 0, len(volumes))
	for id := range volumes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for i := 0; i < len(ids); i++ {
		s.cleanupVolumeCursor = (s.cleanupVolumeCursor + 1) % len(ids)
		vid := ids[s.cleanupVolumeCursor]
		sweep := s.cleanupReceipts[vid]
		if sweep == nil {
			sweep = &cleanupReceiptSweep{buckets: map[volume.ID]*cleanupVolumeProgress{}}
			for id, job := range s.cleanupJobs {
				if s.cleanupBucketValid(id) {
					for i := range job.volumes {
						p := &job.volumes[i]
						if p.id == vid && !p.requestsDone {
							sweep.buckets[id] = p
						}
					}
				}
			}
			s.cleanupReceipts[vid] = sweep
		}
		h, v := s.openCleanupVolume(vid, overdue)
		if v == nil {
			for _, p := range sweep.buckets {
				p.requestsDone = true
			}
			delete(s.cleanupReceipts, vid)
			return true
		}
		if h == nil {
			continue
		}
		// Maintained physical accounting includes receipts and tombstones. Empty
		// buckets on this volume need no traversal through unrelated retry records.
		if sweep.after == nil {
			usage, err := h.StorageUsage()
			if err != nil {
				h.Close()
				continue
			}
			present := map[volume.ID]bool{}
			for _, u := range usage {
				if u.Physical > 0 {
					present[u.Bucket] = true
				}
			}
			for id, p := range sweep.buckets {
				if !present[id] {
					p.objectsDone = true
					p.requestsDone = true
					delete(sweep.buckets, id)
				}
			}
			if len(sweep.buckets) == 0 {
				h.Close()
				delete(s.cleanupReceipts, vid)
				return true
			}
		}
		records, err := h.ScanRequestRecords(sweep.after, 128)
		if err != nil {
			h.Close()
			continue
		}
		var remove []volume.ID
		for _, record := range records {
			if _, ok := sweep.buckets[record.Key.Bucket]; ok && s.cleanupBucketValid(record.Key.Bucket) {
				remove = append(remove, record.ID)
			}
		}
		var generation uint64
		if len(remove) > 0 {
			generation, err = h.ForgetRequestsAccepted(remove)
		}
		h.Close()
		if err != nil {
			continue
		}
		if generation != 0 {
			for _, p := range sweep.buckets {
				p.checkpoint = generation
			}
			if err = v.CheckpointThrough(generation); err != nil {
				return false
			}
			for _, p := range sweep.buckets {
				p.checkpoint = 0
			}
		}
		if len(records) == 0 {
			for _, p := range sweep.buckets {
				p.requestsDone = true
			}
			delete(s.cleanupReceipts, vid)
		} else {
			next := records[len(records)-1].ID
			sweep.after = &next
		}
		return true
	}
	return false
}
