// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
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
		if description.State != BucketDeleting || description.DeletionComplete {
			continue
		}
		bucket, err := bucketID(description.ID)
		if err != nil {
			return
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
			request := c.coordinator.requestSnapshot()
			if request == nil {
				return nil, errControlConflict
			}
			claim, err := marshalControlClaim(request.Claim)
			if err != nil {
				return nil, err
			}
			for _, node := range request.Claim.Required {
				e := client.Encoder{}
				e.Blob(claim)
				e.ID(client.ID(bucket))
				e.U64(expected.Revision)
				if _, err := (peerControlParticipants{runtime: s.control}).call(ctx, node, peerBucketPurge, e.B); err != nil {
					return nil, err
				}
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

// The global DELETE intent is already committed on every member. The follow-up
// slot prevents conflicting catalog changes; irreversible data cleanup is safe
// to repeat after abort/restart. Each page releases opMu for unrelated buckets.
func (s *Server) purgeDeletedBucket(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	claim, err := decodeControlClaim(d.Blob())
	bucket, revision := volume.ID(d.ID()), d.U64()
	if err != nil || d.Done() != nil || claim.Round.Initiator != actor || bucket == systemBucket || s.control == nil {
		return nil, fail(403, "invalid bucket cleanup authority")
	}
	s.opMu.Lock()
	l := s.control.ledger
	l.mu.Lock()
	valid := l.slot != nil && equalControlClaim(l.slot.Claim, claim)
	l.mu.Unlock()
	b, err := s.Bucket(bucket)
	if !valid || err != nil {
		s.opMu.Unlock()
		return nil, errControlConflict
	}
	description := b.Describe()
	if description.State != BucketDeleting || description.Revision != revision {
		s.opMu.Unlock()
		return nil, errControlConflict
	}
	s.mu.RLock()
	var stores []*managedVolume
	for _, v := range s.volumes {
		if !v.config.System {
			stores = append(stores, v)
		}
	}
	s.mu.RUnlock()
	s.opMu.Unlock()
	for _, managed := range stores {
		var after *volume.ObjectID
		for {
			if err := s.ctx.Err(); err != nil {
				return nil, err
			}
			done, err := func() (bool, error) {
				s.opMu.Lock()
				defer s.opMu.Unlock()
				s.mu.RLock()
				attached := s.volumes[managed.config.ID] == managed
				s.mu.RUnlock()
				if !attached {
					return true, nil
				}
				h, err := managed.store.Open(true)
				if err != nil {
					return false, err
				}
				defer h.Close()
				records, err := h.ScanBucketObjects(bucket, after, 32)
				if err != nil {
					return false, err
				}
				if len(records) == 0 {
					return true, nil
				}
				if err := h.PurgeBatch(records); err != nil {
					return false, err
				}
				next := records[len(records)-1].Key.Object
				after = &next
				return false, nil
			}()
			if err != nil {
				return nil, err
			}
			if done {
				break
			}
			// Yield outside opMu/the volume lock. Cleanup must not monopolize a
			// busy device, even when the next index page is already cached.
			select {
			case <-s.ctx.Done():
				return nil, s.ctx.Err()
			case <-time.After(2 * time.Millisecond):
			}
		}
		var cursor *volume.ID
		for {
			if err := s.ctx.Err(); err != nil {
				return nil, err
			}
			done, err := func() (bool, error) {
				s.opMu.Lock()
				defer s.opMu.Unlock()
				s.mu.RLock()
				attached := s.volumes[managed.config.ID] == managed
				s.mu.RUnlock()
				if !attached {
					return true, nil
				}
				h, err := managed.store.Open(true)
				if err != nil {
					return false, err
				}
				defer h.Close()
				records, err := h.ScanRequestRecords(cursor, 64)
				if err != nil {
					return false, err
				}
				if len(records) == 0 {
					return true, nil
				}
				var forgotten []volume.ID
				for _, record := range records {
					if record.Key.Bucket == bucket {
						forgotten = append(forgotten, record.ID)
					}
				}
				if err := h.ForgetRequests(forgotten); err != nil {
					return false, err
				}
				next := records[len(records)-1].ID
				cursor = &next
				return false, nil
			}()
			if err != nil {
				return nil, err
			}
			if done {
				break
			}
			// Yield outside opMu/the volume lock. Cleanup must not monopolize a
			// busy device, even when the next index page is already cached.
			select {
			case <-s.ctx.Done():
				return nil, s.ctx.Err()
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	s.notifyStats()
	return nil, nil
}
