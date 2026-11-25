// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerBucketCodec uint16 = 0x104b

func samePlacementPolicy(before, after BucketPolicy) bool {
	after.Version = before.Version
	after.Compression = before.Compression
	after.Limits = before.Limits
	return before == after
}
func (c *clusterController) finishBucketPolicies(ctx context.Context) {
	s := c.server
	l := c.coordinator.ledger
	l.mu.Lock()
	members := membershipIDs(l.current.Membership)
	busy := l.slot != nil || l.request != nil
	l.mu.Unlock()
	if busy || len(members) == 0 || members[0] != l.node {
		return
	}
	for _, description := range s.bucketSnapshots() {
		if description.DesiredPolicy == nil || description.State == BucketDeleting || len(description.ClonePeers) > 0 {
			continue
		}
		id, _ := bucketID(description.ID)
		need := s.requiredReplicas(*description.DesiredPolicy, s.currentSystem.Load().manifest.Membership)
		countsReady := true
		for _, entry := range s.placement.Load().bucketEntries(client.ID(id)) {
			if len(entry.Locations) != need {
				countsReady = false
				break
			}
		}
		if !countsReady {
			continue
		}
		if description.Policy.PlacementGroup != "" {
			if !s.policyRangesReady(id, description.DesiredPolicy.Version) {
				continue
			}
		} else if !samePlacementPolicy(description.Policy, *description.DesiredPolicy) && !s.replicaPolicyInPlace(description) {
			if err := s.scopes.syncGlobalArchives(ctx); err != nil {
				return
			}
			id, err := bucketID(description.ID)
			if err != nil {
				return
			}
			if !s.policyRangesReady(id, description.DesiredPolicy.Version) {
				continue
			}
		}
		bucket, err := bucketID(description.ID)
		if err != nil {
			return
		}
		_, _, _ = c.change(ctx, nil, func() (*adminDraft, error) {
			b, err := s.Bucket(bucket)
			if err != nil {
				return nil, err
			}
			expected := b.Describe()
			if expected.DesiredPolicy == nil {
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
			if expected.Policy.PlacementGroup == "" && (samePlacementPolicy(expected.Policy, *expected.DesiredPolicy) || s.replicaPolicyInPlace(expected)) {
				for _, node := range request.Claim.Required {
					e := client.Encoder{}
					e.Blob(claim)
					e.ID(client.ID(bucket))
					e.U64(expected.Revision)
					e.U8(uint8(bucketCodec(*expected.DesiredPolicy)))
					if _, err := (peerControlParticipants{runtime: s.control}).call(ctx, node, peerBucketCodec, e.B); err != nil {
						return nil, err
					}
				}
			} else if !s.policyRangesReady(bucket, expected.DesiredPolicy.Version) {
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
			if next.Revision != expected.Revision || next.DesiredPolicy == nil {
				return nil, errControlConflict
			}
			next.Policy = *next.DesiredPolicy
			next.DesiredPolicy = nil
			for i := range next.Receipts {
				if next.Receipts[i].Running {
					next.Receipts[i].Running = false
					next.Receipts[i].Status = 200
				}
			}
			if _, err := preview.buckets.Save(next, next.Revision); err != nil {
				return nil, err
			}
			complete = true
			return draft, nil
		}, client.ID(bucket))
		return
	}
}

// Physical encodings are self describing. Atomic per-object rewrites preserve
// logical versions and original retry receipts, so reads remain valid while a
// committed desired codec is applied. Its policy is published only after ALL
// members confirm; a crash resumes only the objects not already converted.
func (s *Server) applyBucketCodec(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	claim, err := decodeControlClaim(d.Blob())
	bucket, revision, codec := volume.ID(d.ID()), d.U64(), volume.Compression(d.U8())
	if err != nil || d.Done() != nil || claim.Round.Initiator != actor || bucket == systemBucket || codec > volume.XZ || s.control == nil {
		return nil, fail(403, "invalid codec transition authority")
	}
	var after *volume.ObjectID
	ctx := context.WithValue(s.ctx, replicaScanKey{}, true)
	for {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		done, err := func() (bool, error) {
			s.opMu.Lock()
			defer s.opMu.Unlock()
			l := s.control.ledger
			l.mu.Lock()
			valid := l.slot != nil && equalControlClaim(l.slot.Claim, claim)
			l.mu.Unlock()
			b, err := s.Bucket(bucket)
			if !valid || err != nil {
				return false, errControlConflict
			}
			description := b.Describe()
			if description.Revision != revision || description.DesiredPolicy == nil || bucketCodec(*description.DesiredPolicy) != codec || !(samePlacementPolicy(description.Policy, *description.DesiredPolicy) || s.replicaPolicyInPlace(description)) {
				return false, errControlConflict
			}
			records, next, more, err := b.scan(ctx, description, after, 128)
			if err != nil {
				return false, err
			}
			for _, record := range records {
				store, info, found, err := b.selectVolume(description, record.Key, volume.ID{}, false)
				if err != nil {
					return false, err
				}
				if !found || info.Deleted || info.Compression == codec {
					continue
				}
				h, err := store.Open(true)
				if err != nil {
					return false, err
				}
				err = h.Recompress(record.Key, codec)
				h.Close()
				if err != nil {
					return false, err
				}
			}
			if more {
				after = &next
			}
			return !more, nil
		}()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
	}
	s.notifyStats()
	return nil, nil
}

func (s *Server) policyRangesReady(bucket volume.ID, version uint64) bool {
	image := s.placement.Load()
	if image == nil {
		return false
	}
	entries := image.bucketEntries(client.ID(bucket))
	if len(entries) == 0 {
		return false
	}
	for _, entry := range entries {
		if entry.PolicyVersion != version {
			return false
		}
	}
	return true
}
