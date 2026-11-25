// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// The caller has explicitly ended all old operation lifetimes under exclusive
// bucket maintenance. First remove fenced inactive copies on every participant;
// only then may deletion markers on authoritative copies be retired.
func (s *Server) reclaimBucketMetadata(ctx context.Context, b *Bucket) error {
	l := s.leaseRecord(b)
	if s.controller == nil || s.control == nil {
		s.opMu.Lock()
		defer s.opMu.Unlock()
		if err := s.reclaimBucketMetadataLocked(ctx, b, 5); err != nil {
			return err
		}
		return s.reclaimBucketMetadataLocked(ctx, b, 6)
	}
	if l == nil || l.Released || l.ObjectIO || !l.ReferenceAware {
		return ErrBucketForbidden
	}
	ids, _, err := s.leaseMembers(b)
	if err != nil {
		return err
	}
	if len(l.Members) > 0 {
		ids = l.Members
	}
	for _, phase := range []uint8{5, 6} {
		for _, node := range ids {
			if _, err := s.callLease(ctx, node, phase, *l); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) reclaimBucketMetadataLocked(ctx context.Context, b *Bucket, phase uint8) error {
	d := b.Describe()
	eligible := map[volume.ID]bool{}
	for _, v := range b.allVolumes() {
		if bucketClass(d.Policy).Allows(v.Identity().Class) {
			eligible[v.Identity().VolumeID] = true
		}
	}
	for _, v := range s.quotaVolumes() {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, err := v.Open(true)
		if err != nil {
			return err
		}
		err = func() error {
			var cursor *volume.ObjectID
			for {
				records, err := h.ScanBucketObjects(b.ID(), cursor, 128)
				if err != nil {
					return err
				}
				if len(records) == 0 {
					break
				}
				var remove []volume.ObjectRecord
				for _, record := range records {
					// Offline filesystem cleanup cannot prove that salvage copies
					// of a lost range have outlived their recovery purpose.
					if b.lostRange(record.Key.Object) {
						continue
					}
					owned := true
					if s.controller != nil {
						entry, known := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(record.Key.Object))
						if known {
							if entry.State == 4 {
								continue
							}
							local := false
							for _, location := range entry.Locations {
								if location.Node == s.control.ledger.node && location.Role <= 1 {
									local = true
									break
								}
							}
							owned = local && entry.State != 4 && s.volumeAllowedForEntry(client.ID(b.ID()), entry, v.Identity().VolumeID, s.currentSystem.Load())
							explicit := false
							for _, location := range entry.Locations {
								if location.Node == s.control.ledger.node && location.Role <= 1 && location.Volume != (client.ID{}) {
									explicit = true
								}
							}
							if !explicit && !eligible[v.Identity().VolumeID] {
								owned = false
							}
							if owned && len(entry.Locations) == 1 && entry.Locations[0].Volume == (client.ID{}) {
								shadow, err := h.ImportShadow(record.Key)
								if err != nil {
									return err
								}
								owned = !shadow
							}
						}
					}
					if phase == 5 && !owned || phase == 6 && owned && record.Info.Deleted && len(d.ClonePeers) == 0 && !b.lostRange(record.Key.Object) {
						remove = append(remove, record)
					}
				}
				if err := h.PurgeBatch(remove); err != nil {
					return err
				}
				last := records[len(records)-1].Key.Object
				cursor = &last
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if phase != 6 {
				return nil
			}
			var after *volume.ID
			for {
				records, err := h.ScanRequestRecords(after, 128)
				if err != nil {
					return err
				}
				if len(records) == 0 {
					break
				}
				var ids []volume.ID
				for _, record := range records {
					if record.Key.Bucket == b.ID() {
						ids = append(ids, record.ID)
					}
				}
				if err := h.ForgetRequests(ids); err != nil {
					return err
				}
				last := records[len(records)-1].ID
				after = &last
			}
			return nil
		}()
		h.Close()
		if err != nil {
			return err
		}
	}
	b.usage.mu.Lock()
	b.usage.valid = false
	b.usage.mu.Unlock()
	return nil
}
