// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"time"

	"github.com/launix-de/stqr-storage/volume"
)

// Deletion first publishes a blocked namespace. Policy changes first publish a
// desired policy. Resumable workers retain those intents until local copies and
// metadata are committed. Reads remain possible during a policy transition.
func (s *Server) startBucketCleanup(id volume.ID) {
	if s.metadataPreview || s.controller != nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.bucketJobs == nil {
		s.bucketJobs = map[volume.ID]bool{}
	}
	if s.bucketJobs[id] {
		s.mu.Unlock()
		return
	}
	s.bucketJobs[id] = true
	s.workers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.workers.Done()
		ownsSlot := true
		defer func() {
			if ownsSlot {
				s.mu.Lock()
				delete(s.bucketJobs, id)
				s.mu.Unlock()
			}
		}()
		for {
			if s.ctx.Err() != nil {
				return
			}
			err := s.cowLocalDetachStep(id)
			s.opMu.Lock()
			if err == nil {
				err = s.bucketLifecycleOnce(id)
			}
			if err == nil {
				s.mu.Lock()
				delete(s.bucketJobs, id)
				s.mu.Unlock()
				ownsSlot = false
			}
			s.opMu.Unlock()
			if err == nil {
				return
			}
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()
}
func (s *Server) bucketLifecycleOnce(id volume.ID) error {
	if s.metadataPreview {
		return fail(503, "metadata preview cannot run data cleanup")
	}
	b, err := s.Bucket(id)
	if errors.Is(err, ErrBucketNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	d := b.Describe()
	if len(d.ClonePeers) > 0 && (d.CloneDetach || d.State == BucketDeleting || d.DesiredPolicy != nil) {
		return fail(503, "clone connections must materialize before physical cleanup")
	}
	if d.State == BucketDeleting {
		for _, v := range b.allVolumes() {
			var after *volume.Key
			for {
				h, err := v.Open(false)
				if err != nil {
					return err
				}
				records, err := h.ScanObjects(after, 128)
				h.Close()
				if err != nil {
					return err
				}
				if len(records) == 0 {
					break
				}
				for _, r := range records {
					if r.Key.Bucket == id && !r.Info.Deleted {
						h, err := v.Open(true)
						if err != nil {
							return err
						}
						err = h.Delete(r.Key, volume.Options{})
						h.Close()
						if err != nil {
							return err
						}
					}
				}
				last := records[len(records)-1].Key
				after = &last
				if s.ctx.Err() != nil {
					return s.ctx.Err()
				}
			}
		}
		d.DeletionComplete = true
	} else if d.DesiredPolicy != nil {
		desired := *d.DesiredPolicy
		var target *volume.Volume
		for _, v := range s.placementVolumesAt(desired.PlacementGroup, desired.PlacementMaskVersion) {
			if v.State() == volume.Active && bucketClass(desired).Allows(v.Identity().Class) {
				if target == nil || v.Stats().AdmissionFreePhysical > target.Stats().AdmissionFreePhysical {
					target = v
				}
			}
		}
		if target == nil {
			return volume.ErrFull
		}
		for _, source := range b.localVolumes() {
			var after *volume.Key
			for {
				h, err := source.Open(false)
				if err != nil {
					return err
				}
				records, err := h.ScanObjects(after, 128)
				h.Close()
				if err != nil {
					return err
				}
				if len(records) == 0 {
					break
				}
				for _, r := range records {
					if r.Key.Bucket != id {
						continue
					}
					if source == target {
						if !r.Info.Deleted {
							h, err := source.Open(true)
							if err != nil {
								return err
							}
							err = h.Recompress(r.Key, bucketCodec(desired))
							h.Close()
							if err != nil {
								return err
							}
						}
					} else {
						info := r.Info
						info.Compression = bucketCodec(desired)
						if err := copyObject(s.ctx, source, target, r.Key, info); err != nil {
							return err
						}
					}
				}
				last := records[len(records)-1].Key
				after = &last
				if s.ctx.Err() != nil {
					return s.ctx.Err()
				}
			}
		}
		d.Policy = desired
		d.DesiredPolicy = nil
		d.LocalVolumes = []string{target.Identity().VolumeID.String()}
	} else {
		return nil
	}
	for i := range d.Receipts {
		if d.Receipts[i].Running {
			d.Receipts[i].Running = false
			d.Receipts[i].Status = 200
			if d.State == BucketDeleting && d.Receipts[i].Opcode != 0x2008 {
				d.Receipts[i].Status = 409
			}
		}
	}
	_, err = s.buckets.Save(d, d.Revision)
	if err == nil {
		s.updateAdminCapacity()
		err = s.finishLocalPlacementGroups()
	}
	return err
}

// noteBucketDrain publishes replacement locations after copies, before the
// source is detached. Restart/retry therefore never routes to a retired volume.
func (s *Server) noteBucketDrain(source string, targets map[volume.ID]map[string]bool) error {
	for id, destinations := range targets {
		b, err := s.Bucket(id)
		if errors.Is(err, ErrBucketNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		d := b.Describe()
		if len(d.LocalVolumes) == 0 {
			continue
		}
		out := []string{}
		for _, v := range d.LocalVolumes {
			if v != source {
				out = append(out, v)
			}
		}
		for v := range destinations {
			present := false
			for _, old := range out {
				present = present || old == v
			}
			if !present {
				out = append(out, v)
			}
		}
		d.LocalVolumes = out
		if _, err = s.buckets.Save(d, d.Revision); err != nil {
			return err
		}
	}
	return nil
}
