// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type cowDetachProgress struct {
	proof    [32]byte
	after    *volume.ObjectID
	complete bool
}

// Work proceeds in bounded metadata pages. Progress is advisory RAM state:
// first-record-only materialization is durable and makes restart/rescan safe.
// The final ALL catalog change severs the edges under the existing write fences.
func (s *Server) cowDetachStep(ctx context.Context, bucket volume.ID) (bool, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return false, err
	}
	description := b.storageDescription()
	if len(description.ClonePeers) == 0 {
		s.cow.mu.Lock()
		delete(s.cow.detaches, bucket)
		s.cow.mu.Unlock()
		return true, nil
	}
	g, err := s.cowTopology()
	if err != nil {
		return false, err
	}
	proof, err := g.proof(bucket)
	if err != nil {
		return false, err
	}
	s.cow.mu.Lock()
	progress := s.cow.detaches[bucket]
	s.cow.mu.Unlock()
	if progress.proof != proof {
		progress = cowDetachProgress{proof: proof}
	}
	endpoints := append([]volume.ID{bucket}, g.graph[bucket]...)
	if !progress.complete {
		seen := map[volume.ObjectID]bool{}
		more := false
		for _, peer := range g.graph.component(bucket) {
			page, extra, err := s.cowPhysicalPage(ctx, peer, progress.after, 32)
			if err != nil {
				return false, err
			}
			more = more || extra
			for _, id := range page {
				seen[id] = true
			}
		}
		ids := make([]volume.ObjectID, 0, len(seen))
		for id := range seen {
			ids = append(ids, id)
		}
		sortCowIDs(ids)
		if len(ids) > 32 {
			ids = ids[:32]
			more = true
		}
		store := cowServerObjects{server: s, topology: g}
		ctx = context.WithValue(ctx, cowProofKey{}, proof)
		for _, object := range ids {
			for _, endpoint := range endpoints {
				// Reference holders are local to each namespace. Skipping only
				// the endpoint work preserves the outer inventory cursor, including
				// a page whose final candidate is this reserved root. Existing local
				// or legacy physical roots remain untouched, with all tokens intact.
				if bucketLocalReferenceRoot(object) {
					continue
				}
				if endpoint == bucket && description.State == BucketDeleting {
					continue
				}
				own, err := (cowServerObjects{server: s, metadata: true}).capture(ctx, endpoint, object)
				if err != nil {
					return false, err
				}
				if own != nil {
					own.close()
					continue
				}
				snapshot, err := g.graph.resolve(ctx, store, endpoint, object, map[volume.ID]bool{})
				if err != nil {
					return false, err
				}
				if snapshot == nil {
					snapshot = &cowSnapshot{Info: volume.Info{Deleted: true}}
				}
				err = store.materialize(ctx, endpoint, object, snapshot)
				snapshot.close()
				if err != nil {
					return false, err
				}
			}
			next := object
			progress.after = &next
			s.cow.mu.Lock()
			if s.cow.detaches == nil {
				s.cow.detaches = map[volume.ID]cowDetachProgress{}
			}
			s.cow.detaches[bucket] = progress
			s.cow.mu.Unlock()
		}
		if more {
			return false, nil
		}
		progress.complete = true
	}
	build := func(target *Server) error {
		current, err := target.cowTopology()
		if err != nil {
			return err
		}
		now, err := current.proof(bucket)
		if err != nil {
			return err
		}
		if now != proof {
			return fail(503, "clone edges changed before detach")
		}
		var changes []BucketDescription
		for _, endpoint := range endpoints {
			entry, err := target.Bucket(endpoint)
			if err != nil {
				return err
			}
			d := entry.Describe()
			if endpoint == bucket {
				d.ClonePeers = nil
				d.CloneDetach = false
			} else {
				kept := d.ClonePeers[:0]
				for _, edge := range d.ClonePeers {
					if edge.Bucket != bucket.String() {
						kept = append(kept, edge)
					}
				}
				d.ClonePeers = kept
			}
			changes = append(changes, d)
		}
		return target.buckets.saveBatch(changes)
	}
	if s.controller == nil {
		s.opMu.Lock()
		err = build(s)
		s.opMu.Unlock()
	} else {
		frozen := make([]client.ID, 0, len(endpoints))
		for _, id := range endpoints {
			frozen = append(frozen, client.ID(id))
		}
		_, _, err = s.controller.change(ctx, nil, func() (*adminDraft, error) {
			s.opMu.Lock()
			preview, err := s.newAdminPreview()
			s.opMu.Unlock()
			if err != nil {
				return nil, err
			}
			draft := &adminDraft{server: preview, status: 200}
			if err = build(preview); err != nil {
				draft.close()
				return nil, err
			}
			return draft, nil
		}, frozen...)
	}
	if err != nil {
		return false, err
	}
	s.cow.mu.Lock()
	delete(s.cow.detaches, bucket)
	s.cow.mu.Unlock()
	return true, nil
}
func (c *clusterController) finishCowDetaches(ctx context.Context) {
	s := c.server
	view := s.currentSystem.Load()
	if view == nil || len(view.manifest.Membership.Members) == 0 || membershipIDs(view.manifest.Membership)[0] != s.control.ledger.node {
		return
	}
	for _, description := range s.bucketSnapshots() {
		if len(description.ClonePeers) == 0 || !description.CloneDetach && description.DesiredPolicy == nil && description.State != BucketDeleting {
			continue
		}
		id, err := bucketID(description.ID)
		if err != nil {
			return
		}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = s.cowDetachStep(step, id)
		cancel()
		if err != nil {
			return
		}
		// One page/edge decision per tick leaves control and placement work runnable.
		return
	}
}
func (s *Server) cowLocalDetachStep(id volume.ID) error {
	b, err := s.Bucket(id)
	if err != nil {
		return err
	}
	d := b.storageDescription()
	if len(d.ClonePeers) == 0 {
		return nil
	}
	if !d.CloneDetach && d.DesiredPolicy == nil && d.State != BucketDeleting {
		return nil
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	done, err := s.cowDetachStep(ctx, id)
	if err != nil {
		return err
	}
	if !done {
		return errors.New("clone materialization is still in progress")
	}
	return nil
}
