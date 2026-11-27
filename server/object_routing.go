// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type objectRedirect struct{ patch client.MapPatch }

func (e *objectRedirect) Error() string { return "object belongs to another committed owner" }

func (s *Server) routingPatch(bucket volume.ID, id volume.ObjectID) (client.MapPatch, bool) {
	view := s.currentSystem.Load()
	if view == nil {
		return client.MapPatch{}, false
	}
	entry, ok := s.placement.Load().lookup(client.ID(bucket), client.ObjectID(id))
	if !ok {
		return client.MapPatch{}, false
	}
	end, toEnd, _ := client.RangeEnd(entry.Start, entry.PrefixBits)
	patch := client.MapPatch{Membership: view.manifest.Membership.Epoch, Generation: view.manifest.Generation, Revision: view.manifest.Revision, Bucket: client.ID(bucket), Begin: entry.Start, End: end, ToEnd: toEnd, Entries: []client.RangeEntry{entry}}
	for _, member := range view.manifest.Membership.Members {
		if len(member.Addresses) > 0 {
			patch.Addresses = append(patch.Addresses, client.NodeAddress{Node: member.ID, Addresses: member.Addresses})
		}
	}
	return patch, true
}

func (s *Server) checkObjectOwner(bucket volume.ID, id volume.ObjectID, replicaRead ...bool) error {
	if s.controller == nil {
		return nil
	}
	if s.storageWarming.Load() && s.scopes != nil {
		// The system head may precede a durable source fence. A warmup read
		// cannot treat the old head's location as independent authority.
		release, err := s.scopes.guard(bucket, id)
		if err != nil {
			return err
		}
		release()
	}
	if s.scopes != nil && s.scopes.incoming(bucket, id) {
		return fail(503, "target handoff is not yet committed")
	}
	patch, ok := s.routingPatch(bucket, id)
	if !ok {
		return fail(503, "committed placement is not available")
	}
	node := s.control.ledger.node
	for _, loc := range patch.Entries[0].Locations {
		if (loc.Role == 0 || len(replicaRead) > 0 && replicaRead[0] && loc.Role == 1) && loc.Node == node {
			if s.authorityPending(bucket, id) {
				return fail(503, "range authority checkpoint is pending")
			}
			if patch.Entries[0].State == 4 {
				return fail(503, "range is waiting for recovered volumes")
			}
			if loc.Volume != (client.ID{}) {
				s.mu.RLock()
				available := s.volumes[loc.Volume.String()] != nil
				s.mu.RUnlock()
				if !available {
					return fail(503, "authoritative volume is unavailable")
				}
			}
			if loc.Volume == (client.ID{}) {
				view := s.currentSystem.Load()
				for _, origin := range view.manifest.VolumeRecoveries {
					if origin.Complete && origin.Before.Bucket == client.ID(bucket) && sameRangeAuthority(origin.After, patch.Entries[0]) {
						for _, id := range origin.Volumes {
							s.mu.RLock()
							available := s.volumes[id.String()] != nil
							s.mu.RUnlock()
							if !available {
								return fail(503, "recovered volume cohort is incomplete")
							}
						}
					}
				}
			}
			if patch.Entries[0].Runtime != (client.ID{}) && !s.scopes.ramAvailable(bucket, id, patch.Entries[0]) {
				return fail(503, "RAM runtime recovery is not yet committed")
			}
			return nil
		}
	}
	return &objectRedirect{patch: patch}
}

func redirectResponse(patch client.MapPatch) []byte {
	wire, err := patch.MarshalBinary()
	if err != nil {
		return client.ResponseBody(503, nil, "routing patch is not available")
	}
	h := client.Encoder{}
	h.V(1)
	h.V(2)
	h.Blob(wire)
	e := client.Encoder{}
	e.V(301)
	e.Blob(h.B)
	return e.B
}

func (s *Server) getObjectMap(m client.Message, p principal) ([]byte, error) {
	d := client.Decoder{B: m.Body}
	bucket := volume.ID(d.ID())
	var object volume.ObjectID
	copy(object[:], d.Take(32))
	if d.Done() != nil {
		return nil, fail(400, "invalid map query")
	}
	if bucket == systemBucket {
		return nil, ErrBucketForbidden
	}
	if p.connection != nil && p.connection.bucketID != bucket && !p.connection.admin {
		return nil, ErrBucketForbidden
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	if p.connection == nil {
		user, err := s.findAccount(p.User.Name)
		if err != nil || user.ID != p.User.ID {
			return nil, ErrBucketForbidden
		}
		id, _ := bucketID(user.ID)
		info, err := b.Info(id, user.Role == "ADMIN")
		if err != nil || info.Permissions == 0 {
			return nil, ErrBucketForbidden
		}
	}
	patch, ok := s.routingPatch(bucket, object)
	if !ok {
		return nil, fail(503, "placement not yet available")
	}
	if entry := patch.Entries[0]; entry.Runtime != (client.ID{}) {
		for _, loc := range entry.Locations {
			if loc.Role == 0 && loc.Node == s.control.ledger.node && !s.scopes.ramAvailable(bucket, object, entry) {
				return nil, fail(503, "RAM runtime recovery is not yet committed")
			}
		}
	}
	return patch.MarshalBinary()
}
