// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Read permits are explicit at audited external entrypoints only. Internal
// administrative/COW/lease-held reads must retain the full-probe path and must
// not recursively acquire routing guards.
type membershipReadPermit struct{}

func withMembershipRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, membershipReadPermit{}, true)
}
func withoutMembershipRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, membershipReadPermit{}, false)
}
func membershipReadEnabled(ctx context.Context) bool {
	on, _ := ctx.Value(membershipReadPermit{}).(bool)
	return on
}

// A proof is request-local, never an object-owner directory. The caller holds
// object-key -> request-ID (when nonzero) -> node admission until selection and
// acquisition of the selected Volume handle. Imports/map/attach changes need
// exclusive admission. Unrelated Bloom additions may create false positives,
// but cannot invalidate a guarded negative decision for these relevant keys.
// Never pass a proof to an unguarded caller or retain it after releasing guards.
type cohortMembershipProof struct {
	description BucketDescription
	class       volume.Class
	volumes     []*volume.Volume
	absent      map[*volume.Volume]bool
	possible    []*volume.Volume
}

func (b *Bucket) probeCohort(d BucketDescription, key volume.Key) (BucketDescription, volume.Class, []*volume.Volume) {
	s := b.server
	if s.controller != nil && d.DesiredPolicy != nil {
		if entry, ok := s.placement.Load().lookup(client.ID(key.Bucket), client.ObjectID(key.Object)); ok && entry.PolicyVersion == d.DesiredPolicy.Version {
			d.Policy = *d.DesiredPolicy
		}
	}
	class := bucketClass(d.Policy)
	if s.controller != nil {
		if entry, ok := s.placement.Load().lookup(client.ID(key.Bucket), client.ObjectID(key.Object)); ok {
			class = s.rangeClass(d.Policy, entry)
		}
	}
	var out []*volume.Volume
	for _, v := range b.volumesFor(d) {
		if !class.Allows(v.Identity().Class) {
			continue
		}
		if s.controller != nil {
			if patch, ok := s.routingPatch(key.Bucket, key.Object); ok && !s.volumeAllowedForEntry(client.ID(b.ID()), patch.Entries[0], v.Identity().VolumeID, s.currentSystem.Load()) {
				continue
			}
		}
		out = append(out, v)
	}
	return d, class, out
}
func (b *Bucket) membershipProof(d BucketDescription, key volume.Key, request volume.ID) *cohortMembershipProof {
	d, class, cohort := b.probeCohort(d, key)
	proof := &cohortMembershipProof{description: d, class: class, volumes: cohort, absent: make(map[*volume.Volume]bool)}
	for _, v := range cohort {
		maybe := v.MayContainObject(key) || v.MayContainPending(key)
		if request != (volume.ID{}) {
			maybe = maybe || v.MayContainRequest(request) || v.MayContainReplica(request)
		}
		if maybe {
			proof.possible = append(proof.possible, v)
		} else {
			proof.absent[v] = true
		}
	}
	return proof
}

// requestIDCoordination is a separate node-local namespace containing only
// live holders/waiters. Receipt identity remains stored and checked by Volumes;
// this gate merely prevents a guarded negative receipt proof racing another
// normal data request with the same ID on a different object/volume.
type requestIDCoordination struct {
	mu    sync.Mutex
	gates map[volume.ID]*requestIDGate
}
type requestIDGate struct {
	token chan struct{}
	refs  int
}

func (r *requestIDCoordination) acquire(ctx context.Context, id volume.ID) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.gates == nil {
		r.gates = make(map[volume.ID]*requestIDGate)
	}
	gate := r.gates[id]
	if gate == nil {
		gate = &requestIDGate{token: make(chan struct{}, 1)}
		r.gates[id] = gate
	}
	gate.refs++
	r.mu.Unlock()
	drop := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		gate.refs--
		if gate.refs == 0 {
			delete(r.gates, id)
			if len(r.gates) == 0 {
				r.gates = nil
			}
		}
	}
	select {
	case gate.token <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-gate.token
			drop()
			return nil, err
		}
		return func() { <-gate.token; drop() }, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}
func (s *Server) lockRequestID(ctx context.Context, id volume.ID) (func(), error) {
	if id == (volume.ID{}) {
		return func() {}, nil
	}
	return s.requestCoordination.acquire(ctx, id)
}

// A read takes routing guards only through selection + acquiring the physical
// read handle. The returned handle pins bytes; no key/admission lock survives
// into payload copy/network output. Missing-local/COW recursion happens only
// after this function unwinds every guard.
func (b *Bucket) openSelectedRead(ctx context.Context, p principal, id volume.ObjectID) (*volume.Volume, *volume.Handle, BucketDescription, volume.Info, error) {
	s := b.server
	if s == nil {
		return nil, nil, BucketDescription{}, volume.Info{}, fail(503, "bucket not attached")
	}
	key := volume.Key{Bucket: b.ID(), Object: id}
	if err := ctx.Err(); err != nil {
		return nil, nil, BucketDescription{}, volume.Info{}, err
	}
	guarded := membershipReadEnabled(ctx)
	if guarded {
		if s.replicas != nil {
			release := s.replicas.lock(key)
			defer release()
		}
		release, err := s.lockRequestID(ctx, volume.ID{})
		if err != nil {
			return nil, nil, BucketDescription{}, volume.Info{}, err
		}
		defer release()
		s.opMu.RLock()
		defer s.opMu.RUnlock()
	}
	d, err := b.authorize(p, BucketRead)
	if err != nil {
		return nil, nil, d, volume.Info{}, err
	}
	if err = s.checkObjectOwner(b.ID(), id, true); err != nil {
		return nil, nil, d, volume.Info{}, err
	}
	var proof *cohortMembershipProof
	if guarded {
		proof = b.membershipProof(d, key, volume.ID{})
	}
	v, prior, _, err := b.selectVolumeProof(d, key, volume.ID{}, false, proof)
	if err != nil {
		if bindingErr := s.checkObjectBinding(ctx, b.ID(), id, true); bindingErr != nil {
			err = bindingErr
		}
		return nil, nil, d, prior, err
	}
	h, err := v.Open(false)
	if err != nil {
		return nil, nil, d, prior, err
	}
	if err = s.checkObjectBinding(ctx, b.ID(), id, true); err != nil {
		h.Close()
		return nil, nil, d, prior, err
	}
	return v, h, d, prior, nil
}
