// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/binary"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type objectBindingKey struct{}
type objectBinding struct {
	bucket  volume.ID
	object  volume.ObjectID
	epoch   uint64
	runtime client.ID
}

func bindObjectRequest(ctx context.Context, m client.Message) context.Context {
	if len(m.Body) < 80 {
		return ctx
	}
	var binding objectBinding
	copy(binding.bucket[:], m.Body[:16])
	copy(binding.object[:], m.Body[16:48])
	binding.epoch = binary.LittleEndian.Uint64(m.Body[56:64])
	copy(binding.runtime[:], m.Body[64:80])
	return context.WithValue(ctx, objectBindingKey{}, binding)
}

// Mutations call this under opMu; reads call it after acquiring the snapshot
// handle. An upload can outlive a handoff, so its initial parse is insufficient
// to authorize a final write or to label a new RAM era with an old runtime ID.
func (s *Server) checkObjectBinding(ctx context.Context, bucket volume.ID, object volume.ObjectID) error {
	binding, bound := ctx.Value(objectBindingKey{}).(objectBinding)
	if s.controller == nil {
		return nil
	}
	if bound && (binding.bucket != bucket || binding.object != object) {
		return fail(400, "object binding mismatch")
	}
	entry, known := s.placement.Load().lookup(client.ID(bucket), client.ObjectID(object))
	if !known {
		return fail(503, "object placement is not available")
	}
	owned := false
	for _, loc := range entry.Locations {
		if loc.Role == 0 && loc.Node == s.control.ledger.node {
			owned = true
			break
		}
	}
	if !owned {
		patch, known := s.routingPatch(bucket, object)
		if !known {
			return fail(503, "object placement is not available")
		}
		return &objectRedirect{patch: patch}
	}
	if binding.runtime != (client.ID{}) && binding.runtime != entry.Runtime {
		return fail(410, "RAM runtime reset")
	}
	if binding.epoch != 0 && binding.epoch != entry.OwnerEpoch {
		patch, known := s.routingPatch(bucket, object)
		if !known {
			return fail(503, "object placement is not available")
		}
		return &objectRedirect{patch: patch}
	}
	return nil
}
