// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Caller holds the reference bucket gate and has authenticated a current admin.
// A committed account deletion revokes future reference mutations. The ordinary
// root-key/node admission orders already admitted single-copy publications and
// validates the exact immutable account index again before publishing this CAS.
// Prepared distributed operations are excluded by referencePruneSingleCopy and
// validateReferenceMutation. Therefore no time-based expiry, socket-disconnect
// inference, or global request drain is used as evidence of abandonment.
func (s *Server) pruneAbsentAccountPins(ctx context.Context, b *Bucket, p principal, accepted bool) ([]byte, error) {
	if !s.referenceAccountsStable() || !s.referencePruneSingleCopy(b) {
		return nil, fail(503, "reference repair requires stable single-copy authority")
	}
	root := volume.ObjectID(client.ObjectReferenceRootID())
	for retry := 0; retry < 128; retry++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, _, err := b.Get(withMembershipRead(ctx), p, root)
		if errors.Is(err, volume.ErrNotFound) {
			return []byte{1}, nil
		}
		if err != nil {
			return nil, err
		}
		entries, err := decodeObjectReferences(raw, b.ID())
		if err != nil {
			return nil, err
		}
		kept, guarded, err := s.pruneAbsentPlainReferencePinsExact(ctx, b, entries)
		if err != nil {
			return nil, err
		}
		if len(kept) == len(entries) {
			return []byte{1}, nil
		}
		next := encodeObjectReferences(kept, b.ID())
		_, err = b.CAS(guarded, p, root, 0, raw, next, volume.Options{RequestID: volume.ID(client.NewRequestID()), WholeObject: true, Accepted: accepted})
		if errors.Is(err, volume.ErrPrecondition) && !errors.Is(err, volume.ErrRecovery) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return []byte{1}, nil
	}
	return nil, fail(409, "reference root changed during repair; retry")
}
