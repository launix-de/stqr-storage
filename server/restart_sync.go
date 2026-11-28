// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Called only after current bucket ACL and socket binding checks. This proves
// known published prefixes, never execution/nonexecution of an unknown request.
func (s *Server) syncRestartPrefix(ctx context.Context, bucket volume.ID, boot client.ID, sequence uint64) ([]byte, int, error) {
	s.durability.mu.Lock()
	var known *restartPrefix
	if s.restart != nil {
		known = s.restart.prefixes[restartPrefixKey{boot, bucket}]
	}
	if known == nil || known.Unavailable {
		s.durability.mu.Unlock()
		return nil, 410, fail(410, "old durability prefix is not retained")
	}
	if sequence > known.Sequence {
		s.durability.mu.Unlock()
		return nil, 412, fail(412, "unknown old durability sequence")
	}
	cut, confirmed := known.Sequence, known.Confirmed
	refs := append([]restartVolumeCut(nil), known.Volumes...)
	s.durability.mu.Unlock()
	if confirmed < cut {
		for _, ref := range refs {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
			// The pointer was verified against the manifest image before startup. A
			// later store with a coincidentally equal UUID/generation is not a proof.
			store := s.restart.stores[ref.Volume]
			if store == nil {
				return nil, 503, fail(503, "old prefix awaits retained volume")
			}
			if err := store.CheckpointThrough(ref.Generation); err != nil {
				return nil, 0, err
			}
		}
		s.durability.mu.Lock()
		known.Confirmed = cut
		known.Volumes = nil
		s.durability.mu.Unlock()
	}
	return (client.CommitMark{Boot: boot, Sequence: cut}).MarshalBinary(), 200, nil
}
