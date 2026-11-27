// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/volume"
)

// A nonzero-sized process-local identity, never a persisted object version or
// tombstone. Only continuously present, unchanged native-auth identities share
// a stamp across immutable account publications. Deleted IDs retain no slot.
type referenceAccountStamp struct{ identity uint64 }

func sameReferenceAuth(a, b account) bool {
	return a.ID == b.ID && a.Name == b.Name && a.Role == b.Role && a.Disabled == b.Disabled && constantEqual(a.PasswordHash, b.PasswordHash)
}

// Caller owns the unpublished next index and normal administrative publication
// admission. Bootstrap/private drafts have no serving requests. Published maps,
// rows and stamp identities are immutable; old principals keep their old stamp.
func (s *Server) publishAccountIndex(next *accountsIndex) {
	prior := s.accounts.Load()
	if prior != nil && next != prior {
		for i, u := range next.image.Users {
			if j, ok := prior.byID[u.ID]; ok && j < len(prior.referenceAuth) && prior.referenceAuth[j] != nil && sameReferenceAuth(u, prior.image.Users[j]) {
				next.referenceAuth[i] = prior.referenceAuth[j]
			}
		}
	}
	s.accounts.Store(next)
}

func (s *Server) findAccountWithReferenceStamp(name string) (account, *referenceAccountStamp, error) {
	x, err := s.accountIndex()
	if err != nil {
		return account{}, nil, err
	}
	i, ok := x.byName[name]
	if !ok || x.image.Users[i].Disabled || i >= len(x.referenceAuth) || x.referenceAuth[i] == nil {
		return account{}, nil, errors.New("unknown account")
	}
	return cloneAccount(x.image.Users[i]), x.referenceAuth[i], nil
}

// Bind from the SAME immutable row as the authenticated identity. Never replace
// an original stamp with a newer one: delete/recreate is a new identity even if
// every account field is identical. Registered connection revocation also wins.
func (s *Server) referenceAccountSnapshot(p principal) (account, *referenceAccountStamp, error) {
	if p.connection != nil && p.connection.invalid.Load() {
		return account{}, nil, ErrBucketForbidden
	}
	x, err := s.accountIndex()
	if err != nil {
		return account{}, nil, ErrBucketForbidden
	}
	i, ok := x.byID[p.User.ID]
	if !ok || i >= len(x.referenceAuth) || x.referenceAuth[i] == nil {
		return account{}, nil, ErrBucketForbidden
	}
	u := x.image.Users[i]
	if u.Disabled || (u.Role != "ADMIN" && u.Role != "USER") || !sameReferenceAuth(u, p.User) || p.referenceAuth != nil && p.referenceAuth != x.referenceAuth[i] {
		return account{}, nil, ErrBucketForbidden
	}
	return cloneAccount(u), x.referenceAuth[i], nil
}

type referencePruneKey struct{}
type referencePruneGuard struct {
	accounts *accountsIndex
	absent   map[string]struct{}
}

// No control/authority uncertainty is evidence that an account is absent. This
// is a RAM-only check; callers never acquire journal/Volume locks or run scans.
func (s *Server) referenceAccountsStable() bool {
	if s.metadataPreview {
		return false
	}
	s.mu.RLock()
	closed, pending := s.closed, s.cfg.JoinPending
	s.mu.RUnlock()
	if closed || pending {
		return false
	}
	if c := s.control; c != nil {
		a := c.ledger.activity.Load()
		if a == nil || a.Busy {
			return false
		}
	}
	return true
}

// Lazy bounded cleanup only when a root is full. Account IDs in byID are
// present even when disabled. Claims and dependency anchors are never removed.
// The returned context derives from ctx, preserving the collector's original
// connectionWork/fence linkage. CAS must validate this guard under admission.
func (s *Server) pruneAbsentPlainReferencePins(ctx context.Context, entries []objectReferenceEntry) ([]objectReferenceEntry, context.Context, error) {
	if len(entries) < maxObjectReferences || !s.referenceAccountsStable() {
		return entries, ctx, nil
	}
	x, err := s.accountIndex()
	if err != nil {
		return entries, ctx, nil
	}
	prior, _ := ctx.Value(referencePruneKey{}).(*referencePruneGuard)
	if prior != nil && prior.accounts != x {
		return nil, ctx, volume.ErrPrecondition
	}
	absent := map[string]struct{}{}
	if prior != nil {
		for id := range prior.absent {
			absent[id] = struct{}{}
		}
	}
	var kept []objectReferenceEntry
	for i, r := range entries {
		_, present := x.byID[r.account.String()]
		if !r.claim && !r.dependency && !present {
			if kept == nil {
				kept = make([]objectReferenceEntry, 0, len(entries))
				kept = append(kept, entries[:i]...)
			}
			absent[r.account.String()] = struct{}{}
		} else if kept != nil {
			kept = append(kept, r)
		}
	}
	if kept == nil {
		return entries, ctx, nil
	}
	guard := &referencePruneGuard{accounts: x, absent: absent}
	return kept, context.WithValue(ctx, referencePruneKey{}, guard), nil
}

// Caller holds the SAME shared/exclusive node admission as the real mutation,
// before any Volume access. UserDelete/system installation use its exclusive
// side; no queued old actor may publish after that deletion has completed.
func (s *Server) validateReferenceMutation(ctx context.Context, p principal, id volume.ObjectID) error {
	if p.internalCoordination && bucketLocalReferenceRoot(id) && !p.cowInternal {
		if p.referenceAuth == nil {
			return ErrBucketForbidden
		}
		if _, _, err := s.referenceAccountSnapshot(p); err != nil {
			return err
		}
	}
	guard, _ := ctx.Value(referencePruneKey{}).(*referencePruneGuard)
	if guard == nil {
		return nil
	}
	if !p.internalCoordination || p.cowInternal || !bucketLocalReferenceRoot(id) || !s.referenceAccountsStable() {
		return volume.ErrPrecondition
	}
	current, err := s.accountIndex()
	if err != nil || current != guard.accounts {
		return volume.ErrPrecondition
	}
	for absent := range guard.absent {
		if _, present := current.byID[absent]; present {
			return volume.ErrPrecondition
		}
	}
	return nil
}
