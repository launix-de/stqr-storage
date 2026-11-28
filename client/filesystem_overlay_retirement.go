// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
)

// FilesystemDependencyAnchor is an exact application-owned lower lifetime token.
// Possessing a token alone does not prove that it is safe to release it: raw COW
// descendants may have inherited it. Never retire it merely because a user or
// upper bucket can no longer be found.
type FilesystemDependencyAnchor struct {
	Owner  ObjectID `json:"owner"`
	Target ObjectID `json:"target"`
	Token  ID       `json:"token"`
}

// OverlayDependencyRetirementPlan survives deletion of its upper's STLA ledger.
// Capture it under exclusive upper maintenance and durably journal it BEFORE
// deleting the upper. Release requires a NEW authoritative post-deletion clone
// lineage proof; an earlier maintenance proof can race a subsequent clone.
type OverlayDependencyRetirementPlan struct {
	Version uint16                       `json:"version"`
	Upper   BucketID                     `json:"upper"`
	Lower   BucketID                     `json:"lower"`
	Anchors []FilesystemDependencyAnchor `json:"anchors"`
}

func (p OverlayDependencyRetirementPlan) Validate() error {
	if p.Version != 1 || p.Upper == (BucketID{}) || p.Lower == (BucketID{}) || p.Upper == p.Lower {
		return ErrFSFormat
	}
	var previous fsLowerAnchor
	for i, a := range p.Anchors {
		current := fsLowerAnchor{owner: a.Owner, target: a.Target, token: a.Token}
		if current.owner == (ObjectID{}) || current.target == (ObjectID{}) || current.token == (ID{}) || i > 0 && compareFSLowerAnchor(previous, current) >= 0 {
			return ErrFSFormat
		}
		previous = current
	}
	return nil
}

// ReadOverlayDependencyRetirementPlan reads the known overlay root and exact
// lower-anchor ledger (including its encoded extents), never an object inventory.
// A missing upper or malformed/unavailable ledger is not an empty plan. Only an
// absent ledger of a validated, existing overlay means no anchors were recorded.
// The caller must hold exclusive upper maintenance throughout capture/journaling.
func ReadOverlayDependencyRetirementPlan(ctx context.Context, store FilesystemStore, upper, lower BucketID) (OverlayDependencyRetirementPlan, error) {
	ctx = context.WithValue(ctx, fsFreshReadKey{}, true)
	plan := OverlayDependencyRetirementPlan{Version: 1, Upper: upper, Lower: lower}
	if store == nil || plan.Validate() != nil {
		return plan, ErrFSFormat
	}
	if actual, known := filesystemStoreBucket(store); known && actual != upper {
		return plan, ErrFSFormat
	}
	raw, _, err := store.Get(ctx, FilesystemRootID())
	if err != nil {
		return plan, err
	}
	actual, _, err := decodeOverlayRoot(raw)
	if err != nil || actual != lower {
		return plan, ErrFSFormat
	}
	f := &Filesystem{store: filesystemDeferredStore(store), directoryCache: newFilesystemDirectoryCache(), directoryMemoryLimit: 64 << 20, readOnly: true}
	_, entries, err := f.lowerAnchorsSnapshot(ctx, false)
	if errors.Is(err, ErrNotFound) {
		return plan, nil
	}
	if err != nil {
		return plan, err
	}
	for _, e := range entries {
		plan.Anchors = append(plan.Anchors, FilesystemDependencyAnchor{Owner: e.owner, Target: e.target, Token: e.token})
	}
	return plan, plan.Validate()
}

// DeletedBucketDependencyStore supplies generic bucket-lifecycle proof and
// ordinary token operations; the server has no knowledge of the STLA format.
type DeletedBucketDependencyStore interface {
	BucketID() BucketID
	CanRetireDeletedBucketDependencies(context.Context, BucketID) (bool, error)
	UnpinObjectDependency(context.Context, ObjectID, ID) error
	Sync(context.Context) error
}

// Retire releases a previously journaled plan only after fresh authoritative
// proof that its upper was deleted and no surviving COW lineage can use it.
// The caller must retain the durable plan until this method succeeds. Retrying
// after any error repeats the same token-idempotent releases; no absence or
// timeout is interpreted as proof. Only this explicit administrative operation
// requests a final persistence fence; ordinary filesystem writes are unaffected.
func (p OverlayDependencyRetirementPlan) Retire(ctx context.Context, lower DeletedBucketDependencyStore) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if lower == nil || lower.BucketID() != p.Lower {
		return ErrFSFormat
	}
	if len(p.Anchors) == 0 {
		return nil
	}
	safe, err := lower.CanRetireDeletedBucketDependencies(ctx, p.Upper)
	if err != nil {
		return err
	}
	if !safe {
		return ErrWouldBlock
	}
	for _, anchor := range p.Anchors {
		if err = lower.UnpinObjectDependency(filesystemDeferredContext(ctx), anchor.Target, anchor.Token); err != nil {
			return err
		}
	}
	return lower.Sync(ctx)
}
