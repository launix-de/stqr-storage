// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"testing"
)

func TestOverlayRetirementPlanReadsExactLedger(t *testing.T) {
	ctx := context.Background()
	_, overlay, _, store, _, _ := overlayFixture(t, nil)
	upper, lower := BucketID{2}, BucketID{1}
	plan, err := ReadOverlayDependencyRetirementPlan(ctx, store, upper, lower)
	if err != nil || len(plan.Anchors) != 0 {
		t.Fatal(plan, err)
	}
	owner, target := ObjectID{3}, ObjectID{4}
	token, err := overlay.upper.reserveLowerAnchor(ctx, owner, target)
	if err != nil {
		t.Fatal(err)
	}
	guard := &overlayReadStore{MemoryFilesystemStore: store, reads: map[ObjectID]int{}}
	plan, err = ReadOverlayDependencyRetirementPlan(ctx, guard, upper, lower)
	if guard.writes != 0 {
		t.Fatal("plan export wrote objects")
	}
	for id := range guard.reads {
		if id != FilesystemRootID() && id != filesystemLowerAnchorsID {
			t.Fatal("plan export enumerated unrelated objects", id)
		}
	}
	if err != nil || len(plan.Anchors) != 1 || plan.Anchors[0] != (FilesystemDependencyAnchor{Owner: owner, Target: target, Token: token}) {
		t.Fatal(plan, err)
	}
	if _, err = ReadOverlayDependencyRetirementPlan(ctx, store, upper, BucketID{9}); !errors.Is(err, ErrFSFormat) {
		t.Fatal("wrong lower accepted", err)
	}
	// Plan ownership is detached from the live ledger, not a shared mutable view.
	plan.Anchors[0].Token = ID{99}
	again, err := ReadOverlayDependencyRetirementPlan(ctx, store, upper, lower)
	if err != nil || again.Anchors[0].Token != token {
		t.Fatal("export modified ledger", err)
	}
}
func TestOverlayRetirementPlanMissingUpperIsNotEmpty(t *testing.T) {
	_, err := ReadOverlayDependencyRetirementPlan(context.Background(), NewMemoryFilesystemStore(), BucketID{2}, BucketID{1})
	if !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
func TestOverlayRetirementPlanRejectsInvalidJournalAnchors(t *testing.T) {
	p := OverlayDependencyRetirementPlan{Version: 1, Upper: BucketID{2}, Lower: BucketID{1}, Anchors: []FilesystemDependencyAnchor{{Owner: ObjectID{3}, Target: ObjectID{4}, Token: ID{5}}}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	p.Anchors = append(p.Anchors, p.Anchors[0])
	if p.Validate() == nil {
		t.Fatal("duplicate ledger entry accepted")
	}
	p.Anchors = p.Anchors[:1]
	p.Anchors[0].Token = ID{}
	if p.Validate() == nil {
		t.Fatal("empty token accepted")
	}
}

type retirementProofStore struct {
	bucket                    BucketID
	safe                      bool
	failUnpin, failSync       bool
	proofCalls, unpins, syncs int
	tokens                    []ID
}

func (s *retirementProofStore) BucketID() BucketID { return s.bucket }
func (s *retirementProofStore) CanRetireDeletedBucketDependencies(ctx context.Context, subject BucketID) (bool, error) {
	if subject != (BucketID{2}) {
		panic("wrong upper")
	}
	if deferred, _ := ctx.Value(filesystemDeferredKey{}).(bool); deferred {
		panic("proof must not be deferred")
	}
	s.proofCalls++
	return s.safe, nil
}
func (s *retirementProofStore) UnpinObjectDependency(ctx context.Context, target ObjectID, token ID) error {
	if deferred, _ := ctx.Value(filesystemDeferredKey{}).(bool); !deferred {
		panic("retirement should batch accepted releases")
	}
	s.unpins++
	s.tokens = append(s.tokens, token)
	if s.failUnpin {
		s.failUnpin = false
		return errors.New("lost unpin reply")
	}
	return nil
}
func (s *retirementProofStore) Sync(context.Context) error {
	s.syncs++
	if s.failSync {
		s.failSync = false
		return errors.New("lost sync reply")
	}
	return nil
}
func TestOverlayRetirementRequiresPostDeletionProofAndRetriesSameTokens(t *testing.T) {
	ctx := context.Background()
	p := OverlayDependencyRetirementPlan{Version: 1, Upper: BucketID{2}, Lower: BucketID{1}, Anchors: []FilesystemDependencyAnchor{{Owner: ObjectID{3}, Target: ObjectID{4}, Token: ID{5}}}}
	s := &retirementProofStore{bucket: p.Lower}
	if err := p.Retire(ctx, s); !errors.Is(err, ErrWouldBlock) || s.unpins != 0 || s.syncs != 0 {
		t.Fatal("unproven release", err, s)
	}
	s.safe = true
	s.failUnpin = true
	if err := p.Retire(ctx, s); err == nil || s.syncs != 0 {
		t.Fatal("lost unpin reported complete", err)
	}
	s.failSync = true
	if err := p.Retire(ctx, s); err == nil {
		t.Fatal("lost sync reported complete")
	}
	if err := p.Retire(ctx, s); err != nil {
		t.Fatal(err)
	}
	if s.proofCalls != 4 || s.unpins != 3 || s.syncs != 2 {
		t.Fatal(s)
	}
	for _, token := range s.tokens {
		if token != p.Anchors[0].Token {
			t.Fatal("retry changed token")
		}
	}
	s.bucket = BucketID{9}
	if err := p.Retire(ctx, s); !errors.Is(err, ErrFSFormat) {
		t.Fatal("wrong lower accepted", err)
	}
}

func TestOverlayRetirementMalformedLedgerIsNotEmpty(t *testing.T) {
	ctx := context.Background()
	_, overlay, _, store, _, _ := overlayFixture(t, nil)
	if _, err := overlay.upper.reserveLowerAnchor(ctx, ObjectID{3}, ObjectID{4}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	object := store.objects[filesystemLowerAnchorsID]
	object.data = []byte("malformed")
	store.objects[filesystemLowerAnchorsID] = object
	store.mu.Unlock()
	if _, err := ReadOverlayDependencyRetirementPlan(ctx, store, BucketID{2}, BucketID{1}); err == nil {
		t.Fatal("corrupt ledger treated as an empty plan")
	}
}
