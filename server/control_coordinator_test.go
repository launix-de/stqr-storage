// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

var errControlLostReply = errors.New("injected lost reply after durable operation")

// These tests reopen real participant journals but deliberately model system
// publication. They exercise coordinator crash recovery, not network/storage
// availability of a deployed cluster.
type controlTestParticipants struct {
	fixtures  map[client.ID]*controlLedgerFixture
	mu        sync.Mutex
	failPhase string
	failNode  client.ID
}

func (p *controlTestParticipants) after(phase string, node client.ID, proof client.Proof, err error) (client.Proof, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil && p.failPhase == phase && p.failNode == node {
		p.failPhase = ""
		return client.Proof{}, errControlLostReply
	}
	return proof, err
}
func (p *controlTestParticipants) Reserve(_ context.Context, node client.ID, c controlClaim) error {
	err := p.fixtures[node].ledger.reserve(c)
	_, err = p.after("reserve", node, client.Proof{}, err)
	return err
}
func (p *controlTestParticipants) Prepare(_ context.Context, node client.ID, c controlProposal) (client.Proof, error) {
	proof, err := p.fixtures[node].ledger.prepare(c)
	return p.after("prepare", node, proof, err)
}
func (p *controlTestParticipants) Commit(_ context.Context, node client.ID, d client.Decision, cert client.Certificate) (client.Proof, error) {
	proof, err := p.fixtures[node].ledger.commit(d, cert)
	return p.after("commit", node, proof, err)
}
func (p *controlTestParticipants) Apply(_ context.Context, node client.ID, d client.Decision) (client.Proof, error) {
	proof, err := p.fixtures[node].ledger.markApplied(d)
	return p.after("apply", node, proof, err)
}
func (p *controlTestParticipants) Release(_ context.Context, node client.ID, d client.Decision, cert client.Certificate) (client.Proof, error) {
	l := p.fixtures[node].ledger
	if err := l.release(d, cert); err != nil {
		return client.Proof{}, err
	}
	proof, err := l.releasedProof(d)
	return p.after("release", node, proof, err)
}
func (p *controlTestParticipants) Abort(_ context.Context, node client.ID, c controlClaim) (client.Proof, error) {
	proof, err := p.fixtures[node].ledger.abortClaim(c)
	return p.after("abort", node, proof, err)
}

func controlCoordinatorFixture(t *testing.T) (*controlCoordinator, *controlTestParticipants, controlProposal) {
	t.Helper()
	base := controlTestBase()
	base.Membership.Members[0].ID = client.ID{2}
	a := newControlLedgerFixture(t, 2, base)
	b := newControlLedgerFixture(t, 1, controlBase{Membership: client.Membership{Cluster: client.ID{7}}})
	p := &controlTestParticipants{fixtures: map[client.ID]*controlLedgerFixture{{2}: a, {1}: b}}
	c := &controlCoordinator{ledger: a.ledger, participants: p}
	proposal := controlJoinProposal()
	proposal.Claim.Base = base
	proposal.Claim.Round.Initiator = client.ID{2}
	return c, p, proposal
}

func restartControlCoordinator(t *testing.T, p *controlTestParticipants) *controlCoordinator {
	t.Helper()
	for _, f := range p.fixtures {
		f.restart(t)
	}
	return &controlCoordinator{ledger: p.fixtures[client.ID{2}].ledger, participants: p}
}

func TestControlCoordinatorRecoversLostAcknowledgements(t *testing.T) {
	for _, phase := range []string{"prepare", "commit", "apply", "release"} {
		for _, node := range []byte{1, 2} {
			t.Run(phase+client.ID{node}.String(), func(t *testing.T) {
				ctx := context.Background()
				c, p, proposal := controlCoordinatorFixture(t)
				claim, err := c.begin(ctx, controlMembership, proposal.Claim.Required)
				if err != nil {
					t.Fatal(err)
				}
				proposal.Claim = claim
				if err = c.prepared(ctx, proposal, []byte("public-result")); err != nil {
					t.Fatal(err)
				}
				p.failPhase, p.failNode = phase, client.ID{node}
				if _, err = c.resume(ctx); !errors.Is(err, errControlLostReply) {
					t.Fatal("missing injected failure", err)
				}
				c = restartControlCoordinator(t, p)
				if phase == "release" && node == 2 {
					if c.ledger.slot != nil || c.ledger.request == nil {
						t.Fatal("did not reproduce local release/request completion window")
					}
					next := controlClaim{Round: client.RoundID{Membership: 2, Generation: 1, Initiator: client.ID{1}, Attempt: 20}, Base: cloneControlBase(c.ledger.current), Required: []client.ID{{1}, {2}}}
					if err := c.ledger.reserve(next); !errors.Is(err, errControlConflict) {
						t.Fatal("unfinished requester lost its barrier after restart", err)
					}
				}
				result, err := c.resume(ctx)
				if err != nil || string(result) != "public-result" {
					t.Fatal("request did not recover", string(result), err)
				}
				c = restartControlCoordinator(t, p)
				if c.ledger.request != nil {
					t.Fatal("completed requester resurrected")
				}
				for _, f := range p.fixtures {
					if f.ledger.slot != nil || f.ledger.current.Membership.Epoch != 2 {
						t.Fatal("participant not released/admitted")
					}
				}
			})
		}
	}
}

func TestControlCoordinatorCrashBeforeOwnReservation(t *testing.T) {
	ctx := context.Background()
	c, p, proposal := controlCoordinatorFixture(t)
	p.failPhase, p.failNode = "reserve", client.ID{1}
	claim, err := c.begin(ctx, controlMembership, proposal.Claim.Required)
	if !errors.Is(err, errControlLostReply) {
		t.Fatal(err)
	}
	if c.ledger.slot != nil || p.fixtures[client.ID{1}].ledger.slot == nil {
		t.Fatal("did not reproduce first remote grant window")
	}
	c = restartControlCoordinator(t, p)
	p.failPhase, p.failNode = "abort", client.ID{1}
	if _, err := c.resume(ctx); !errors.Is(err, errControlLostReply) {
		t.Fatal(err)
	}
	c = restartControlCoordinator(t, p)
	if _, err := c.resume(ctx); err != nil {
		t.Fatal(err)
	}
	for _, f := range p.fixtures {
		if f.ledger.slot != nil || !f.ledger.closed[claim.Round] {
			t.Fatal("abandoned reservation not fenced everywhere")
		}
		if err := f.ledger.reserve(claim); err == nil {
			t.Fatal("delayed RESERVE resurrected abandoned request")
		}
	}
	if c.ledger.request != nil {
		t.Fatal("requester did not forget cancelled round")
	}
	next, err := c.begin(ctx, controlMembership, proposal.Claim.Required)
	if err != nil || next.Round.Attempt != claim.Round.Attempt+1 {
		t.Fatal("attempt counter reused", err)
	}
}

func TestControlCoordinatorCrashBeforePublicResultAborts(t *testing.T) {
	ctx := context.Background()
	c, p, proposal := controlCoordinatorFixture(t)
	claim, err := c.begin(ctx, controlMembership, proposal.Claim.Required)
	if err != nil {
		t.Fatal(err)
	}
	proposal.Claim = claim
	if _, err := c.ledger.prepare(proposal); err != nil {
		t.Fatal(err)
	}
	c = restartControlCoordinator(t, p)
	if _, err := c.resume(ctx); err != nil {
		t.Fatal(err)
	}
	for _, f := range p.fixtures {
		if f.ledger.current.Sequence != 0 || f.ledger.slot != nil {
			t.Fatal("unpublished result committed")
		}
	}
}

func TestControlCompletedProofsSurviveLaterRounds(t *testing.T) {
	ctx := context.Background()
	c, p, original := controlCoordinatorFixture(t)
	claim, err := c.begin(ctx, controlMembership, original.Claim.Required)
	if err != nil {
		t.Fatal(err)
	}
	original.Claim = claim
	if err := c.prepared(ctx, original, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.resume(ctx); err != nil {
		t.Fatal(err)
	}
	decision, _ := original.decision()
	prior := c.ledger.completed[decision]
	// The formerly pending member initiates a subsequent global change.
	next := &controlCoordinator{ledger: p.fixtures[client.ID{1}].ledger, participants: p}
	claim, err = next.begin(ctx, controlAdministration, original.Claim.Required)
	if err != nil {
		t.Fatal(err)
	}
	proposal := controlProposal{Claim: claim, Kind: controlAdministration, Image: [32]byte{10}}
	if err := next.prepared(ctx, proposal, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := next.resume(ctx); err != nil {
		t.Fatal(err)
	}
	restartControlCoordinator(t, p)
	for _, f := range p.fixtures {
		if _, err := f.ledger.prepare(original); err != nil {
			t.Fatal("late READY lost", err)
		}
		if _, err := f.ledger.commit(decision, prior.Slot.Certificate); err != nil {
			t.Fatal("late COMMIT lost", err)
		}
		if _, err := f.ledger.markApplied(decision); err != nil {
			t.Fatal("late APPLIED lost", err)
		}
		if err := f.ledger.release(decision, prior.Release.Certificate); err != nil {
			t.Fatal("late RELEASE lost", err)
		}
		if proof, err := f.ledger.releasedProof(decision); err != nil || proof != f.ledger.completed[decision].Proof {
			t.Fatal("release proof changed", err)
		}
		if f.ledger.current.Sequence != 2 {
			t.Fatal("late retry rolled back current head")
		}
	}
}

func TestControlAbortBindsClaimAcrossRestart(t *testing.T) {
	f := newControlLedgerFixture(t, 1, controlTestBase())
	proposal := controlJoinProposal()
	if _, err := f.ledger.abortClaim(proposal.Claim); err != nil {
		t.Fatal(err)
	}
	changed := proposal.Claim
	changed.Required = []client.ID{{1}, {2}, {3}}
	if _, err := f.ledger.abortClaim(changed); err == nil {
		t.Fatal("abort proof reused for a different claim")
	}
	f.restart(t)
	if _, err := f.ledger.abortClaim(changed); err == nil {
		t.Fatal("replayed abort proof lost claim binding")
	}
}

func TestControlRecoveryDoesNotCancelLiveBuilder(t *testing.T) {
	ctx := context.Background()
	c, _, proposal := controlCoordinatorFixture(t)
	claim, err := c.begin(ctx, controlMembership, proposal.Claim.Required)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.resume(ctx); !errors.Is(err, errControlConflict) {
		t.Fatal("recovery cancelled a live builder", err)
	}
	if c.ledger.request == nil || c.ledger.closed[claim.Round] {
		t.Fatal("live request was abandoned")
	}
	if err := c.cancelBuilding(ctx); err != nil {
		t.Fatal(err)
	}
	if c.ledger.request != nil || !c.ledger.closed[claim.Round] {
		t.Fatal("explicit build failure not cancelled")
	}
}
