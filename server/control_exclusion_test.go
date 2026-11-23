// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func exclusionTestProposal(t *testing.T) controlProposal {
	t.Helper()
	base := controlTestBase()
	base.Membership.Members = append(base.Membership.Members, client.Member{ID: client.ID{2}}, client.Member{ID: client.ID{3}})
	x := &controlExclusion{Operation: client.ID{8}, Nodes: []client.ID{{3}}, Administrator: client.ID{9}, Username: "admin", AcceptDataLoss: true, ExternalFencingConfirmed: true}
	next, err := exclusionMembership(base.Membership, x)
	if err != nil {
		t.Fatal(err)
	}
	return controlProposal{Kind: controlForceDisconnect, Image: [32]byte{5}, NextMembership: &next, Claim: controlClaim{Base: base, Required: []client.ID{{1}, {2}}, Round: client.RoundID{Membership: 1, Generation: 1, Initiator: client.ID{1}, Attempt: 1}, Exclusion: x}}
}

func TestExclusionRequiresEverySurvivorAndPersistsExactDecision(t *testing.T) {
	p := exclusionTestProposal(t)
	a := newControlLedgerFixture(t, 1, p.Claim.Base)
	b := newControlLedgerFixture(t, 2, p.Claim.Base)
	removed := newControlLedgerFixture(t, 3, p.Claim.Base)
	if err := removed.ledger.reserve(p.Claim); err == nil {
		t.Fatal("excluded node became a survivor witness")
	}
	var ready client.Certificate
	for _, f := range []*controlLedgerFixture{a, b} {
		if err := f.ledger.reserve(p.Claim); err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		proof, err := f.ledger.prepare(p)
		if err != nil {
			t.Fatal(err)
		}
		ready = append(ready, proof)
	}
	decision, err := p.decision()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ledger.commit(decision, ready[:1]); err == nil {
		t.Fatal("missing survivor accepted as ALL")
	}
	var applied client.Certificate
	for _, f := range []*controlLedgerFixture{a, b} {
		if _, err := f.ledger.commit(decision, ready); err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		if err := f.ledger.abort(p.Claim.Round); err == nil {
			t.Fatal("committed exclusion rolled back")
		}
		proof, err := f.ledger.markApplied(decision)
		if err != nil {
			t.Fatal(err)
		}
		applied = append(applied, proof)
	}
	for _, f := range []*controlLedgerFixture{a, b} {
		if err := f.ledger.release(decision, applied); err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		if !equalControlMembership(f.ledger.current.Membership, *p.NextMembership) {
			t.Fatal("exclusion lost on reopen")
		}
		if err := f.ledger.reserve(p.Claim); err == nil {
			t.Fatal("old exclusion resurrected")
		}
	}
	// This is a participant-journal test, not a claim that the server can
	// recover an excluded primary's ranges or replace a coordinator mid-round.
}

func TestExclusionWireBindsFlagsActorAndCanonicalSurvivors(t *testing.T) {
	p := exclusionTestProposal(t)
	wire, err := marshalControlProposal(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeControlProposal(wire)
	if err != nil || !equalControlProposal(got, p) {
		t.Fatal("round trip", err)
	}
	before, _ := p.decision()
	for n := range wire {
		if _, err := decodeControlProposal(wire[:n]); err == nil {
			t.Fatal("truncated exclusion", n)
		}
	}
	if _, err := decodeControlProposal(append(wire, 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
	for _, change := range []func(*controlProposal){
		func(p *controlProposal) { p.Claim.Exclusion.AcceptDataLoss = false },
		func(p *controlProposal) { p.Claim.Exclusion.ExternalFencingConfirmed = false },
		func(p *controlProposal) { p.Claim.Exclusion.Administrator = client.ID{8} },
		func(p *controlProposal) { p.Claim.Exclusion.Username = "another-admin" },
	} {
		candidate := exclusionTestProposal(t)
		change(&candidate)
		after, err := candidate.decision()
		if err != nil || after == before {
			t.Fatal("exclusion statement not bound to decision", err)
		}
	}
	for _, change := range []func(*controlProposal){
		func(p *controlProposal) { p.Claim.Exclusion.AcceptDataLoss = false },
		func(p *controlProposal) { p.Claim.Exclusion.ExternalFencingConfirmed = false },
		func(p *controlProposal) { p.Claim.Exclusion.Nodes = []client.ID{{3}, {3}} },
		func(p *controlProposal) { p.Claim.Exclusion.Nodes = []client.ID{{4}} },
		func(p *controlProposal) { p.Claim.Required = []client.ID{{1}} },
		func(p *controlProposal) { p.Claim.Round.Initiator = client.ID{2} },
		func(p *controlProposal) { p.Kind = controlMembership },
	} {
		candidate := exclusionTestProposal(t)
		change(&candidate)
		fixture := newControlLedgerFixture(t, 1, candidate.Claim.Base)
		if fixture.ledger.checkProposal(candidate) {
			t.Fatal("invalid exclusion admitted")
		}
	}
}

func TestExclusionReservationConflictReleasesOnlyItsOwnPromises(t *testing.T) {
	p := exclusionTestProposal(t)
	a := newControlLedgerFixture(t, 1, p.Claim.Base)
	b := newControlLedgerFixture(t, 2, p.Claim.Base)
	old := controlClaim{Base: p.Claim.Base, Required: []client.ID{{1}, {2}, {3}}, Round: client.RoundID{Membership: 1, Generation: 1, Initiator: client.ID{3}, Attempt: 7}}
	if err := b.ledger.reserve(old); err != nil {
		t.Fatal(err)
	}
	oldProposal := controlProposal{Claim: old, Kind: controlAdministration, Image: [32]byte{6}}
	removed := newControlLedgerFixture(t, 3, p.Claim.Base)
	var oldReady client.Certificate
	for _, fixture := range []*controlLedgerFixture{a, b, removed} {
		if fixture != b {
			if err := fixture.ledger.reserve(old); err != nil {
				t.Fatal(err)
			}
		}
		proof, err := fixture.ledger.prepare(oldProposal)
		if err != nil {
			t.Fatal(err)
		}
		oldReady = append(oldReady, proof)
	}
	decision, err := oldProposal.decision()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ledger.commit(decision, oldReady); err != nil {
		t.Fatal(err)
	}
	ready, err := b.ledger.prepare(oldProposal)
	if err != nil {
		t.Fatal(err)
	}
	participants := &controlTestParticipants{fixtures: map[client.ID]*controlLedgerFixture{{1}: a, {2}: b}}
	controller := &clusterController{server: &Server{}, coordinator: &controlCoordinator{ledger: a.ledger, participants: participants}}
	if _, _, err := controller.changeWithExclusion(context.Background(), p.NextMembership, nil, nil, p.Claim.Exclusion); !errors.Is(err, errControlConflict) {
		t.Fatal("expected old promise conflict", err)
	}
	for _, f := range []*controlLedgerFixture{a, b} {
		f.restart(t)
	}
	if a.ledger.slot == nil || !equalControlClaim(a.ledger.slot.Claim, old) || a.ledger.slot.Ready != oldReady[0] || a.ledger.request != nil {
		t.Fatal("partial force promise trapped existing global control")
	}
	if b.ledger.slot == nil || !equalControlClaim(b.ledger.slot.Claim, old) || b.ledger.slot.Ready != ready || b.ledger.slot.Committed.RecordSequence == 0 {
		t.Fatal("failed force changed existing READY")
	}
	// Every survivor also rejects a delayed packet from the cancelled force.
	failed := p.Claim
	failed.Round.Attempt = 1
	if err := a.ledger.reserve(failed); err == nil {
		t.Fatal("late force reserve resurrected")
	}
	if err := b.ledger.reserve(failed); err == nil {
		t.Fatal("late force reserve displaced old READY")
	}
}

func TestExclusionConstructionMarkerPreventsAbandoningSealedRecovery(t *testing.T) {
	p := exclusionTestProposal(t)
	a := newControlLedgerFixture(t, 1, p.Claim.Base)
	b := newControlLedgerFixture(t, 2, p.Claim.Base)
	participants := &controlTestParticipants{fixtures: map[client.ID]*controlLedgerFixture{{1}: a, {2}: b}}
	coordinator := &controlCoordinator{ledger: a.ledger, participants: participants}
	claim, err := coordinator.beginWithExclusion(context.Background(), controlForceDisconnect, p.Claim.Required, p.Claim.Exclusion)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.ledger.startForceConstruction(claim); err != nil {
		t.Fatal(err)
	}
	if err := a.ledger.startForceConstruction(claim); err != nil {
		t.Fatal("marker replay", err)
	}
	for _, f := range []*controlLedgerFixture{a, b} {
		if err := f.ledger.startForceSlot(claim); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []*controlLedgerFixture{a, b} {
		f.restart(t)
	}
	coordinator = &controlCoordinator{ledger: a.ledger, participants: participants}
	request := coordinator.requestSnapshot()
	if request == nil || !request.ConstructionStarted || request.ResultReady {
		t.Fatal("construction boundary lost on restart")
	}
	if err := coordinator.cancelBuilding(context.Background()); err == nil {
		t.Fatal("sealed recovery builder abandoned")
	}
	if _, err := coordinator.resume(context.Background()); err == nil {
		t.Fatal("generic coordinator aborted force reconstruction")
	}
	for _, f := range []*controlLedgerFixture{a, b} {
		if f.ledger.slot == nil || !equalControlClaim(f.ledger.slot.Claim, claim) {
			t.Fatal("force reconstruction lost a survivor promise")
		}
		if _, err := f.ledger.abortClaim(claim); err == nil {
			t.Fatal("delayed ABORT discarded participant recovery state")
		}
	}
}

func TestExclusionSuspendsUncommittedGlobalRoundUntilAbortOrCommit(t *testing.T) {
	for _, finish := range []string{"abort", "commit"} {
		t.Run(finish, func(t *testing.T) {
			p := exclusionTestProposal(t)
			fixtures := map[client.ID]*controlLedgerFixture{}
			for _, id := range []byte{1, 2, 3} {
				fixtures[client.ID{id}] = newControlLedgerFixture(t, id, p.Claim.Base)
			}
			participants := &controlTestParticipants{fixtures: fixtures}
			coordinator := &controlCoordinator{ledger: fixtures[client.ID{1}].ledger, participants: participants}
			ctx := context.Background()
			oldClaim, err := coordinator.begin(ctx, controlAdministration, []client.ID{{1}, {2}, {3}})
			if err != nil {
				t.Fatal(err)
			}
			old := controlProposal{Claim: oldClaim, Kind: controlAdministration, Image: [32]byte{6}}
			if err := coordinator.prepared(ctx, old, []byte("old public result")); err != nil {
				t.Fatal(err)
			}
			var ready client.Certificate
			for _, node := range oldClaim.Required {
				proof, err := participants.Prepare(ctx, node, old)
				if err != nil {
					t.Fatal(err)
				}
				ready = append(ready, proof)
			}
			oldDecision, _ := old.decision()
			force, err := coordinator.beginWithExclusion(ctx, controlForceDisconnect, p.Claim.Required, p.Claim.Exclusion)
			if err != nil {
				t.Fatal("could not suspend old requester", err)
			}
			for _, id := range force.Required {
				l := fixtures[id].ledger
				if l.slot == nil || l.slot.Suspended == nil || !equalControlClaim(l.slot.Suspended.Claim, oldClaim) {
					t.Fatal("old READY was discarded")
				}
				if _, err := l.commit(oldDecision, ready); err == nil {
					t.Fatal("old COMMIT bypassed force promise")
				}
			}
			for _, fixture := range fixtures {
				fixture.restart(t)
			}
			coordinator = &controlCoordinator{ledger: fixtures[client.ID{1}].ledger, participants: participants}
			if finish == "abort" {
				if err := coordinator.cancelBuilding(ctx); err != nil {
					t.Fatal(err)
				}
				restored := coordinator.requestSnapshot()
				if restored == nil || restored.Claim.Round != oldClaim.Round || !restored.ResultReady {
					t.Fatal("original requester not restored")
				}
				result, err := coordinator.resume(ctx)
				if err != nil || string(result) != "old public result" {
					t.Fatal("restored round could not finish", err)
				}
			} else {
				if err := coordinator.rebindBuilding(force); err != nil {
					t.Fatal(err)
				}
				if err := coordinator.ledger.startForceConstruction(force); err != nil {
					t.Fatal(err)
				}
				for _, id := range force.Required {
					if err := fixtures[id].ledger.startForceSlot(force); err != nil {
						t.Fatal(err)
					}
				}
				p.Claim = force
				if err := coordinator.prepared(ctx, p, []byte("forced result")); err != nil {
					t.Fatal(err)
				}
				result, err := coordinator.resume(ctx)
				if err != nil || string(result) != "forced result" {
					t.Fatal(err)
				}
				for _, id := range force.Required {
					fixtures[id].restart(t)
					l := fixtures[id].ledger
					if l.slot != nil || l.suspendedRequest != nil || l.request != nil || !l.closed[oldClaim.Round] || !equalControlMembership(l.current.Membership, *p.NextMembership) {
						t.Fatal("force left obsolete requester authority")
					}
					if _, err := l.commit(oldDecision, ready); err == nil {
						t.Fatal("obsolete COMMIT resurrected")
					}
				}
			}
		})
	}
}

func TestExclusionRetiresReleasedRequesterAfterLostFinalAck(t *testing.T) {
	p := exclusionTestProposal(t)
	fixtures := map[client.ID]*controlLedgerFixture{}
	for _, id := range []byte{1, 2, 3} {
		fixtures[client.ID{id}] = newControlLedgerFixture(t, id, p.Claim.Base)
	}
	participants := &controlTestParticipants{fixtures: fixtures}
	old := &controlCoordinator{ledger: fixtures[client.ID{2}].ledger, participants: participants}
	ctx := context.Background()
	claim, err := old.begin(ctx, controlAdministration, []client.ID{{1}, {2}, {3}})
	if err != nil {
		t.Fatal(err)
	}
	proposal := controlProposal{Claim: claim, Kind: controlAdministration, Image: [32]byte{6}}
	if err := old.prepared(ctx, proposal, []byte("old result")); err != nil {
		t.Fatal(err)
	}
	participants.failPhase, participants.failNode = "release", client.ID{2}
	if _, err := old.resume(ctx); !errors.Is(err, errControlLostReply) {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		fixture.restart(t)
	}
	if fixtures[client.ID{2}].ledger.slot != nil || fixtures[client.ID{2}].ledger.request == nil {
		t.Fatal("lost final acknowledgement fixture")
	}
	force := &controlCoordinator{ledger: fixtures[client.ID{1}].ledger, participants: participants}
	forceClaim, err := force.beginWithExclusion(ctx, controlForceDisconnect, p.Claim.Required, p.Claim.Exclusion)
	if err != nil {
		t.Fatal("released requester blocked force", err)
	}
	p.Claim = forceClaim
	if err := force.prepared(ctx, p, []byte("force result")); err != nil {
		t.Fatal(err)
	}
	if _, err := force.resume(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range forceClaim.Required {
		fixtures[id].restart(t)
		if fixtures[id].ledger.request != nil || fixtures[id].ledger.slot != nil {
			t.Fatal("old requester survived force completion")
		}
	}
}

func TestExclusionWireBindsOriginalMembership(t *testing.T) {
	p := exclusionTestProposal(t)
	p.Claim.Exclusion.ExpectedMembership = p.Claim.Base.Membership.Epoch
	wire, err := marshalControlProposal(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeControlProposal(wire)
	if err != nil || !equalControlProposal(p, got) || exclusionObservedEpoch(got.Claim) != p.Claim.Base.Membership.Epoch {
		t.Fatal("original epoch round trip", err)
	}
	for n := range wire {
		if _, err := decodeControlProposal(wire[:n]); err == nil {
			t.Fatal("truncated original epoch accepted", n)
		}
	}
	legacy := exclusionTestProposal(t)
	before, _ := legacy.decision()
	after, _ := p.decision()
	if before == after {
		t.Fatal("original epoch not bound to decision")
	}
	p.Claim.Exclusion.ExpectedMembership++
	if _, err := marshalControlProposal(p); err == nil {
		t.Fatal("future observed membership accepted")
	}
}

func TestExclusionNeverAbortsSuspendedCommittedParent(t *testing.T) {
	p := exclusionTestProposal(t)
	fixtures := map[client.ID]*controlLedgerFixture{}
	for _, id := range []byte{1, 2, 3} {
		fixtures[client.ID{id}] = newControlLedgerFixture(t, id, p.Claim.Base)
	}
	participants := &controlTestParticipants{fixtures: fixtures}
	coordinator := &controlCoordinator{ledger: fixtures[client.ID{1}].ledger, participants: participants}
	ctx := context.Background()
	old, err := coordinator.begin(ctx, controlAdministration, []client.ID{{1}, {2}, {3}})
	if err != nil {
		t.Fatal(err)
	}
	proposal := controlProposal{Claim: old, Kind: controlAdministration, Image: [32]byte{6}}
	if err := coordinator.prepared(ctx, proposal, []byte("known parent")); err != nil {
		t.Fatal(err)
	}
	var ready client.Certificate
	for _, node := range old.Required {
		proof, err := participants.Prepare(ctx, node, proposal)
		if err != nil {
			t.Fatal(err)
		}
		ready = append(ready, proof)
	}
	decision, _ := proposal.decision()
	for _, node := range old.Required {
		if _, err := participants.Commit(ctx, node, decision, ready); err != nil {
			t.Fatal(err)
		}
		if _, err := participants.Apply(ctx, node, decision); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := coordinator.beginWithExclusion(ctx, controlForceDisconnect, p.Claim.Required, p.Claim.Exclusion); err != nil {
		t.Fatal(err)
	}
	for _, node := range p.Claim.Required {
		fixture := fixtures[node]
		if _, err := fixture.ledger.abortClaim(old); err == nil {
			t.Fatal("committed parent aborted")
		}
		fixture.restart(t)
		if fixture.ledger.slot == nil || fixture.ledger.slot.Suspended == nil || fixture.ledger.slot.Suspended.Committed.RecordSequence == 0 {
			t.Fatal("late abort damaged parent journal")
		}
	}
	coordinator = &controlCoordinator{ledger: fixtures[client.ID{1}].ledger, participants: participants}
	if err := coordinator.cancelBuilding(ctx); err != nil {
		t.Fatal(err)
	}
	if result, err := coordinator.resume(ctx); err != nil || string(result) != "known parent" {
		t.Fatal("restored committed parent could not finish", err)
	}
}
