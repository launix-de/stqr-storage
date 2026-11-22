// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type controlLedgerFixture struct {
	ledger        *controlLedger
	path          string
	cluster, node volume.ID
}

func controlTestBase() controlBase {
	return controlBase{Membership: client.Membership{Epoch: 1, Cluster: client.ID{7}, Members: []client.Member{{ID: client.ID{1}}}}, Generation: 1, MapRevision: 1}
}

func newControlLedgerFixture(t *testing.T, node byte, initial controlBase) *controlLedgerFixture {
	t.Helper()
	f := &controlLedgerFixture{path: filepath.Join(t.TempDir(), "control.stqr"), cluster: volume.ID{7}, node: volume.ID{node}}
	l, err := openControlLedger(f.path, f.cluster, f.node, &initial)
	if err != nil {
		t.Fatal(err)
	}
	f.ledger = l
	t.Cleanup(func() { f.ledger.journal.close() })
	return f
}

func (f *controlLedgerFixture) restart(t *testing.T) {
	t.Helper()
	if err := f.ledger.journal.close(); err != nil {
		t.Fatal(err)
	}
	l, err := openControlLedger(f.path, f.cluster, f.node, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.ledger = l
}

func controlJoinProposal() controlProposal {
	base := controlTestBase()
	return controlProposal{Claim: controlClaim{Round: client.RoundID{Membership: 1, Generation: 1, Initiator: client.ID{1}, Attempt: 1}, Base: base, Required: []client.ID{{1}, {2}}}, Kind: controlMembership, Image: [32]byte{9}, NextMembership: &client.Membership{Epoch: 2, Cluster: client.ID{7}, Members: []client.Member{{ID: client.ID{1}}, {ID: client.ID{2}}}}}
}

func TestControlMembershipParticipantBarriersSurviveEveryPhase(t *testing.T) {
	a := newControlLedgerFixture(t, 1, controlTestBase())
	b := newControlLedgerFixture(t, 2, controlBase{Membership: client.Membership{Cluster: client.ID{7}}})
	proposal := controlJoinProposal()
	decision, err := proposal.decision()
	if err != nil {
		t.Fatal(err)
	}
	var ready []client.Proof
	for _, f := range []*controlLedgerFixture{a, b} {
		if err := f.ledger.reserve(proposal.Claim); err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		other := proposal.Claim
		other.Round.Initiator = client.ID{2}
		if err := f.ledger.reserve(other); !errors.Is(err, errControlConflict) {
			t.Fatal("reserved slot changed on restart", err)
		}
		proof, err := f.ledger.prepare(proposal)
		if err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		if again, err := f.ledger.prepare(proposal); err != nil || again != proof {
			t.Fatal("prepare retry lost its durable proof", err)
		}
		if _, err := f.ledger.markApplied(decision); err == nil {
			t.Fatal("applied before COMMIT")
		}
		ready = append(ready, proof)
	}
	certificate, err := client.NewCertificate(ready)
	if err != nil {
		t.Fatal(err)
	}
	missing, _ := client.NewCertificate(ready[:1])
	if _, err := a.ledger.commit(decision, missing); err == nil {
		t.Fatal("missing participant silently removed from ALL")
	}
	var applied []client.Proof
	for _, f := range []*controlLedgerFixture{a, b} {
		committed, err := f.ledger.commit(decision, certificate)
		if err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		if again, err := f.ledger.commit(decision, certificate); err != nil || again != committed {
			t.Fatal("commit retry lost its durable proof", err)
		}
		if err := f.ledger.abort(proposal.Claim.Round); err == nil {
			t.Fatal("committed decision was aborted")
		}
		// The participant-state unit test models publication here. Actual object
		// transfer/head publication still needs separate system-image integration.
		proof, err := f.ledger.markApplied(decision)
		if err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		if !reflect.DeepEqual(f.ledger.current.Membership, *proposal.NextMembership) || f.ledger.current.Head != decision.Digest || f.ledger.current.SystemImage != proposal.Image {
			t.Fatal("applied membership/image lost on restart")
		}
		if again, err := f.ledger.prepare(proposal); err != nil || again != f.ledger.slot.Ready {
			t.Fatal("late PREPARE did not reuse the recorded result", err)
		}
		applied = append(applied, proof)
	}
	completed, _ := client.NewCertificate(applied)
	for _, f := range []*controlLedgerFixture{a, b} {
		if err := f.ledger.release(decision, completed); err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		if err := f.ledger.release(decision, completed); err != nil {
			t.Fatal("finalization retry", err)
		}
		if f.ledger.slot != nil {
			t.Fatal("finalized slot stayed occupied")
		}
		if err := f.ledger.reserve(proposal.Claim); err == nil {
			t.Fatal("delayed message resurrected a closed round")
		}
	}
	// A different member may initiate the next global administrative round.
	// No coordinator/initial seed receives a permanently privileged role.
	next := controlClaim{Round: client.RoundID{Membership: 2, Generation: 1, Initiator: client.ID{2}, Attempt: 1}, Base: cloneControlBase(a.ledger.current), Required: []client.ID{{1}, {2}}}
	for _, f := range []*controlLedgerFixture{a, b} {
		if err := f.ledger.reserve(next); err != nil {
			t.Fatal("another member cannot coordinate", err)
		}
	}
}

func TestControlAbortAndConflictingPrepare(t *testing.T) {
	f := newControlLedgerFixture(t, 1, controlTestBase())
	p := controlJoinProposal()
	if err := f.ledger.reserve(p.Claim); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ledger.prepare(p); err != nil {
		t.Fatal(err)
	}
	changed := p
	changed.Image[0]++
	if _, err := f.ledger.prepare(changed); err == nil {
		t.Fatal("prepared two digests for one slot")
	}
	if err := f.ledger.abort(p.Claim.Round); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	if err := f.ledger.reserve(p.Claim); err == nil {
		t.Fatal("aborted round resurrected after restart")
	}
	p.Claim.Round.Attempt++
	if err := f.ledger.reserve(p.Claim); err != nil {
		t.Fatal("fresh attempt blocked", err)
	}
}
