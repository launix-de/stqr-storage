// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestForceControlInspectionDistinguishesReadyCommitAndReleased(t *testing.T) {
	coordinator, participants, proposal := controlCoordinatorFixture(t)
	claim, err := coordinator.begin(context.Background(), controlMembership, proposal.Claim.Required)
	if err != nil {
		t.Fatal(err)
	}
	proposal.Claim = claim
	var ready client.Certificate
	for _, node := range claim.Required {
		proof, err := participants.Prepare(context.Background(), node, proposal)
		if err != nil {
			t.Fatal(err)
		}
		ready = append(ready, proof)
	}
	inspect := func(node client.ID) forceControlState {
		t.Helper()
		server := &Server{control: &controlRuntime{ledger: participants.fixtures[node].ledger}}
		e := client.Encoder{}
		e.U16(1)
		raw, err := server.receiveForceControlState(e.B, claim.Round.Initiator)
		if err != nil {
			t.Fatal(err)
		}
		var state forceControlState
		if decodeControlRecord(raw, &state) != nil || state.Schema != 1 || state.Node != node {
			t.Fatal("invalid inspection")
		}
		return state
	}
	for _, node := range claim.Required {
		state := inspect(node)
		if state.Slot == nil || state.Slot.Ready.RecordSequence == 0 || state.Committed != nil {
			t.Fatal("READY misrepresented as COMMIT")
		}
	}
	decision, err := proposal.decision()
	if err != nil {
		t.Fatal(err)
	}
	witness := claim.Required[0]
	if _, err := participants.Commit(context.Background(), witness, decision, ready); err != nil {
		t.Fatal(err)
	}
	participants.fixtures[witness].restart(t)
	state := inspect(witness)
	if state.Committed == nil || state.Committed.Decision != decision || state.Committed.Applied.RecordSequence != 0 || !state.Committed.Certificate.Covers(claim.Required) {
		t.Fatal("durable pre-APPLY COMMIT not discoverable")
	}
	var applied client.Certificate
	for _, node := range claim.Required {
		if _, err := participants.Commit(context.Background(), node, decision, ready); err != nil {
			t.Fatal(err)
		}
		proof, err := participants.Apply(context.Background(), node, decision)
		if err != nil {
			t.Fatal(err)
		}
		applied = append(applied, proof)
	}
	for _, node := range claim.Required {
		if _, err := participants.Release(context.Background(), node, decision, applied); err != nil {
			t.Fatal(err)
		}
		participants.fixtures[node].restart(t)
		state := inspect(node)
		if state.Slot != nil || state.Committed == nil || state.Committed.Decision != decision || state.Current.Head != decision.Digest {
			t.Fatal("released COMMIT proof not retained")
		}
	}
	server := &Server{control: &controlRuntime{ledger: participants.fixtures[witness].ledger}}
	if _, err := server.receiveForceControlState([]byte{1, 0}, client.ID{99}); err == nil {
		t.Fatal("unadmitted inspector")
	}
	for _, wire := range [][]byte{nil, {1}, {1, 0, 0}, {2, 0}} {
		if _, err := server.receiveForceControlState(wire, claim.Round.Initiator); err == nil {
			t.Fatal("invalid request accepted")
		}
	}
}
