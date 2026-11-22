// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestBucketMutationFenceSurvivesRestartAndAbort(t *testing.T) {
	fixture := newControlLedgerFixture(t, 1, controlTestBase())
	bucket := client.ID{9}
	claim, err := fixture.ledger.beginRequest(controlAdministration, []client.ID{{1}}, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.ledger.reserve(claim); err != nil {
		t.Fatal(err)
	}
	fixture.restart(t)
	s := &Server{control: &controlRuntime{ledger: fixture.ledger}}
	if !s.bucketFrozen(volume.ID(bucket)) || s.bucketFrozen(volume.ID{10}) {
		t.Fatal("fence missing or blocked unrelated bucket")
	}
	if _, err := fixture.ledger.abortClaim(claim); err != nil {
		t.Fatal(err)
	}
	if s.bucketFrozen(volume.ID(bucket)) {
		t.Fatal("abort did not release bucket fence")
	}
	fixture.restart(t)
	s.control.ledger = fixture.ledger
	if s.bucketFrozen(volume.ID(bucket)) {
		t.Fatal("aborted fence revived")
	}
}
