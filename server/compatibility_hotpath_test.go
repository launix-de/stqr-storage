// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestCompatibilityHotpath(t *testing.T) {
	s := &Server{}
	expected := currentOperationSemantics()
	profile := s.protocolVersion()
	if !reflect.DeepEqual(profile.Operations, expected) {
		t.Fatal("default semantics changed")
	}
	// Exhaustively preserve the wire key for every uint16, including leading zeros.
	for n := 0; n <= 0xffff; n++ {
		op := uint16(n)
		if got := operationKey(op); got != fmt.Sprintf("%04x", op) {
			t.Fatalf("opcode %x: %s", op, got)
		}
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		p := s.protocolVersion()
		if err := checkPeerOperation(p, profile, client.ObjectReference); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("peer compatibility hot path allocates: %g", allocs)
	}
	// Fresh editable profiles must not mutate the process-wide immutable default.
	changed := s.protocolVersion()
	changed.Operations = currentOperationSemantics()
	key := operationKey(client.ObjectReference)
	changed.Operations[key] = operationSemantics{Emit: 99, Min: 99, Max: 99}
	s.compatibility.Store(&changed)
	if statusOf(checkPeerOperation(s.protocolVersion(), profile, client.ObjectReference)) != 426 {
		t.Fatal("server override ignored")
	}
	if got := (&Server{}).protocolVersion().Operations[key]; got != expected[key] {
		t.Fatal("override leaked into another server")
	}
	s.compatibility.Store(nil)
	if !reflect.DeepEqual(s.protocolVersion().Operations, expected) {
		t.Fatal("reset did not restore default")
	}
}

func BenchmarkPeerCompatibility(b *testing.B) {
	s := &Server{}
	peer := s.protocolVersion()
	b.ReportAllocs()
	for b.Loop() {
		if err := checkPeerOperation(s.protocolVersion(), peer, client.ObjectReference); err != nil {
			b.Fatal(err)
		}
	}
}
