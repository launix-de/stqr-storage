// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"testing"
)

func TestControlDecisionCanonicalBinding(t *testing.T) {
	first, err := NewDecision(1, 1, [32]byte{}, 7, []byte("members and initial state"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := first.MarshalBinary()
	if got, err := DecodeDecision(b); err != nil || got != first {
		t.Fatal("decision codec", err)
	}
	for i := range b {
		if _, err := DecodeDecision(b[:i]); err == nil {
			t.Fatal("accepted truncation", i)
		}
	}
	for _, next := range []Decision{
		mustDecision(t, 2, 1, first.Previous, 7, []byte("members and initial state")),
		mustDecision(t, 1, 2, first.Previous, 7, []byte("members and initial state")),
		mustDecision(t, 1, 1, first.Digest, 7, []byte("members and initial state")),
		mustDecision(t, 1, 1, first.Previous, 8, []byte("members and initial state")),
		mustDecision(t, 1, 1, first.Previous, 7, []byte("different state")),
	} {
		if next.Digest == first.Digest {
			t.Fatal("decision digest failed to bind an input")
		}
	}
}

func mustDecision(t *testing.T, membership, sequence uint64, previous [32]byte, kind uint16, payload []byte) Decision {
	t.Helper()
	d, err := NewDecision(membership, sequence, previous, kind, payload)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestControlCertificateRequiresExactlyAllParticipants(t *testing.T) {
	proofs := []Proof{{Node: ID{2}, RecordSequence: 8, Digest: [32]byte{8}}, {Node: ID{1}, RecordSequence: 3, Digest: [32]byte{3}}}
	c, err := NewCertificate(proofs)
	if err != nil || !c.Covers([]ID{{2}, {1}}) {
		t.Fatal("ALL certificate", err)
	}
	for _, required := range [][]ID{{{1}}, {{1}, {2}, {3}}, {{1}, {1}}, {{1}, {3}}} {
		if c.Covers(required) {
			t.Fatal("certificate silently changed the required set")
		}
	}
	b, _ := c.MarshalBinary()
	got, err := DecodeCertificate(b)
	if err != nil || !got.Covers([]ID{{1}, {2}}) {
		t.Fatal("certificate decode", err)
	}
	encoded, _ := got.MarshalBinary()
	if !bytes.Equal(encoded, b) {
		t.Fatal("noncanonical certificate")
	}
	for i := range b {
		if _, err := DecodeCertificate(b[:i]); err == nil {
			t.Fatal("accepted truncated certificate", i)
		}
	}
	if _, err := NewCertificate(append(proofs, proofs[0])); err == nil {
		t.Fatal("duplicate vote counted")
	}
}

func TestControlProofStrictCodec(t *testing.T) {
	p := Proof{Node: ID{1}, RecordSequence: 3, Digest: [32]byte{9}}
	wire, err := p.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeProof(wire); err != nil || got != p {
		t.Fatal("proof changed", err)
	}
	for i := range wire {
		if _, err := DecodeProof(wire[:i]); err == nil {
			t.Fatal("truncated proof accepted", i)
		}
	}
	if _, err := DecodeProof(append(wire, 0)); err == nil {
		t.Fatal("trailing proof bytes accepted")
	}
	if _, err := (Proof{}).MarshalBinary(); err == nil {
		t.Fatal("empty proof accepted")
	}
}
