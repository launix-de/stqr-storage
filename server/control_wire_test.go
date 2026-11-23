// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestControlWireStrictRoundTrips(t *testing.T) {
	p := controlJoinProposal()
	claim, err := marshalControlClaim(p.Claim)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := marshalControlProposal(p)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := p.decision()
	if err != nil {
		t.Fatal(err)
	}
	cert, err := client.NewCertificate([]client.Proof{{Node: client.ID{1}, RecordSequence: 3, Digest: [32]byte{1}}, {Node: client.ID{2}, RecordSequence: 4, Digest: [32]byte{2}}})
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := marshalControlCertificate(decision, cert)
	if err != nil {
		t.Fatal(err)
	}
	apply, err := marshalControlApply(decision)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		body   []byte
		decode func([]byte) error
	}{
		{"claim", claim, func(b []byte) error {
			got, err := decodeControlClaim(b)
			if err == nil && !equalControlClaim(got, p.Claim) {
				t.Fatal("claim changed")
			}
			return err
		}},
		{"proposal", proposal, func(b []byte) error {
			got, err := decodeControlProposal(b)
			if err == nil && !equalControlProposal(got, p) {
				t.Fatal("proposal changed")
			}
			if err == nil {
				after, _ := got.decision()
				if after != decision {
					t.Fatal("wire changed decision digest")
				}
			}
			return err
		}},
		{"certificate", certificate, func(b []byte) error {
			got, c, err := decodeControlCertificate(b)
			if err == nil && (got != decision || !c.Covers(p.Claim.Required)) {
				t.Fatal("certificate changed")
			}
			return err
		}},
		{"apply", apply, func(b []byte) error {
			got, err := decodeControlApply(b)
			if err == nil && got != decision {
				t.Fatal("decision changed")
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.decode(tc.body); err != nil {
				t.Fatal(err)
			}
			for i := range tc.body {
				if tc.decode(tc.body[:i]) == nil {
					t.Fatalf("truncation %d accepted", i)
				}
			}
			if tc.decode(append(append([]byte{}, tc.body...), 0)) == nil {
				t.Fatal("trailing byte accepted")
			}
			bad := append([]byte{}, tc.body...)
			bad[0]++
			if tc.decode(bad) == nil {
				t.Fatal("unknown schema accepted")
			}
		})
	}
	p.Claim.Required[0], p.Claim.Required[1] = p.Claim.Required[1], p.Claim.Required[0]
	if _, err := marshalControlClaim(p.Claim); err == nil {
		t.Fatal("noncanonical required set accepted")
	}
}

func FuzzControlWireDecoders(f *testing.F) {
	p := controlJoinProposal()
	claim, _ := marshalControlClaim(p.Claim)
	proposal, _ := marshalControlProposal(p)
	f.Add(claim)
	f.Add(proposal)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = decodeControlClaim(b)
		_, _ = decodeControlProposal(b)
		_, _, _ = decodeControlCertificate(b)
		_, _ = decodeControlApply(b)
	})
}

func TestControlWireBucketFenceIsCanonicalAndBindsDecision(t *testing.T) {
	p := controlJoinProposal()
	before, err := p.decision()
	if err != nil {
		t.Fatal(err)
	}
	p.Claim.FrozenBuckets = []client.ID{{9}}
	wire, err := marshalControlClaim(p.Claim)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeControlClaim(wire)
	if err != nil || !equalControlClaim(got, p.Claim) {
		t.Fatal("fence lost in wire", err)
	}
	after, err := p.decision()
	if err != nil || before == after {
		t.Fatal("decision did not bind bucket fences", err)
	}
	for i := 0; i < len(wire); i++ {
		if _, err := decodeControlClaim(wire[:i]); err == nil {
			t.Fatal("accepted truncated fenced claim", i)
		}
	}
	p.Claim.FrozenBuckets = []client.ID{{9}, {9}}
	if _, err := marshalControlClaim(p.Claim); err == nil {
		t.Fatal("accepted duplicate fence")
	}
	p.Claim.FrozenBuckets = []client.ID{{10}, {9}}
	if _, err := marshalControlClaim(p.Claim); err == nil {
		t.Fatal("accepted noncanonical fence")
	}
}
