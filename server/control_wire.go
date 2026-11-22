// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"

	"github.com/launix-de/stqr-storage/client"
)

// Control wire schema 1 is explicitly encoded; Go field names, JSON and local
// journal representation are not part of the peer protocol. Integer encoding
// and length-prefixed blobs are the standard client Encoder representation.
const controlWireSchema uint16 = 1

func controlMembershipWire(m client.Membership) ([]byte, error) {
	ack := client.NodeHelloAck{Membership: m}
	if m.Epoch != 0 {
		ack.Generation = 1
	}
	return ack.MarshalBinary()
}

func marshalControlClaim(c controlClaim) ([]byte, error) {
	membership, err := controlMembershipWire(c.Base.Membership)
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U16(controlWireSchema)
	e.Blob(membership)
	e.U64(c.Base.Sequence)
	e.B = append(e.B, c.Base.Head[:]...)
	e.U64(c.Base.Generation)
	e.U64(c.Base.MapRevision)
	e.B = append(e.B, c.Base.SystemImage[:]...)
	e.U64(c.Round.Membership)
	e.U64(c.Round.Generation)
	e.ID(c.Round.Initiator)
	e.U64(c.Round.Attempt)
	e.V(uint64(len(c.Required)))
	for _, id := range c.Required {
		e.ID(id)
	}
	if _, err := decodeControlClaim(e.B); err != nil {
		return nil, err
	}
	return e.B, nil
}

func decodeControlClaim(body []byte) (controlClaim, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	membership, err := client.DecodeNodeHelloAck(d.Blob())
	c := controlClaim{Base: controlBase{Membership: membership.Membership, Sequence: d.U64()}}
	copy(c.Base.Head[:], d.Take(32))
	c.Base.Generation, c.Base.MapRevision = d.U64(), d.U64()
	copy(c.Base.SystemImage[:], d.Take(32))
	c.Round = client.RoundID{Membership: d.U64(), Generation: d.U64(), Initiator: d.ID(), Attempt: d.U64()}
	n := d.Count(10000)
	if n == 0 || n > len(d.B)/16 {
		return controlClaim{}, client.ErrProtocol
	}
	c.Required = make([]client.ID, n)
	for i := range c.Required {
		c.Required[i] = d.ID()
		if c.Required[i] == (client.ID{}) || i > 0 && bytes.Compare(c.Required[i-1][:], c.Required[i][:]) >= 0 {
			return controlClaim{}, client.ErrProtocol
		}
	}
	if schema != controlWireSchema || err != nil || d.Done() != nil || len(body) > client.MaxMessage-20 || membership.ControlSequence != 0 || membership.MapRevision != 0 || membership.Generation != 1 || c.Base.Membership.Epoch == 0 || c.Round.Membership != c.Base.Membership.Epoch || c.Round.Generation != c.Base.Generation || c.Round.Initiator == (client.ID{}) || c.Round.Attempt == 0 {
		return controlClaim{}, client.ErrProtocol
	}
	return c, nil
}

func marshalControlProposal(p controlProposal) ([]byte, error) {
	claim, err := marshalControlClaim(p.Claim)
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U16(controlWireSchema)
	e.Blob(claim)
	e.U16(p.Kind)
	e.B = append(e.B, p.Image[:]...)
	e.Bool(p.NextMembership != nil)
	if p.NextMembership != nil {
		wire, err := controlMembershipWire(*p.NextMembership)
		if err != nil {
			return nil, err
		}
		e.Blob(wire)
	}
	if _, err := decodeControlProposal(e.B); err != nil {
		return nil, err
	}
	return e.B, nil
}

func decodeControlProposal(body []byte) (controlProposal, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	claim, err := decodeControlClaim(d.Blob())
	p := controlProposal{Claim: claim, Kind: d.U16()}
	copy(p.Image[:], d.Take(32))
	if d.Bool() {
		ack, membershipErr := client.DecodeNodeHelloAck(d.Blob())
		if membershipErr != nil || ack.ControlSequence != 0 || ack.MapRevision != 0 || ack.Generation != 1 {
			return controlProposal{}, client.ErrProtocol
		}
		p.NextMembership = &ack.Membership
	}
	if schema != controlWireSchema || err != nil || d.Done() != nil || len(body) > client.MaxMessage-20 || p.Image == ([32]byte{}) || p.Kind != controlAdministration && p.Kind != controlMembership || p.Kind == controlAdministration && p.NextMembership != nil || p.Kind == controlMembership && p.NextMembership == nil {
		return controlProposal{}, client.ErrProtocol
	}
	return p, nil
}

func marshalControlCertificate(decision client.Decision, certificate client.Certificate) ([]byte, error) {
	d, err := decision.MarshalBinary()
	if err != nil {
		return nil, err
	}
	c, err := certificate.MarshalBinary()
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U16(controlWireSchema)
	e.B = append(e.B, d...)
	e.Blob(c)
	return e.B, nil
}

func decodeControlCertificate(body []byte) (client.Decision, client.Certificate, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	decision, err := client.DecodeDecision(d.Take(80))
	certificate, certErr := client.DecodeCertificate(d.Blob())
	if schema != controlWireSchema || err != nil || certErr != nil || d.Done() != nil {
		return client.Decision{}, nil, client.ErrProtocol
	}
	return decision, certificate, nil
}

func marshalControlApply(decision client.Decision) ([]byte, error) {
	wire, err := decision.MarshalBinary()
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U16(controlWireSchema)
	e.B = append(e.B, wire...)
	return e.B, nil
}

func decodeControlApply(body []byte) (client.Decision, error) {
	d := client.Decoder{B: body}
	schema := d.U16()
	decision, err := client.DecodeDecision(d.Take(80))
	if schema != controlWireSchema || err != nil || d.Done() != nil {
		return client.Decision{}, client.ErrProtocol
	}
	return decision, nil
}
