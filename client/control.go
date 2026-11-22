// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"crypto/sha256"
	"sort"
)

type Decision struct {
	Membership, Sequence uint64
	Previous, Digest     [32]byte
}

type Proof struct {
	Node           ID
	RecordSequence uint64
	Digest         [32]byte
}

type Certificate []Proof

const (
	PeerControlReserve uint16 = 0x1040 + iota
	PeerControlPrepare
	PeerControlCommit
	PeerControlApply
	PeerControlRelease
	PeerControlAbort
)

// NewDecision binds the preceding committed decision and the complete canonical
// payload. A coordinator/round/scope must be part of that payload, not inferred
// from the connection which happens to deliver it.
func NewDecision(membership, sequence uint64, previous [32]byte, kind uint16, payload []byte) (Decision, error) {
	d := Decision{Membership: membership, Sequence: sequence, Previous: previous}
	if membership == 0 || sequence == 0 {
		return d, ErrProtocol
	}
	e := Encoder{}
	e.Text("STQR control decision v1")
	e.U64(membership)
	e.U64(sequence)
	e.B = append(e.B, previous[:]...)
	e.U16(kind)
	e.Blob(payload)
	d.Digest = sha256.Sum256(e.B)
	return d, nil
}

func (m Decision) MarshalBinary() ([]byte, error) {
	if m.Membership == 0 || m.Sequence == 0 || m.Digest == ([32]byte{}) {
		return nil, ErrProtocol
	}
	e := Encoder{}
	writeDecision(&e, m)
	return e.B, nil
}

func writeDecision(e *Encoder, m Decision) {
	e.U64(m.Membership)
	e.U64(m.Sequence)
	e.B = append(e.B, m.Previous[:]...)
	e.B = append(e.B, m.Digest[:]...)
}

func readDecision(d *Decoder) Decision {
	m := Decision{Membership: d.U64(), Sequence: d.U64()}
	copy(m.Previous[:], d.Take(32))
	copy(m.Digest[:], d.Take(32))
	return m
}

func DecodeDecision(body []byte) (Decision, error) {
	d := Decoder{B: body}
	m := readDecision(&d)
	if d.Done() != nil || m.Membership == 0 || m.Sequence == 0 || m.Digest == ([32]byte{}) {
		return Decision{}, ErrProtocol
	}
	return m, nil
}

func (p Proof) valid() bool {
	return p.Node != (ID{}) && p.RecordSequence != 0 && p.Digest != ([32]byte{})
}

func (p Proof) MarshalBinary() ([]byte, error) {
	if !p.valid() {
		return nil, ErrProtocol
	}
	e := Encoder{}
	writeProof(&e, p)
	return e.B, nil
}

func DecodeProof(body []byte) (Proof, error) {
	d := Decoder{B: body}
	p := readProof(&d)
	if d.Done() != nil || !p.valid() {
		return Proof{}, ErrProtocol
	}
	return p, nil
}

func writeProof(e *Encoder, p Proof) {
	e.ID(p.Node)
	e.U64(p.RecordSequence)
	e.B = append(e.B, p.Digest[:]...)
}

func readProof(d *Decoder) Proof {
	p := Proof{Node: d.ID(), RecordSequence: d.U64()}
	copy(p.Digest[:], d.Take(32))
	return p
}

func NewCertificate(proofs []Proof) (Certificate, error) {
	c := append(Certificate{}, proofs...)
	sort.Slice(c, func(i, j int) bool { return bytes.Compare(c[i].Node[:], c[j].Node[:]) < 0 })
	_, err := c.MarshalBinary()
	return c, err
}

func (c Certificate) MarshalBinary() ([]byte, error) {
	if len(c) == 0 || len(c) > 10000 {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.V(uint64(len(c)))
	for i, p := range c {
		if !p.valid() || i > 0 && bytes.Compare(c[i-1].Node[:], p.Node[:]) >= 0 {
			return nil, ErrProtocol
		}
		writeProof(&e, p)
	}
	return e.B, nil
}

func DecodeCertificate(body []byte) (Certificate, error) {
	d := Decoder{B: body}
	n := d.Count(10000)
	if n == 0 || n > len(d.B)/56 {
		return nil, ErrProtocol
	}
	c := make(Certificate, n)
	for i := range c {
		c[i] = readProof(&d)
		if !c[i].valid() || i > 0 && bytes.Compare(c[i-1].Node[:], c[i].Node[:]) >= 0 {
			return nil, ErrProtocol
		}
	}
	if d.Done() != nil {
		return nil, ErrProtocol
	}
	return c, nil
}

// Covers requires exactly one proof for every bound participant. A transport
// timeout or missing proof cannot silently reduce ALL to a different set.
func (c Certificate) Covers(required []ID) bool {
	if len(c) != len(required) || len(c) == 0 {
		return false
	}
	ids := append([]ID{}, required...)
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	for i := range ids {
		if ids[i] == (ID{}) || i > 0 && ids[i] == ids[i-1] || c[i].Node != ids[i] || !c[i].valid() {
			return false
		}
	}
	return true
}
