// SPDX-License-Identifier: GPL-3.0-only
package client

import "net/url"

// PeerTransport advertises identity binding and telemetry only, not JOIN,
// coordinator binding, placement, replication or authority over remote objects.
const CapabilityPeerTransport uint64 = 1
const CapabilityNodeAdmin uint64 = 2
const NodeHelloMessage uint16 = 0x1000

type NodeHello struct {
	Cluster, Node ID
	Membership    uint64
	Addresses     []string
	Boot          ID
}
type MemberTag struct{ Name, Value string }
type Member struct {
	ID        ID
	Addresses []string
	State     uint8
	Tags      []MemberTag
}
type Membership struct {
	Epoch    uint64
	Cluster  ID
	Members  []Member
	Excluded []ID
}
type NodeHelloAck struct {
	Membership                               Membership
	ControlSequence, Generation, MapRevision uint64
}

func validPeerAddress(s string) bool {
	u, e := url.Parse(s)
	return e == nil && u.Scheme == "http" && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && (u.Path == "" || u.Path == "/")
}
func writeAddresses(e *Encoder, addresses []string) error {
	e.V(uint64(len(addresses)))
	for _, address := range addresses {
		if !validPeerAddress(address) {
			return ErrProtocol
		}
		e.Text(address)
	}
	return nil
}
func readAddresses(d *Decoder) []string {
	count := d.Count(64)
	addresses := make([]string, count)
	for i := range addresses {
		addresses[i] = d.Text()
		if !validPeerAddress(addresses[i]) {
			d.Err = ErrProtocol
		}
	}
	return addresses
}
func (m NodeHello) MarshalBinary() ([]byte, error) {
	if m.Cluster == (ID{}) || m.Node == (ID{}) || m.Boot == (ID{}) || m.Membership == 0 || len(m.Addresses) > 64 {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.ID(m.Cluster)
	e.ID(m.Node)
	e.U64(m.Membership)
	if err := writeAddresses(&e, m.Addresses); err != nil {
		return nil, err
	}
	e.ID(m.Boot)
	if len(e.B) > MaxMessage-20 {
		return nil, ErrProtocol
	}
	return e.B, nil
}
func DecodeNodeHello(body []byte) (NodeHello, error) {
	if len(body) > MaxMessage-20 {
		return NodeHello{}, ErrProtocol
	}
	d := Decoder{B: body}
	m := NodeHello{Cluster: d.ID(), Node: d.ID(), Membership: d.U64()}
	m.Addresses = readAddresses(&d)
	m.Boot = d.ID()
	if d.Done() != nil || m.Cluster == (ID{}) || m.Node == (ID{}) || m.Boot == (ID{}) || m.Membership == 0 {
		return NodeHello{}, ErrProtocol
	}
	return m, nil
}
func (m NodeHelloAck) MarshalBinary() ([]byte, error) {
	membership := m.Membership
	if membership.Epoch == 0 || membership.Cluster == (ID{}) || len(membership.Members) == 0 || len(membership.Members) > 10000 || len(membership.Excluded) > 10000 || m.Generation == 0 {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.U64(membership.Epoch)
	e.ID(membership.Cluster)
	e.V(uint64(len(membership.Members)))
	seen := map[ID]bool{}
	for _, member := range membership.Members {
		if member.ID == (ID{}) || seen[member.ID] || member.State > 5 || len(member.Addresses) > 64 || len(member.Tags) > 64 {
			return nil, ErrProtocol
		}
		seen[member.ID] = true
		e.ID(member.ID)
		if err := writeAddresses(&e, member.Addresses); err != nil {
			return nil, err
		}
		e.U8(member.State)
		e.V(uint64(len(member.Tags)))
		for _, tag := range member.Tags {
			e.Text(tag.Name)
			e.Text(tag.Value)
		}
	}
	e.V(uint64(len(membership.Excluded)))
	for _, id := range membership.Excluded {
		if id == (ID{}) || seen[id] {
			return nil, ErrProtocol
		}
		seen[id] = true
		e.ID(id)
	}
	e.U64(m.ControlSequence)
	e.U64(m.Generation)
	e.U64(m.MapRevision)
	if len(e.B) > MaxMessage-20 {
		return nil, ErrProtocol
	}
	return e.B, nil
}
func DecodeNodeHelloAck(body []byte) (NodeHelloAck, error) {
	if len(body) > MaxMessage-20 {
		return NodeHelloAck{}, ErrProtocol
	}
	d := Decoder{B: body}
	m := NodeHelloAck{}
	membership := &m.Membership
	membership.Epoch = d.U64()
	membership.Cluster = d.ID()
	count := d.Count(10000)
	if d.Err != nil || count > len(d.B)/19 {
		return m, ErrProtocol
	}
	membership.Members = make([]Member, count)
	seen := map[ID]bool{}
	for i := range membership.Members {
		member := &membership.Members[i]
		member.ID = d.ID()
		member.Addresses = readAddresses(&d)
		member.State = d.U8()
		tags := d.Count(64)
		member.Tags = make([]MemberTag, tags)
		for j := range member.Tags {
			member.Tags[j] = MemberTag{d.Text(), d.Text()}
		}
		if member.ID == (ID{}) || seen[member.ID] || member.State > 5 {
			return m, ErrProtocol
		}
		seen[member.ID] = true
	}
	count = d.Count(10000)
	if d.Err != nil || count > len(d.B)/16 {
		return m, ErrProtocol
	}
	membership.Excluded = make([]ID, count)
	for i := range membership.Excluded {
		id := d.ID()
		if id == (ID{}) || seen[id] {
			return m, ErrProtocol
		}
		seen[id] = true
		membership.Excluded[i] = id
	}
	m.ControlSequence = d.U64()
	m.Generation = d.U64()
	m.MapRevision = d.U64()
	if d.Done() != nil || membership.Epoch == 0 || membership.Cluster == (ID{}) || len(membership.Members) == 0 || m.Generation == 0 {
		return m, ErrProtocol
	}
	return m, nil
}
